use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bincode::{deserialize, serialize};
use futures::channel::oneshot;
use futures::StreamExt;
use log::*;
use prost::Message as _;
use tikv_raft::eraftpb::{self, ConfChange, ConfChangeType, Entry, EntryType};
use tikv_raft::{prelude::*, raw_node::RawNode, Config as RaftConfig};
use tokio::sync::RwLock;
use tokio::time::timeout;

use rust_box::handy_grpc::client::{
    Client as GrpcClient, Mailbox as GrpcMailbox, Message as GrpcMessage,
};
use rust_box::handy_grpc::Priority;

use crate::error::{Error, Result};
use crate::message::{Merger, Message, Proposals, RaftMessage, RaftResponse, ReplyChan, Status};
use crate::message::{Receiver, Sender};
use crate::raft::Store;
use crate::raft::{active_mailbox_querys, active_mailbox_sends};
use crate::raft_server::{send_message_active_requests, send_proposal_active_requests};
use crate::storage::{LogStore, MemStorage};
use crate::Config;

struct MessageSender {
    message: eraftpb::Message,
    client: Peer,
    client_id: u64,
    chan: Sender<(Priority, Message)>,
    max_retries: usize,
    timeout: Duration,
}

impl MessageSender {
    /// attempt to send a message MessageSender::max_retries times at MessageSender::timeout
    /// inteval.
    async fn send(mut self) {
        let mut current_retry = 0usize;
        loop {
            match self.client.send_message(&self.message).await {
                Ok(_) => {
                    return;
                }
                Err(e) => {
                    if current_retry < self.max_retries {
                        current_retry += 1;
                        tokio::time::sleep(self.timeout).await;
                    } else {
                        warn!(
                            "error sending message after {}/{} retries: {:?}",
                            current_retry, self.max_retries, e
                        );
                        if let Err(e) = self
                            .chan
                            .send((
                                Priority::MIN,
                                Message::ReportUnreachable {
                                    node_id: self.client_id,
                                },
                            ))
                            .await
                        {
                            warn!(
                                "error ReportUnreachable after {}/{} retries: {:?}",
                                current_retry, self.max_retries, e
                            );
                        }
                        return;
                    }
                }
            }
        }
    }
}

struct QuerySender {
    query: Vec<u8>,
    client: Peer,
    chan: oneshot::Sender<RaftResponse>,
    max_retries: usize,
    timeout: Duration,
}

impl QuerySender {
    async fn send(self) {
        let mut client = match self.client.grpc_client().await {
            Ok(c) => c,
            Err(e) => {
                if let Err(e) = self.chan.send(RaftResponse::Error(e.to_string())) {
                    warn!("send_query, Message::Query, send result error: {:?}", e);
                }
                return;
            }
        };

        let query = RaftMessage::Query { query: self.query }.encode();
        let query = match query {
            Ok(q) => q,
            Err(e) => {
                if let Err(e) = self.chan.send(RaftResponse::Error(e.to_string())) {
                    warn!("send_query, Message::Query, send result error: {:?}", e);
                }
                return;
            }
        };

        let res = match timeout(
            self.timeout,
            client.send(GrpcMessage {
                ver: 1,
                priority: 0,
                data: query,
            }),
        )
        .await
        {
            Err(_) => RaftResponse::Error("Message::Query timeout".into()),
            Ok(Err(e)) => RaftResponse::Error(e.to_string()),
            Ok(Ok(raft_resp)) => match RaftResponse::decode(raft_resp.data.as_slice()) {
                Ok(raft_resp) => raft_resp,
                Err(e) => RaftResponse::Error(e.to_string()),
            },
        };

        if let Err(e) = self.chan.send(res) {
            warn!("send_query, Message::Query, send result error: {:?}", e);
        }
    }
}

#[derive(Clone)]
pub struct Peer {
    addr: String,
    client_transfer: Arc<RwLock<Option<(GrpcClient, GrpcMailbox)>>>,
    client_transfer_queue_cap: usize, //@TODO config ...
    grpc_fails: Arc<AtomicU64>,
    grpc_fail_time: Arc<AtomicI64>,
    crw_timeout: Duration,
    concurrency_limit: usize,
    grpc_breaker_threshold: u64,
    grpc_breaker_retry_interval: i64,
    active_tasks: Arc<AtomicI64>,
}

impl Peer {
    pub fn new(
        addr: String,
        crw_timeout: Duration,
        concurrency_limit: usize,
        grpc_breaker_threshold: u64,
        grpc_breaker_retry_interval: i64,
    ) -> Peer {
        debug!("connecting to node at {}...", addr);
        Peer {
            addr,
            client_transfer: Arc::new(RwLock::new(None)),
            client_transfer_queue_cap: 100_000,
            grpc_fails: Arc::new(AtomicU64::new(0)),
            grpc_fail_time: Arc::new(AtomicI64::new(0)),
            crw_timeout,
            concurrency_limit,
            grpc_breaker_threshold,
            grpc_breaker_retry_interval,
            active_tasks: Arc::new(AtomicI64::new(0)),
        }
    }

    #[inline]
    pub fn active_tasks(&self) -> i64 {
        self.active_tasks.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn grpc_fails(&self) -> u64 {
        self.grpc_fails.load(Ordering::SeqCst)
    }

    #[inline]
    async fn _grpc_client(&self) -> Result<(GrpcClient, GrpcMailbox)> {
        log::info!("_grpc_client, addr: {}", self.addr);
        let mut c = GrpcClient::new(self.addr.clone())
            .concurrency_limit(self.concurrency_limit)
            .connect_timeout(self.crw_timeout)
            .connect()
            .await
            .map_err(|e| anyhow::Error::msg(e.to_string()))?;

        let mbox = c.transfer_start(self.client_transfer_queue_cap).await;
        Ok((c, mbox))
    }

    #[inline]
    pub async fn grpc_client(&self) -> Result<GrpcClient> {
        if let Some((c, _)) = self.client_transfer.read().await.as_ref() {
            return Ok(c.clone());
        }

        let (c, mbox) = self._grpc_client().await?;
        self.client_transfer
            .write()
            .await
            .replace((c.clone(), mbox));
        Ok(c)
    }

    #[inline]
    pub async fn grpc_client_transfer(&self) -> Result<GrpcMailbox> {
        if let Some((_, mbox)) = self.client_transfer.read().await.as_ref() {
            return Ok(mbox.clone());
        }

        let (c, mbox) = self._grpc_client().await?;
        self.client_transfer
            .write()
            .await
            .replace((c, mbox.clone()));
        Ok(mbox)
    }

    ///Raft Message
    #[inline]
    pub async fn send_message(&self, msg: &eraftpb::Message) -> Result<()> {
        if !self.available() {
            return Err(Error::Msg("The gRPC remote service is unavailable".into()));
        }

        let msg = GrpcMessage {
            ver: 1,
            priority: 255,
            data: RaftMessage::Raft {
                msg: msg.encode_to_vec(), //eraftpb::Message::encode_to_vec(msg),
            }
            .encode()?, //eraftpb::Message::encode_to_vec(msg),
        };
        self.active_tasks.fetch_add(1, Ordering::SeqCst);
        let reply = self._send_message(msg).await;
        self.active_tasks.fetch_sub(1, Ordering::SeqCst);
        match reply {
            Ok(()) => {
                self.record_success();
                Ok(())
            }
            Err(e) => {
                self.record_failure();
                Err(e)
            }
        }
    }

    #[inline]
    async fn _send_message(&self, msg: GrpcMessage) -> Result<()> {
        let mut transfer = self.grpc_client_transfer().await?;
        tokio::time::timeout(self.crw_timeout, transfer.quick_send(msg))
            .await
            .map_err(|_| Error::Elapsed)?
            .map_err(|e| Error::SendError(e.to_string()))?;
        Ok(())
    }

    #[inline]
    pub async fn send_proposal(&self, msg: Vec<u8>) -> Result<Vec<u8>> {
        if !self.available() {
            return Err(Error::Msg("The gRPC remote service is unavailable".into()));
        }

        let msg = GrpcMessage {
            ver: 1,
            priority: 1,
            data: RaftMessage::Propose { proposal: msg }.encode()?,
        };

        let _active_tasks = self.active_tasks.fetch_add(1, Ordering::SeqCst);
        let reply = self._send_proposal(msg).await;
        self.active_tasks.fetch_sub(1, Ordering::SeqCst);
        match reply {
            Ok(reply) => {
                self.record_success();
                Ok(reply)
            }
            Err(e) => {
                self.record_failure();
                Err(e)
            }
        }
    }

    #[inline]
    async fn _send_proposal(&self, msg: GrpcMessage) -> Result<Vec<u8>> {
        let mut c = self.grpc_client().await?;

        let result = tokio::time::timeout(self.crw_timeout, c.send(msg)).await;
        let result = result.map_err(|_| Error::Elapsed)??;
        Ok(result.data)
    }

    #[inline]
    pub async fn change_config(&self, change: ConfChange) -> Result<RaftResponse> {
        let mut c = self.grpc_client().await?;
        let msg = GrpcMessage {
            ver: 1,
            priority: 255,
            data: RaftMessage::ConfigChange {
                change: change.encode_to_vec(),
            }
            .encode()?,
        };
        let result = tokio::time::timeout(self.crw_timeout, c.send(msg)).await;
        let result = result.map_err(|_| Error::Elapsed)??;
        Ok(RaftResponse::decode(&result.data)?)
    }

    #[inline]
    pub fn _addr(&self) -> &str {
        &self.addr
    }

    #[inline]
    pub fn record_failure(&self) {
        self.grpc_fails.fetch_add(1, Ordering::SeqCst);
        self.grpc_fail_time
            .store(chrono::Local::now().timestamp_millis(), Ordering::SeqCst);
    }

    #[inline]
    pub fn record_success(&self) {
        self.grpc_fails.store(0, Ordering::SeqCst);
    }

    #[inline]
    pub fn available(&self) -> bool {
        self.grpc_fails.load(Ordering::SeqCst) < self.grpc_breaker_threshold
            || (chrono::Local::now().timestamp_millis()
                - self.grpc_fail_time.load(Ordering::SeqCst))
                > self.grpc_breaker_retry_interval
    }
}

pub struct RaftNode<S: Store> {
    inner: RawNode<MemStorage>,
    pub peers: HashMap<u64, Option<Peer>>,
    pub rcv: Receiver<(Priority, Message)>,
    pub snd: Sender<(Priority, Message)>,
    store: S,
    uncommitteds: HashMap<u64, ReplyChan>,
    should_quit: bool,
    seq: AtomicU64,
    last_snap_time: Instant,
    request_votes: usize,
    cfg: Arc<Config>,
}

impl<S: Store + 'static> RaftNode<S> {
    pub fn new_leader(
        rcv: Receiver<(Priority, Message)>,
        snd: Sender<(Priority, Message)>,
        id: u64,
        store: S,
        logger: &slog::Logger,
        cfg: Arc<Config>,
    ) -> Result<Self> {
        let config = Self::new_config(id, &cfg.raft_cfg);
        config.validate()?;

        let mut s = Snapshot::default();
        // Because we don't use the same configuration to initialize every node, so we use
        // a non-zero index to force new followers catch up logs by snapshot first, which will
        // bring all nodes to the same initial state.
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![id];

        let mut storage: MemStorage = MemStorage::create();
        storage.apply_snapshot(s)?;
        let mut inner = RawNode::new(&config, storage, logger)?;
        let peers = HashMap::new();
        let seq = AtomicU64::new(0);
        let last_snap_time = Instant::now(); // + cfg.snapshot_interval;

        inner.raft.become_candidate();
        inner.raft.become_leader();

        // let msg_tx = Self::start_message_sender();
        let uncommitteds = HashMap::new();
        let node = RaftNode {
            inner,
            rcv,
            peers,
            store,
            // msg_tx,
            uncommitteds,
            seq,
            snd,
            should_quit: false,
            last_snap_time,
            request_votes: 0,
            cfg,
        };
        Ok(node)
    }

    pub fn new_follower(
        rcv: Receiver<(Priority, Message)>,
        snd: Sender<(Priority, Message)>,
        id: u64,
        store: S,
        logger: &slog::Logger,
        cfg: Arc<Config>,
    ) -> Result<Self> {
        let config = Self::new_config(id, &cfg.raft_cfg);
        config.validate()?;

        let storage = MemStorage::create();
        let inner = RawNode::new(&config, storage, logger)?;
        let peers = HashMap::new();
        let seq = AtomicU64::new(0);
        let last_snap_time = Instant::now(); // + cfg.snapshot_interval;
                                             // let msg_tx = Self::start_message_sender();
        let uncommitteds = HashMap::new();
        Ok(RaftNode {
            inner,
            rcv,
            peers,
            store,
            // msg_tx,
            uncommitteds,
            seq,
            snd,
            should_quit: false,
            last_snap_time,
            request_votes: 0,
            cfg,
        })
    }

    #[inline]
    fn new_config(id: u64, cfg: &RaftConfig) -> RaftConfig {
        let mut cfg = cfg.clone();
        cfg.id = id;
        cfg
    }

    #[inline]
    pub fn peer(&self, id: u64) -> Option<Peer> {
        match self.peers.get(&id) {
            Some(Some(p)) => Some(p.clone()),
            _ => None,
        }
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.inner.raft.leader_id == self.inner.raft.id
    }

    #[inline]
    pub fn id(&self) -> u64 {
        self.raft.id
    }

    #[inline]
    pub fn add_peer(&mut self, addr: &str, id: u64) -> Peer {
        let peer = Peer::new(
            addr.to_string(),
            self.cfg.grpc_timeout,
            self.cfg.grpc_concurrency_limit,
            self.cfg.grpc_breaker_threshold,
            self.cfg.grpc_breaker_retry_interval.as_millis() as i64,
        );
        self.peers.insert(id, Some(peer.clone()));
        peer
    }

    #[inline]
    fn leader(&self) -> u64 {
        self.raft.leader_id
    }

    #[inline]
    fn has_leader(&self) -> bool {
        self.raft.leader_id > 0
    }

    #[inline]
    fn peer_addrs(&self) -> HashMap<u64, String> {
        self.peers
            .iter()
            .filter_map(|(&id, peer)| {
                peer.as_ref()
                    .map(|Peer { addr, .. }| (id, addr.to_string()))
            })
            .collect()
    }

    #[inline]
    fn status(&self) -> Status {
        debug!("raft status.ss: {:?}", self.inner.status().ss);
        let leader_id = self.raft.leader_id;
        Status {
            id: self.inner.raft.id,
            leader_id,
            uncommitteds: self.uncommitteds.len(),
            request_votes: self.request_votes,
            active_mailbox_sends: active_mailbox_sends(),
            active_mailbox_querys: active_mailbox_querys(),
            active_send_proposal_grpc_requests: send_proposal_active_requests(),
            active_send_message_grpc_requests: send_message_active_requests(),
            peers: self.peer_addrs(),
        }
    }

    // forward query request to leader
    #[inline]
    async fn forward_query(&self, query: Vec<u8>, chan: oneshot::Sender<RaftResponse>) {
        let id = self.leader();
        let peer = match self.peer(id) {
            Some(peer) => peer,
            None => {
                if let Err(e) = chan.send(RaftResponse::WrongLeader {
                    leader_id: id,
                    leader_addr: None,
                }) {
                    warn!(
                        "forward_query, Message::Query, RaftResponse send error: {:?}",
                        e
                    );
                }
                return;
            }
        };

        let query_sender = QuerySender {
            query,
            client: peer,
            chan,
            timeout: Duration::from_millis(1000),
            max_retries: 0,
        };
        tokio::spawn(query_sender.send());
    }

    #[inline]
    async fn send_query(&self, query: &[u8], chan: oneshot::Sender<RaftResponse>) {
        let data = self.store.query(query).await.unwrap_or_default();
        if let Err(e) = chan.send(RaftResponse::Response { data }) {
            warn!("Message::Query, RaftResponse send error: {:?}", e);
        }
    }

    #[inline]
    fn send_wrong_leader(&self, chan: oneshot::Sender<RaftResponse>) {
        let leader_id = self.leader();
        // leader can't be an empty node
        let leader_addr = self
            .peers
            .get(&leader_id)
            .and_then(|peer| peer.as_ref().map(|p| p.addr.clone()));
        let raft_response = RaftResponse::WrongLeader {
            leader_id,
            leader_addr,
        };
        if let Err(e) = chan.send(raft_response) {
            warn!("send_wrong_leader, RaftResponse send error: {:?}", e);
        }
    }

    #[inline]
    fn _send_error(&self, chan: oneshot::Sender<RaftResponse>, e: String) {
        let raft_response = RaftResponse::Error(e);
        if let Err(e) = chan.send(raft_response) {
            warn!("send_error, RaftResponse send error: {:?}", e);
        }
    }

    #[inline]
    fn send_is_leader(&self, chan: oneshot::Sender<RaftResponse>) {
        if let Err(e) = chan.send(RaftResponse::IsLeader {
            leader_id: self.leader(),
        }) {
            warn!("Message::IsLeader, RaftResponse send error: {:?}", e);
        }
    }

    #[inline]
    fn send_status(&self, chan: oneshot::Sender<RaftResponse>) {
        if let Err(e) = chan.send(RaftResponse::Status(self.status())) {
            warn!("Message::Status, RaftResponse send error: {:?}", e);
        }
    }

    #[inline]
    fn take_and_propose(&mut self, merger: &mut Merger) {
        if let Some((data, reply_chans)) = merger.take() {
            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            self.uncommitteds.insert(seq, reply_chans);
            let seq = serialize(&seq).unwrap();
            let data = serialize(&data).unwrap();
            if let Err(e) = self.propose(seq, data) {
                error!("propose to raft error, {:?}", e);
            }
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let mut heartbeat = self.cfg.heartbeat;
        let mut now = Instant::now();
        let mut snapshot_received = self.is_leader();
        let mut merger = Merger::new(
            self.cfg.proposal_batch_size,
            self.cfg.proposal_batch_timeout,
        );
        info!("snapshot_received: {:?}", snapshot_received);
        info!("has_leader: {:?}", self.has_leader());

        loop {
            if self.should_quit {
                warn!("Quitting raft");
                return Ok(());
            }
            match timeout(heartbeat, self.rcv.next()).await {
                Ok(Some((_, Message::ConfigChange { chan, mut change }))) => {
                    info!("change Received, {:?}", change);
                    // whenever a change id is 0, it's a message to self.
                    if change.get_node_id() == 0 {
                        change.set_node_id(self.id());
                    }

                    if !self.is_leader() {
                        // wrong leader send client cluster data
                        // TODO: retry strategy in case of failure
                        self.send_wrong_leader(chan);
                    } else {
                        // leader assign new id to peer
                        info!("received request from: {}", change.get_node_id());
                        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
                        self.uncommitteds
                            .insert(seq, ReplyChan::One((chan, Instant::now())));
                        if let Err(e) = self.propose_conf_change(serialize(&seq).unwrap(), change) {
                            warn!("propose_conf_change, error: {:?}", e);
                        }
                    }
                }
                Ok(Some((_, Message::Raft(m)))) => {
                    debug!(
                        "raft message: to={} from={} msg_type={:?}, commit={}, {:?}",
                        self.raft.id,
                        m.from,
                        m.msg_type,
                        m.get_commit(),
                        m
                    );
                    let msg_type = m.get_msg_type();
                    if MessageType::MsgRequestVote == msg_type {
                        self.request_votes += 1;
                    }

                    if !snapshot_received && msg_type == MessageType::MsgHeartbeat {
                        info!(
                            "raft message, snapshot_received: {}, has_leader: {}, {:?}",
                            snapshot_received,
                            self.has_leader(),
                            m
                        );
                    } else {
                        if let Err(e) = self.step(*m) {
                            warn!(
                                "step error, {:?}, msg_type: {:?}, snapshot_received: {}",
                                e, msg_type, snapshot_received
                            );
                        }
                        if msg_type == MessageType::MsgSnapshot {
                            snapshot_received = true;
                        }
                    }
                }
                Ok(Some((_, Message::Propose { proposal, chan }))) => {
                    let now = Instant::now();
                    if !self.is_leader() {
                        debug!("Message::Propose, send_wrong_leader {:?}", proposal);
                        self.send_wrong_leader(chan);
                    } else {
                        merger.add(proposal, chan);
                        self.take_and_propose(&mut merger);
                    }
                    if now.elapsed() > self.cfg.heartbeat {
                        info!("Message::Propose elapsed: {:?}", now.elapsed());
                    }
                }

                Ok(Some((_, Message::Query { query, chan }))) => {
                    let now = Instant::now();
                    if !self.is_leader() {
                        debug!("[forward_query] query.len: {:?}", query.len());
                        self.forward_query(query, chan).await;
                    } else {
                        debug!("Message::Query, {:?}", query);
                        self.send_query(&query, chan).await;
                    }
                    if now.elapsed() > self.cfg.heartbeat {
                        info!("Message::Query elapsed: {:?}", now.elapsed());
                    }
                }

                Ok(Some((_, Message::IsLeader { chan }))) => {
                    if !self.is_leader() {
                        // TODO: retry strategy in case of failure
                        info!("requested Id, but not leader");
                        self.send_wrong_leader(chan);
                    } else {
                        self.send_is_leader(chan);
                    }
                }
                Ok(Some((_, Message::Status { chan }))) => {
                    self.send_status(chan);
                }
                Ok(Some((_, Message::ReportUnreachable { node_id }))) => {
                    debug!("Message::ReportUnreachable, node_id: {}", node_id);
                    self.report_unreachable(node_id);
                }
                Ok(None) => {
                    error!("Recv None");
                    return Err(Error::RecvError("Recv None".into()));
                }
                Err(_) => {
                    self.take_and_propose(&mut merger);
                }
            }

            let elapsed = now.elapsed();
            now = Instant::now();
            if elapsed >= heartbeat {
                heartbeat = self.cfg.heartbeat;
                if elapsed > Duration::from_millis(500) {
                    warn!("raft tick elapsed: {:?}", elapsed);
                }
                self.tick();
            } else {
                heartbeat -= elapsed;
            }

            let on_ready_now = Instant::now();
            if let Err(e) = self.on_ready().await {
                error!(
                    "raft on_ready(..) error: {:?}, elapsed: {:?}",
                    e,
                    on_ready_now.elapsed()
                );
                return Err(e);
            }
            if on_ready_now.elapsed() > Duration::from_millis(200) {
                warn!("raft on_ready(..) elapsed: {:?}", on_ready_now.elapsed());
            }
        }
    }

    async fn on_ready(&mut self) -> Result<()> {
        if !self.has_ready() {
            return Ok(());
        }

        let mut ready = self.ready();

        if !ready.messages().is_empty() {
            // Send out the messages.
            self.send_messages(ready.take_messages());
        }

        if *ready.snapshot() != Snapshot::default() {
            let snapshot = ready.snapshot();
            self.store.restore(snapshot.get_data()).await?;
            let store = self.mut_store();
            store.apply_snapshot(snapshot.clone())?;
        }

        self.handle_committed_entries(ready.take_committed_entries())
            .await?;

        if !ready.entries().is_empty() {
            let entries = ready.entries();
            let store = self.mut_store();
            store.append(entries)?;
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            let store = self.mut_store();
            store.set_hard_state(hs)?;
        }

        if !ready.persisted_messages().is_empty() {
            // Send out the persisted messages come from the node.
            self.send_messages(ready.take_persisted_messages());
        }
        let mut light_rd = self.advance(ready);

        if let Some(commit) = light_rd.commit_index() {
            let store = self.mut_store();
            store.set_hard_state_comit(commit)?;
        }
        // Send out the messages.
        self.send_messages(light_rd.take_messages());
        // Apply all committed entries.
        self.handle_committed_entries(light_rd.take_committed_entries())
            .await?;
        self.advance_apply();

        Ok(())
    }

    fn send_messages(&mut self, msgs: Vec<eraftpb::Message>) {
        for message in msgs {
            // for message in ready.messages.drain(..) {
            let client_id = message.get_to();
            let client = match self.peer(client_id) {
                Some(peer) => peer,
                None => continue,
            };

            let message_sender = MessageSender {
                message,
                client,
                client_id,
                chan: self.snd.clone(),
                max_retries: 0,
                timeout: Duration::from_millis(500),
            };
            tokio::spawn(message_sender.send());
        }
    }

    async fn handle_committed_entries(&mut self, committed_entries: Vec<Entry>) -> Result<()> {
        // Fitler out empty entries produced by new elected leaders.
        for entry in committed_entries {
            if entry.data.is_empty() {
                // From new elected leaders.
                continue;
            }
            if let EntryType::EntryConfChange = entry.get_entry_type() {
                self.handle_config_change(&entry).await?;
            } else {
                self.handle_normal(&entry).await?;
            }
        }
        Ok(())
    }

    #[inline]
    async fn handle_config_change(&mut self, entry: &Entry) -> Result<()> {
        info!("handle_config_change, entry: {:?}", entry);
        let seq: u64 = deserialize(entry.get_context())?;
        let change = ConfChange::decode(entry.get_data()).map_err(|e| Error::Msg(e.to_string()))?;
        let id = change.get_node_id();

        let change_type = change.get_change_type();

        match change_type {
            ConfChangeType::AddNode => {
                let addr: String = deserialize(change.get_context())?;
                info!("adding {} ({}) to peers", addr, id);
                self.add_peer(&addr, id);
            }
            ConfChangeType::RemoveNode => {
                if change.get_node_id() == self.id() {
                    self.should_quit = true;
                    warn!("quiting the cluster");
                } else {
                    self.peers.remove(&change.get_node_id());
                }
            }
            _ => unimplemented!(),
        }

        if let Ok(cs) = self.apply_conf_change(&change) {
            let last_applied = self.raft.raft_log.applied;
            let snapshot = prost::bytes::Bytes::from(self.store.snapshot().await?);
            {
                let store = self.mut_store();
                store.set_conf_state(&cs)?;
                store.compact(last_applied)?;
                store.create_snapshot(snapshot)?;
            }
        }

        if let Some(sender) = self.uncommitteds.remove(&seq) {
            let response = match change_type {
                ConfChangeType::AddNode => RaftResponse::JoinSuccess {
                    assigned_id: id,
                    peer_addrs: self.peer_addrs(),
                },
                ConfChangeType::RemoveNode => RaftResponse::Ok,
                _ => unimplemented!(),
            };
            if let ReplyChan::One((sender, _)) = sender {
                if sender.send(response).is_err() {
                    warn!("error sending response")
                }
            }
        }
        Ok(())
    }

    #[inline]
    async fn handle_normal(&mut self, entry: &Entry) -> Result<()> {
        let seq: u64 = deserialize(entry.get_context())?;
        debug!(
            "[handle_normal] seq:{}, senders.len(): {}",
            seq,
            self.uncommitteds.len()
        );

        match (
            deserialize::<Proposals>(entry.get_data())?,
            self.uncommitteds.remove(&seq),
        ) {
            (Proposals::One(data), chan) => {
                let apply_start = std::time::Instant::now();
                let reply = self.store.apply(&data).await;
                if apply_start.elapsed().as_secs() > 3 {
                    log::warn!("apply, cost time: {:?}", apply_start.elapsed());
                }
                if let Some(ReplyChan::One((chan, inst))) = chan {
                    let res = match reply {
                        Ok(data) => RaftResponse::Response { data },
                        Err(e) => RaftResponse::Error(e.to_string()),
                    };
                    if let Err(_resp) = chan.send(res) {
                        warn!(
                            "[handle_normal] send RaftResponse error, seq:{}, cost time: {:?}",
                            seq,
                            inst.elapsed()
                        );
                    }
                }
            }
            (Proposals::More(mut datas), chans) => {
                let mut chans = if let Some(ReplyChan::More(chans)) = chans {
                    Some(chans)
                } else {
                    None
                };
                while let Some(data) = datas.pop() {
                    let apply_start = std::time::Instant::now();
                    let reply = self.store.apply(&data).await;
                    if apply_start.elapsed().as_secs() > 3 {
                        log::warn!("apply, cost time: {:?}", apply_start.elapsed());
                    }
                    if let Some((chan, inst)) = chans.as_mut().and_then(|cs| cs.pop()) {
                        if inst.elapsed().as_secs() > 3 {
                            warn!(
                                "[handle_normal] cost time, {:?}, chan is canceled: {}",
                                inst.elapsed(),
                                chan.is_canceled()
                            );
                        }
                        let res = match reply {
                            Ok(data) => RaftResponse::Response { data },
                            Err(e) => RaftResponse::Error(e.to_string()),
                        };
                        if let Err(_resp) = chan.send(res) {
                            warn!(
                                "[handle_normal] send RaftResponse error, seq:{}, cost time: {:?}",
                                seq,
                                inst.elapsed()
                            );
                        }
                    }
                }
            }
        }

        if Instant::now() > self.last_snap_time + self.cfg.snapshot_interval {
            self.last_snap_time = Instant::now();
            let last_applied = self.raft.raft_log.applied;
            let snapshot = prost::bytes::Bytes::from(self.store.snapshot().await?);
            let store = self.mut_store();
            store.compact(last_applied)?;
            let first_index = store.first_index().unwrap_or(0);
            let last_index = store.last_index().unwrap_or(0);
            let result = store.create_snapshot(snapshot);
            info!(
                "create snapshot cost time: {:?}, first_index: {:?}, last_index: {:?}, {}, create snapshot result: {:?}",
                Instant::now() - self.last_snap_time,
                first_index,
                last_index,
                (last_index as i64 - first_index as i64),
                result
            );
        }
        Ok(())
    }
}

impl<S: Store> Deref for RaftNode<S> {
    type Target = RawNode<MemStorage>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S: Store> DerefMut for RaftNode<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
