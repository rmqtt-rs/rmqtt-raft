use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bincode::{deserialize, serialize};
use futures::channel::oneshot;
use futures::future::FutureExt;
use log::{debug, info, warn};
use once_cell::sync::Lazy;
use tikv_raft::eraftpb::{ConfChange, ConfChangeType};
use tokio::time::timeout;

use rust_box::collections::PriorityQueue;
use rust_box::handy_grpc::client::{Client as GrpcClient, Message as GrpcMessage};
use rust_box::handy_grpc::Priority;
use rust_box::mpsc::with_priority_channel;

use crate::error::{Error, Result};
use crate::message::{Message, PriorityQueueType, RaftMessage, RaftResponse, Status};
use crate::raft_node::{Peer, RaftNode};
use crate::raft_server::RaftServer;
use crate::Config;

type Sender<T> = rust_box::mpsc::Sender<T, rust_box::mpsc::SendError<T>>;
type Receiver<T> = rust_box::mpsc::Receiver<T>;

type DashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;

#[async_trait]
pub trait Store {
    async fn apply(&mut self, message: &[u8]) -> Result<Vec<u8>>;
    async fn query(&self, query: &[u8]) -> Result<Vec<u8>>;
    async fn snapshot(&self) -> Result<Vec<u8>>;
    async fn restore(&mut self, snapshot: &[u8]) -> Result<()>;
}

struct ProposalSender {
    proposal: Vec<u8>,
    client: Peer,
}

impl ProposalSender {
    async fn send(self) -> Result<RaftResponse> {
        match self.client.send_proposal(self.proposal).await {
            Ok(reply) => {
                let raft_response: RaftResponse = deserialize(&reply)?;
                Ok(raft_response)
            }
            Err(e) => {
                warn!("error sending proposal {:?}", e);
                Err(e)
            }
        }
    }
}

/// A mailbox to send messages to a running raft node.
#[derive(Clone)]
pub struct Mailbox {
    peers: Arc<DashMap<(u64, String), Peer>>,
    sender: Sender<(Priority, Message)>,
    grpc_timeout: Duration,
    grpc_concurrency_limit: usize,
    grpc_breaker_threshold: u64,
    grpc_breaker_retry_interval: i64,
}

static MAILBOX_SENDS: Lazy<Arc<AtomicIsize>> = Lazy::new(|| Arc::new(AtomicIsize::new(0)));
static MAILBOX_QUERYS: Lazy<Arc<AtomicIsize>> = Lazy::new(|| Arc::new(AtomicIsize::new(0)));

pub fn active_mailbox_sends() -> isize {
    MAILBOX_SENDS.load(Ordering::SeqCst)
}

pub fn active_mailbox_querys() -> isize {
    MAILBOX_QUERYS.load(Ordering::SeqCst)
}

impl Mailbox {
    #[inline]
    pub fn pears(&self) -> Vec<(u64, Peer)> {
        self.peers
            .iter()
            .map(|p| {
                let (id, _) = p.key();
                (*id, p.value().clone())
            })
            .collect::<Vec<_>>()
    }

    #[inline]
    async fn peer(&self, leader_id: u64, leader_addr: String) -> Peer {
        self.peers
            .entry((leader_id, leader_addr.clone()))
            .or_insert_with(|| {
                Peer::new(
                    leader_addr,
                    self.grpc_timeout,
                    self.grpc_concurrency_limit,
                    self.grpc_breaker_threshold,
                    self.grpc_breaker_retry_interval,
                )
            })
            .clone()
    }

    #[inline]
    async fn send_to_leader(
        &self,
        proposal: Vec<u8>,
        leader_id: u64,
        leader_addr: String,
    ) -> Result<RaftResponse> {
        let peer = self.peer(leader_id, leader_addr).await;
        let proposal_sender = ProposalSender {
            proposal,
            client: peer,
        };
        proposal_sender.send().await
    }

    #[inline]
    pub async fn send(&self, message: Vec<u8>) -> Result<Vec<u8>> {
        MAILBOX_SENDS.fetch_add(1, Ordering::SeqCst);
        let reply = self._send(message).await;
        MAILBOX_SENDS.fetch_sub(1, Ordering::SeqCst);
        reply
    }

    #[inline]
    async fn _send(&self, message: Vec<u8>) -> Result<Vec<u8>> {
        let (target_leader_id, target_leader_addr) = {
            let (tx, rx) = oneshot::channel();
            let proposal = Message::Propose {
                proposal: message.clone(),
                chan: tx,
            };
            let mut sender = self.sender.clone();
            sender
                .send((1, proposal))
                .await //.try_send(proposal)
                .map_err(|e| Error::SendError(e.to_string()))?;
            let reply = timeout(self.grpc_timeout, rx).await;
            let reply = reply
                .map_err(|e| Error::RecvError(e.to_string()))?
                .map_err(|e| Error::RecvError(e.to_string()))?;
            match reply {
                RaftResponse::Response { data } => return Ok(data),
                RaftResponse::WrongLeader {
                    leader_id,
                    leader_addr,
                } => (leader_id, leader_addr),
                RaftResponse::Error(e) => return Err(Error::from(e)),
                _ => {
                    warn!("Recv other raft response: {:?}", reply);
                    return Err(Error::Unknown);
                }
            }
        };

        debug!(
            "This node not is Leader, leader_id: {:?}, leader_addr: {:?}",
            target_leader_id, target_leader_addr
        );

        if let Some(target_leader_addr) = target_leader_addr {
            if target_leader_id != 0 {
                return match self
                    .send_to_leader(message, target_leader_id, target_leader_addr.clone())
                    .await?
                {
                    RaftResponse::Response { data } => Ok(data),
                    RaftResponse::WrongLeader {
                        leader_id,
                        leader_addr,
                    } => {
                        warn!("The target node is not the Leader, target_leader_id: {}, target_leader_addr: {:?}, actual_leader_id: {}, actual_leader_addr: {:?}",
                            target_leader_id, target_leader_addr, leader_id, leader_addr);
                        Err(Error::NotLeader)
                    }
                    RaftResponse::Error(e) => Err(Error::from(e)),
                    _ => {
                        warn!("Recv other raft response, target_leader_id: {}, target_leader_addr: {:?}", target_leader_id, target_leader_addr);
                        Err(Error::Unknown)
                    }
                };
            }
        }

        Err(Error::LeaderNotExist)
    }

    #[inline]
    pub async fn query(&self, query: Vec<u8>) -> Result<Vec<u8>> {
        MAILBOX_QUERYS.fetch_add(1, Ordering::SeqCst);
        let reply = self._query(query).await;
        MAILBOX_QUERYS.fetch_sub(1, Ordering::SeqCst);
        reply
    }

    #[inline]
    async fn _query(&self, query: Vec<u8>) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        let mut sender = self.sender.clone();
        match sender
            .send((Priority::MIN, Message::Query { query, chan: tx }))
            .await
        {
            Ok(()) => match timeout(self.grpc_timeout, rx).await {
                Ok(Ok(RaftResponse::Response { data })) => Ok(data),
                Ok(Ok(RaftResponse::Error(e))) => Err(Error::from(e)),
                _ => Err(Error::Unknown),
            },
            Err(e) => Err(Error::SendError(e.to_string())),
        }
    }

    #[inline]
    pub async fn leave(&self) -> Result<()> {
        let mut change = ConfChange::default();
        // set node id to 0, the node will set it to self when it receives it.
        change.set_node_id(0);
        change.set_change_type(ConfChangeType::RemoveNode);
        let mut sender = self.sender.clone();
        let (chan, rx) = oneshot::channel();
        match sender
            .send((Priority::MAX, Message::ConfigChange { change, chan }))
            .await
        {
            Ok(()) => match rx.await {
                Ok(RaftResponse::Ok) => Ok(()),
                Ok(RaftResponse::Error(e)) => Err(Error::from(e)),
                _ => Err(Error::Unknown),
            },
            Err(e) => Err(Error::SendError(e.to_string())),
        }
    }

    #[inline]
    pub async fn status(&self) -> Result<Status> {
        let (tx, rx) = oneshot::channel();
        let mut sender = self.sender.clone();
        match sender
            .send((Priority::MAX, Message::Status { chan: tx }))
            .await
        {
            Ok(_) => match timeout(self.grpc_timeout, rx).await {
                Ok(Ok(RaftResponse::Status(status))) => Ok(status),
                Ok(Ok(RaftResponse::Error(e))) => Err(Error::from(e)),
                _ => Err(Error::Unknown),
            },
            Err(e) => Err(Error::SendError(e.to_string())),
        }
    }
}

pub struct Raft<S: Store + 'static> {
    store: S,
    channel_queue: PriorityQueueType<Priority, Message>,
    tx: Sender<(Priority, Message)>,
    rx: Receiver<(Priority, Message)>,
    addr: SocketAddr,
    logger: slog::Logger,
    cfg: Arc<Config>,
}

impl<S: Store + Send + Sync + 'static> Raft<S> {
    /// creates a new node with the given address and store.
    pub fn new<A: ToSocketAddrs>(
        addr: A,
        store: S,
        logger: slog::Logger,
        cfg: Config,
    ) -> Result<Self> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| Error::from("None"))?;
        let channel_queue = Arc::new(parking_lot::RwLock::new(PriorityQueue::default()));
        //let (tx, rx) = mpsc::channel(100_000);
        let (tx, rx) = with_priority_channel::<Priority, Message>(channel_queue.clone(), 10_000);
        let cfg = Arc::new(cfg);
        Ok(Self {
            store,
            channel_queue,
            tx,
            rx,
            addr,
            logger,
            cfg,
        })
    }

    /// gets the node's `Mailbox`.
    pub fn mailbox(&self) -> Mailbox {
        Mailbox {
            peers: Arc::new(DashMap::default()),
            sender: self.tx.clone(),
            grpc_timeout: self.cfg.grpc_timeout,
            grpc_concurrency_limit: self.cfg.grpc_concurrency_limit,
            grpc_breaker_threshold: self.cfg.grpc_breaker_threshold,
            grpc_breaker_retry_interval: self.cfg.grpc_breaker_retry_interval.as_millis() as i64,
        }
    }

    /// Returns channel queue len
    pub fn channel_queue_len(&self) -> usize {
        self.channel_queue.read().len()
    }

    /// find leader id and leader address
    pub async fn find_leader_info(&self, peer_addrs: Vec<String>) -> Result<Option<(u64, String)>> {
        let grpc_timeout = self.cfg.grpc_timeout;
        let mut futs = Vec::new();
        for addr in peer_addrs {
            let fut = async {
                let _addr = addr.clone();
                match Self::request_leader(addr, grpc_timeout).await {
                    Ok(reply) => Ok(reply),
                    Err(e) => {
                        info!("find_leader, addr: {}, {:?}", _addr, e);
                        Err(e)
                    }
                }
            };
            futs.push(fut.boxed());
        }

        let (leader_id, leader_addr) = match futures::future::select_ok(futs).await {
            Ok(((leader_id, leader_addr), _)) => (leader_id, leader_addr),
            Err(_e) => return Ok(None),
        };

        info!("leader_id: {}, leader_addr: {}", leader_id, leader_addr);
        if leader_id == 0 {
            Ok(None)
        } else {
            Ok(Some((leader_id, leader_addr)))
        }
    }

    async fn request_leader(peer_addr: String, grpc_timeout: Duration) -> Result<(u64, String)> {
        let mut client = GrpcClient::new(peer_addr.clone())
            .concurrency_limit(1)
            .connect_timeout(grpc_timeout)
            .connect()
            .await
            .map_err(|e| Error::Msg(e.to_string()))?;

        let msg = GrpcMessage {
            ver: 1,
            priority: 0,
            data: RaftMessage::IsLeader.encode()?,
        };
        let result = tokio::time::timeout(grpc_timeout, client.send(msg)).await;
        let result = result.map_err(|_| Error::Elapsed)??;
        let raft_resp = RaftResponse::decode(&result.data)?;
        match raft_resp {
            RaftResponse::IsLeader { leader_id } => Ok((leader_id, peer_addr)),
            _ => {
                log::warn!("request leader info error, {:?}", raft_resp);
                Err(Error::LeaderNotExist)
            }
        }
    }

    /// Create a new leader for the cluster, with id 1. There has to be exactly one node in the
    /// cluster that is initialised that way
    pub async fn lead(self, node_id: u64) -> Result<()> {
        let server = RaftServer::new(self.tx.clone(), self.addr, self.cfg.clone());

        let node = RaftNode::new_leader(
            self.channel_queue.clone(),
            self.rx,
            self.tx,
            node_id,
            self.store,
            &self.logger,
            self.cfg.clone(),
            server.channel_queue.clone(),
        )?;

        let server_handle = async {
            if let Err(e) = server.run().await {
                warn!("raft server run error: {:?}", e);
                Err(e)
            } else {
                Ok(())
            }
        };
        let node_handle = async {
            if let Err(e) = node.run().await {
                warn!("node run error: {:?}", e);
                Err(e)
            } else {
                Ok(())
            }
        };

        tokio::try_join!(server_handle, node_handle)?;
        info!("leaving leader node");

        Ok(())
    }

    /// Tries to join a new cluster at `addr`, getting an id from the leader, or finding it if
    /// `addr` is not the current leader of the cluster
    pub async fn join(
        self,
        node_id: u64,
        leader_id: Option<u64>,
        leader_addr: String,
    ) -> Result<()> {
        // 1. try to discover the leader and obtain an id from it, if leader_id is None.
        info!("attempting to join peer cluster at {}", leader_addr);
        let (leader_id, leader_addr): (u64, String) = if let Some(leader_id) = leader_id {
            (leader_id, leader_addr)
        } else {
            Self::request_leader(leader_addr, self.cfg.grpc_timeout).await?
        };

        let server = RaftServer::new(self.tx.clone(), self.addr, self.cfg.clone());

        // 2. run server and node to prepare for joining
        let mut node = RaftNode::new_follower(
            self.channel_queue.clone(),
            self.rx,
            self.tx,
            node_id,
            self.store,
            &self.logger,
            self.cfg.clone(),
            server.channel_queue.clone(),
        )?;
        let peer = node.add_peer(&leader_addr, leader_id);

        let server_handle = async {
            if let Err(e) = server.run().await {
                warn!("raft server run error: {:?}", e);
                Err(e)
            } else {
                Ok(())
            }
        };

        //try remove from the cluster
        let mut change_remove = ConfChange::default();
        change_remove.set_node_id(node_id);
        change_remove.set_change_type(ConfChangeType::RemoveNode);
        //let change_remove = RiteraftConfChange {
        //    inner: ConfChange::encode_to_vec(&change_remove),
        //};

        let raft_response = peer.change_config(change_remove).await?;

        info!("change_remove raft_response: {:?}", raft_response);

        // 3. Join the cluster
        // TODO: handle wrong leader
        let mut change = ConfChange::default();
        change.set_node_id(node_id);
        change.set_change_type(ConfChangeType::AddNode);
        change.set_context(serialize(&self.addr.to_string())?);
        // change.set_context(serialize(&self.addr)?);

        //let change = RiteraftConfChange {
        //    inner: ConfChange::encode_to_vec(&change),
        //};
        let raft_response = peer.change_config(change).await?;
        if let RaftResponse::JoinSuccess {
            assigned_id,
            peer_addrs,
        } = raft_response
        {
            info!(
                "change_config response.assigned_id: {:?}, peer_addrs: {:?}",
                assigned_id, peer_addrs
            );
            for (id, addr) in peer_addrs {
                if id != assigned_id {
                    node.add_peer(&addr, id);
                }
            }
        } else {
            return Err(Error::JoinError);
        }

        let node_handle = async {
            if let Err(e) = node.run().await {
                warn!("node run error: {:?}", e);
                Err(e)
            } else {
                Ok(())
            }
        };
        let _ = tokio::try_join!(server_handle, node_handle)?;
        info!("leaving follower node");
        Ok(())
    }
}
