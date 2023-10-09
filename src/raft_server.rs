use std::sync::Arc;
use std::time::Duration;

use futures::channel::oneshot;
use futures::StreamExt;

use log::{info, warn};
use prost::Message as _;
use tikv_raft::eraftpb::{self, ConfChange};
use tokio::time::timeout;

use rust_box::collections::PriorityQueue;
use rust_box::handy_grpc::server::run as grpc_run;
use rust_box::handy_grpc::transferpb;
use rust_box::handy_grpc::Priority;
use rust_box::mpsc::with_priority_channel;

use super::message::PriorityQueueType;
use super::message::RaftMessage;
use super::message::Sender;
use super::message::{Message, RaftResponse, ServerGrpcMessage};
use super::raft::parse_socket_addrs;
use super::{error, Config};

#[derive(Clone)]
pub struct RaftServer {
    pub(crate) channel_queue: PriorityQueueType<Priority, ServerGrpcMessage>,
    snd: Sender<(Priority, Message)>,
    laddr: String,
    timeout: Duration,
    cfg: Arc<Config>,
}

impl RaftServer {
    pub fn new(snd: Sender<(Priority, Message)>, laddr: String, cfg: Arc<Config>) -> Self {
        let channel_queue = Arc::new(parking_lot::RwLock::new(PriorityQueue::default()));
        RaftServer {
            channel_queue,
            snd,
            laddr,
            timeout: cfg.grpc_timeout,
            cfg,
        }
    }

    pub async fn run(&self) -> error::Result<()> {
        let laddr = self.laddr.clone();
        let _cfg = self.cfg.clone();
        info!("listening gRPC requests on: {}", laddr);

        let (tx, mut rx) = with_priority_channel::<Priority, ServerGrpcMessage>(
            self.channel_queue.clone(),
            10_000,
        );

        let run_receiver_fut = async move {
            loop {
                match parse_socket_addrs(&laddr) {
                    Err(_) => {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        continue;
                    }
                    Ok(sock_laddrs) => {
                        for sock_laddr in sock_laddrs {
                            if let Err(e) = grpc_run(
                                sock_laddr,
                                tx.clone(),
                                None,
                                None,
                                #[cfg(feature = "reuseaddr")]
                                _cfg.reuseaddr,
                                #[cfg(feature = "reuseport")]
                                _cfg.reuseport,
                            )
                            .await
                            {
                                log::error!(
                                    "Run gRPC receiver error, {}({}), {:?}",
                                    laddr,
                                    sock_laddr.to_string(),
                                    e
                                );
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        };

        let recv_data_fut = async move {
            while let Some((p, (msg, reply_tx))) = rx.next().await {
                let this = self.clone();
                tokio::spawn(async move {
                    match RaftMessage::decode(msg.data.as_slice()) {
                        Err(e) => {
                            log::error!("RaftMessage decode error, {:?}", e);
                            if let Some(reply_tx) = reply_tx {
                                if let Err(e) = reply_tx.send(Err(e)) {
                                    log::error!("gRPC send result failure, {:?}", e);
                                }
                            }
                        }
                        Ok(RaftMessage::IsLeader) => {
                            if let Some(reply_tx) = reply_tx {
                                this._request_leader_info(reply_tx).await;
                            } else {
                                unreachable!()
                            }
                        }
                        Ok(RaftMessage::ConfigChange { change }) => {
                            if let Some(reply_tx) = reply_tx {
                                this._change_config(change, reply_tx).await;
                            } else {
                                unreachable!()
                            }
                        }
                        Ok(RaftMessage::Raft { msg }) => {
                            this._send_raft_message(msg).await;
                        }
                        Ok(RaftMessage::Propose { proposal }) => {
                            if let Some(reply_tx) = reply_tx {
                                this._send_proposal(proposal, p, reply_tx).await;
                            } else {
                                unreachable!()
                            }
                        }
                        Ok(RaftMessage::Query { query }) => {
                            if let Some(reply_tx) = reply_tx {
                                this._send_query(query, p, reply_tx).await;
                            } else {
                                unreachable!()
                            }
                        }
                        Ok(RaftMessage::Status) => {
                            unreachable!()
                        }
                        _ => {
                            unreachable!()
                        }
                    }
                });
            }
            log::error!("Recv None");
        };

        futures::future::join(recv_data_fut, run_receiver_fut).await;
        Ok(())
    }

    #[inline]
    async fn _request_leader_info(
        &self,
        reply_tx: oneshot::Sender<anyhow::Result<transferpb::Message>>,
    ) {
        let (tx, rx) = oneshot::channel();
        let res = if let Err(e) = self
            .snd
            .clone()
            .send((Priority::MIN, Message::IsLeader { chan: tx }))
            .await
        {
            Err(anyhow::Error::new(e))
        } else {
            let reply = timeout(self.timeout, rx).await;
            let resp = match reply {
                Ok(Ok(resp)) => resp,
                Ok(Err(e)) => {
                    warn!("canceled waiting for reply, {:?}", e);
                    RaftResponse::Error("Canceled".into())
                }
                Err(e) => {
                    warn!("timeout waiting for reply, {:?}", e);
                    RaftResponse::Error("Request id timeout".into())
                }
            };
            resp.encode().map(|data| transferpb::Message {
                ver: 1,
                priority: 0,
                data,
            })
        };
        if let Err(e) = reply_tx.send(res) {
            log::error!("send reply message error, {:?}", e);
        }
    }

    #[inline]
    async fn _change_config(
        &self,
        change: Vec<u8>,
        reply_tx: oneshot::Sender<anyhow::Result<transferpb::Message>>,
    ) {
        let res = {
            match ConfChange::decode(change.as_ref()) {
                Err(e) => Err(anyhow::Error::new(e)),
                Ok(change) => {
                    let (tx, rx) = oneshot::channel();
                    if let Err(e) = self
                        .snd
                        .clone()
                        .send((Priority::MAX, Message::ConfigChange { change, chan: tx }))
                        .await
                    {
                        Err(anyhow::Error::new(e))
                    } else {
                        let resp = match timeout(self.timeout, rx).await {
                            Ok(Ok(raft_resp)) => raft_resp,
                            Ok(Err(e)) => {
                                warn!("canceled waiting for reply, {:?}", e);
                                RaftResponse::Error("Canceled".into())
                            }
                            Err(e) => {
                                warn!("timeout waiting for reply, {:?}", e);
                                RaftResponse::Error("ConfigChange timeout".into())
                            }
                        };
                        resp.encode().map(|data| transferpb::Message {
                            ver: 1,
                            priority: 0,
                            data,
                        })
                    }
                }
            }
        };
        if let Err(e) = reply_tx.send(res) {
            log::error!("send reply message error, {:?}", e);
        }
    }

    #[inline]
    async fn _send_raft_message(&self, req: Vec<u8>) {
        match eraftpb::Message::decode(req.as_ref()) {
            Err(e) => {
                log::error!("decode raft message error, {:?}", e);
            }
            Ok(raft_msg) => {
                if let Err(e) = self
                    .snd
                    .clone()
                    .send((Priority::MAX, Message::Raft(Box::new(raft_msg))))
                    .await
                {
                    log::error!("send raft message error, {:?}", e);
                }
            }
        }
    }

    #[inline]
    async fn _send_proposal(
        &self,
        proposal: Vec<u8>,
        priority: Priority,
        reply_tx: oneshot::Sender<anyhow::Result<transferpb::Message>>,
    ) {
        let res = {
            let (tx, rx) = oneshot::channel();
            if let Err(e) = self
                .snd
                .clone()
                .send((priority, Message::Propose { proposal, chan: tx }))
                .await
            {
                Err(anyhow::Error::new(e))
            } else {
                let resp = match timeout(self.timeout, rx).await {
                    Ok(Ok(raft_resp)) => raft_resp,
                    Ok(Err(e)) => {
                        warn!("canceled waiting for reply, {:?}", e);
                        RaftResponse::Error("Canceled".into())
                    }
                    Err(e) => {
                        warn!("timeout waiting for reply, {:?}", e);
                        RaftResponse::Error("Proposal timeout".into())
                    }
                };
                resp.encode().map(|data| transferpb::Message {
                    ver: 1,
                    priority: 0,
                    data,
                })
            }
        };
        if let Err(e) = reply_tx.send(res) {
            log::error!("send reply message error, {:?}", e);
        }
    }

    #[inline]
    async fn _send_query(
        &self,
        query: Vec<u8>,
        priority: Priority,
        reply_tx: oneshot::Sender<anyhow::Result<transferpb::Message>>,
    ) {
        let res = {
            let (tx, rx) = oneshot::channel();
            if let Err(e) = self
                .snd
                .clone()
                .send((priority, Message::Query { query, chan: tx }))
                .await
            {
                Err(anyhow::Error::new(e))
            } else {
                let resp = match timeout(self.timeout, rx).await {
                    Ok(Ok(raft_resp)) => raft_resp,
                    Ok(Err(e)) => {
                        warn!("canceled waiting for send query reply, {:?}", e);
                        RaftResponse::Error("Canceled".into())
                    }
                    Err(e) => {
                        warn!("timeout waiting for send query reply, {:?}", e);
                        RaftResponse::Error("Query timeout".into())
                    }
                };
                resp.encode().map(|data| transferpb::Message {
                    ver: 1,
                    priority: 0,
                    data,
                })
            }
        };
        if let Err(e) = reply_tx.send(res) {
            log::error!("send reply message error, {:?}", e);
        }
    }
}
