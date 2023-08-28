use std::net::SocketAddr;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bincode::serialize;
use futures::channel::{mpsc, oneshot};
use futures::SinkExt;
use log::{info, warn};
use once_cell::sync::Lazy;
use tokio::time::timeout;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::message::{Message, RaftResponse};
use crate::raft_service::raft_service_server::{RaftService, RaftServiceServer};
use crate::raft_service::{
    self, ConfChange as RiteraftConfChange, Empty, Message as RiteraftMessage,
};
use crate::{error, Config};

pub struct RaftServer {
    snd: mpsc::Sender<Message>,
    laddr: SocketAddr,
    timeout: Duration,
    cfg: Arc<Config>,
}

impl RaftServer {
    pub fn new(snd: mpsc::Sender<Message>, laddr: SocketAddr, cfg: Arc<Config>) -> Self {
        RaftServer {
            snd,
            laddr,
            timeout: cfg.grpc_timeout,
            cfg,
        }
    }

    pub async fn run(self) -> error::Result<()> {
        let laddr = self.laddr;
        let _cfg = self.cfg.clone();
        info!("listening gRPC requests on: {}", laddr);
        let svc = RaftServiceServer::new(self);
        let server = Server::builder().add_service(svc);

        #[cfg(any(feature = "reuseport", feature = "reuseaddr"))]
        #[cfg(all(feature = "socket2", feature = "tokio-stream"))]
        {
            log::info!(
                "reuseaddr: {}, reuseport: {}",
                _cfg.reuseaddr,
                _cfg.reuseport
            );
            let listener = raft_service::bind(laddr, 1024, _cfg.reuseaddr, _cfg.reuseport)?;
            server.serve_with_incoming(listener).await?;
        }
        #[cfg(not(any(feature = "reuseport", feature = "reuseaddr")))]
        server.serve(laddr).await?;

        info!("server has quit");
        Ok(())
    }
}

#[tonic::async_trait]
impl RaftService for RaftServer {
    async fn request_id(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<raft_service::IdRequestReponse>, Status> {
        let mut sender = self.snd.clone();
        let (tx, rx) = oneshot::channel();
        let _ = sender.send(Message::RequestId { chan: tx }).await;
        //let response = rx.await;
        let reply = timeout(self.timeout, rx)
            .await
            .map_err(|_e| Status::unavailable("recv timeout for reply"))?
            .map_err(|_e| Status::unavailable("recv canceled for reply"))?;
        match reply {
            RaftResponse::WrongLeader {
                leader_id,
                leader_addr,
            } => {
                warn!("sending wrong leader");
                Ok(Response::new(raft_service::IdRequestReponse {
                    code: raft_service::ResultCode::WrongLeader as i32,
                    data: serialize(&(leader_id, leader_addr)).unwrap(),
                }))
            }
            RaftResponse::RequestId { leader_id } => {
                Ok(Response::new(raft_service::IdRequestReponse {
                    code: raft_service::ResultCode::Ok as i32,
                    data: serialize(&leader_id).unwrap(),
                }))
            }
            _ => unreachable!(),
        }
    }

    async fn change_config(
        &self,
        req: Request<RiteraftConfChange>,
    ) -> Result<Response<raft_service::RaftResponse>, Status> {
        let change = protobuf::Message::parse_from_bytes(req.into_inner().inner.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let mut sender = self.snd.clone();

        let (tx, rx) = oneshot::channel();

        let message = Message::ConfigChange { change, chan: tx };

        match sender.send(message).await {
            Ok(_) => (),
            Err(_) => warn!("send error"),
        }

        let mut reply = raft_service::RaftResponse::default();

        match timeout(self.timeout, rx).await {
            Ok(Ok(raft_response)) => {
                reply.inner = serialize(&raft_response).expect("serialize error");
            }
            Ok(_) => (),
            Err(e) => {
                reply.inner =
                    serialize(&RaftResponse::Error("timeout".into())).expect("serialize error");
                warn!("timeout waiting for reply, {:?}", e);
            }
        }

        Ok(Response::new(reply))
    }

    async fn send_message(
        &self,
        request: Request<RiteraftMessage>,
    ) -> Result<Response<raft_service::RaftResponse>, Status> {
        let message = protobuf::Message::parse_from_bytes(request.into_inner().inner.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        SEND_MESSAGE_ACTIVE_REQUESTS.fetch_add(1, Ordering::SeqCst);
        let reply = match self.snd.clone().try_send(Message::Raft(Box::new(message))) {
            Ok(()) => {
                let response = RaftResponse::Ok;
                Ok(Response::new(raft_service::RaftResponse {
                    inner: serialize(&response).unwrap(),
                }))
            }
            Err(_) => Err(Status::unavailable("error for try send message")),
        };
        SEND_MESSAGE_ACTIVE_REQUESTS.fetch_sub(1, Ordering::SeqCst);
        reply
    }

    async fn send_proposal(
        &self,
        req: Request<raft_service::Proposal>,
    ) -> Result<Response<raft_service::RaftResponse>, Status> {
        SEND_PROPOSAL_ACTIVE_REQUESTS.fetch_add(1, Ordering::SeqCst);
        let proposal = req.into_inner().inner;
        let mut sender = self.snd.clone();
        let (tx, rx) = oneshot::channel();
        let message = Message::Propose { proposal, chan: tx };

        let reply = match sender.try_send(message) {
            Ok(()) => match timeout(self.timeout, rx).await {
                Ok(Ok(raft_response)) => match serialize(&raft_response) {
                    Ok(resp) => Ok(Response::new(raft_service::RaftResponse { inner: resp })),
                    Err(e) => {
                        warn!("serialize error, {}", e);
                        Err(Status::unavailable("serialize error"))
                    }
                },
                Ok(Err(e)) => {
                    warn!("recv error for reply, {}", e);
                    Err(Status::unavailable("recv error for reply"))
                }
                Err(e) => {
                    warn!("timeout waiting for reply, {}", e);
                    Err(Status::unavailable("timeout waiting for reply"))
                }
            },
            Err(e) => {
                warn!("error for try send message, {}", e);
                Err(Status::unavailable("error for try send message"))
            }
        };

        SEND_PROPOSAL_ACTIVE_REQUESTS.fetch_sub(1, Ordering::SeqCst);
        reply
    }

    async fn send_query(
        &self,
        req: Request<raft_service::Query>,
    ) -> Result<Response<raft_service::RaftResponse>, Status> {
        let query = req.into_inner().inner;
        let mut sender = self.snd.clone();
        let (tx, rx) = oneshot::channel();
        let message = Message::Query { query, chan: tx };
        let mut reply = raft_service::RaftResponse::default();
        match sender.try_send(message) {
            Ok(()) => {
                // if we don't receive a response after 2secs, we timeout
                match timeout(self.timeout, rx).await {
                    Ok(Ok(raft_response)) => {
                        reply.inner = serialize(&raft_response).expect("serialize error");
                    }
                    Ok(Err(e)) => {
                        reply.inner = serialize(&RaftResponse::Error(e.to_string()))
                            .expect("serialize error");
                        warn!("send query error, {}", e);
                    }
                    Err(_e) => {
                        reply.inner = serialize(&RaftResponse::Error("timeout".into()))
                            .expect("serialize error");
                        warn!("timeout waiting for send query reply");
                    }
                }
            }
            Err(e) => {
                reply.inner =
                    serialize(&RaftResponse::Error(e.to_string())).expect("serialize error");
                warn!("send query error, {}", e)
            }
        }

        Ok(Response::new(reply))
    }
}

static SEND_PROPOSAL_ACTIVE_REQUESTS: Lazy<Arc<AtomicIsize>> =
    Lazy::new(|| Arc::new(AtomicIsize::new(0)));

static SEND_MESSAGE_ACTIVE_REQUESTS: Lazy<Arc<AtomicIsize>> =
    Lazy::new(|| Arc::new(AtomicIsize::new(0)));

pub fn send_proposal_active_requests() -> isize {
    SEND_PROPOSAL_ACTIVE_REQUESTS.load(Ordering::SeqCst)
}

pub fn send_message_active_requests() -> isize {
    SEND_MESSAGE_ACTIVE_REQUESTS.load(Ordering::SeqCst)
}
