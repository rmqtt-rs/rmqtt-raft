use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::channel::oneshot;
use serde::{Deserialize, Serialize};
use tikv_raft::eraftpb::{self, ConfChange};

use rust_box::collections::PriorityQueue;

pub(crate) type Sender<T> = rust_box::mpsc::Sender<T, rust_box::mpsc::SendError<T>>;
pub(crate) type Receiver<T> = rust_box::mpsc::Receiver<T>;
pub(crate) type PriorityQueueType<P, T> = Arc<parking_lot::RwLock<PriorityQueue<P, T>>>;

pub(crate) use rust_box::handy_grpc::client::Message as GrpcMessage;
pub(crate) use rust_box::handy_grpc::server::Message as ServerGrpcMessage;

#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug)]
pub enum RaftMessage {
    Propose {
        proposal: Vec<u8>,
        //chan: Sender<RaftResponse>,
    },
    Query {
        query: Vec<u8>,
        //chan: Sender<RaftResponse>,
    },
    ConfigChange {
        change: Vec<u8>, //ConfChange,
                         //chan: Sender<RaftResponse>,
    },
    IsLeader, //chan: Sender<RaftResponse>,
    ReportUnreachable {
        node_id: u64,
    },
    Raft {
        msg: Vec<u8>,
        //msg: eraftpb::Message,
    },
    Status,
}

impl RaftMessage {
    #[inline]
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(bincode::serialize(self).map_err(anyhow::Error::new)?)
    }
    #[inline]
    pub fn decode(data: &[u8]) -> anyhow::Result<RaftMessage> {
        Ok(bincode::deserialize(data).map_err(anyhow::Error::new)?)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RaftResponse {
    WrongLeader {
        leader_id: u64,
        leader_addr: Option<String>,
    },
    JoinSuccess {
        assigned_id: u64,
        peer_addrs: HashMap<u64, String>,
    },
    IsLeader {
        leader_id: u64,
    },
    Error(String),
    Response {
        data: Vec<u8>,
    },
    Status(Status),
    Ok,
}

impl RaftResponse {
    #[inline]
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(bincode::serialize(self).map_err(anyhow::Error::new)?)
    }
    #[inline]
    pub fn decode(data: &[u8]) -> anyhow::Result<RaftResponse> {
        Ok(bincode::deserialize(data).map_err(anyhow::Error::new)?)
    }
}

#[allow(dead_code)]
pub enum Message {
    Propose {
        proposal: Vec<u8>,
        chan: oneshot::Sender<RaftResponse>,
    },
    Query {
        query: Vec<u8>,
        chan: oneshot::Sender<RaftResponse>,
    },
    ConfigChange {
        change: ConfChange,
        chan: oneshot::Sender<RaftResponse>,
    },
    IsLeader {
        chan: oneshot::Sender<RaftResponse>,
    },
    ReportUnreachable {
        node_id: u64,
    },
    Raft(Box<eraftpb::Message>),
    Status {
        chan: oneshot::Sender<RaftResponse>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Status {
    pub id: u64,
    pub leader_id: u64,
    pub uncommitteds: usize,
    pub request_votes: usize,
    pub node_channel_queues: usize,
    pub server_channel_queues: usize,
    pub active_mailbox_sends: isize,
    pub active_mailbox_querys: isize,
    pub peers: HashMap<u64, String>,
}

impl Status {
    #[inline]
    pub fn is_started(&self) -> bool {
        self.leader_id > 0
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.leader_id == self.id
    }
}

pub(crate) enum ReplyChan {
    One((oneshot::Sender<RaftResponse>, Instant)),
    More(Vec<(oneshot::Sender<RaftResponse>, Instant)>),
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Proposals {
    One(Vec<u8>),
    More(Vec<Vec<u8>>),
}

impl Proposals {
    pub fn len(&self) -> usize {
        match self {
            Proposals::One(_) => 1,
            Proposals::More(more) => more.len(),
        }
    }
}

pub(crate) struct Merger {
    proposals: Vec<Vec<u8>>,
    chans: Vec<(oneshot::Sender<RaftResponse>, Instant)>,
    start_collection_time: i64,
    proposal_batch_size: usize,
    proposal_batch_timeout: i64,
}

impl Merger {
    pub fn new(proposal_batch_size: usize, proposal_batch_timeout: Duration) -> Self {
        log::info!(
            "proposal_batch_size: {:?}, proposal_batch_timeout: {:?}",
            proposal_batch_size,
            proposal_batch_timeout
        );
        Self {
            proposals: Vec::new(),
            chans: Vec::new(),
            start_collection_time: 0,
            proposal_batch_size,
            proposal_batch_timeout: proposal_batch_timeout.as_millis() as i64,
        }
    }

    #[inline]
    pub fn add(&mut self, proposal: Vec<u8>, chan: oneshot::Sender<RaftResponse>) {
        self.proposals.push(proposal);
        self.chans.push((chan, Instant::now()));
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.proposals.len()
    }

    #[inline]
    pub fn take(&mut self) -> Option<(Proposals, ReplyChan)> {
        let max = self.proposal_batch_size;
        let len = self.len();
        let len = if len > max { max } else { len };
        if len > 0 && (len == max || self.timeout()) {
            let data = if len == 1 {
                match (self.proposals.pop(), self.chans.pop()) {
                    (Some(proposal), Some(chan)) => {
                        Some((Proposals::One(proposal), ReplyChan::One(chan)))
                    }
                    _ => unreachable!(),
                }
            } else {
                let mut proposals = self.proposals.drain(0..len).collect::<Vec<_>>();
                let mut chans = self.chans.drain(0..len).collect::<Vec<_>>();
                proposals.reverse();
                chans.reverse();
                Some((Proposals::More(proposals), ReplyChan::More(chans)))
            };
            self.start_collection_time = chrono::Local::now().timestamp_millis();
            data
        } else {
            None
        }
    }

    #[inline]
    fn timeout(&self) -> bool {
        chrono::Local::now().timestamp_millis()
            > (self.start_collection_time + self.proposal_batch_timeout)
    }
}

#[tokio::test]
async fn test_merger() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut merger = Merger::new(50, Duration::from_millis(200));
    use futures::channel::oneshot::channel;
    use std::time::Duration;

    let add = |merger: &mut Merger| {
        let (tx, rx) = channel();
        merger.add(vec![1, 2, 3], tx);
        rx
    };

    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;
    const MAX: i64 = 111;
    let count = Arc::new(AtomicI64::new(0));
    let mut futs = Vec::new();
    for _ in 0..MAX {
        let rx = add(&mut merger);
        let count1 = count.clone();
        let fut = async move {
            let r = tokio::time::timeout(Duration::from_secs(3), rx).await;
            match r {
                Ok(_) => {}
                Err(_) => {
                    println!("timeout ...");
                }
            }
            count1.fetch_add(1, Ordering::SeqCst);
        };

        futs.push(fut);
    }

    let sends = async {
        loop {
            if let Some((_data, chan)) = merger.take() {
                match chan {
                    ReplyChan::One((tx, _)) => {
                        let _ = tx.send(RaftResponse::Ok);
                    }
                    ReplyChan::More(txs) => {
                        for (tx, _) in txs {
                            let _ = tx.send(RaftResponse::Ok);
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            if merger.len() == 0 {
                break;
            }
        }
    };

    let count_p = count.clone();
    let count_print = async move {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            println!("count_p: {}", count_p.load(Ordering::SeqCst));
            if count_p.load(Ordering::SeqCst) >= MAX {
                break;
            }
        }
    };
    println!("futs: {}", futs.len());
    futures::future::join3(futures::future::join_all(futs), sends, count_print).await;

    Ok(())
}
