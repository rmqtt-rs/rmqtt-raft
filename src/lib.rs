use std::time::Duration;

pub use crate::error::{Error, Result};
pub use crate::message::Status;
pub use crate::raft::{Mailbox, Raft, Store};
pub use tikv_raft::ReadOnlyOption;

mod error;
mod message;
mod raft;
mod raft_node;
mod raft_server;
mod storage;

#[derive(Clone)]
pub struct Config {
    #[cfg(feature = "reuseaddr")]
    pub reuseaddr: bool,
    #[cfg(all(
        feature = "reuseport",
        not(any(target_os = "solaris", target_os = "illumos"))
    ))]
    pub reuseport: bool,
    pub grpc_timeout: Duration,
    pub grpc_concurrency_limit: usize,
    //GRPC failed to fuse threshold
    pub grpc_breaker_threshold: u64,
    pub grpc_breaker_retry_interval: Duration,
    //Proposal batchs
    pub proposal_batch_size: usize,
    pub proposal_batch_timeout: Duration,
    //Snapshot generation interval
    pub snapshot_interval: Duration,
    pub heartbeat: Duration,
    pub raft_cfg: tikv_raft::Config,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            #[cfg(feature = "reuseaddr")]
            reuseaddr: false,
            #[cfg(all(
                feature = "reuseport",
                not(any(target_os = "solaris", target_os = "illumos"))
            ))]
            reuseport: false,
            grpc_timeout: Duration::from_secs(6),
            grpc_concurrency_limit: 200,
            grpc_breaker_threshold: 4,
            grpc_breaker_retry_interval: Duration::from_millis(2500),
            proposal_batch_size: 50,
            proposal_batch_timeout: Duration::from_millis(200),
            snapshot_interval: Duration::from_secs(600),
            heartbeat: Duration::from_millis(100),
            raft_cfg: tikv_raft::Config {
                election_tick: 10,
                heartbeat_tick: 5,
                check_quorum: true,
                pre_vote: true,
                ..Default::default()
            },
        }
    }
}
