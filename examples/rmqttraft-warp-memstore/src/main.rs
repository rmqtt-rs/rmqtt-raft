#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use async_trait::async_trait;
use bincode::{deserialize, serialize};
use rmqtt_raft::{Config, Mailbox, Raft, Result as RaftResult, Store};
use serde::{Deserialize, Serialize};
use slog::{info, Level};
use slog::{Drain, Record};
use slog_term::{CountingWriter, RecordDecorator, ThreadSafeTimestampFn};
use std::collections::HashMap;
use std::convert::Infallible;
use std::io;
use std::io::Write;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use structopt::StructOpt;
use warp::{reply, Filter};

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(long)]
    id: u64,
    #[structopt(long)]
    raft_laddr: String,
    #[structopt(name = "peer-addr", long)]
    peer_addrs: Vec<String>,
    #[structopt(long)]
    web_server: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub enum Message {
    Insert { key: String, value: String },
    Get { key: String },
}

#[derive(Clone)]
struct HashStore(Arc<RwLock<HashMap<String, String>>>);

impl HashStore {
    fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }
    fn get(&self, key: &str) -> Option<String> {
        self.0.read().unwrap().get(key).cloned()
    }
}

#[async_trait]
impl Store for HashStore {
    async fn apply(&mut self, message: &[u8]) -> RaftResult<Vec<u8>> {
        let message: Message = deserialize(message).unwrap();
        let message: Vec<u8> = match message {
            Message::Insert { key, value } => {
                let mut db = self.0.write().unwrap();
                let v = serialize(&value).unwrap();
                db.insert(key, value);
                v
            }
            _ => Vec::new(),
        };
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        Ok(message)
    }

    async fn query(&self, query: &[u8]) -> RaftResult<Vec<u8>> {
        let query: Message = deserialize(query).unwrap();
        let data: Vec<u8> = match query {
            Message::Get { key } => {
                if let Some(val) = self.get(&key) {
                    serialize(&val).unwrap()
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        };
        Ok(data)
    }

    async fn snapshot(&self) -> RaftResult<Vec<u8>> {
        Ok(serialize(&self.0.read().unwrap().clone())?)
    }

    async fn restore(&mut self, snapshot: &[u8]) -> RaftResult<()> {
        let new: HashMap<String, String> = deserialize(snapshot).unwrap();
        let mut db = self.0.write().unwrap();
        let _ = std::mem::replace(&mut *db, new);
        Ok(())
    }
}

fn with_mailbox(
    mailbox: Arc<Mailbox>,
) -> impl Filter<Extract = (Arc<Mailbox>,), Error = Infallible> + Clone {
    warp::any().map(move || mailbox.clone())
}

fn with_store(store: HashStore) -> impl Filter<Extract = (HashStore,), Error = Infallible> + Clone {
    warp::any().map(move || store.clone())
}

async fn put(
    mailbox: Arc<Mailbox>,
    key: String,
    value: String,
) -> Result<impl warp::Reply, Infallible> {
    let message = Message::Insert { key, value };
    let message = serialize(&message).unwrap();
    let result = mailbox.send(message).await;
    match result {
        Ok(r) => {
            let result: String = deserialize(&r).unwrap();
            Ok(reply::json(&result))
        }
        Err(e) => Ok(reply::json(&format!("put error, {:?}", e))),
    }
}

async fn get(store: HashStore, key: String) -> Result<impl warp::Reply, Infallible> {
    let response = store.get(&key);
    Ok(reply::json(&response))
}

async fn leave(mailbox: Arc<Mailbox>) -> Result<impl warp::Reply, Infallible> {
    mailbox.leave().await.unwrap();
    Ok(reply::json(&"OK".to_string()))
}

async fn status(mailbox: Arc<Mailbox>) -> Result<impl warp::Reply, Infallible> {
    let response = mailbox
        .status()
        .await
        .map(|res| reply::json(&res))
        .unwrap_or_else(|e| reply::json(&e.to_string()));
    Ok(response)
}

//target\release\rmqttraft-warp-memstore.exe --id 1 --raft-laddr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8081"
//target\release\rmqttraft-warp-memstore.exe --id 2 --raft-laddr "127.0.0.1:5002" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8082"
//target\release\rmqttraft-warp-memstore.exe --id 3 --raft-laddr "127.0.0.1:5003" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --web-server "0.0.0.0:8083"

//target\release\rmqttraft-warp-memstore.exe --id 4 --raft-laddr "127.0.0.1:5004" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --web-server "0.0.0.0:8084"
//target\release\rmqttraft-warp-memstore.exe --id 5 --raft-laddr "127.0.0.1:5005" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --web-server "0.0.0.0:8085"

//./target/release/rmqttraft-warp-memstore --id 1 --raft-laddr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8081"
//./target/release/rmqttraft-warp-memstore --id 2 --raft-laddr "127.0.0.1:5002" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8082"
//./target/release/rmqttraft-warp-memstore --id 3 --raft-laddr "127.0.0.1:5003" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --web-server "0.0.0.0:8083"

//target\debug\rmqttraft-warp-memstore.exe --id 1 --raft-laddr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8081"
//target\debug\rmqttraft-warp-memstore.exe --id 2 --raft-laddr "127.0.0.1:5002" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8082"
//target\debug\rmqttraft-warp-memstore.exe --id 3 --raft-laddr "127.0.0.1:5003" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --web-server "0.0.0.0:8083"

//./target/debug/rmqttraft-warp-memstore --id 1 --raft-laddr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8081" > out_1.log 2>&1 &
//./target/debug/rmqttraft-warp-memstore --id 2 --raft-laddr "127.0.0.1:5002" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8082" > out_2.log 2>&1 &
//./target/debug/rmqttraft-warp-memstore --id 3 --raft-laddr "127.0.0.1:5003" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --web-server "0.0.0.0:8083" > out_3.log 2>&1 &

// wrk -c 100 -t4 -d60s -H "Connection: keep-alive" "http://127.0.0.1:8081/put/key1/val-1"
// wrk -c 100 -t4 -d60s -H "Connection: keep-alive" "http://127.0.0.1:8082/put/key1/val-2"
// wrk -c 100 -t6 -d60s -H "Connection: keep-alive" "http://127.0.0.1:8083/get/key1"

// ab -n 500000 -c 20 "http://127.0.0.1:8081/put/key1/val-1"
// ab -n 500000 -c 50 "http://127.0.0.1:8082/put/key1/val-2"
// ab -n 500000 -c 20 "http://127.0.0.1:8083/get/key1"

// ab -n 500000 -c 500 "http://127.0.0.1:8081/put/key1/val-1"
// ab -n 500000 -c 500 "http://127.0.0.1:8082/put/key2/val-2"
// ab -n 500000 -c 500 "http://127.0.0.1:8083/put/key3/val-3"

//http://127.0.0.1:8083/status

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let logger = conf_logger();
    let _guard = slog_scope::set_global_logger(logger.clone());

    // converts log to slog
    //let _log_guard = slog_stdlog::init().unwrap();
    slog_stdlog::init_with_level(log::Level::Info).unwrap();

    let options = Options::from_args();
    let store = HashStore::new();
    info!(logger, "peer_addrs: {:?}", options.peer_addrs);
    let cfg = Config {
        reuseaddr: true,
        reuseport: true,
        grpc_timeout: Duration::from_secs(6),
        grpc_concurrency_limit: 200,
        proposal_batch_size: 50,
        ..Default::default()
    };
    let raft = Raft::new(options.raft_laddr, store.clone(), logger.clone(), cfg)?;
    let leader_info = raft.find_leader_info(options.peer_addrs).await?;
    info!(logger, "leader_info: {:?}", leader_info);

    let mailbox = Arc::new(raft.mailbox());
    let (raft_handle, mailbox) = match leader_info {
        Some((leader_id, leader_addr)) => {
            info!(logger, "running in follower mode");
            let handle = tokio::spawn(raft.join(options.id, Some(leader_id), leader_addr));
            (handle, mailbox)
        }
        None => {
            info!(logger, "running in leader mode");
            let handle = tokio::spawn(raft.lead(options.id));
            (handle, mailbox)
        }
    };

    let put_kv = warp::get()
        .and(warp::path!("put" / String / String))
        .and(with_mailbox(mailbox.clone()))
        .and_then(|key, value, mailbox: Arc<Mailbox>| put(mailbox, key, value));

    let get_kv = warp::get()
        .and(warp::path!("get" / String))
        .and(with_store(store.clone()))
        .and_then(|key, store: HashStore| get(store, key));

    let leave_kv = warp::get()
        .and(warp::path!("leave"))
        .and(with_mailbox(mailbox.clone()))
        .and_then(leave);

    let status = warp::get()
        .and(warp::path!("status"))
        .and(with_mailbox(mailbox.clone()))
        .and_then(status);

    let routes = put_kv.or(get_kv).or(leave_kv).or(status);

    if let Some(addr) = options.web_server {
        let _server = tokio::spawn(async move {
            warp::serve(routes)
                .run(SocketAddr::from_str(&addr).unwrap())
                .await;
        });
    }

    tokio::try_join!(raft_handle)?.0?;
    Ok(())
}

fn conf_logger() -> slog::Logger {
    let custom_timestamp = |io: &mut dyn io::Write| {
        write!(
            io,
            "{}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f")
        )
    };

    let print_msg_header = |fn_timestamp: &dyn ThreadSafeTimestampFn<Output = io::Result<()>>,
                            mut rd: &mut dyn RecordDecorator,
                            record: &Record,
                            _use_file_location: bool|
     -> io::Result<bool> {
        rd.start_timestamp()?;
        fn_timestamp(&mut rd)?;

        rd.start_whitespace()?;
        write!(rd, " ")?;

        rd.start_level()?;
        write!(rd, "{}", record.level().as_short_str())?;

        rd.start_location()?;
        if record.function().is_empty() {
            write!(rd, " {}.{} | ", record.module(), record.line())?;
        } else {
            write!(
                rd,
                " {}::{}.{} | ",
                record.module(),
                record.function(),
                record.line()
            )?;
        }

        rd.start_msg()?;
        let mut count_rd = CountingWriter::new(&mut rd);
        write!(count_rd, "{}", record.msg())?;
        Ok(count_rd.count() != 0)
    };

    //let decorator = slog_term::TermDecorator::new().build();
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let drain = slog_term::FullFormat::new(plain)
        .use_custom_timestamp(custom_timestamp)
        .use_custom_header_print(print_msg_header)
        .build()
        .fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let drain = drain.filter_level(Level::Info).fuse();
    //let logger = slog::Logger::root(drain, slog_o!("version" => env!("CARGO_PKG_VERSION")));
    slog::Logger::root(drain, o!())

    //    let decorator = slog_term::TermDecorator::new().build();
    //    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    //    let drain = slog_async::Async::new(drain).build().fuse();
    //    let drain = drain.filter_level(Level::Info).fuse();
    //    let logger = slog::Logger::root(drain, slog_o!("version" => env!("CARGO_PKG_VERSION")));
}
