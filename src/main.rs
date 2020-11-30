use std::collections::hash_map::DefaultHasher;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::hash::Hasher;
use std::io;
use std::net::{AddrParseError, SocketAddr};
use std::sync::Arc;
use std::time::Instant;

use clap::{App, Arg};
use tokio::net::UdpSocket;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

const LOCAL_BIND: &str = "LOCAL_BIND";
const LOCAL_CONNECT: &str = "LOCAL_CONNECT";
const REMOTE_BIND: &str = "REMOTE_BIND";
const REMOTE_CONNECT: &str = "REMOTE_CONNECT";

#[derive(Debug)]
enum MainError {
    ArgLocalBind(AddrParseError),
    ArgLocalConnect(AddrParseError),
    ArgRemoteBind(AddrParseError),
    ArgRemoteConnect(AddrParseError),
    LocalBind(io::Error),
    LocalConnect(io::Error),
    LocalRecvLoop(io::Error),
    RemoteBind(io::Error),
    RemoteConnect(io::Error),
    TaskJoin(tokio::task::JoinError),
}

struct Packet {
    src_hash: u64,
    content_hash: u64,
    time: Instant,
}

struct Group {
    local_sock: Arc<UdpSocket>,
    recent_recv: VecDeque<Packet>,
    remote_addrs: Vec<SocketAddr>,
}

struct State {
    initial_group: Option<Group>,
    groups: Vec<Group>,
}

impl fmt::Display for MainError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use MainError::*;
        write!(
            f,
            "{}",
            match self {
                ArgLocalBind(e) => format!("local-bind invalid: {}", e),
                ArgLocalConnect(e) => format!("local-connect invalid: {}", e),
                ArgRemoteBind(e) => format!("remote-bind invalid: {}", e),
                ArgRemoteConnect(e) => format!("remote-connect invalid: {}", e),
                LocalBind(e) => format!("bind local socket: {}", e),
                LocalConnect(e) => format!("connect local socket: {}", e),
                LocalRecvLoop(e) => format!("local recv loop: {}", e),
                RemoteBind(e) => format!("bind remote socket: {}", e),
                RemoteConnect(e) => format!("connect remote socket: {}", e),
                TaskJoin(e) => format!("task join: {}", e),
            }
        )
    }
}

impl State {
    fn local_inital_set<'a>(
        self: &mut Self,
        local_sock: Arc<UdpSocket>,
        remote_addrs: Vec<SocketAddr>,
    ) {
        eprintln!("local_initial_set");
        self.initial_group = Some(Group {
            local_sock,
            recent_recv: VecDeque::<Packet>::new(),
            remote_addrs,
        });
    }

    fn local_initial_try(self: &Self) -> Option<Vec<SocketAddr>> {
        eprintln!("local_initial_try");
        Some(self.initial_group.as_ref()?.remote_addrs.clone())
    }

    fn new() -> Self {
        State {
            initial_group: None,
            groups: Vec::new(),
        }
    }
}

fn hash_bytes(buf: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(buf);
    hasher.finish()
}

struct LocalRecvLoopConfig {
    initial_remote_addrs: Vec<SocketAddr>,
    local_connect: Option<SocketAddr>,
    local_sock: Arc<UdpSocket>,
    remote_sock: Arc<UdpSocket>,
}

async fn local_recv_loop(
    c: LocalRecvLoopConfig,
    state: RwLock<State>,
) -> Result<(), tokio::io::Error> {
    let mut local_connected = match c.local_connect {
        Some(addr) => {
            c.local_sock.connect(addr).await?;
            Some(addr)
        }
        _ => None,
    };

    loop {
        let mut buf = [0u8; 16 * 1024];
        let (len, addr) = c.local_sock.recv_from(&mut buf).await?;
        eprintln!("local_recv_loop got datagram {} {}", len, addr);
        let data = &buf[..len];
        let data_hash = hash_bytes(data);

        // reconnect local socket if necessary
        if Some(addr) != local_connected {
            eprintln!("local_recv_loop reconnect local socket");
            c.local_sock.connect(addr).await?;
            eprintln!("local_recv_loop reconnected local socket");
            local_connected = Some(addr);
        }

        // create group for initial remote if necessary
        eprintln!("local_recv_loop acquire state read");
        let remote_addrs_tried = state.read().await.local_initial_try();
        eprintln!("local_recv_loop released state read");
        let remote_addrs = match remote_addrs_tried {
            Some(r) => r,
            _ => {
                eprintln!("local_recv_loop acquire state write");
                state
                    .write()
                    .await
                    .local_inital_set(c.local_sock.clone(), c.initial_remote_addrs.clone());
                eprintln!("local_recv_loop released state write");
                c.initial_remote_addrs.clone()
            }
        };
        eprintln!("local_recv_loop remote_addrs len {}", remote_addrs.len());

        // forward the packet
        for remote_addr in remote_addrs.iter() {
            if let Err(e) = c.remote_sock.send_to(data, remote_addr).await {
                eprintln!("local_recv_loop send_to {} {}", remote_addr, e);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let matches = App::new("udp_multipath")
        .arg(
            Arg::with_name(LOCAL_BIND)
                .default_value("[::]:0")
                .long("local-bind")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name(LOCAL_CONNECT)
                .takes_value(true)
                .long("local-connect")
                .takes_value(true),
        )
        .arg(
            Arg::with_name(REMOTE_BIND)
                .default_value("[::]:0")
                .long("remote-bind")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name(REMOTE_CONNECT)
                .takes_value(true)
                .help("comma separate multiple")
                .long("remote-connect")
                .required(false)
                .takes_value(true),
        )
        .get_matches();

    let get_sa_arg = |k, e: fn(AddrParseError) -> MainError| {
        matches
            .value_of(k)
            .map(|v| v.parse::<SocketAddr>().map_err(e))
            .transpose()
    };

    let get_sas_arg =
        |k, e: fn(AddrParseError) -> MainError| -> Result<Vec<SocketAddr>, MainError> {
            matches
                .value_of(k)
                .map_or(Vec::<Result<SocketAddr, MainError>>::new(), |s| {
                    s.split(",")
                        .map(|v| v.parse::<SocketAddr>().map_err(e))
                        .collect()
                })
                .into_iter()
                .collect()
        };

    let local_bind = get_sa_arg(LOCAL_BIND, MainError::ArgLocalBind)?.unwrap();
    let local_connect = get_sa_arg(LOCAL_CONNECT, MainError::ArgLocalConnect)?;
    let remote_bind = get_sa_arg(REMOTE_BIND, MainError::ArgRemoteBind)?.unwrap();
    let remote_connect = get_sas_arg(REMOTE_CONNECT, MainError::ArgRemoteConnect)?;

    let local_sock = Arc::new(
        UdpSocket::bind(local_bind)
            .await
            .map_err(MainError::LocalBind)?,
    );
    let remote_sock = Arc::new(
        UdpSocket::bind(remote_bind)
            .await
            .map_err(MainError::RemoteBind)?,
    );

    let local_recv_loop_config = LocalRecvLoopConfig {
        initial_remote_addrs: remote_connect,
        local_connect,
        local_sock: local_sock.clone(),
        remote_sock: remote_sock.clone(),
    };

    let state = RwLock::new(State::new());

    let local_recv_loop_task = tokio::spawn(local_recv_loop(local_recv_loop_config, state));

    eprintln!("main await");
    local_recv_loop_task
        .await
        .map_err(MainError::TaskJoin)?
        .map_err(MainError::LocalRecvLoop)?;

    Ok(())
}
