#![feature(drain_filter)]

mod state;

use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::Hasher;
use std::io;
use std::net::{AddrParseError, Ipv6Addr, SocketAddr, UdpSocket};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use clap::{App, Arg};
use socket2;

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
    LocalAddr(io::Error),
    LocalBind(io::Error),
    RemoteBind(io::Error),
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
                LocalAddr(e) => format!("bind local addr: {}", e),
                LocalBind(e) => format!("bind local socket: {}", e),
                RemoteBind(e) => format!("bind remote socket: {}", e),
            }
        )
    }
}

fn hash_bytes(buf: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(buf);
    hasher.finish()
}

struct LocalRecvLoopConfig {
    local_connect: Option<SocketAddr>,
    local_sock: Arc<UdpSocket>,
    remote_sock: Arc<UdpSocket>,
}

fn local_recv_loop(
    c: LocalRecvLoopConfig,
    group: Arc<Mutex<state::HotGroupLtr>>,
) -> io::Result<()> {
    let mut local_connected = match c.local_connect {
        Some(addr) => {
            c.local_sock.connect(addr)?;
            eprintln!("local_recv_loop done initial connect");
            Some(addr)
        }
        _ => None,
    };
    let local_bind = c.local_sock.local_addr()?;
    eprintln!("local_recv_loop done local_addr");

    loop {
        let mut buf = [0u8; 16 * 1024];
        //eprintln!("local_recv_loop before recv_from");
        let (len, addr) = c.local_sock.recv_from(&mut buf)?;
        //eprintln!("local_recv_loop done recv_from {} {}", len, addr);
        let data = &buf[..len];

        // reconnect local socket if necessary
        if Some(addr) != local_connected {
            //eprintln!("local_recv_loop reconnect local socket {}", addr);
            c.local_sock.connect(addr)?;
            //eprintln!("local_recv_loop reconnected local socket");
            local_connected = Some(addr);
        }

        // forward the packet
        for remote_addr in group.lock().unwrap().remote_addrs() {
            if let Err(e) = c.remote_sock.send_to(data, remote_addr) {
                println!("RemoteSendToErr,{},{},{}", local_bind, remote_addr, e);
                //eprintln!("local_recv_loop send_to {} {}", remote_addr, e);
            }
        }
    }
}

struct RemoteRecvLoopConfig {
    remote_sock: Arc<UdpSocket>,
    warm_queue: SyncSender<WarmReq>,
}

fn remote_recv_loop(
    c: RemoteRecvLoopConfig,
    state: Arc<RwLock<state::HotStateRtl>>,
) -> io::Result<()> {
    loop {
        let mut buf = [0u8; 16 * 1024];
        //eprintln!("remote_recv_loop before recv_from");
        let (len, addr) = c.remote_sock.recv_from(&mut buf)?;
        //eprintln!("remote_recv_loop done recv_from {} {}", len, addr);
        let now = Instant::now();
        let data = &buf[..len];
        let data_hash = hash_bytes(data);

        if let Some(group) = state.read().unwrap().lookup(&addr) {
            //eprintln!("remote_recv_loop existing group");
            let mut send_result_opt: Option<io::Result<usize>> = None;
            {
                let mut group_lock = group.lock().unwrap();
                let is_fresh = group_lock.check_and_add_packet(512, data_hash);
                if is_fresh {
                    send_result_opt = Some(group_lock.get_local_sock().send(data));
                }
            }
            if let Some(r) = send_result_opt {
                // was fresh
                let warm_queue_result = c.warm_queue.try_send(
                    WarmReqFreshPacket {
                        content_hash: data_hash,
                        now,
                        remote_addr: addr,
                    }
                    .into(),
                );
                if let Err(e) = r {
                    eprintln!("remote_recv_loop send fail {}", e);
                }
                if let Err(e) = warm_queue_result {
                    eprintln!("remote_recv_loop warm queue fail {}", e);
                }
            }
        } else {
            eprintln!("remote_recv_loop no group");
            let warm_queue_result = c.warm_queue.try_send(
                WarmReqUnknownSource {
                    content_hash: data_hash,
                    now,
                    remote_addr: addr,
                }
                .into(),
            );
            if let Err(e) = warm_queue_result {
                eprintln!("remote_recv_loop warm queue fail {}", e);
            }
        }
    }
}

struct WarmLoopConfig {
    queue: Receiver<WarmReq>,
    state: state::WarmState,
}

fn warm_loop(mut config: WarmLoopConfig) -> Result<(), Box<dyn std::error::Error>> {
    let sa_any = SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 0);
    let warm_config = state::WarmConfig {
        expiry: Duration::from_secs(60),
        match_window: Duration::from_secs(10),
    };
    let mut last_dump_time = Instant::now();
    loop {
        let req_enum_opt = config.queue.recv_timeout(Duration::from_secs(5));
        let now = Instant::now();
        match req_enum_opt {
            Ok(req_enum) => {
                use WarmReq::*;
                match req_enum {
                    FreshPacket(req) => {
                        config.state.handle_known_packet(
                            &warm_config,
                            &req.now,
                            &req.remote_addr,
                            req.content_hash,
                        );
                    }
                    UnknownSource(req) => {
                        let local_info_f = || {
                            let sock = Arc::new(ds_bind(sa_any).unwrap());
                            let name = sock.local_addr().unwrap().to_string();
                            state::LocalInfo { name, sock }
                        };
                        config.state.handle_new_remote(
                            &warm_config,
                            local_info_f,
                            &req.now,
                            &req.remote_addr,
                            req.content_hash,
                        );
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                return Err(RecvTimeoutError::Disconnected.into());
            }
        }
        if now - last_dump_time > Duration::from_secs(5) {
            println!(
                "=begin warm state\n{}\n=end warm state",
                config.state.dump_groupus()
            );
            last_dump_time = now;
        }
    }
}

enum WarmReq {
    FreshPacket(WarmReqFreshPacket),
    UnknownSource(WarmReqUnknownSource),
}

struct WarmReqFreshPacket {
    now: Instant,
    content_hash: u64,
    remote_addr: SocketAddr,
}

struct WarmReqUnknownSource {
    now: Instant,
    content_hash: u64,
    remote_addr: SocketAddr,
}

impl From<WarmReqFreshPacket> for WarmReq {
    fn from(a: WarmReqFreshPacket) -> Self {
        Self::FreshPacket(a)
    }
}

impl From<WarmReqUnknownSource> for WarmReq {
    fn from(a: WarmReqUnknownSource) -> Self {
        Self::UnknownSource(a)
    }
}

fn ds_bind(addr: SocketAddr) -> io::Result<UdpSocket> {
    let s2 = socket2::Socket::new(socket2::Domain::ipv6(), socket2::Type::dgram(), None)?;
    s2.set_only_v6(false)?;
    s2.bind(&socket2::SockAddr::from(addr))?;
    let s_std: std::net::UdpSocket = s2.into_udp_socket();
    Ok(s_std)
}

fn main() -> Result<(), MainError> {
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

    let mut state = state::WarmState::new();

    let remote_sock = Arc::new(ds_bind(remote_bind).map_err(MainError::RemoteBind)?);
    let mut local_recv_loop_thread: Option<thread::JoinHandle<_>> = None;

    if !remote_connect.is_empty() {
        let sock = Arc::new(ds_bind(local_bind).map_err(MainError::LocalBind)?);
        let name = sock.local_addr().map_err(MainError::LocalAddr)?.to_string();
        let config = LocalRecvLoopConfig {
            local_connect,
            local_sock: sock.clone(),
            remote_sock: remote_sock.clone(),
        };
        state.add_static_group(state::LocalInfo { name, sock }, &remote_connect);
        let group = state
            .get_hot_state_ltr()
            .read()
            .unwrap()
            .lookup(&remote_connect[0])
            .unwrap()
            .clone();
        local_recv_loop_thread = Some(thread::spawn(|| local_recv_loop(config, group)));
    }

    let (warm_queue_sender, warm_queue_receiver) = sync_channel(1024);

    let remote_recv_loop_thread = {
        let config = RemoteRecvLoopConfig {
            remote_sock,
            warm_queue: warm_queue_sender,
        };
        let hot_state_rtl = state.get_hot_state_rtl().clone();
        thread::spawn(|| remote_recv_loop(config, hot_state_rtl))
    };

    // run background task on main thread
    {
        let config = WarmLoopConfig {
            queue: warm_queue_receiver,
            state,
        };
        let warm_loop_result = warm_loop(config);
        eprintln!("warm_loop_result {:?}", warm_loop_result);
    }

    let remote_recv_loop_result = remote_recv_loop_thread.join();
    eprintln!("remote_recv_loop_result {:?}", remote_recv_loop_result);
    if let Some(local_recv_loop_thread) = local_recv_loop_thread {
        let local_recv_loop_result = local_recv_loop_thread.join();
        eprintln!("local_recv_loop_result {:?}", local_recv_loop_result);
    }

    eprintln!("all threads joined");

    /*
    let local_sock = Arc::new(
        UdpSocket::bind(local_bind)
            .await
            .map_err(MainError::LocalBind)?,
    );

    let remote_recv_loop_config = RemoteRecvLoopConfig {
        remote_sock: remote_sock.clone(),
    };

    let state = Arc::new(RwLock::new(State::new()));

    let local_recv_loop_task = tokio::spawn(local_recv_loop(local_recv_loop_config, state.clone()));
    let remote_recv_loop_task =
        tokio::spawn(remote_recv_loop(remote_recv_loop_config, state.clone()));

    //eprintln!("main await");
    remote_recv_loop_task
        .await
        .map_err(MainError::TaskJoin)?
        .map_err(MainError::RemoteRecvLoop)?;
    local_recv_loop_task
        .await
        .map_err(MainError::TaskJoin)?
        .map_err(MainError::LocalRecvLoop)?;
        */

    Ok(())
}
