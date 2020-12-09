mod state;

use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::Hasher;
use std::io;
use std::net::{AddrParseError, IpAddr, Ipv6Addr, SocketAddr, UdpSocket};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
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
    LocalBind(io::Error),
    LocalRecvLoop(io::Error),
    RemoteBind(io::Error),
    RemoteRecvLoop(io::Error),
    TaskJoin(Box<dyn std::any::Any + Send + 'static>),
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
                LocalRecvLoop(e) => format!("local recv loop: {}", e),
                RemoteBind(e) => format!("bind remote socket: {}", e),
                RemoteRecvLoop(e) => format!("remote recv loop: {}", e),
                TaskJoin(e) => format!("task join: {:?}", e),
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
) -> Result<(), io::Error> {
    let mut local_connected = match c.local_connect {
        Some(addr) => {
            c.local_sock.connect(addr);
            Some(addr)
        }
        _ => None,
    };
    let local_bind = c.local_sock.local_addr()?;

    loop {
        let mut buf = [0u8; 16 * 1024];
        let (len, addr) = c.local_sock.recv_from(&mut buf)?;
        let now = Instant::now();
        //eprintln!("local_recv_loop got datagram {} {}", len, addr);
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

async fn remote_recv_loop(
    c: RemoteRecvLoopConfig,
    state: Arc<RwLock<state::HotStateRtl>>,
) -> Result<(), io::Error> {
    loop {
        let mut buf = [0u8; 16 * 1024];
        let (len, addr) = c.remote_sock.recv_from(&mut buf)?;
        let now = Instant::now();
        //eprintln!("remote_recv_loop got datagram {} {}", len, addr);
        let data = &buf[..len];
        let data_hash = hash_bytes(data);

        if let Some(group) = state.read().unwrap().lookup(&addr) {
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

/*
struct GroupRecvLoopConfig {
    remote_idle_expiry: Duration,
    remote_sock: Arc<UdpSocket>,
}

async fn group_recv_loop(
    c: GroupRecvLoopConfig,
    state: Arc<RwLock<State>>,
    group: Arc<RwLock<Group>>,
) -> Result<(), io::Error> {
    let local_bind = group.read().unwrap().local_bind;
    let local_sock = group.read().unwrap().local_sock.clone();

    //eprintln!("group_recv_loop spawned for {}", local_bind);

    loop {
        let mut buf = [0u8; 16 * 1024];
        let (len, addr) = local_sock.recv_from(&mut buf).await?;
        let now = Instant::now();
        //eprintln!("group_recv_loop got datagram {} {}", len, addr);
        let data = &buf[..len];

        let mut need_cleanup = false;

        let mut cleanup_remote_addrs = Vec::<SocketAddr>::new();
        let remotes = group.read().unwrap().remotes.clone();
        //eprintln!("group_recv_loop forwarding to {} remotes", remotes.len());
        for remote in remotes.iter() {
            if now - remote.last_recv_time >= c.remote_idle_expiry {
                cleanup_remote_addrs.push(remote.addr);
                continue;
            }
            if let Err(e) = c.remote_sock.send_to(data, remote.addr).await {
                println!("RemoteSendToErr,{},{},{}", local_bind, remote.addr, e);
                cleanup_remote_addrs.push(remote.addr);
                continue;
            }
        }

        if !cleanup_remote_addrs.is_empty() {
            let mut group_write = group.write().unwrap();
            let mut old_remotes = Vec::<Remote>::new();
            std::mem::swap(&mut old_remotes, &mut group_write.remotes);
            group_write.remotes = old_remotes
                .iter()
                .filter(|r| {
                    now - r.last_recv_time < c.remote_idle_expiry
                        && !cleanup_remote_addrs.contains(&r.addr)
                })
                .map(|r| *r)
                .collect();
            if group_write.remotes.is_empty() {
                // remove references from state
                let mut state_write = state.write().unwrap();
                for remote in old_remotes.iter() {
                    state_write.addr_map.remove(&remote.addr);
                }
                for packet in group_write.recent_recv.iter() {
                    state_write.packet_map.remove(&packet);
                }
                println!(
                    "RemoveGroup,{},{},{}",
                    local_bind,
                    old_remotes.len(),
                    group_write.recent_recv.len()
                );
                break;
            }
        }
    }

    Ok(())
}
*/

fn ds_bind(addr: SocketAddr) -> io::Result<UdpSocket> {
    let s2 = socket2::Socket::new(socket2::Domain::ipv6(), socket2::Type::dgram(), None)?;
    s2.set_only_v6(false)?;
    s2.set_nonblocking(true)?;
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
        let config = LocalRecvLoopConfig {
            local_connect,
            local_sock: sock.clone(),
            remote_sock: remote_sock.clone(),
        };
        state.add_static_group(sock.clone(), &remote_connect);
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
        thread::spawn(|| remote_recv_loop(config, hot_state_rtl));
    };

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
