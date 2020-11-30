use std::collections::hash_map::DefaultHasher;
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hasher;
use std::io;
use std::net::{AddrParseError, IpAddr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use clap::{App, Arg};
use tokio::net::UdpSocket;

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
    TaskJoin(tokio::task::JoinError),
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct Packet {
    content_hash: u64,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct Remote {
    addr: SocketAddr,
    last_recv_time: Instant,
}

struct Group {
    leader: Option<SocketAddr>,
    local_bind: SocketAddr,
    local_sock: Arc<UdpSocket>,
    recent_recv: VecDeque<Packet>,
    recent_recv_set: HashSet<Packet>,
    remotes: Vec<Remote>,
}

enum RemoteTryResult {
    Found {
        is_dup: bool,
        is_new_remote: bool,
        local_bind: SocketAddr,
        local_sock: Arc<UdpSocket>,
        old_leader: Option<SocketAddr>, // only set if this is new leader
    },
    NotFound,
}

struct State {
    addr_map: HashMap<SocketAddr, Arc<RwLock<Group>>>,
    initial_group: Option<Arc<RwLock<Group>>>,
    local_connect: Option<SocketAddr>,
    packet_map: HashMap<Packet, Arc<RwLock<Group>>>,
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
                TaskJoin(e) => format!("task join: {}", e),
            }
        )
    }
}

impl State {
    fn local_connect_set(self: &mut Self, addr: SocketAddr) {
        self.local_connect = Some(addr)
    }

    fn local_initial_set(
        self: &mut Self,
        local_bind: SocketAddr,
        local_sock: Arc<UdpSocket>,
        remote_addrs: &[SocketAddr],
        now: Instant,
    ) {
        let group = Arc::new(RwLock::new(Group {
            leader: None,
            local_bind, // for debug only
            local_sock,
            recent_recv: VecDeque::new(),
            recent_recv_set: HashSet::new(),
            remotes: remote_addrs
                .iter()
                .map(|a| Remote {
                    addr: *a,
                    last_recv_time: now,
                })
                .collect(),
        }));
        self.initial_group = Some(group.clone());
        for addr in remote_addrs.iter() {
            self.addr_map.insert(*addr, group.clone());
        }
    }

    fn local_initial_try(self: &Self) -> Option<Vec<SocketAddr>> {
        Some(
            self.initial_group
                .as_ref()?
                .read()
                .unwrap()
                .remotes
                .iter()
                .map(|r| r.addr)
                .collect(),
        )
    }

    fn remote_new_group(
        self: &mut Self,
        addr: SocketAddr,
        content_hash: u64,
        now: Instant,
        local_bind: SocketAddr,
        local_sock: Arc<UdpSocket>,
    ) -> Arc<RwLock<Group>> {
        let mut group = Group {
            leader: Some(addr),
            local_bind, // for debug only
            local_sock: local_sock.clone(),
            recent_recv: VecDeque::new(),
            recent_recv_set: HashSet::new(),
            remotes: Vec::new(),
        };
        let packet = Packet { content_hash };
        group.recent_recv.push_back(packet);
        group.recent_recv_set.insert(packet);
        group.remotes.push(Remote {
            addr,
            last_recv_time: now,
        });
        let group_shared = Arc::new(RwLock::new(group));
        self.addr_map.insert(addr, group_shared.clone());
        self.packet_map.insert(packet, group_shared.clone());
        group_shared
    }

    fn remote_try(self: &Self, addr: &SocketAddr, content_hash: u64) -> RemoteTryResult {
        use RemoteTryResult::*;
        let mut found_by_content = false;
        match self
            .addr_map
            .get(addr)
            .or_else(|| {
                found_by_content = true;
                self.packet_map.get(&Packet { content_hash })
            })
            .as_ref()
        {
            Some(group) => {
                let group_read = group.read().unwrap();
                let is_dup =
                    found_by_content || group_read.recent_recv.contains(&Packet { content_hash });
                Found {
                    is_dup,
                    is_new_remote: found_by_content,
                    local_bind: group_read.local_bind,
                    local_sock: group_read.local_sock.clone(),
                    old_leader: if !found_by_content && !is_dup && group_read.leader != Some(*addr)
                    {
                        group_read.leader
                    } else {
                        None
                    },
                }
            }
            _ => NotFound,
        }
    }

    fn remote_update(
        self: &mut Self,
        recent_recv_limit: usize,
        addr: &SocketAddr,
        content_hash: u64,
        is_dup: bool,
        is_new_remote: bool,
        now: Instant,
    ) {
        let group_opt = if is_new_remote {
            self.packet_map.get(&Packet { content_hash })
        } else {
            self.addr_map.get(addr)
        }
        .map(|g| g.clone());
        match group_opt {
            Some(group) => {
                let mut group_write = group.write().unwrap();
                if !is_dup {
                    group_write.leader = Some(*addr);
                    if group_write.recent_recv.len() >= recent_recv_limit {
                        let deleted = group_write.recent_recv.pop_front();
                        if let Some(deleted) = deleted {
                            group_write.recent_recv_set.remove(&deleted);
                            self.packet_map.remove(&deleted);
                        }
                    }
                    let packet = Packet { content_hash };
                    group_write.recent_recv.push_back(packet);
                    group_write.recent_recv_set.insert(packet);
                    self.packet_map.insert(packet, group.clone());
                }
                if is_new_remote {
                    group_write.remotes.push(Remote {
                        addr: *addr,
                        last_recv_time: now,
                    });
                    self.addr_map.insert(*addr, group.clone());
                }
                for remote in group_write.remotes.iter_mut() {
                    if remote.addr == *addr {
                        remote.last_recv_time = now;
                        break;
                    }
                }
            }
            _ => {}
        }
    }

    fn new() -> Self {
        State {
            addr_map: HashMap::new(),
            initial_group: None,
            local_connect: None,
            packet_map: HashMap::new(),
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
    state: Arc<RwLock<State>>,
) -> Result<(), io::Error> {
    let mut local_connected = match c.local_connect {
        Some(addr) => {
            c.local_sock.connect(addr).await?;
            state.write().unwrap().local_connect_set(addr);
            Some(addr)
        }
        _ => None,
    };
    let local_bind = c.local_sock.local_addr()?;

    loop {
        let mut buf = [0u8; 16 * 1024];
        let (len, addr) = c.local_sock.recv_from(&mut buf).await?;
        let now = Instant::now();
        //eprintln!("local_recv_loop got datagram {} {}", len, addr);
        let data = &buf[..len];

        // reconnect local socket if necessary
        if Some(addr) != local_connected {
            //eprintln!("local_recv_loop reconnect local socket {}", addr);
            c.local_sock.connect(addr).await?;
            //eprintln!("local_recv_loop reconnected local socket");
            state.write().unwrap().local_connect_set(addr);
            local_connected = Some(addr);
        }

        // create group for initial remote if necessary
        //eprintln!("local_recv_loop acquire state read");
        // readguard isn't released if used directly in match
        // https://doc.rust-lang.org/stable/reference/destructors.html#temporary-scopes
        let remote_addrs_tried = state.read().unwrap().local_initial_try();
        //eprintln!("local_recv_loop released state read");
        let remote_addrs = match remote_addrs_tried {
            Some(r) => r,
            _ => {
                //eprintln!("local_recv_loop acquire state write");
                state.write().unwrap().local_initial_set(
                    c.local_sock.local_addr()?,
                    c.local_sock.clone(),
                    &c.initial_remote_addrs,
                    now,
                );
                //eprintln!("local_recv_loop released state write");
                c.initial_remote_addrs.clone()
            }
        };
        //eprintln!("local_recv_loop remote_addrs len {}", remote_addrs.len());

        // forward the packet
        for remote_addr in remote_addrs.iter() {
            if let Err(e) = c.remote_sock.send_to(data, remote_addr).await {
                println!("RemoteSendToErr,{},{},{}", local_bind, remote_addr, e);
                //eprintln!("local_recv_loop send_to {} {}", remote_addr, e);
            }
        }
    }
}

struct RemoteRecvLoopConfig {
    remote_sock: Arc<UdpSocket>,
}

async fn remote_recv_loop(
    c: RemoteRecvLoopConfig,
    state: Arc<RwLock<State>>,
) -> Result<(), io::Error> {
    use RemoteTryResult::*;
    loop {
        let mut buf = [0u8; 16 * 1024];
        let (len, addr) = c.remote_sock.recv_from(&mut buf).await?;
        let now = Instant::now();
        //eprintln!("remote_recv_loop got datagram {} {}", len, addr);
        let data = &buf[..len];
        let data_hash = hash_bytes(data);

        // see if this remote address is already in a group
        let mut remote_info = state.read().unwrap().remote_try(&addr, data_hash);
        let mut updated = false;
        match remote_info {
            NotFound => {
                // need to create group
                let local_connect = state.read().unwrap().local_connect;
                if let Some(local_connect) = local_connect {
                    let new_sock = Arc::new(
                        UdpSocket::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0))
                            .await?,
                    );
                    let local_bind = new_sock.local_addr()?;
                    new_sock.connect(local_connect).await?;
                    let group = state.write().unwrap().remote_new_group(
                        addr,
                        data_hash,
                        now,
                        new_sock.local_addr()?,
                        new_sock,
                    );
                    remote_info = Found {
                        is_dup: false,
                        is_new_remote: false,
                        local_bind: group.read().unwrap().local_bind,
                        local_sock: group.read().unwrap().local_sock.clone(),
                        old_leader: None,
                    };
                    let group_c = GroupRecvLoopConfig {
                        remote_idle_expiry: Duration::from_secs(60),
                        remote_sock: c.remote_sock.clone(),
                    };
                    tokio::spawn(group_recv_loop(group_c, state.clone(), group));
                    updated = true;
                    println!(
                        "RemoteNewGroup,{},{},{},{}",
                        local_bind, local_connect, addr, data_hash
                    );
                }
            }
            _ => {}
        }

        match remote_info {
            Found {
                is_dup,
                is_new_remote,
                local_bind,
                local_sock,
                old_leader,
            } => {
                // only forward if not duplicate
                if !is_dup {
                    //eprintln!("remote_recv_loop about to send to local");
                    local_sock.send(data).await?;
                    //eprintln!("remote_recv_loop sent to local");
                }
                if is_new_remote {
                    println!("NewRemote,{},{},{}", local_bind, addr, data_hash);
                }
                if let Some(old_leader) = old_leader {
                    println!("NewLeader,{},{},{}", local_bind, addr, old_leader);
                }
                // update in background
                if !updated {
                    tokio::spawn({
                        let state = state.clone();
                        async move {
                            //eprintln!("background update");
                            state.write().unwrap().remote_update(
                                64,
                                &addr,
                                data_hash,
                                is_dup,
                                is_new_remote,
                                now,
                            )
                        }
                    });
                }
            }
            _ => {}
        }
    }
}

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

        let mut errored_remote_addrs = Vec::<SocketAddr>::new();
        let remotes = group.read().unwrap().remotes.clone();
        //eprintln!("group_recv_loop forwarding to {} remotes", remotes.len());
        for remote in remotes.iter() {
            if now - remote.last_recv_time >= c.remote_idle_expiry {
                need_cleanup = true;
                continue;
            }
            if let Err(e) = c.remote_sock.send_to(data, remote.addr).await {
                println!("RemoteSendToErr,{},{},{}", local_bind, remote.addr, e);
                need_cleanup = true;
                errored_remote_addrs.push(remote.addr);
            }
        }

        if need_cleanup {
            let mut group_write = group.write().unwrap();
            let mut old_remotes = Vec::<Remote>::new();
            std::mem::swap(&mut old_remotes, &mut group_write.remotes);
            group_write.remotes = old_remotes
                .iter()
                .filter(|r| {
                    now - r.last_recv_time < c.remote_idle_expiry
                        && !errored_remote_addrs.contains(&r.addr)
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

    Ok(())
}
