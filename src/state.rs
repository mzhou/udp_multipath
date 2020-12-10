use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::net::{SocketAddr, UdpSocket};
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

struct IndexedQueue<T>
where
    T: Clone + Eq + Hash,
{
    deque: VecDeque<T>,
    set: HashSet<T>,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct HotRemote {
    addr: SocketAddr,
}

pub struct HotGroupLtr {
    remotes: Vec<HotRemote>,
}

type HotGroupLtrRef = Arc<Mutex<HotGroupLtr>>;

pub struct HotGroupRtl {
    local_sock: Arc<UdpSocket>,
    recent_packets: IndexedQueue<Packet>,
}

type HotGroupRtlRef = Arc<Mutex<HotGroupRtl>>;

// for tasks forwarding from local to remote
pub struct HotStateLtr {
    addr_map: HashMap<SocketAddr, HotGroupLtrRef>,
}

type HotStateLtrRef = Arc<RwLock<HotStateLtr>>;

// for tasks forwarding from remote to local
pub struct HotStateRtl {
    addr_map: HashMap<SocketAddr, HotGroupRtlRef>,
}

type HotStateRtlRef = Arc<RwLock<HotStateRtl>>;

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct Packet {
    content_hash: u64,
}

pub struct WarmConfig {
    pub match_window: Duration,
}

struct WarmGroup {
    remotes: Vec<WarmRemote>,
}

type WarmGroupRef = Rc<RefCell<WarmGroup>>;

struct WarmRemote {
    addr: SocketAddr,
    last_recv_time: Option<Instant>, // None means sticky
}

struct WarmPendingRemote {
    addr: SocketAddr,
    first_recv_time: Instant,
    seen_packets: Vec<Packet>,
}

type WarmPendingRemoteRef = Rc<RefCell<WarmPendingRemote>>;

// designed for access from only a single thread
pub struct WarmState {
    addr_map: HashMap<SocketAddr, WarmGroupRef>,
    hot_state_ltr: HotStateLtrRef,
    hot_state_rtl: HotStateRtlRef,
    packet_map: HashMap<Packet, WarmGroupRef>,
    pending_addr_map: HashMap<SocketAddr, WarmPendingRemoteRef>,
    pending_packet_map: HashMap<Packet, Vec<WarmPendingRemoteRef>>,
}

impl HotGroupLtr {
    pub fn remote_addrs(self: &Self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.remotes.iter().map(|r| r.addr)
    }
}

impl HotGroupRtl {
    /// @return true if newly added
    pub fn check_and_add_packet(self: &mut Self, limit: usize, content_hash: u64) -> bool {
        self.recent_packets.insert(limit, Packet { content_hash })
    }

    pub fn get_local_sock(self: &Self) -> &UdpSocket {
        self.local_sock.as_ref()
    }
}

impl HotStateLtr {
    pub fn new() -> Self {
        Self {
            addr_map: HashMap::new(),
        }
    }

    pub fn add<'a>(self: &'a mut Self, addr: &SocketAddr) {
        self.addr_map
            .entry(*addr)
            .or_insert(Arc::new(Mutex::new(HotGroupLtr {
                remotes: vec![HotRemote { addr: *addr }],
            })));
    }

    pub fn alias(self: &mut Self, addr: &SocketAddr, existing_addr: &SocketAddr) {
        let get_result = self.addr_map.get(existing_addr).map(|v| v.clone());
        if let Some(group_arc) = get_result {
            group_arc
                .lock()
                .unwrap()
                .remotes
                .push(HotRemote { addr: *addr });
            self.addr_map.insert(*addr, group_arc);
        }
    }

    pub fn del(self: &mut Self, addr: &SocketAddr) {
        if let Some(v) = self.addr_map.remove(addr) {
            v.lock().unwrap().remotes.retain(|r| r.addr != *addr);
        }
    }

    pub fn lookup(self: &Self, addr: &SocketAddr) -> Option<&HotGroupLtrRef> {
        self.addr_map.get(addr)
    }
}

impl HotStateRtl {
    pub fn new() -> Self {
        Self {
            addr_map: HashMap::new(),
        }
    }

    pub fn add<'a>(self: &'a mut Self, addr: &SocketAddr, local_sock: Arc<UdpSocket>) {
        self.addr_map
            .entry(*addr)
            .or_insert(Arc::new(Mutex::new(HotGroupRtl {
                local_sock,
                recent_packets: IndexedQueue::new(),
            })));
    }

    pub fn alias(self: &mut Self, addr: &SocketAddr, existing_addr: &SocketAddr) {
        let get_result = self.addr_map.get(existing_addr).map(|v| v.clone());
        if let Some(group_arc) = get_result {
            self.addr_map.insert(*addr, group_arc);
        }
    }

    pub fn del(self: &mut Self, addr: &SocketAddr) {
        self.addr_map.remove(addr);
    }

    pub fn lookup<'a>(self: &'a Self, addr: &SocketAddr) -> Option<&'a Mutex<HotGroupRtl>> {
        self.addr_map.get(addr).map(|r| r.as_ref())
    }
}

impl<T> IndexedQueue<T>
where
    T: Clone + Eq + Hash,
{
    /// @return true if newly added
    pub fn insert(self: &mut Self, limit: usize, data: T) -> bool {
        if self.deque.len() >= limit {
            let popped = self.deque.pop_front().unwrap();
            self.set.remove(&popped);
        }
        let inserted = self.set.insert(data.clone());
        if inserted {
            self.deque.push_back(data);
        }
        inserted
    }

    pub fn new() -> Self {
        Self {
            deque: VecDeque::new(),
            set: HashSet::new(),
        }
    }
}

impl WarmState {
    pub fn add_static_group(
        self: &mut Self,
        local_sock: Arc<UdpSocket>,
        remote_addrs: &[SocketAddr],
    ) {
        let group = Rc::new(RefCell::new(WarmGroup {
            remotes: remote_addrs
                .iter()
                .map(|addr| WarmRemote {
                    addr: *addr,
                    last_recv_time: None,
                })
                .collect(),
        }));
        let mut is_first = true;
        for addr in remote_addrs.iter() {
            self.addr_map.insert(*addr, group.clone());
            if is_first {
                self.hot_state_ltr.write().unwrap().add(addr);
                self.hot_state_rtl
                    .write()
                    .unwrap()
                    .add(addr, local_sock.clone());
            } else {
                self.hot_state_ltr
                    .write()
                    .unwrap()
                    .alias(addr, &remote_addrs[0]);
                self.hot_state_rtl
                    .write()
                    .unwrap()
                    .alias(addr, &remote_addrs[0]);
            }
            is_first = false;
        }
    }

    pub fn get_hot_state_ltr(&self) -> HotStateLtrRef {
        self.hot_state_ltr.clone()
    }

    pub fn get_hot_state_rtl(&self) -> HotStateRtlRef {
        self.hot_state_rtl.clone()
    }

    pub fn handle_new_remote<F>(
        self: &mut Self,
        config: &WarmConfig,
        local_sock_f: F,
        now: &Instant,
        addr: &SocketAddr,
        content_hash: u64,
    ) where
        F: FnOnce() -> Arc<UdpSocket>,
    {
        let existing_group = self.addr_map.get(addr);
        match existing_group {
            Some(_) => {
                // do nothing if already added, other than ensure pending is clean
                self.cleanup_pending_remote(addr);
            }
            None => {
                // otherwise look for matching packet
                let matching_group = self.packet_map.get(&Packet { content_hash });
                match matching_group {
                    Some(g) => {
                        {
                            let mut g_mut = g.borrow_mut();
                            // add to existing group
                            g_mut.remotes.push(WarmRemote {
                                addr: *addr,
                                last_recv_time: Some(*now),
                            });
                            self.addr_map.insert(*addr, g.clone());
                            self.hot_state_ltr
                                .write()
                                .unwrap()
                                .alias(addr, &g_mut.remotes[0].addr); // we will never have an empty group
                        }
                        self.cleanup_pending_remote(addr);
                    }
                    None => {
                        // otherwise look for pending remote
                        let pending_entry = self.pending_addr_map.entry(*addr);
                        match pending_entry {
                            Entry::Occupied(o) => {
                                // check if enough time passed without a match
                                let first_recv_time = o.get().borrow().first_recv_time;
                                if *now - first_recv_time > config.match_window {
                                    // create new group
                                    self.addr_map.insert(
                                        *addr,
                                        Rc::new(RefCell::new(WarmGroup {
                                            remotes: vec![WarmRemote {
                                                addr: *addr,
                                                last_recv_time: Some(*now),
                                            }],
                                        })),
                                    );
                                    self.hot_state_ltr.write().unwrap().add(addr);
                                    self.hot_state_rtl
                                        .write()
                                        .unwrap()
                                        .add(addr, local_sock_f());
                                    self.cleanup_pending_remote(addr);
                                } else {
                                    // register pending packet
                                    o.get()
                                        .borrow_mut()
                                        .seen_packets
                                        .push(Packet { content_hash });
                                }
                            }
                            Entry::Vacant(v) => {
                                // create pending entry
                                let pending_value = Rc::new(RefCell::new(WarmPendingRemote {
                                    addr: *addr,
                                    first_recv_time: *now,
                                    seen_packets: vec![Packet { content_hash }],
                                }));
                                v.insert(pending_value.clone());
                                self.pending_packet_map
                                    .entry(Packet { content_hash })
                                    .or_default()
                                    .push(pending_value.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn handle_known_packet(
        self: &mut Self,
        now: &Instant,
        addr: &SocketAddr,
        content_hash: u64,
    ) {
    }

    pub fn new() -> Self {
        Self {
            addr_map: HashMap::new(),
            hot_state_ltr: Arc::new(RwLock::new(HotStateLtr::new())),
            hot_state_rtl: Arc::new(RwLock::new(HotStateRtl::new())),
            packet_map: HashMap::new(),
            pending_addr_map: HashMap::new(),
            pending_packet_map: HashMap::new(),
        }
    }

    fn cleanup_pending_remote(self: &mut Self, addr: &SocketAddr) {
        if let Some(v) = self.pending_addr_map.remove(addr) {
            for packet in v.borrow().seen_packets.iter() {
                self.pending_packet_map.remove(packet);
            }
        }
    }
}
