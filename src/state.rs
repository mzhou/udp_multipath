use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::net::{SocketAddr, UdpSocket};
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

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

struct IndexedQueue<T>
where
    T: Clone + Eq + Hash,
{
    deque: VecDeque<T>,
    set: HashSet<T>,
}

pub struct LocalInfo {
    pub name: String,
    pub sock: Arc<UdpSocket>,
}

struct MappedQueue<K, V>
where
    K: Clone + Eq + Hash,
{
    deque: VecDeque<K>,
    map: HashMap<K, V>,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct Packet {
    content_hash: u64,
}

pub struct WarmConfig {
    pub expiry: Duration,
    pub match_num_packets: usize,
    pub match_window: Duration,
}

struct WarmGroup {
    last_recv_time: Option<Instant>, // None means sticky
    name: String,
    remotes: Vec<WarmRemote>,
}

type WarmGroupRef = Rc<RefCell<WarmGroup>>;

struct WarmPendingRemote {
    addr: SocketAddr,
    first_recv_time: Instant,
    seen_packets: Vec<Packet>,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct WarmRemote {
    addr: SocketAddr,
    last_recv_time: Option<Instant>, // None means sticky
}

type WarmPendingRemoteRef = Rc<RefCell<WarmPendingRemote>>;

// designed for access from only a single thread
pub struct WarmState {
    addr_map: HashMap<SocketAddr, WarmGroupRef>,
    hot_state_ltr: HotStateLtrRef,
    hot_state_rtl: HotStateRtlRef,
    packet_map: MappedQueue<Packet, WarmGroupRef>,
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

    pub fn lookup<'a>(self: &'a Self, addr: &SocketAddr) -> Option<&'a HotGroupLtrRef> {
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
            eprintln!(
                "HotStateRtl::alias linked {} to existing {}",
                addr.to_string(),
                existing_addr.to_string()
            );
            self.addr_map.insert(*addr, group_arc);
        } else {
            eprintln!(
                "HotStateRtl::alias couldn't link {} to existing {}",
                addr.to_string(),
                existing_addr.to_string()
            );
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

impl<K, V> MappedQueue<K, V>
where
    K: Clone + Eq + Hash,
{
    /// @return true if newly added
    pub fn insert(self: &mut Self, limit: usize, k: K, v: V) -> bool {
        if self.deque.len() >= limit {
            let popped = self.deque.pop_front().unwrap();
            self.map.remove(&popped);
        }
        if self.map.contains_key(&k) {
            return false;
        }
        self.map.insert(k.clone(), v);
        self.deque.push_back(k);
        true
    }

    pub fn get(self: &Self, k: &K) -> Option<&V> {
        self.map.get(k)
    }

    pub fn new() -> Self {
        Self {
            deque: VecDeque::new(),
            map: HashMap::new(),
        }
    }
}

impl WarmState {
    pub fn add_static_group(self: &mut Self, local_info: LocalInfo, remote_addrs: &[SocketAddr]) {
        let group = Rc::new(RefCell::new(WarmGroup {
            last_recv_time: None,
            name: local_info.name,
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
                    .add(addr, local_info.sock.clone());
                is_first = false;
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
        }
    }

    pub fn dump_groups(&self, now: &Instant) -> String {
        let mut seen_local_names = HashSet::<String>::new();
        self.addr_map
            .iter()
            .map(|a| a.1.borrow())
            .filter_map(|entry| {
                let name = &entry.name;
                if !seen_local_names.insert(name.clone()) {
                    None
                } else {
                    let remotes_str = entry
                        .remotes
                        .iter()
                        .map(|r| {
                            format!(
                                "{}={}",
                                r.addr.to_string(),
                                match r.last_recv_time {
                                    None => -1,
                                    Some(last_recv_time) =>
                                        (*now - last_recv_time).as_millis() as i32,
                                }
                            )
                        })
                        .collect::<Vec<String>>()
                        .join(",");
                    Some(format!("{} {}", name, remotes_str))
                }
            })
            .collect::<Vec<String>>()
            .join("\n")
    }

    pub fn get_hot_state_ltr(&self) -> HotStateLtrRef {
        self.hot_state_ltr.clone()
    }

    pub fn get_hot_state_rtl(&self) -> HotStateRtlRef {
        self.hot_state_rtl.clone()
    }

    pub fn handle_known_packet(
        self: &mut Self,
        config: &WarmConfig,
        now: &Instant,
        addr: &SocketAddr,
        content_hash: u64,
    ) {
        let mut added_addrs = Vec::<SocketAddr>::new();
        let mut expired_addrs = Vec::<SocketAddr>::new();
        if let Some(group_rc) = self.addr_map.get(addr).map(|g| g.clone()) {
            let mut group = group_rc.borrow_mut();
            let name = group.name.clone();
            // update group summary
            if group.last_recv_time != None {
                //eprintln!(
                //    "handle_known_packet setting {} last_recv_time to {:?}",
                //    name, now
                //);
                group.last_recv_time = Some(*now);
            }
            // update and expire remotes
            assert!(!group.remotes.is_empty());
            // true means remove, false means keep
            group.remotes.drain_filter(|r| {
                match r.last_recv_time {
                    None => {
                        // do nothing, not even update static remotes
                        false
                    }
                    Some(old_last_recv_time) => {
                        // update if this is the one in question
                        if r.addr == *addr {
                            r.last_recv_time = Some(*now);
                            //eprintln!(
                            //    "handle_known_packet setting {} {} last_recv_time to {:?}",
                            //    name,
                            //    addr.to_string(),
                            //    now
                            //);
                            false
                        } else if *now > old_last_recv_time + config.expiry {
                            // otherwise expire
                            eprintln!(
                                "handle_known_packet expiring {} from {}",
                                r.addr.to_string(),
                                name
                            );
                            // note to update hot state
                            expired_addrs.push(r.addr);
                            true
                        } else {
                            // otherwise keep
                            false
                        }
                    }
                }
            });
            assert!(!group.remotes.is_empty());
            // update packet map
            self.packet_map.insert(
                config.match_num_packets,
                Packet { content_hash },
                group_rc.clone(),
            );
            // check if there's any pending we can drag into the group based on content hash
            if let Some(potential_remotes) = self.pending_packet_map.get(&Packet { content_hash }) {
                for remote in potential_remotes.iter().map(|r| r.borrow()) {
                    group.remotes.push(WarmRemote {
                        addr: remote.addr,
                        last_recv_time: Some(*now),
                    });
                    self.addr_map.insert(remote.addr, group_rc.clone());
                    added_addrs.push(remote.addr);
                }
            }
            // update remotes in hot state
            let first_addr = group.remotes[0].addr;
            if !expired_addrs.is_empty() || !added_addrs.is_empty() {
                {
                    let mut hot_state_ltr = self.hot_state_ltr.write().unwrap();
                    for addr in expired_addrs.iter() {
                        hot_state_ltr.del(addr);
                    }
                    for addr in added_addrs.iter() {
                        hot_state_ltr.alias(addr, &first_addr);
                    }
                }
                {
                    let mut hot_state_rtl = self.hot_state_rtl.write().unwrap();
                    for addr in expired_addrs.iter() {
                        hot_state_rtl.del(addr);
                    }
                    for addr in added_addrs.iter() {
                        hot_state_rtl.alias(addr, &first_addr);
                    }
                }
            }
        } else {
            eprintln!(
                "handle_known_packet couldn't find group for {}",
                addr.to_string()
            );
        }
        // clean up pending entries for anyone we added
        for a in added_addrs.iter() {
            self.cleanup_pending_remote(a);
        }
        // clean up references to deleted remotes
        for a in expired_addrs.iter() {
            self.addr_map.remove(&a);
        }
    }

    pub fn handle_new_remote<F>(
        self: &mut Self,
        config: &WarmConfig,
        local_info_f: F,
        now: &Instant,
        addr: &SocketAddr,
        content_hash: u64,
    ) where
        F: FnOnce() -> LocalInfo,
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
                        let Self {
                            ref mut pending_addr_map,
                            ref pending_packet_map,
                            ..
                        } = self;
                        let pending_entry = pending_addr_map.entry(*addr);
                        match pending_entry {
                            Entry::Occupied(o) => {
                                // check if enough time passed without a match
                                let first_recv_time = o.get().borrow().first_recv_time;
                                if *now - first_recv_time > config.match_window {
                                    // gather all remotes with relevant packet hashes
                                    let addrs = Self::get_pending_addrs_by_packets(
                                        pending_packet_map,
                                        &o.get().borrow().seen_packets[..],
                                    );
                                    let first_addr = addrs[0];
                                    eprintln!("handle_new_remote creating group first_addr {} addrs.len {}", first_addr.to_string(), addrs.len());
                                    // create new group
                                    let local_info = local_info_f();
                                    let group_rc = Rc::new(RefCell::new(WarmGroup {
                                        last_recv_time: Some(*now),
                                        name: local_info.name,
                                        remotes: addrs
                                            .iter()
                                            .map(|a| WarmRemote {
                                                addr: *a,
                                                last_recv_time: Some(*now),
                                            })
                                            .collect(),
                                    }));
                                    addrs.iter().for_each(|a| {
                                        self.addr_map.insert(*a, group_rc.clone());
                                    });
                                    {
                                        let mut hot_state_ltr = self.hot_state_ltr.write().unwrap();
                                        hot_state_ltr.add(&first_addr);
                                        addrs[1..]
                                            .iter()
                                            .for_each(|a| hot_state_ltr.alias(a, &first_addr));
                                    }
                                    {
                                        let mut hot_state_rtl = self.hot_state_rtl.write().unwrap();
                                        hot_state_rtl.add(&first_addr, local_info.sock);
                                        addrs[1..]
                                            .iter()
                                            .for_each(|a| hot_state_rtl.alias(a, &first_addr));
                                    }
                                    addrs.iter().for_each(|a| self.cleanup_pending_remote(a));
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
                                eprintln!("handle_new_remote made pending entry {} {} packet remotes len {}", *addr, content_hash, self.pending_packet_map.get(&Packet{ content_hash}).unwrap().len());
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn new() -> Self {
        Self {
            addr_map: HashMap::new(),
            hot_state_ltr: Arc::new(RwLock::new(HotStateLtr::new())),
            hot_state_rtl: Arc::new(RwLock::new(HotStateRtl::new())),
            packet_map: MappedQueue::new(),
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

    fn get_pending_addrs_by_packets(
        pending_packet_map: &HashMap<Packet, Vec<WarmPendingRemoteRef>>,
        packets: &[Packet],
    ) -> Vec<SocketAddr> {
        packets
            .iter()
            .filter_map(|p| Some(pending_packet_map.get(p)?.iter().map(|r| r.borrow().addr)))
            .flatten()
            .collect::<HashSet<SocketAddr>>()
            .into_iter()
            .collect()
    }
}
