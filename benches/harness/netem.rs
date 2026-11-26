//! Émulateur de réseau minimal, façon `tc netem` : un proxy UDP qui s'intercale
//! entre N clients et le serveur QUIC et dégrade les paquets qui le traversent.
//!
//!   client 1 ─┐                           ┌─ back 1 ─┐
//!   client 2 ─┼─► [front |  proxy  | tx] ─┼─ back 2 ─┼─► serveur
//!   client N ─┘                           └─ back N ─┘
//!
//! Chaque client dispose de ses propres liens montant et descendant indépendants
//! (sa propre latence, gigue, perte et file de débit), mais un UNIQUE thread
//! d'ordonnancement (`netem-tx`) gère les horloges de tous les clients pour éviter
//! de saturer le CPU avec des dizaines de threads en attente active.
//!
//! `NetemProxy` arrête et rejoint proprement tous ses threads lorsqu'il est droppé.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub struct NetProfile {
    pub name: &'static str,
    /// Latence aller simple (le RTT vaut donc environ 2x cette valeur).
    pub delay: Duration,
    /// Variation aléatoire uniforme dans [-jitter, +jitter].
    pub jitter: Duration,
    /// Probabilité de perte de chaque paquet, entre 0.0 et 1.0.
    pub loss: f64,
    /// Débit maximal du lien en bits/s (None = illimité).
    pub bandwidth_bps: Option<u64>,
}

/// Générateur pseudo-aléatoire xorshift64* à graine fixe : deux exécutions
/// perdent les mêmes paquets, ce qui rend les runs comparables.
struct Rng(u64);

impl Rng {
    fn next_f64(&mut self) -> f64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Décide, pour chaque paquet d'un sens donné, s'il est perdu et sinon QUAND il arrive.
struct Shaper {
    profile: NetProfile,
    rng: Rng,
    link_free_at: Instant,
    last_delivery: Instant,
}

impl Shaper {
    fn new(profile: NetProfile, seed: u64) -> Self {
        let now = Instant::now();
        Shaper { profile, rng: Rng(seed.max(1)), link_free_at: now, last_delivery: now }
    }

    /// None = paquet perdu ; Some(t) = instant de livraison.
    fn schedule(&mut self, len: usize) -> Option<Instant> {
        if self.rng.next_f64() < self.profile.loss {
            return None;
        }
        let now = Instant::now();

        // 1) Débit : le paquet attend que le lien soit libre, puis met len*8/débit secondes à passer.
        let mut sent = now;
        if let Some(bps) = self.profile.bandwidth_bps {
            let tx_time = Duration::from_secs_f64(len as f64 * 8.0 / bps as f64);
            self.link_free_at = self.link_free_at.max(now) + tx_time;
            sent = self.link_free_at;
        }

        // 2) Propagation : latence fixe +/- gigue.
        let jitter = self.profile.jitter.as_secs_f64() * (2.0 * self.rng.next_f64() - 1.0);
        let delay = (self.profile.delay.as_secs_f64() + jitter).max(0.0);
        let at = sent + Duration::from_secs_f64(delay);

        // 3) FIFO : un paquet ne double jamais le précédent (comme sur un lien point à point).
        self.last_delivery = self.last_delivery.max(at);
        Some(self.last_delivery)
    }
}

enum Target {
    Upstream(Arc<UdpSocket>),
    Client(SocketAddr),
}

struct Scheduled {
    at: Instant,
    id: u64,
    target: Target,
    pkt: Vec<u8>,
}

impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.id == other.id
    }
}
impl Eq for Scheduled {}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> Ordering {
        self.at.cmp(&other.at).then_with(|| self.id.cmp(&other.id))
    }
}

type TxQueue = Arc<(Mutex<BinaryHeap<Reverse<Scheduled>>>, Condvar)>;

fn enqueue(queue: &TxQueue, next_id: &AtomicU64, at: Instant, target: Target, pkt: Vec<u8>) {
    let id = next_id.fetch_add(1, AtomicOrdering::Relaxed);
    let (lock, cvar) = &**queue;
    let mut heap = lock.lock().unwrap();
    let wake = heap.peek().map_or(true, |top| at < top.0.at);
    heap.push(Reverse(Scheduled { at, id, target, pkt }));
    if wake {
        cvar.notify_one();
    }
}

/// Proxy multi-clients avec arrêt propre au `Drop`.
pub struct NetemProxy {
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    queue: TxQueue,
    threads: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl NetemProxy {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for NetemProxy {
    fn drop(&mut self) {
        self.shutdown.store(true, AtomicOrdering::Relaxed);
        self.queue.1.notify_all();
        // On extrait les handles hors verrou pour éviter tout blocage pendant qu'un thread en ajoute.
        loop {
            let batch = std::mem::take(&mut *self.threads.lock().unwrap());
            if batch.is_empty() {
                break;
            }
            for h in batch {
                let _ = h.join();
            }
        }
    }
}

/// Démarre un proxy multi-clients vers `upstream` (le serveur).
pub fn spawn_proxy(upstream: SocketAddr, profile: NetProfile) -> std::io::Result<NetemProxy> {
    let front = UdpSocket::bind("127.0.0.1:0")?;
    front.set_read_timeout(Some(Duration::from_millis(50)))?;
    let addr = front.local_addr()?;

    let shutdown = Arc::new(AtomicBool::new(false));
    let queue: TxQueue = Arc::new((Mutex::new(BinaryHeap::new()), Condvar::new()));
    let next_id = Arc::new(AtomicU64::new(0));
    let threads: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));

    // ---- 1) Thread unique d'émission (horloge précise pour tous les clients) ----
    {
        let front_tx = front.try_clone()?;
        let queue = Arc::clone(&queue);
        let shutdown = Arc::clone(&shutdown);
        let h = spawn("netem-tx", move || {
            let (lock, cvar) = &*queue;
            loop {
                let item = {
                    let mut heap = lock.lock().unwrap();
                    loop {
                        if shutdown.load(AtomicOrdering::Relaxed) {
                            return;
                        }
                        let Some(top) = heap.peek() else {
                            heap = cvar.wait(heap).unwrap();
                            continue;
                        };
                        let now = Instant::now();
                        if top.0.at <= now {
                            break heap.pop().unwrap().0;
                        }
                        let remaining = top.0.at - now;
                        if remaining > Duration::from_micros(1500) {
                            let (guard, _) = cvar
                                .wait_timeout(heap, remaining - Duration::from_millis(1))
                                .unwrap();
                            heap = guard;
                        } else {
                            drop(heap);
                            thread::yield_now();
                            heap = lock.lock().unwrap();
                        }
                    }
                };
                match item.target {
                    Target::Upstream(sock) => {
                        let _ = sock.send(&item.pkt);
                    }
                    Target::Client(to) => {
                        let _ = front_tx.send_to(&item.pkt, to);
                    }
                }
            }
        })?;
        threads.lock().unwrap().push(h);
    }

    // ---- 2) Réception côté clients : crée un lien dédié par client vu ----
    {
        let queue = Arc::clone(&queue);
        let next_id = Arc::clone(&next_id);
        let shutdown = Arc::clone(&shutdown);
        let threads_ref = Arc::clone(&threads);
        let h = spawn("netem-up-rx", move || {
            let mut clients: HashMap<SocketAddr, (Arc<UdpSocket>, Shaper)> = HashMap::new();
            let mut client_idx = 0u64;
            let mut buf = vec![0u8; 65_535];

            while !shutdown.load(AtomicOrdering::Relaxed) {
                let Ok((n, from)) = front.recv_from(&mut buf) else {
                    continue;
                };
                let (back, up_shaper) = clients.entry(from).or_insert_with(|| {
                    client_idx += 1;
                    let back = UdpSocket::bind("127.0.0.1:0").expect("bind back socket");
                    back.connect(upstream).expect("connect back socket");
                    back.set_read_timeout(Some(Duration::from_millis(50))).expect("timeout");
                    let back = Arc::new(back);

                    // Thread de réception descendant pour ce client (bloquant, 0 % CPU au repos)
                    let down_back = Arc::clone(&back);
                    let down_queue = Arc::clone(&queue);
                    let down_id = Arc::clone(&next_id);
                    let down_shutdown = Arc::clone(&shutdown);
                    let mut down_shaper = Shaper::new(
                        profile,
                        0x9ABC_DEF0_u64.wrapping_add(client_idx.wrapping_mul(0x9E37_79B9)),
                    );
                    if let Ok(dh) = spawn(&format!("netem-down-{client_idx}"), move || {
                        let mut dbuf = vec![0u8; 65_535];
                        while !down_shutdown.load(AtomicOrdering::Relaxed) {
                            let Ok(m) = down_back.recv(&mut dbuf) else {
                                continue;
                            };
                            if let Some(at) = down_shaper.schedule(m) {
                                enqueue(&down_queue, &down_id, at, Target::Client(from), dbuf[..m].to_vec());
                            }
                        }
                    }) {
                        threads_ref.lock().unwrap().push(dh);
                    }

                    let up_shaper = Shaper::new(
                        profile,
                        0x1234_5678_u64.wrapping_add(client_idx.wrapping_mul(0x517C_C1B7)),
                    );
                    (back, up_shaper)
                });

                if let Some(at) = up_shaper.schedule(n) {
                    enqueue(
                        &queue,
                        &next_id,
                        at,
                        Target::Upstream(Arc::clone(back)),
                        buf[..n].to_vec(),
                    );
                }
            }
        })?;
        threads.lock().unwrap().push(h);
    }

    Ok(NetemProxy { addr, shutdown, queue, threads })
}

fn spawn(name: &str, f: impl FnOnce() + Send + 'static) -> std::io::Result<JoinHandle<()>> {
    thread::Builder::new().name(name.into()).spawn(f)
}
