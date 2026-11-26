//! Benchmark multi-clients de pubsub sur réseaux virtuels (proxy UDP, voir harness/netem.rs).
//!
//!   cargo bench --bench netem_report                  -> 50 clients, tous les profils
//!   cargo bench --bench netem_report -- 4g wan        -> 50 clients, seulement certains profils
//!   cargo bench --bench netem_report -- --clients 10  -> choisir le nombre de clients
//!
//! Pour chaque profil, N clients se connectent en parallèle (chacun avec son propre
//! lien réseau indépendant) et s'abonnent au même topic (fan-out 1 -> N) :
//!   - connexion de N clients en parallèle (temps total),
//!   - latence aller-retour FIABLE (mesurée pour chaque abonné à chaque publication),
//!   - latence et taux de perte des DATAGRAMMES (mesurés sur tous les abonnés),
//!   - débit d'une rafale de messages fiables livrés aux N abonnés.

mod harness;

use harness::netem::{NetProfile, spawn_proxy};
use hdrhistogram::Histogram;
use pubsub::PSClient;
use pubsub::common::PubSubMessage;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

const fn ms(v: u64) -> Duration {
    Duration::from_millis(v)
}
const fn us(v: u64) -> Duration {
    Duration::from_micros(v)
}

/// Profils de réseau. delay = aller simple ; les pertes s'appliquent dans les deux sens.
const PROFILES: &[NetProfile] = &[
    // Calibration : proxy SANS dégradation, pour mesurer le coût du proxy lui-même.
    NetProfile { name: "proxy-0",  delay: us(0),   jitter: us(0),   loss: 0.0,   bandwidth_bps: None },
    NetProfile { name: "lan",      delay: us(500), jitter: us(100), loss: 0.0,   bandwidth_bps: Some(1_000_000_000) },
    NetProfile { name: "wifi",     delay: ms(3),   jitter: ms(2),   loss: 0.005, bandwidth_bps: Some(100_000_000) },
    NetProfile { name: "wan",      delay: ms(15),  jitter: ms(3),   loss: 0.001, bandwidth_bps: Some(50_000_000) },
    NetProfile { name: "4g",       delay: ms(30),  jitter: ms(10),  loss: 0.01,  bandwidth_bps: Some(10_000_000) },
    NetProfile { name: "degraded", delay: ms(50),  jitter: ms(20),  loss: 0.05,  bandwidth_bps: Some(2_000_000) },
];

const DEFAULT_CLIENTS: usize = 50;
const RELIABLE_MSGS: u64 = 50; // 50 publications x 50 clients = 2 500 mesures de latence
const DGRAM_MSGS: u64 = 40;    // 40 publications x 50 clients = 2 000 livraisons attendues
const BURST: u64 = 50;         // 50 x 50 = 2 500 livraisons de 1 Ko (< 256 par file client)
const BURST_SIZE: usize = 1024;
const PAYLOAD: usize = 64;

#[derive(Default)]
struct Row {
    connect: Duration,
    reliable: Option<Histogram<u64>>,
    reliable_failed: u64,
    dgram: Option<Histogram<u64>>,
    dgram_lost: u64,
    dgram_total: u64,
    burst: Option<Duration>,
}

fn parse_args() -> (usize, Vec<String>) {
    let mut clients = DEFAULT_CLIENTS;
    let mut filters = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        if a == "--clients" {
            if let Some(n) = args.next().and_then(|s| s.parse().ok()) {
                clients = n;
            }
        } else if !a.starts_with('-') {
            filters.push(a);
        }
    }
    (clients.max(1), filters)
}

fn main() {
    let (n_clients, filters) = parse_args();

    let rt = harness::runtime();
    rt.block_on(async {
        let (server_addr, cert) = harness::start_server().await;

        println!("\nBenchmark multi-clients : {n_clients} clients par profil (fan-out 1 -> {n_clients})");
        println!("Profils (delay = aller simple, pertes appliquées dans chaque sens, 1 lien par client) :");
        for p in PROFILES {
            println!(
                "  {:<9} delay {:>6.1} ms  ±{:>5.1} ms  pertes {:>4.1} %  débit {}",
                p.name,
                p.delay.as_secs_f64() * 1e3,
                p.jitter.as_secs_f64() * 1e3,
                p.loss * 100.0,
                p.bandwidth_bps.map_or("illimité".into(), |b| format!("{} Mbit/s", b / 1_000_000)),
            );
        }

        print_header(n_clients);

        if filters.is_empty() || filters.iter().any(|f| f == "direct") {
            let row = run_profile("direct", server_addr, cert.clone(), None, n_clients).await;
            print_row("direct", Duration::ZERO, n_clients, &row);
        }

        for p in PROFILES {
            if !filters.is_empty() && !filters.iter().any(|f| f == p.name) {
                continue;
            }
            let proxy = spawn_proxy(server_addr, *p).expect("proxy");
            let row = run_profile(p.name, proxy.addr(), cert.clone(), Some(p), n_clients).await;
            print_row(p.name, p.delay * 2, n_clients, &row);
            // `proxy` est droppé ici : ses threads s'arrêtent avant le profil suivant.
        }

        println!(
            "\nRTT théo = 2 x delay. \
             fiable : {RELIABLE_MSGS} pub x {n_clients} = {} mesures. \
             datagr. : {DGRAM_MSGS} pub x {n_clients} = {} livraisons. \
             rafale : {BURST} pub x {n_clients} x {BURST_SIZE} o (débit total livré par le serveur).",
            RELIABLE_MSGS * n_clients as u64,
            DGRAM_MSGS * n_clients as u64,
        );
    });
}

async fn run_profile(
    name: &str,
    addr: SocketAddr,
    cert: PathBuf,
    profile: Option<&NetProfile>,
    n_clients: usize,
) -> Row {
    let mut row = Row::default();
    let rtt = profile.map_or(Duration::ZERO, |p| (p.delay + p.jitter) * 2);

    // --- 1) Connexion des N clients en parallèle ---
    let t = Instant::now();
    let mut set = JoinSet::new();
    for _ in 0..n_clients {
        let cert = cert.clone();
        set.spawn(async move { harness::connect_client(addr, cert).await });
    }
    let mut clients = Vec::with_capacity(n_clients);
    while let Some(res) = set.join_next().await {
        clients.push(res.expect("tâche connexion"));
    }
    row.connect = t.elapsed();

    let rel = format!("{name}_rel");
    let dgr = format!("{name}_dgr");
    let mut seq = 0u64;

    // --- 2) Création des topics par le client 0, puis confirmation avant les Subscribe ---
    // Sans cette confirmation, un Subscribe d'un autre client pourrait arriver avant CreateTopic.
    for (topic, reliable) in [(&rel, true), (&dgr, false)] {
        clients[0]
            .send_message(PubSubMessage::CreateTopic(topic.clone(), reliable))
            .await
            .unwrap();
        clients[0]
            .send_message(PubSubMessage::Subscribe(topic.clone()))
            .await
            .unwrap();
    }
    seq += 1;
    publish_seq(&clients[0], &rel, seq, PAYLOAD).await;
    assert!(
        wait_seq(&clients[0], &rel, seq, Duration::from_secs(30)).await.is_some(),
        "{name} : setup client 0 échoué"
    );

    // Abonnement des N-1 autres clients en parallèle
    let mut sub_set = JoinSet::new();
    for c in clients.iter().skip(1) {
        let c = c.clone();
        let rel = rel.clone();
        let dgr = dgr.clone();
        sub_set.spawn(async move {
            c.send_message(PubSubMessage::Subscribe(rel)).await.unwrap();
            c.send_message(PubSubMessage::Subscribe(dgr)).await.unwrap();
        });
    }
    while let Some(res) = sub_set.join_next().await {
        res.expect("subscribe");
    }

    // Attente que tous les abonnements soient effectifs côté serveur :
    // on envoie des messages d'échauffement jusqu'à ce que les N clients reçoivent un même message.
    let mut all_ready = false;
    for _ in 0..10 {
        seq += 1;
        publish_seq(&clients[0], &rel, seq, PAYLOAD).await;
        let (_, failed) =
            wait_all_seq(&clients, &rel, seq, Instant::now(), Duration::from_secs(5)).await;
        if failed == 0 {
            all_ready = true;
            break;
        }
    }
    assert!(all_ready, "{name} : tous les {n_clients} clients n'ont pas reçu le warmup");

    // Échauffement court pour stabiliser les estimations RTT de QUIC sur toutes les connexions
    for _ in 0..5 {
        seq += 1;
        publish_seq(&clients[0], &rel, seq, PAYLOAD).await;
        wait_all_seq(&clients, &rel, seq, Instant::now(), Duration::from_secs(15)).await;
    }

    // --- 3) Latence des messages fiables (fan-out 1 -> N) ---
    let mut h = new_histogram();
    for _ in 0..RELIABLE_MSGS {
        seq += 1;
        let start = Instant::now();
        publish_seq(&clients[0], &rel, seq, PAYLOAD).await;
        let (durations, failed) =
            wait_all_seq(&clients, &rel, seq, start, Duration::from_secs(15)).await;
        for d in durations {
            h.saturating_record(d.as_nanos() as u64);
        }
        row.reliable_failed += failed;
    }
    row.reliable = Some(h);

    // --- 4) Datagrammes (fan-out 1 -> N) ---
    let dgram_timeout = (Duration::from_millis(200) + rtt * 2).max(Duration::from_millis(300));
    let mut h = new_histogram();
    for _ in 0..DGRAM_MSGS {
        seq += 1;
        let start = Instant::now();
        publish_seq(&clients[0], &dgr, seq, PAYLOAD).await;
        let (durations, lost) =
            wait_all_seq(&clients, &dgr, seq, start, dgram_timeout).await;
        for d in durations {
            h.saturating_record(d.as_nanos() as u64);
        }
        row.dgram_lost += lost;
        row.dgram_total += n_clients as u64;
    }
    row.dgram = Some(h);

    // --- 5) Rafale fiable : BURST messages livrés aux N clients ---
    let first = seq + 1;
    let start = Instant::now();
    for _ in 0..BURST {
        seq += 1;
        publish_seq(&clients[0], &rel, seq, BURST_SIZE).await;
    }
    if wait_all_count(&clients, &rel, first, BURST, Duration::from_secs(60)).await {
        row.burst = Some(start.elapsed());
    }

    // --- 6) Nettoyage propre des N connexions avant le profil suivant ---
    for c in &clients {
        c.close();
    }
    // Laisse un bref instant au serveur pour traiter les fermetures QUIC
    tokio::time::sleep(Duration::from_millis(100)).await;

    row
}

// ---------------------------------------------------------------------------
// Messages numérotés et attentes parallèles sur N clients
// ---------------------------------------------------------------------------

async fn publish_seq(client: &PSClient, topic: &str, seq: u64, size: usize) {
    let mut payload = format!("{seq:010}").into_bytes();
    payload.resize(size.max(10), b'x');
    client
        .send_message(PubSubMessage::Publish(topic.to_string(), payload))
        .await
        .expect("publish");
}

fn seq_of(msg: &[u8]) -> u64 {
    std::str::from_utf8(&msg[..10.min(msg.len())]).ok().and_then(|s| s.parse().ok()).unwrap_or(0)
}

/// Attend le message numéro `seq` sur un client ; renvoie l'instant d'arrivée.
async fn wait_seq(client: &PSClient, topic: &str, seq: u64, timeout: Duration) -> Option<Instant> {
    let wait = async {
        loop {
            let msg = client.recv(topic).await;
            if seq_of(&msg) == seq {
                return Instant::now();
            }
        }
    };
    tokio::time::timeout(timeout, wait).await.ok()
}

/// Attend `seq` sur tous les clients en parallèle ; renvoie les latences reçues et le nb d'échecs.
async fn wait_all_seq(
    clients: &[PSClient],
    topic: &str,
    seq: u64,
    start: Instant,
    timeout: Duration,
) -> (Vec<Duration>, u64) {
    let mut set = JoinSet::new();
    for c in clients {
        let c = c.clone();
        let topic = topic.to_string();
        set.spawn(async move { wait_seq(&c, &topic, seq, timeout).await });
    }
    let mut durations = Vec::with_capacity(clients.len());
    let mut failed = 0;
    while let Some(res) = set.join_next().await {
        match res.expect("wait_seq") {
            Some(arrived) => durations.push(arrived.saturating_duration_since(start)),
            None => failed += 1,
        }
    }
    (durations, failed)
}

/// Attend `count` messages (>= `first`) sur chacun des clients en parallèle.
async fn wait_all_count(
    clients: &[PSClient],
    topic: &str,
    first: u64,
    count: u64,
    timeout: Duration,
) -> bool {
    let mut set = JoinSet::new();
    for c in clients {
        let c = c.clone();
        let topic = topic.to_string();
        set.spawn(async move {
            let wait = async {
                let mut got = 0;
                while got < count {
                    let msg = c.recv(&topic).await;
                    if seq_of(&msg) >= first {
                        got += 1;
                    }
                }
            };
            tokio::time::timeout(timeout, wait).await.is_ok()
        });
    }
    let mut all_ok = true;
    while let Some(res) = set.join_next().await {
        all_ok &= res.expect("wait_count");
    }
    all_ok
}

// ---------------------------------------------------------------------------
// Affichage
// ---------------------------------------------------------------------------

fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 120_000_000_000, 3).unwrap()
}

fn fmt_ms(ns: u64) -> String {
    format!("{:.2}", ns as f64 / 1e6)
}

fn print_header(n_clients: usize) {
    println!(
        "\n{:<9} {:>9} {:>8} | {:>8} {:>8} {:>8} {:>5} | {:>8} {:>7} | {:>9} {:>8}",
        "",
        format!("cx x{n_clients}"),
        "RTT",
        "fiable",
        "",
        "",
        "",
        "datagr.",
        "",
        "rafale",
        ""
    );
    println!(
        "{:<9} {:>9} {:>8} | {:>8} {:>8} {:>8} {:>5} | {:>8} {:>7} | {:>9} {:>8}",
        "profil", "ms", "théo ms", "p50 ms", "p99 ms", "max ms", "échec", "p50 ms", "perdus", "livr/s", "Mbit/s"
    );
    println!("{}", "-".repeat(111));
}

fn print_row(name: &str, theo: Duration, n_clients: usize, row: &Row) {
    let rel = row.reliable.as_ref().unwrap();
    let dg = row.dgram.as_ref().unwrap();
    let total_deliveries = BURST * n_clients as u64;
    let (burst_rate, burst_mbps) = match row.burst {
        Some(d) => {
            let s = d.as_secs_f64();
            (
                format!("{:.0}", total_deliveries as f64 / s),
                format!("{:.2}", (total_deliveries as usize * BURST_SIZE * 8) as f64 / s / 1e6),
            )
        }
        None => ("timeout".into(), "-".into()),
    };
    let dg_p50 = if dg.len() > 0 { fmt_ms(dg.value_at_quantile(0.5)) } else { "-".into() };
    let loss_pct = if row.dgram_total > 0 {
        row.dgram_lost as f64 * 100.0 / row.dgram_total as f64
    } else {
        0.0
    };
    println!(
        "{:<9} {:>9.1} {:>8.1} | {:>8} {:>8} {:>8} {:>5} | {:>8} {:>6.1}% | {:>9} {:>8}",
        name,
        row.connect.as_secs_f64() * 1e3,
        theo.as_secs_f64() * 1e3,
        fmt_ms(rel.value_at_quantile(0.5)),
        fmt_ms(rel.value_at_quantile(0.99)),
        fmt_ms(rel.max()),
        row.reliable_failed,
        dg_p50,
        loss_pct,
        burst_rate,
        burst_mbps,
    );
}
