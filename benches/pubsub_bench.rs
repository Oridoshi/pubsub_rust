//! Benchmarks Criterion de pubsub.
//!
//!   cargo bench --bench pubsub_bench               -> tout
//!   cargo bench --bench pubsub_bench -- codec      -> seulement la (dé)sérialisation
//!   cargo bench --bench pubsub_bench -- roundtrip  -> seulement la latence réseau
//!
//! Rapport HTML : target/criterion/report/index.html

mod harness;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use pubsub::common::PubSubMessage;
use std::hint::black_box;
use std::time::{Duration, Instant};

/// 1) Micro-bench : coût CPU pur de l'encodage/décodage d'un Publish, sans réseau.
fn bench_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec");

    for size in [16usize, 256, 4096, 65536] {
        let msg = PubSubMessage::Publish("bench/topic".into(), harness::payload(size));
        let bytes = msg.serialize();

        // Affiche aussi un débit en Mo/s, plus parlant que des ns quand la taille varie.
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("serialize", size), &msg, |b, msg| {
            b.iter(|| black_box(msg).serialize())
        });
        group.bench_with_input(BenchmarkId::new("deserialize", size), &bytes, |b, bytes| {
            b.iter(|| PubSubMessage::deserialize(black_box(bytes)).unwrap())
        });
    }
    group.finish();
}

/// 2) et 3) Benchs de bout en bout : vrai serveur + vrai client QUIC sur localhost.
fn bench_network(c: &mut Criterion) {
    // --- Setup : exécuté UNE fois, hors de toute mesure ---
    let rt = harness::runtime();
    let client = rt.block_on(harness::start_server_and_client());
    rt.block_on(harness::setup_topic(&client, "bench_reliable", true));
    rt.block_on(harness::setup_topic(&client, "bench_unreliable", false));

    // --- 2) Latence aller-retour d'un message (publish -> serveur -> abonné) ---
    let mut group = c.benchmark_group("roundtrip");
    group.measurement_time(Duration::from_secs(10)); // le réseau est plus bruité : on mesure plus longtemps

    let cases: [(&str, &str, &[usize]); 2] = [
        ("reliable", "bench_reliable", &[16, 1024, 16384]),
        // un datagramme doit tenir dans un paquet UDP (~1200 octets) : pas de gros messages
        ("unreliable", "bench_unreliable", &[16, 1024]),
    ];
    for (mode, topic, sizes) in cases {
        for &size in sizes {
            let payload = harness::payload(size);
            group.bench_with_input(BenchmarkId::new(mode, size), &payload, |b, payload| {
                // iter_custom : c'est NOUS qui mesurons le temps de `iters` itérations,
                // car le code est async et doit tourner dans le runtime tokio.
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += harness::roundtrip(&client, topic, payload).await;
                        }
                        total
                    })
                })
            });
        }
    }
    group.finish();

    // --- 3) Débit : rafale de BURST messages, on attend qu'ils soient TOUS revenus ---
    // BURST < 256 : au-delà, la file d'envoi du serveur vers le client (capacité 256)
    // déborde et le serveur JETTE des messages (try_send) -> le bench planterait (timeout).
    const BURST: usize = 100;
    let mut group = c.benchmark_group("burst");
    group.throughput(Throughput::Elements(BURST as u64)); // affiche des messages/s

    for size in [16usize, 1024] {
        let payload = harness::payload(size);
        group.bench_with_input(BenchmarkId::new("reliable", size), &payload, |b, payload| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let start = Instant::now();
                    for _ in 0..iters {
                        for _ in 0..BURST {
                            harness::publish(&client, "bench_reliable", payload).await;
                        }
                        for _ in 0..BURST {
                            harness::wait_one(&client, "bench_reliable").await;
                        }
                    }
                    start.elapsed()
                })
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_codec, bench_network);
criterion_main!(benches);
