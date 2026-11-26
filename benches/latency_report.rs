//! Rapport de latence par percentiles (p50, p99, p99.9...).
//!
//!   cargo bench --bench latency_report
//!
//! Criterion donne un temps "typique" avec un intervalle de confiance. En réseau,
//! ce qui fait mal ce sont les messages LENTS (la queue de distribution) : ici on
//! enregistre chaque aller-retour dans un histogramme HDR pour les voir.

mod harness;

use hdrhistogram::Histogram;

const WARMUP: usize = 2_000;
const SAMPLES: usize = 20_000;

fn main() {
    let rt = harness::runtime();
    rt.block_on(async {
        let client = harness::start_server_and_client().await;
        harness::setup_topic(&client, "lat_reliable", true).await;
        harness::setup_topic(&client, "lat_unreliable", false).await;

        println!(
            "\n{:<12} {:>7} | {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
            "mode", "octets", "min", "p50", "p90", "p99", "p99.9", "max"
        );
        println!("{}", "-".repeat(84));

        for (mode, topic, size) in [
            ("reliable", "lat_reliable", 16usize),
            ("reliable", "lat_reliable", 1024),
            ("reliable", "lat_reliable", 16384),
            ("unreliable", "lat_unreliable", 16),
            ("unreliable", "lat_unreliable", 1024),
        ] {
            let payload = harness::payload(size);

            // échauffement : non enregistré
            for _ in 0..WARMUP {
                harness::roundtrip(&client, topic, &payload).await;
            }

            // histogramme en nanosecondes, de 1 ns à 60 s, 3 chiffres significatifs
            let mut h = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();
            for _ in 0..SAMPLES {
                let d = harness::roundtrip(&client, topic, &payload).await;
                h.saturating_record(d.as_nanos() as u64);
            }

            let us = |ns: u64| format!("{:.1}µs", ns as f64 / 1_000.0);
            println!(
                "{:<12} {:>7} | {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                mode,
                size,
                us(h.min()),
                us(h.value_at_quantile(0.50)),
                us(h.value_at_quantile(0.90)),
                us(h.value_at_quantile(0.99)),
                us(h.value_at_quantile(0.999)),
                us(h.max()),
            );
        }
        println!("\n{SAMPLES} échantillons par ligne, après {WARMUP} allers-retours d'échauffement.");
    });
}
