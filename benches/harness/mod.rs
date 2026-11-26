//! Code partagé par les benchmarks : démarre un vrai serveur et un vrai client
//! QUIC sur 127.0.0.1, puis fournit des petites fonctions de mesure.
#![allow(dead_code)]

pub mod netem;

use pubsub::client::CliOpt;
use std::net::SocketAddr;
use std::path::PathBuf;
use pubsub::common::PubSubMessage;
use pubsub::server::ServOpt;
use pubsub::{PSClient, PSServer};
use std::time::{Duration, Instant};

/// Port dédié aux benchs (différent de celui de serv-test : 4433).
pub const PORT: u16 = 44_550;

/// Au-delà de ce délai, on considère qu'un message a été perdu ou jeté.
pub const TIMEOUT: Duration = Duration::from_secs(2);

/// Runtime tokio multi-thread, comme en production.
pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime tokio")
}

/// Démarre le serveur et connecte un client directement (sans émulation réseau).
/// À appeler UNE seule fois par processus, car le port est fixe.
pub async fn start_server_and_client() -> PSClient {
    let (server_addr, cert) = start_server().await;
    connect_client(server_addr, cert).await
}

/// Démarre le serveur ; renvoie son adresse et le chemin du certificat.
/// À appeler UNE seule fois par processus, car le port est fixe.
pub async fn start_server() -> (SocketAddr, PathBuf) {
    // rustls a deux backends crypto compilés (ring et aws-lc-rs) : on choisit ring
    // explicitement, comme dans serv-test. L'erreur "déjà installé" est ignorée.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let (cert, key) = PSServer::create_certificate(None, None).expect("certificat");

    let server = PSServer::new();
    let opts = ServOpt {
        key,
        cert: cert.clone(),
        stateless_retry: false,
        listen: ([127, 0, 0, 1], PORT).into(),
        block: None,
        connection_limit: None,
    };
    tokio::spawn(async move {
        if let Err(e) = server.start(opts).await {
            eprintln!("serveur arrêté : {e}");
        }
    });
    // le serveur n'expose pas de signal "prêt" : on lui laisse le temps de se lier au port
    tokio::time::sleep(Duration::from_millis(300)).await;
    (([127, 0, 0, 1], PORT).into(), cert)
}

/// Connecte un client à `addr` : le serveur lui-même, ou un proxy netem placé devant.
pub async fn connect_client(addr: SocketAddr, cert: PathBuf) -> PSClient {
    let mut client = PSClient::new();
    client
        .create_and_connect(CliOpt {
            url_server: format!("https://{addr}").parse().unwrap(),
            server_name: Some("localhost".into()), // le certificat auto-signé est émis pour "localhost"
            cert: Some(cert),
            bind: ([127, 0, 0, 1], 0).into(),
        })
        .await
        .expect("connexion au serveur");
    client
}

/// Crée un topic, s'y abonne, et vérifie qu'un message fait bien l'aller-retour
/// (si ce n'est pas le cas, mieux vaut planter ici que mesurer n'importe quoi).
pub async fn setup_topic(client: &PSClient, topic: &str, reliable: bool) {
    client
        .send_message(PubSubMessage::CreateTopic(topic.into(), reliable))
        .await
        .unwrap();
    client
        .send_message(PubSubMessage::Subscribe(topic.into()))
        .await
        .unwrap();
    roundtrip(client, topic, b"warmup").await;
}

/// Charge utile de `size` octets. Le client rejette l'UTF-8 invalide, donc on n'envoie que des 'x'.
pub fn payload(size: usize) -> Vec<u8> {
    vec![b'x'; size]
}

pub async fn publish(client: &PSClient, topic: &str, payload: &[u8]) {
    client
        .send_message(PubSubMessage::Publish(topic.to_string(), payload.to_vec()))
        .await
        .expect("publish");
}

/// Attend qu'un message arrive dans l'inbox du client, puis le retire.
/// Attente active avec `yield_now` : on ne veut pas ajouter la granularité
/// d'un `sleep` (~1 ms sous Windows) à des latences de l'ordre de 100 µs.
pub async fn wait_one(client: &PSClient, topic: &str) -> Vec<u8> {
    let wait = async {
        loop {
            if let Some(msg) = client.try_recv(topic).await {
                return msg;
            }
            tokio::task::yield_now().await;
        }
    };
    tokio::time::timeout(TIMEOUT, wait).await.unwrap_or_else(|_| {
        panic!("aucun message reçu sur '{topic}' après {TIMEOUT:?} (perdu ou jeté par le serveur ?)")
    })
}

/// Un aller-retour complet : client -> serveur -> client (abonné au topic).
pub async fn roundtrip(client: &PSClient, topic: &str, payload: &[u8]) -> Duration {
    let start = Instant::now();
    publish(client, topic, payload).await;
    let msg = wait_one(client, topic).await;
    let elapsed = start.elapsed();
    assert_eq!(msg.len(), payload.len(), "message reçu corrompu");
    elapsed
}
