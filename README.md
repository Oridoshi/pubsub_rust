# pubsub

Bibliothèque Rust de **publication/abonnement** (pub/sub) asynchrone au-dessus de **QUIC**
([quinn](https://github.com/quinn-rs/quinn)), conçue pour être intégrée comme couche réseau
dans un jeu multijoueur. Un serveur gère des *topics* ; des clients peuvent créer un topic,
s'y abonner, y publier et recevoir des messages.

## Fonctionnalités

- Transport **QUIC chiffré (TLS 1.3)** avec un flux bidirectionnel long par client
- Topics **fiables** (flux ordonné) ou **non fiables** (datagrammes QUIC), choisis à la création du topic
- Chaîne de traitement par topic (`inbox_worker` → `outbox_worker`) via canaux `tokio::sync::mpsc` bornés avec contre-pression
- File d'envoi et tâche d'écriture dédiées par client : un abonné lent ne bloque pas les autres
- Suppression automatique d'un topic lorsqu'il n'a plus d'abonnés
- Réception asynchrone sans attente active côté client (`recv` via `tokio::sync::Notify`, ou `try_recv`)
- Protocole binaire léger avec délimitation par longueur (4 octets big-endian + données)
- Suite de **benchmarks** complète (micro-benchs Criterion, percentiles HDR et émulation de réseaux virtuels multi-clients)

## Structure

```text
pubsub/
├── src/
│   ├── lib.rs             # point d'entrée de la bibliothèque (PSClient, PSServer)
│   ├── common/mod.rs      # protocole (PubSubMessage), framing, ALPN ("pubsub/1")
│   ├── server.rs          # PSServer, ServOpt, gestion des topics et diffusion
│   └── client.rs          # PSClient, CliOpt, envoi et réception (flux + datagrammes)
└── benches/
    ├── harness/
    │   ├── mod.rs         # démarrage serveur/client partagé pour les benchs
    │   └── netem.rs       # émulateur réseau UDP multi-clients (latence, gigue, pertes, débit)
    ├── pubsub_bench.rs    # micro-benchs & débit Criterion (rapports HTML)
    ├── latency_report.rs  # distribution de latence (p50, p90, p99, p99.9) en local
    └── netem_report.rs    # benchmarks sur réseaux émulés (LAN, Wi-Fi, WAN, 4G, dégradé)
```

## Prérequis

- Rust (édition **2024**)
- Un certificat et une clé privée au format **DER** ou **PEM** côté serveur
- Le certificat du serveur (`cert.der`) côté client

## Démarrage rapide

### 1. Installer le fournisseur crypto (une fois dans `main`)

```rust
rustls::crypto::ring::default_provider()
    .install_default()
    .expect("failed to install crypto provider");
```

### 2. Générer un certificat auto-signé (développement)

```rust
let (cert_path, key_path) = PSServer::create_certificate(None, None)?;
```

Les fichiers sont créés dans le dossier local de données de l'application (sous-dossier `tls`).
La fonction ne les écrase jamais s'ils existent déjà. Le certificat généré est valide pour `localhost`.

### 3. Lancer le serveur

```rust
use pubsub::server::ServOpt;
use pubsub::PSServer;

let server = PSServer::new();
server.start(ServOpt {
    key: key_path,
    cert: cert_path.clone(),
    stateless_retry: false,
    listen: "[::]:4433".parse()?,
    block: None,
    connection_limit: None,
}).await?;
```

### 4. Connecter un client et échanger des messages

```rust
use pubsub::client::CliOpt;
use pubsub::common::PubSubMessage;
use pubsub::PSClient;

let mut client = PSClient::new();
client.create_and_connect(CliOpt {
    url_server: "https://localhost:4433".parse()?,
    server_name: None,            // nom vérifié dans le certificat
    cert: Some(cert_path),        // certificat du serveur de confiance
    bind: "[::]:0".parse()?,
}).await?;

// Créer un topic fiable, s'abonner et publier
client.send_message(PubSubMessage::CreateTopic("chat".into(), true)).await?;
client.send_message(PubSubMessage::Subscribe("chat".into())).await?;
client.send_message(PubSubMessage::Publish("chat".into(), b"Hello!".to_vec())).await?;

// Lire le message reçu (attente asynchrone sans bloquer le CPU)
let msg = client.recv("chat").await;

// Ou lecture non bloquante :
// let maybe_msg = client.try_recv("chat").await;

// Fermeture propre de la connexion
client.close();
```

## Protocole

Sur le flux fiable, chaque message est encadré ainsi : `[longueur : 4 octets big-endian][contenu]`
(taille maximale : 1 Mo). Le contenu commence par un octet de commande :

| Octet | Commande    | Charge utile                       |
|-------|-------------|------------------------------------|
| `0`   | CreateTopic | nom du topic + 1 octet (`0`/`1` fiable) |
| `2`   | Subscribe   | nom du topic                       |
| `3`   | Unsubscribe | nom du topic                       |
| `4`   | Publish     | nom du topic + `\0` + données      |

- Les noms de topic ne doivent pas contenir l'octet `\0`.
- Les datagrammes (topics non fiables) transportent directement le contenu, sans préfixe de longueur, dans la limite de la taille maximale d'un datagramme QUIC (~1 200 octets).

## Benchmarks

Trois suites de benchmarks sont disponibles depuis le dossier `pubsub/` :

```bash
# 1. Micro-benchmarks (codec) + latence/rafale avec Criterion (génère target/criterion/report/index.html)
cargo bench --bench pubsub_bench
cargo bench --bench pubsub_bench -- codec        # filtrer un groupe

# 2. Distribution de latence en local (p50, p90, p99, p99.9) via hdrhistogram
cargo bench --bench latency_report

# 3. Réseaux virtuels multi-clients (fan-out 1 -> 50 par défaut)
cargo bench --bench netem_report                 # 50 clients, tous les profils
cargo bench --bench netem_report -- wifi 4g      # filtrer certains profils
cargo bench --bench netem_report -- --clients 10 # choisir le nombre de clients
```

### Ordres de grandeur mesurés (fan-out 1 → 50 clients)

| Profil | Latence aller simple | Pertes | Débit / lien | Fiable p50 | Fiable p99 | Rafale (livr/s) |
|---|---|---|---|---|---|---|
| `direct` | 0 ms | 0 % | illimité | 0,33 ms | 0,77 ms | ~450 000 |
| `lan` | 0,5 ms ± 0,1 | 0 % | 1 Gbit/s | 1,46 ms | 2,35 ms | ~91 500 |
| `wifi` | 3 ms ± 2 | 0,5 % | 100 Mbit/s | 10,9 ms | 22,5 ms | ~50 300 |
| `wan` | 15 ms ± 3 | 0,1 % | 50 Mbit/s | 43,5 ms | 47,6 ms | ~17 000 |
| `4g` | 30 ms ± 10 | 1 % | 10 Mbit/s | 66,6 ms | 286 ms | ~3 500 |
| `degraded` | 50 ms ± 20 | 5 % | 2 Mbit/s | 117 ms | 471 ms | ~980 |

*(Mesures indicatives en local sur Windows avec proxy UDP émulé, 0 échec côté fiable.)*

## État du projet

En cours de développement, en vue d'une intégration dans un jeu multijoueur.

- [x] Serveur : connexions QUIC, gestion des topics, abonnements, diffusion fan-out
- [x] Messages fiables (streams) et non fiables (datagrammes) côté serveur et client
- [x] Client : envoi de commandes (`send_message`), réception (`recv`, `try_recv`), fermeture (`close`)
- [x] Suite de benchmarks (Criterion, histogrammes HDR, émulateur réseau multi-clients)
- [ ] Méthodes dédiées côté client (`create_topic`, `subscribe`, `unsubscribe`, `publish`)
- [ ] Réponses du serveur (`Ack` / `Error`)
- [ ] Refuser les `Publish` par datagramme sur un topic fiable
- [ ] Supporter les charges binaires arbitraires côté client (actuellement validation UTF-8)
- [ ] Tests automatisés de bout en bout

## Sécurité

`key.der` est une **clé privée** : ne la versionnez jamais (ajoutez-la au `.gitignore`)
et ne la distribuez pas aux clients. Seul `cert.der` se partage.
