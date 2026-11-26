use std::{
    ascii, fs, io,
    net::SocketAddr,
    path::{self, Path, PathBuf},
    str,
    sync::Arc,
};
use std::collections::{HashMap};
use anyhow::{Context, Result, anyhow};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::pki_types::pem::PemObject;
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Connection, Incoming, RecvStream};
use tracing::{error, info};
use crate::common;
use crate::common::MessageHandler;
use tokio::sync::{mpsc};

type Clients = Arc<tokio::sync::Mutex<HashMap<usize, Arc<Client>>>>;

struct Client {
    connection: Connection,
    tx: mpsc::Sender<Vec<u8>>,   // file d'envoi (messages déjà sérialisés)
}

impl Client {
    fn new(connection: Connection, send: quinn::SendStream) -> Self {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(256);
        tokio::spawn(Self::writer_task(send, rx));
        Client { connection, tx }
    }

    async fn writer_task(mut send: quinn::SendStream, mut rx: mpsc::Receiver<Vec<u8>>) {
        while let Some(data) = rx.recv().await {
            if let Err(e) = common::send_message(&mut send, &data).await {
                error!("écriture vers le client échouée : {e}");
                break;   // le flux est cassé : on arrête
            }
        }
        // tous les Sender détruits, ou erreur : on ferme proprement le flux
        let _ = send.finish();
    }
}

struct Topic {
    name: String,
    subscribers: Vec<Arc<Client>>,
    reliable: bool, // true if the topic is reliable, false if it is not

    tx_in: mpsc::Sender<Vec<u8>>,
}

pub struct ServOpt {
    pub key: PathBuf,
    pub cert: PathBuf,
    pub stateless_retry: bool,
    pub listen: SocketAddr,
    pub block: Option<SocketAddr>,
    pub connection_limit: Option<usize>,
}

type Topics = Arc<tokio::sync::Mutex<HashMap<String, Topic>>>;

#[derive(Clone)]
pub struct PSServer {
    topics: Topics, //hashmap of topics with name as key and topic as value
    clients: Clients,
}

impl PSServer {
    pub fn new() -> Self {
        PSServer {
            topics: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            clients: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    pub async fn start(&self, options: ServOpt) -> Result<()> {
        let (cert, key) = Self::get_key_and_cert(&options.cert, &options.key)?;

        let mut server_crypto = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert, key)
            .context("failed to create server config")?;
        server_crypto.alpn_protocols = common::ALPN_PUBSUB.iter().map(|&x| x.into()).collect();

        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(server_crypto).context("invalid QUIC crypto config")?,
        ));

        // setup transport config
        let transport_config = Arc::get_mut(&mut server_config.transport)
            .context("transport config already shared")?;
        transport_config.max_concurrent_bidi_streams(1_u8.into()); // un seul flux par client

        let endpoint = quinn::Endpoint::server(server_config, options.listen)
            .context("failed to create server endpoint")?;
        eprintln!("listening on {}", endpoint.local_addr()?);

        while let Some(conn) = endpoint.accept().await {
            if options
                .connection_limit
                .is_some_and(|n| endpoint.open_connections() >= n)
            {
                info!("refusing due to open connection limit");
                conn.refuse();
            } else if Some(conn.remote_address()) == options.block {
                info!("refusing blocked client IP address");
                conn.refuse();
            } else if options.stateless_retry && !conn.remote_address_validated() {
                info!("requiring connection to validate its address");
                if let Err(e) = conn.retry() {
                    error!("failed to send retry: {e}");
                }
            } else {
                info!("accepting connection");
                let server = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = server.handle_connection(conn).await {
                        error!("connection failed: {e}");
                    }
                });
            }
        }

        Ok(())
    }

    fn get_key_and_cert(cert_path: &Path, key_path: &Path,) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
        let (cert, key) = {
            let key = if key_path.extension().is_some_and(|x| x == "der") {
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                    fs::read(key_path).context("failed to read private key file")?,
                ))
            } else {
                PrivateKeyDer::from_pem_file(key_path)
                    .context("failed to read PEM from private key file")?
            };

            let cert_chain = if cert_path.extension().is_some_and(|x| x == "der") {
                vec![CertificateDer::from(
                    fs::read(cert_path).context("failed to read certificate chain file")?,
                )]
            } else {
                CertificateDer::pem_file_iter(cert_path)
                    .context("failed to read PEM from certificate chain file")?
                    .collect::<Result<_, _>>()
                    .context("invalid PEM-encoded certificate")?
            };

            (cert_chain, key)
        };

        Ok((cert, key))
    }
    /// SI PAS DE CERTIFICAT ET DE CLE, CREER UN CERTIFICAT ET UNE CLE
    pub fn create_certificate(cert_path: Option<PathBuf>, key_path: Option<PathBuf>) -> Result<(PathBuf,PathBuf)> {
        let default_dir = || -> Result<PathBuf> {
            let dirs = directories_next::ProjectDirs::from("org", "orga", "pubsub")
                .context("impossible de déterminer le dossier de données")?;
            Ok(dirs.data_local_dir().join("tls"))
        };

        let cert_path = match cert_path {
            Some(p) => p,
            None => default_dir()?.join("cert.der"),
        };
        let key_path = match key_path {
            Some(p) => p,
            None => default_dir()?.join("key.der"),
        };

        if cert_path.exists() && key_path.exists() {
            return Ok((cert_path, key_path));   // déjà là, rien à faire
        }
        if cert_path.exists() || key_path.exists() {
            anyhow::bail!("un seul des deux fichiers existe : état incohérent, supprime-les");
        }

        let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .context("échec de la génération du certificat")?;
        let key = PrivatePkcs8KeyDer::from(generated.signing_key.serialize_der());

        if let Some(parent) = cert_path.parent() {
            fs::create_dir_all(parent).context("failed to create certificate directory")?;
        }
        fs::write(&cert_path, generated.cert.der()).context("failed to write certificate")?;

        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent).context("failed to create key directory")?;
        }
        fs::write(&key_path, key.secret_pkcs8_der()).context("failed to write private key")?;

        Ok((cert_path, key_path))
    }
    async fn handle_connection(&self, incoming: Incoming) -> Result<()> {
        let connection = incoming.await?;
        let id = connection.stable_id();
        info!("accepted connection from {} with id {}", connection.remote_address(), id);

        let (send, mut recv) = match connection.accept_bi().await {
            Ok(s) => s,
            Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                info!("connection closed by peer");
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        let client = Arc::new(Client::new(connection.clone(), send));
        self.clients.lock().await.insert(id, Arc::clone(&client));

        let dgram_handler = ServerMessageHandler {
            server: self.clone(),
            client: Arc::clone(&client)
        };
        let dgram_conn = connection.clone();

        let dgram_task = tokio::spawn(async move {
            let _ = Self::read_loop_dgram(dgram_conn, &dgram_handler).await;
        });//TODO: voir si faire handle custom

        let handler = ServerMessageHandler {
            server: self.clone(),
            client: Arc::clone(&client),
        };
        let result = Self::read_loop(&handler, &mut recv).await;

        // nettoyage dans tous les cas
        dgram_task.abort();
        self.clients.lock().await.remove(&id);

        {
            let mut topics = self.topics.lock().await;
            topics.retain(|name, t| {
                let before = t.subscribers.len();
                t.subscribers.retain(|c| c.connection.stable_id() != id);
                let was_subscribed = t.subscribers.len() != before;
                let remove = was_subscribed && t.subscribers.is_empty();
                if remove { info!("Topic {name} supprimé : plus d'abonnés"); }
                !remove
            });
        }   // verrou relâché ici

        connection.close(0u32.into(), b"bye");

        result
    }
    async fn read_loop(handler: &ServerMessageHandler, recv: &mut RecvStream) -> Result<()> {
        let id = handler.client.connection.stable_id();
        loop {
            let Some(data) = common::read_message(recv).await? else {
                info!("client {id} closed its stream");
                return Ok(());
            };

            // malformé : on coupe la connexion (le ? remonte l'erreur)
            let msg = common::PubSubMessage::deserialize(&data).map_err(|e| anyhow!(e))?;

            // erreur métier : on journalise et on continue
            if let Err(e) = msg.handle_message(handler).await {
                error!("client {id} : {e}");
                // plus tard : renvoyer un message Error au client
            }
        }
    }

    async fn read_loop_dgram(conn: Connection, handler: &ServerMessageHandler) -> Result<()> {
        loop {
            match conn.read_datagram().await {
                Ok(bytes) => {
                    match common::PubSubMessage::deserialize(&bytes) {
                        // seul Publish a du sens en datagramme
                        Ok(msg @ common::PubSubMessage::Publish(..)) => {
                            if let Err(e) = msg.handle_message(handler).await {
                                error!("datagramme refusé : {e}");
                            }
                        }
                        Ok(_) => error!("commande interdite en datagramme"),
                        Err(e) => error!("datagramme invalide : {e}"),
                    }
                }
                Err(_) => {
                    info!("client {} closed its datagram stream", conn.stable_id());
                    return Ok(());
                }
            }
        }
    }

    async fn inbox_worker(mut rx: mpsc::Receiver<Vec<u8>>, tx_out: mpsc::Sender<Vec<u8>>) {
        while let Some(msg) = rx.recv().await {
            // transformation / filtrage / job éventuel ici
            if tx_out.send(msg).await.is_err() { break; }
        }
        // rx fermé (plus de Sender) : on sort, ce qui ferme tx_out et arrête l'étage suivant
    }

    async fn outbox_worker(server: PSServer, name: String, mut rx: mpsc::Receiver<Vec<u8>>) {
        while let Some(msg) = rx.recv().await {
            let (subs, reliable) = {
                let topics = server.topics.lock().await;
                match topics.get(&name) {
                    Some(t) => (t.subscribers.clone(), t.reliable),
                    None => return,
                }
            };
            let data = common::PubSubMessage::Publish(name.clone(), msg).serialize();

            for sub in &subs {
                if reliable {
                    match sub.tx.try_send(data.clone()) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            error!("client {} trop lent, message jeté", sub.connection.stable_id());
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            // tâche d'écriture morte : le client sera nettoyé par handle_connection
                        }
                    }
                } else if let Err(e) = common::send_unreliable_message(&sub.connection, data.clone()) {
                    error!("datagramme échoué : {e}");
                }
            }
        }
    }

    ///check if the topic has any subscribers, if yes do nothing, if no, delete the topic, if the topic does not exist, return an error
    async fn delete_topic(&self, topic: String) -> std::result::Result<(), String> {
        let mut topics = self.topics.lock().await;
        if let Some(t) = topics.get(&topic) {
            if t.subscribers.is_empty() {
                topics.remove(&topic);
                info!("Topic {} deleted because it has no subscribers", topic);
                Ok(())
            } else {
                info!("Topic {} not deleted because it has subscribers", topic);
                Ok(())
            }
        } else {
            Err(format!("Topic {} does not exist", topic))
        }
    }
}

#[derive(Clone)]
struct ServerMessageHandler{
    server: PSServer,
    client: Arc<Client>
}

impl MessageHandler for ServerMessageHandler {
    async fn on_create_topic(&self, topic: String, reliable: bool) -> Result<(), String> {
        let (tx_in, rx_in) = mpsc::channel::<Vec<u8>>(1024);
        let (tx_out, rx_out) = mpsc::channel::<Vec<u8>>(1024);

        {
            let mut topics = self.server.topics.lock().await;
            if topics.contains_key(&topic) {
                return Err(format!("Topic {topic} already exists"));
            }
            topics.insert(topic.clone(), Topic {
                name: topic.clone(),
                subscribers: Vec::new(),
                reliable,
                tx_in,
            });
        }   // verrou relâché

        tokio::spawn(PSServer::inbox_worker(rx_in, tx_out));
        tokio::spawn(PSServer::outbox_worker(self.server.clone(), topic.clone(), rx_out));

        info!("Topic {} created (reliable: {})", topic, reliable);
        Ok(())
    }

    async fn on_subscribe(&self, topic: String) -> std::result::Result<(), String> {
        let mut topics = self.server.topics.lock().await;
        if let Some(t) = topics.get_mut(&topic) {
            let id = self.client.connection.stable_id();
            if !t.subscribers.iter().any(|c| c.connection.stable_id() == id) {t.subscribers.push(Arc::clone(&self.client))};

            info!("Client {} subscribed to topic {}", id, topic);
            Ok(())
        } else {
            Err(format!("Topic {} does not exist", topic))
        }
    }//TODO: send message to cli to inform if all ok or if error, to be able to display it in the cli, and not only in the server log

    async fn on_unsubscribe(&self, topic: String) -> std::result::Result<(), String> {
        let mut topics = self.server.topics.lock().await;
        if let Some(t) = topics.get_mut(&topic) && t.subscribers.iter().any(|c| c.connection.stable_id() == self.client.connection.stable_id()) {
            t.subscribers.retain(|c| c.connection.stable_id() != self.client.connection.stable_id());
            info!("Client {} unsubscribed from topic {}", self.client.connection.stable_id(), topic);

            drop(topics);
            self.server.delete_topic(topic).await?;
            Ok(())
        } else {
            Err(format!("Topic {} does not exist", topic))
        }
    }//TODO: send message to cli to inform if all ok or if error, to be able to display it in the cli, and not only in the server log

    ///put the message in the inbox of the topic, if the topic exists, otherwise return an error
    async fn on_publish(&self, topic: String, message: Vec<u8>) -> Result<(), String> {
        // on clone le Sender sous verrou court, puis on envoie HORS verrou
        let tx = {
            let topics = self.server.topics.lock().await;
            topics.get(&topic).ok_or(format!("Topic {topic} does not exist"))?.tx_in.clone()
        };
        tx.try_send(message).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => "topic saturé".to_string(),
            mpsc::error::TrySendError::Closed(_) => "topic fermé".to_string(),
        })
    }//TODO: VOIR SI REMPLACER TRY_SEND PAR TIMEOUT
}