use std::{
    ascii, fs, io,
    net::SocketAddr,
    path::{self, Path, PathBuf},
    str,
    sync::Arc,
};
use std::collections::{HashMap, VecDeque};
use std::net::ToSocketAddrs;
use anyhow::{Result, anyhow, bail};
use rustls::pki_types::{CertificateDer};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{Connection, Endpoint, Incoming, RecvStream};
use tracing::{error, info, info_span, Instrument};
use url::Url;
use crate::common;
use crate::common::MessageHandler;

#[derive(Clone)]
pub struct Topic {
    name: String,
    inbox: VecDeque<Vec<u8>>,
}

struct Server {
    connection: Connection,
    endpoint: Endpoint,
    send: tokio::sync::Mutex<quinn::SendStream>,
}

pub struct CliOpt {
    ///URL OF THE SERVER TO CONNECT TO
    pub url_server: Url,

    ///USE TO OVERRIDE THE HOSTNAME IN THE URL FOR CERTIFICATE VERIFICATION
    pub server_name: Option<String>,

    ///CUSTOM CERTIFICATE AUTHORITY TO TRUST, IN DER FORMAT
    pub cert: Option<PathBuf>,

    ///ADDRESS TO BIND ON
    pub bind: SocketAddr,
}

type Topics = Arc<tokio::sync::Mutex<HashMap<String, Topic>>>;

#[derive(Clone)]
pub struct PSClient {
    server: Option<Arc<Server>>,
    pub topics: Topics,
    notify: Arc<tokio::sync::Notify>,
}


impl PSClient {
    pub fn new() -> Self {
        PSClient {
            server: None,
            topics: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    ///Ouvre la connexion avec le serveur et ouvre un stream de communication. Le serveur doit être en écoute sur l'adresse et le port spécifiés.
    pub async fn create_and_connect(&mut self, option: CliOpt) -> Result<()> {
        let url = option.url_server;
        let url_host = url.host_str().ok_or_else(|| anyhow!("URL sans hôte"))?;
        let url_host = url_host.trim_start_matches('[').trim_end_matches(']').to_string();
        let remote = (url_host.clone(), url.port().unwrap_or(4433))
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| anyhow!("Could not resolve address"))?;

        //certificate handling
        let cert_name = option.server_name.unwrap_or(url_host);
        let mut roots = rustls::RootCertStore::empty();
        if let Some(ca_path) = option.cert {
            roots.add(CertificateDer::from(fs::read(ca_path)?))?;
        } else {
            let dirs = directories_next::ProjectDirs::from("org", "orga", "pubsub").unwrap();
            match fs::read(dirs.data_local_dir().join("tls").join("cert.der")) {
                Ok(cert) => {
                    roots.add(CertificateDer::from(cert))?;
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    info!("Local server certificate not found");
                }
                Err(e) => {
                    error!("Failed to open local server certificate: {}", e);
                }
            }
        }

        //create the client configuration
        let mut client_crypto = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        client_crypto.alpn_protocols = common::ALPN_PUBSUB.iter().map(|&x| x.into()).collect();

        let client_config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));
        let mut endpoint = quinn::Endpoint::client(option.bind)?;
        endpoint.set_default_client_config(client_config);

        //connect to the server
        let conn = endpoint
            .connect(remote, &cert_name)?
            .await
            .map_err(|e| anyhow!("Failed to connect: {}", e))?;

        info!("Connected to {} at {}", cert_name, remote);

        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow!("Failed to open stream: {}", e))?;

        //SI BESOIN INFO AU CLIENT SUR LE SERVEUR, ON PEUT LUI ENVOYER ICI, AVANT DE LANCER LA READ_LOOP

        //store the server connection and endpoint in the PSClient struct
        let server = Arc::new(Server {
            connection: conn.clone(),
            endpoint,
            send: tokio::sync::Mutex::new(send),
        });
        self.server = Some(Arc::clone(&server));

        let handler = ClientMessageHandler {
            client: self.clone(),
            server: Arc::clone(&server),
        };

        let dgram_handler = handler.clone();
        let dgram_conn = conn.clone();

        let dgram_task = tokio::spawn(async move {
            let _ = Self::read_loop_dgram(dgram_conn, &dgram_handler).await;
        });

        tokio::spawn(async move {
            if let Err(e) = Self::read_loop(&handler, &mut recv).await {
                error!("read loop failed: {e}");
            }
        });

        Ok(())
    }

    async fn read_loop(handler: &ClientMessageHandler, recv: &mut RecvStream) -> Result<()> {
        let id = handler.server.connection.stable_id();
        loop {
            let Some(data) = common::read_message(recv).await? else {
                info!("client {id} closed its stream");
                return Ok(());
            };
            let msg = common::PubSubMessage::deserialize(&data).map_err(|e| anyhow!(e))?;
            msg.handle_message(handler).await.map_err(|e| anyhow!(e))?;
        }
    }

    async fn read_loop_dgram(conn: Connection, handler: &ClientMessageHandler) -> Result<()> {
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

    /// Retire et renvoie le plus ancien message reçu sur `topic` (None si l'inbox est vide).
    pub async fn try_recv(&self, topic: &str) -> Option<Vec<u8>> {
        self.topics.lock().await.get_mut(topic)?.inbox.pop_front()
    }

    /// Attend et retire le plus ancien message reçu sur `topic` (sans attente active).
    pub async fn recv(&self, topic: &str) -> Vec<u8> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(msg) = self.try_recv(topic).await {
                return msg;
            }
            notified.await;
        }
    }

    /// Ferme proprement la connexion QUIC (arrête les tâches de lecture associées).
    pub fn close(&self) {
        if let Some(server) = &self.server {
            server.connection.close(0u32.into(), b"bye");
        }
    }

    pub async fn send_message(&self, message: common::PubSubMessage) -> Result<()> {
        if let Some(server) = &self.server {
            let data = message.serialize();
            let mut send = server.send.lock().await;
            common::send_message(&mut *send, &data).await?;
            Ok(())
        } else {
            Err(anyhow!("Not connected to a server"))
        }
    }
}

#[derive(Clone)]
struct ClientMessageHandler {
    client: PSClient,
    server: Arc<Server>,
}

impl MessageHandler for ClientMessageHandler {
    ///print the message received from the server
    async fn on_create_topic(&self, topic: String, reliable: bool) -> std::result::Result<(), String> {
        info!("Topic created: {} (reliable: {})", topic, reliable);
        Ok(())
    }

    async fn on_subscribe(&self, topic: String) -> std::result::Result<(), String> {
        info!("Subscribed to topic: {}", topic);
        Ok(())
    }

    async fn on_unsubscribe(&self, topic: String) -> std::result::Result<(), String> {
        info!("Unsubscribed from topic: {}", topic);
        Ok(())
    }

    async fn on_publish(&self, topic: String, message: Vec<u8>) -> std::result::Result<(), String> {
        let data = String::from_utf8(message.clone()).map_err(|e| format!("Invalid UTF-8 message: {}", e))?;
        info!("Received message on topic {}: {}", topic, data);

        self.client.topics.lock().await.entry(topic.clone()).or_insert(Topic {
            name: topic.clone(),
            inbox: VecDeque::new(),
        }).inbox.push_back(message.clone());
        self.client.notify.notify_waiters();

        Ok(())
    }//TODO: a tester dans prog test, pour voir si le message est bien reçu et stocké dans l'inbox du topic
}