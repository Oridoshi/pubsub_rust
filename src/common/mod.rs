use anyhow::bail;
use quinn::{Connection, RecvStream};
use bytes::Bytes;

#[allow(unused)]
pub(crate) const ALPN_PUBSUB: &[&[u8]] = &[b"pubsub/1"];

pub enum PubSubMessage{//creer un bool qui répond au client sur ce qu'il fait (si sub le server répond avec un type sub et dit tu est bien sub ?)
    CreateTopic(String, bool),
    Subscribe(String),
    Unsubscribe(String),
    Publish(String, Vec<u8>),
    //Ack, //pour dire que le message a été reçu et traité
    //Error,
}

pub(crate) trait MessageHandler {
    async fn on_create_topic(&self, topic: String, reliable: bool) -> Result<(), String>;
    async fn on_subscribe(&self, topic: String) -> Result<(), String>;
    async fn on_unsubscribe(&self, topic: String) -> Result<(), String>;
    async fn on_publish(&self, topic: String, message: Vec<u8>) -> Result<(), String>;
    async fn on_disconnect(&self) -> Result<(), String> {
        Ok(())
    }
}

impl PubSubMessage {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            PubSubMessage::CreateTopic(topic, reliable) => {
                let mut data = vec![0u8]; // 0 for CreateTopic
                data.extend_from_slice(topic.as_bytes());
                data.push(if *reliable { 1 } else { 0 }); // 1 for reliable, 0 for unreliable
                data
            }
            PubSubMessage::Subscribe(topic) => {
                let mut data = vec![2u8]; // 2 for Subscribe
                data.extend_from_slice(topic.as_bytes());
                data
            }
            PubSubMessage::Unsubscribe(topic) => {
                let mut data = vec![3u8]; // 3 for Unsubscribe
                data.extend_from_slice(topic.as_bytes());
                data
            }
            PubSubMessage::Publish(topic, message) => {
                let mut data = vec![4u8]; // 4 for Publish
                data.extend_from_slice(topic.as_bytes());
                data.push(0); // separator between topic and message
                data.extend_from_slice(message);
                data
            }
        }
    }

    pub fn deserialize(data: &[u8]) -> Result<PubSubMessage, String> {
        if data.is_empty() {
            return Err("Empty data".to_string());
        }
        match data[0] {
            0 => {
                if data.len() < 2 {
                    return Err("Invalid CreateTopic message".to_string());
                }
                let reliable = match data[data.len() - 1] {
                    0 => false,
                    1 => true,
                    _ => return Err("Invalid reliability flag in CreateTopic message".to_string()),
                };
                let topic = String::from_utf8(data[1..data.len() - 1].to_vec()).map_err(|e| e.to_string())?;
                Ok(PubSubMessage::CreateTopic(topic, reliable))
            }
            2 => Ok(PubSubMessage::Subscribe(String::from_utf8(data[1..].to_vec()).map_err(|e| e.to_string())?)),
            3 => Ok(PubSubMessage::Unsubscribe(String::from_utf8(data[1..].to_vec()).map_err(|e| e.to_string())?)),
            4 => {
                let sep = data[1..].iter().position(|&x| x == 0).ok_or("Missing separator")? + 1;
                let topic = String::from_utf8(data[1..sep].to_vec()).map_err(|e| e.to_string())?;
                let message = data[sep + 1..].to_vec();
                Ok(PubSubMessage::Publish(topic, message))
            }
            _ => Err("Unknown message type".to_string()),
        }
    }

    pub async fn handle_message<H: MessageHandler>(&self, handler: &H)-> Result<(), String> {
        match self {
            PubSubMessage::CreateTopic(topic, reliable) => handler.on_create_topic(topic.clone(), *reliable).await,
            PubSubMessage::Subscribe(topic) => handler.on_subscribe(topic.clone()).await,
            PubSubMessage::Unsubscribe(topic) => handler.on_unsubscribe(topic.clone()).await,
            PubSubMessage::Publish(topic, message) => handler.on_publish(topic.clone(), message.clone()).await,
        }
    }
}

///Vérif si le message est bien formé, si oui, le retourne, sinon, retourne une erreur
pub(crate) async fn read_message(recv: &mut RecvStream) -> anyhow::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match recv.read_exact(&mut len_buf).await {
        Ok(()) => {}
        Err(quinn::ReadExactError::FinishedEarly(0)) => return Ok(None), // fermeture propre
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 1024 * 1024 {
        bail!("message trop grand : {len} octets");
    }
    let mut data = vec![0u8; len];
    recv.read_exact(&mut data).await?;
    Ok(Some(data))
}

pub(crate) async fn send_message(send: &mut quinn::SendStream, data: &[u8]) -> anyhow::Result<()> {
    let len = data.len() as u32;
    send.write_all(&len.to_be_bytes()).await?;
    send.write_all(data).await?;
    Ok(())
}

pub(crate) fn send_unreliable_message(connection: &Connection, data: Vec<u8>) -> anyhow::Result<()> {
    match connection.max_datagram_size() {
        None => bail!("datagrammes non supportés par le pair"),
        Some(max) if data.len() > max => {
            bail!("message trop gros pour un datagramme ({} > {max})", data.len())
        }
        Some(_) => {}
    }
    connection.send_datagram(Bytes::from(data))?;
    Ok(())
}