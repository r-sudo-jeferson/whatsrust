//! Full retained sources and bounded document retrieval. Unsupported content is
//! kept visible; decoding never grants company authority or asserts authenticity.
use crate::storage::{InboundEnvelope, Store};
use anyhow::{anyhow, ensure, Result};
use prost::Message;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    io::{self, Cursor, Seek, SeekFrom, Write},
    sync::Arc,
};
use wacore_binary::jid::Jid;
use waproto::whatsapp::{self as wa, message::DocumentMessage};
use whatsapp_rust::Client;

const MAX_DOCUMENT: usize = 10 * 1024 * 1024;
#[derive(serde::Serialize)]
pub struct PrivateSource {
    pub envelope: InboundEnvelope,
    pub kind: &'static str,
    pub chat: String,
    pub sender: String,
    pub message_id: String,
    pub is_group: bool,
    pub is_from_me: bool,
}
fn decode(envelope: &InboundEnvelope) -> Result<(Value, wa::Message)> {
    ensure!(envelope.kind == "decoded", "unsupported_source");
    let value: Value =
        serde_json::from_slice(&envelope.payload).map_err(|_| anyhow!("source_invalid"))?;
    let bytes: Vec<u8> =
        serde_json::from_value(value["protobuf"].clone()).map_err(|_| anyhow!("source_invalid"))?;
    let message = wa::Message::decode(bytes.as_slice()).map_err(|_| anyhow!("source_invalid"))?;
    Ok((value, message))
}
fn document(message: &wa::Message) -> Option<&DocumentMessage> {
    message.document_message.as_deref().or_else(|| {
        message
            .document_with_caption_message
            .as_ref()?
            .message
            .as_ref()?
            .document_message
            .as_deref()
    })
}
impl Store {
    pub async fn private_source(&self, id: &str) -> Result<PrivateSource> {
        let envelope = self
            .inbound_envelope(id)
            .await
            .map_err(|_| anyhow!("source_unavailable"))?;
        let mut out = PrivateSource {
            envelope,
            kind: "unsupported",
            chat: String::new(),
            sender: String::new(),
            message_id: String::new(),
            is_group: true,
            is_from_me: true,
        };
        if out.envelope.kind != "decoded" {
            return Ok(out);
        }
        let (value, message) = decode(&out.envelope)?;
        let info = &value["info"];
        let chat: Jid = serde_json::from_value(info["source"]["chat"].clone())
            .map_err(|_| anyhow!("source_invalid"))?;
        let sender: Jid = serde_json::from_value(info["source"]["sender"].clone())
            .map_err(|_| anyhow!("source_invalid"))?;
        out.chat = chat.to_non_ad().to_string();
        out.sender = sender.to_non_ad().to_string();
        out.message_id = info["id"]
            .as_str()
            .ok_or_else(|| anyhow!("source_invalid"))?
            .into();
        out.is_group = info["source"]["is_group"].as_bool().unwrap_or(true);
        out.is_from_me = info["source"]["is_from_me"].as_bool().unwrap_or(true);
        out.kind = if document(&message).is_some() {
            "document"
        } else if message.conversation.is_some() || message.extended_text_message.is_some() {
            "text"
        } else {
            "unsupported"
        };
        Ok(out)
    }
    pub async fn download_private(
        &self,
        transport: &dyn MediaTransport,
        id: &str,
    ) -> Result<InboundEnvelope> {
        let envelope = self
            .inbound_envelope(id)
            .await
            .map_err(|_| anyhow!("source_unavailable"))?;
        let (_, message) = decode(&envelope)?;
        let document = document(&message).ok_or_else(|| anyhow!("unsupported_source"))?;
        let length = document
            .file_length
            .ok_or_else(|| anyhow!("media_metadata_missing"))?;
        ensure!(
            length > 0 && length <= MAX_DOCUMENT as u64,
            "media_limit_exceeded"
        );
        let hash = document
            .file_sha256
            .as_deref()
            .ok_or_else(|| anyhow!("media_metadata_missing"))?;
        ensure!(hash.len() == 32, "media_metadata_invalid");
        if let Some(cached) = self
            .retained_media(id)
            .await
            .map_err(|_| anyhow!("source_unavailable"))?
        {
            return Ok(cached);
        }
        let bytes = transport
            .fetch(document)
            .await
            .map_err(|_| anyhow!("media_download_pending"))?;
        ensure!(
            bytes.len() as u64 == length && bytes.len() <= MAX_DOCUMENT,
            "media_size_mismatch"
        );
        ensure!(
            Sha256::digest(&bytes).as_slice() == hash,
            "media_hash_mismatch"
        );
        let retained = self
            .persist_inbound_envelope("media", id, &bytes)
            .await
            .map_err(|_| anyhow!("media_storage_failed"))?;
        self.inbound_envelope(&retained)
            .await
            .map_err(|_| anyhow!("media_storage_failed"))
    }
}

#[async_trait::async_trait]
pub trait MediaTransport: Send + Sync {
    async fn fetch(&self, document: &DocumentMessage) -> Result<Vec<u8>>;
}
#[async_trait::async_trait]
impl MediaTransport for Arc<Client> {
    async fn fetch(&self, document: &DocumentMessage) -> Result<Vec<u8>> {
        let path = document
            .direct_path
            .as_deref()
            .ok_or_else(|| anyhow!("media_metadata_missing"))?;
        ensure!(
            path.starts_with('/')
                && !path.starts_with("//")
                && path.len() <= 4096
                && !path.chars().any(|c| c.is_control() || c == '\\'),
            "media_metadata_invalid"
        );
        ensure!(
            document.media_key.as_ref().is_some_and(|k| k.len() == 32),
            "media_metadata_invalid"
        );
        let buffer = self
            .download_to_writer(document, MediaBuffer::new(MAX_DOCUMENT))
            .await
            .map_err(|_| anyhow!("media_download_pending"))?;
        Ok(buffer.into_bytes())
    }
}
/// A bounded streaming destination, including rewinds performed by the existing
/// read-only download retry path. No plaintext temporary file is written.
pub struct MediaBuffer {
    inner: Cursor<Vec<u8>>,
    limit: usize,
    rewound: bool,
}
impl MediaBuffer {
    pub fn new(limit: usize) -> Self {
        Self {
            inner: Cursor::new(Vec::new()),
            limit,
            rewound: false,
        }
    }
    pub fn into_bytes(self) -> Vec<u8> {
        self.inner.into_inner()
    }
}
impl Write for MediaBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .inner
            .position()
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("media_limit_exceeded"))?;
        if end > self.limit as u64 {
            return Err(io::Error::other("media_limit_exceeded"));
        }
        if self.rewound && self.inner.position() == 0 {
            self.inner.get_mut().clear();
        }
        self.rewound = false;
        self.inner.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Seek for MediaBuffer {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let old = self.inner.position();
        let next = self.inner.seek(from)?;
        if next > self.limit as u64 {
            self.inner.set_position(old);
            return Err(io::Error::other("media_limit_exceeded"));
        }
        self.rewound = next == 0;
        Ok(next)
    }
}
