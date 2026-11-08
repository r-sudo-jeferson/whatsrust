//! Full retained sources and bounded typed media retrieval. Unsupported content is
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
use waproto::whatsapp::{
    self as wa,
    message::{AudioMessage, DocumentMessage, ImageMessage},
};
use whatsapp_rust::Client;

const MAX_DOCUMENT: usize = 10 * 1024 * 1024;
#[derive(serde::Serialize)]
pub struct PrivateSource {
    pub envelope: InboundEnvelope,
    pub kind: &'static str,
    pub message_type: String,
    pub reason: &'static str,
    pub has_reply: bool,
    pub has_mentions: bool,
    pub fields: Vec<super::capabilities::FieldPolicy>,
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
enum Media<'a> {
    Document(&'a DocumentMessage),
    Image(&'a ImageMessage),
    Audio(&'a AudioMessage),
}
impl<'a> Media<'a> {
    fn from_message(message: &'a wa::Message) -> Result<Self> {
        let shape = serde_json::to_value(message)?;
        ensure!(!restricted_content(&shape), "unsupported_source");
        ensure!(
            !super::capabilities::classify_message_fields(&shape)
                .iter()
                .any(|f| f.reason == "unmapped_schema_field"),
            "unsupported_source"
        );
        if let Some(m) = document(message) {
            return Ok(Self::Document(m));
        }
        if let Some(m) = message.image_message.as_deref() {
            return Ok(Self::Image(m));
        }
        if let Some(m) = message.audio_message.as_deref() {
            return Ok(Self::Audio(m));
        }
        Err(anyhow!("unsupported_source"))
    }
    fn metadata(&self) -> (Option<u64>, Option<&[u8]>) {
        match self {
            Self::Document(m) => (m.file_length, m.file_sha256.as_deref()),
            Self::Image(m) => (m.file_length, m.file_sha256.as_deref()),
            Self::Audio(m) => (m.file_length, m.file_sha256.as_deref()),
        }
    }
    async fn fetch(&self, transport: &dyn MediaTransport) -> Result<Vec<u8>> {
        match self {
            Self::Document(m) => transport.fetch(m).await,
            Self::Image(m) => transport.fetch_image(m).await,
            Self::Audio(m) => transport.fetch_audio(m).await,
        }
    }
}
fn restricted_content(value: &Value) -> bool {
    match value {
        Value::Object(fields) => fields.iter().any(|(field, v)| {
            !v.is_null()
                && (super::capabilities::message_field_policy(field).reason
                    == "retention_policy_required"
                    || restricted_content(v))
        }),
        Value::Array(values) => values.iter().any(restricted_content),
        _ => false,
    }
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
            message_type: "unknown".into(),
            reason: "unmapped_source",
            has_reply: false,
            has_mentions: false,
            fields: Vec::new(),
            chat: String::new(),
            sender: String::new(),
            message_id: String::new(),
            is_group: true,
            is_from_me: true,
        };
        if out.envelope.kind != "decoded" {
            out.message_type = out.envelope.kind.clone();
            out.reason = "specialized_handling_pending";
            if out.envelope.kind == "event" {
                let kind = out
                    .envelope
                    .source_id
                    .split_once(':')
                    .map_or(out.envelope.source_id.as_str(), |(kind, _)| kind);
                let policy = super::capabilities::event_type_policy(kind);
                out.message_type = kind.into();
                out.reason = policy.reason;
            } else if out.envelope.kind == "raw" {
                if let Ok(value) = serde_json::from_slice::<Value>(&out.envelope.payload) {
                    if let Some(tag) = value["tag"].as_str() {
                        out.message_type = tag.to_owned();
                    }
                }
                out.reason = "raw_protocol_source";
            }
            return Ok(out);
        }
        let Ok((value, message)) = decode(&out.envelope) else {
            out.reason = "malformed_message";
            return Ok(out);
        };
        let shape = serde_json::to_value(&message)?;
        out.fields = super::capabilities::classify_message_fields(&shape);
        out.message_type = out
            .fields
            .iter()
            .find(|f| f.reason != "context_metadata")
            .map(|f| f.message_type.clone())
            .unwrap_or_else(|| "unknown".into());
        out.reason = out
            .fields
            .iter()
            .find(|f| f.reason != "context_metadata")
            .map(|f| f.reason)
            .unwrap_or("unknown_message");
        out.has_reply = has_context(&shape, "stanza_id", 0);
        out.has_mentions = has_context(&shape, "mentioned_jid", 0);
        let info = &value["info"];
        let chat: Result<Jid, _> = serde_json::from_value(info["source"]["chat"].clone());
        let sender: Result<Jid, _> = serde_json::from_value(info["source"]["sender"].clone());
        let (Ok(chat), Ok(sender), Some(message_id)) = (chat, sender, info["id"].as_str()) else {
            out.reason = "malformed_source_context";
            return Ok(out);
        };
        out.chat = chat.to_non_ad().to_string();
        out.sender = sender.to_non_ad().to_string();
        out.message_id = message_id.into();
        out.is_group = info["source"]["is_group"].as_bool().unwrap_or(true);
        out.is_from_me = info["source"]["is_from_me"].as_bool().unwrap_or(true);
        out.kind = if document(&message).is_some() {
            "document"
        } else if message
            .image_message
            .as_ref()
            .is_some_and(|m| valid_media(m.file_length, m.file_sha256.as_deref()))
        {
            "image"
        } else if message
            .audio_message
            .as_ref()
            .is_some_and(|m| valid_media(m.file_length, m.file_sha256.as_deref()))
        {
            if message
                .audio_message
                .as_ref()
                .is_some_and(|m| m.ptt == Some(true))
            {
                "voice_note"
            } else {
                "audio"
            }
        } else if message.conversation.is_some() || message.extended_text_message.is_some() {
            "text"
        } else {
            "unsupported"
        };
        // Privacy wrappers and multiple content fields never inherit a nested media authorization.
        let protected = out.fields.iter().any(|f| {
            matches!(
                f.reason,
                "retention_policy_required" | "unmapped_schema_field"
            )
        });
        if protected || restricted_content(&shape) {
            out.kind = "unsupported";
        }
        if matches!(out.kind, "document" | "image" | "audio" | "voice_note") {
            out.message_type = out.kind.into();
            out.reason = "company_attribution_required";
        }
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
        let media = Media::from_message(&message)?;
        let (length, hash) = media.metadata();
        let length = length.ok_or_else(|| anyhow!("media_metadata_missing"))?;
        ensure!(
            length > 0 && length <= MAX_DOCUMENT as u64,
            "media_limit_exceeded"
        );
        let hash = hash.ok_or_else(|| anyhow!("media_metadata_missing"))?;
        ensure!(hash.len() == 32, "media_metadata_invalid");
        if let Some(cached) = self
            .retained_media(id)
            .await
            .map_err(|_| anyhow!("source_unavailable"))?
        {
            return Ok(cached);
        }
        let bytes = media.fetch(transport).await.map_err(download_failure)?;
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
    async fn fetch_image(&self, _image: &ImageMessage) -> Result<Vec<u8>> {
        Err(anyhow!("unsupported_source"))
    }
    async fn fetch_audio(&self, _audio: &AudioMessage) -> Result<Vec<u8>> {
        Err(anyhow!("unsupported_source"))
    }
}
#[async_trait::async_trait]
impl MediaTransport for Arc<Client> {
    async fn fetch(&self, document: &DocumentMessage) -> Result<Vec<u8>> {
        validate_download_metadata(
            document.direct_path.as_deref(),
            document.media_key.as_deref(),
        )?;
        let buffer = self
            .download_to_writer(document, MediaBuffer::new(MAX_DOCUMENT))
            .await
            .map_err(download_failure)?;
        Ok(buffer.into_bytes())
    }
    async fn fetch_image(&self, image: &ImageMessage) -> Result<Vec<u8>> {
        validate_download_metadata(image.direct_path.as_deref(), image.media_key.as_deref())?;
        let buffer = self
            .download_to_writer(image, MediaBuffer::new(MAX_DOCUMENT))
            .await
            .map_err(download_failure)?;
        Ok(buffer.into_bytes())
    }
    async fn fetch_audio(&self, audio: &AudioMessage) -> Result<Vec<u8>> {
        validate_download_metadata(audio.direct_path.as_deref(), audio.media_key.as_deref())?;
        let buffer = self
            .download_to_writer(audio, MediaBuffer::new(MAX_DOCUMENT))
            .await
            .map_err(download_failure)?;
        Ok(buffer.into_bytes())
    }
}
// Preserve only fixed connector categories and the pinned streaming verifier's
// exact integrity failures (wacore/src/download.rs). Never expose arbitrary
// transport error text, addresses, paths, payloads or credentials.
fn download_failure(error: anyhow::Error) -> anyhow::Error {
    for cause in error.chain() {
        let code = match cause.to_string().as_str() {
            "media_metadata_missing" => "media_metadata_missing",
            "media_metadata_invalid" => "media_metadata_invalid",
            "media_limit_exceeded" => "media_limit_exceeded",
            "unsupported_source" => "unsupported_source",
            "media_integrity_failed"
            | "MAC mismatch"
            | "Invalid final media size"
            | "Empty plaintext after decrypt"
            | "Invalid PKCS7 padding"
            | "Bad PKCS7 padding bytes" => "media_integrity_failed",
            _ => continue,
        };
        return anyhow!(code);
    }
    anyhow!("media_download_pending")
}

fn validate_download_metadata(path: Option<&str>, key: Option<&[u8]>) -> Result<()> {
    let path = path.ok_or_else(|| anyhow!("media_metadata_missing"))?;
    ensure!(
        path.starts_with('/')
            && !path.starts_with("//")
            && path.len() <= 4096
            && !path.chars().any(|c| c.is_control() || c == '\\'),
        "media_metadata_invalid"
    );
    ensure!(key.is_some_and(|k| k.len() == 32), "media_metadata_invalid");
    Ok(())
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

fn valid_media(length: Option<u64>, hash: Option<&[u8]>) -> bool {
    length.is_some_and(|n| n > 0 && n <= MAX_DOCUMENT as u64) && hash.is_some_and(|h| h.len() == 32)
}
fn has_context(value: &Value, field: &str, depth: usize) -> bool {
    if depth > 12 {
        return false;
    }
    match value {
        Value::Object(map) => map.iter().any(|(key, child)| {
            if key == "context_info" {
                let v = &child[field];
                if v.as_str().is_some_and(|s| !s.is_empty())
                    || v.as_array().is_some_and(|a| !a.is_empty())
                {
                    return true;
                }
            }
            has_context(child, field, depth + 1)
        }),
        Value::Array(items) => items.iter().any(|v| has_context(v, field, depth + 1)),
        _ => false,
    }
}
