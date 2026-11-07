//! Durable bridge between the protocol's transport ACK and the application inbox.
use super::Store;
use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use parking_lot::Mutex;
use prost::Message;
use rusqlite::params;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use wacore::store::error::db_err;
use wacore_binary::node::Node;
use waproto::whatsapp;
use whatsapp_rust::{inbound_journal::InboundJournal, types::message::MessageInfo};

pub struct ProtocolJournal {
    store: Store,
    active: Mutex<HashMap<String, String>>,
    failed: Mutex<HashSet<String>>,
}

fn source_key(info: &MessageInfo) -> String {
    // Length-delimited JSON prevents ambiguous concatenation of untrusted IDs.
    serde_json::json!([info.source.chat.to_string(), info.source.sender.to_string(), info.id]).to_string()
}
fn raw_bytes(node: &Node) -> Result<Vec<u8>> {
    // Serialize through Value to make attribute map key ordering deterministic.
    Ok(serde_json::to_vec(&serde_json::to_value(node)?)?)
}
fn raw_id(node: &Node) -> Result<String> {
    let payload = raw_bytes(node)?;
    let mut hash = Sha256::new();
    for bytes in [b"raw".as_slice(), b"stanza".as_slice(), payload.as_slice()] {
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    Ok(hash.finalize().iter().map(|b| format!("{b:02x}")).collect())
}
fn decoded_parts(node: &Node) -> i64 {
    if node.tag == "enc" || node.tag == "plaintext" { return 1; }
    node.children().unwrap_or_default().iter().map(decoded_parts).sum()
}

impl ProtocolJournal {
    pub fn new(store: Store) -> Self {
        Self { store, active: Mutex::new(HashMap::new()), failed: Mutex::new(HashSet::new()) }
    }
    async fn finish(&self, id: String) -> Result<bool> {
        ensure!(!self.failed.lock().contains(&id), "source persistence failed");
        let result = self.store.run(move |conn| {
            let required: i64 = conn.query_row("SELECT required_decoded FROM protocol_sources WHERE raw_id=?1", [&id], |r| r.get(0)).map_err(db_err)?;
            let count: i64 = conn.query_row("SELECT count(*) FROM inbound_envelopes WHERE kind='decoded' AND source_id=?1", [&id], |r| r.get(0)).map_err(db_err)?;
            if count < required {
                return Err(wacore::store::error::StoreError::Database("decoded source is not durable".into()));
            }
            conn.execute("UPDATE protocol_sources SET completed=1 WHERE raw_id=?1", [&id]).map_err(db_err)?;
            Ok(count > 0)
        }).await?;
        Ok(result)
    }
}

#[async_trait]
impl InboundJournal for ProtocolJournal {
    async fn begin(&self, node: &Node, info: Option<&MessageInfo>) -> Result<bool> {
        let id = self.store.persist_inbound_envelope("raw", "stanza", &raw_bytes(node)?).await?;
        let required = decoded_parts(node);
        let raw = id.clone();
        let ready = self.store.run(move |conn| {
            conn.execute("INSERT INTO protocol_sources(raw_id,required_decoded) VALUES(?1,?2) ON CONFLICT DO NOTHING", params![raw, required]).map_err(db_err)?;
            conn.query_row("SELECT completed=1 OR (required_decoded>0 AND required_decoded <= (SELECT count(*) FROM inbound_envelopes WHERE kind='decoded' AND source_id=?1)) FROM protocol_sources WHERE raw_id=?1", [&raw], |r| r.get::<_, bool>(0)).map_err(db_err)
        }).await?;
        self.failed.lock().remove(&id);
        if let Some(info) = info { self.active.lock().insert(source_key(info), id.clone()); }
        if ready { self.finish(id).await?; }
        Ok(ready)
    }

    async fn decoded(&self, info: &MessageInfo, message: &whatsapp::Message) -> Result<()> {
        let id = self.active.lock().get(&source_key(info)).cloned().ok_or_else(|| anyhow!("source was not retained"))?;
        let result = async {
            let payload = serde_json::to_vec(&serde_json::json!({"info": info, "protobuf": message.encode_to_vec()}))?;
            self.store.persist_inbound_envelope("decoded", &id, &payload).await?;
            Ok::<_, anyhow::Error>(())
        }.await;
        if result.is_err() { self.failed.lock().insert(id); }
        result
    }

    async fn complete(&self, node: &Node) -> Result<bool> {
        let id = raw_id(node)?;
        let result = self.finish(id.clone()).await;
        if result.is_ok() {
            self.active.lock().retain(|_, raw| raw != &id);
        }
        result
    }
}
