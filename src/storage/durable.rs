//! Private durable source journal. The application ACK is separate from delivery
//! into an in-memory channel and must follow the Go inbox transaction's commit.

use super::{now_secs, Store};
use rusqlite::params;
use sha2::{Digest, Sha256};
use wacore::store::error::{db_err, Result, StoreError};

// Deliberately no Debug: these records contain private protocol and message data.
#[derive(Clone, serde::Serialize)]
pub struct InboundEnvelope {
    pub id: String,
    pub kind: String,
    pub source_id: String,
    pub payload: Vec<u8>,
    pub received_count: i64,
}

impl Store {
    fn require_encrypted_journal(&self) -> Result<()> {
        if self.encryption_key.is_none() {
            return Err(StoreError::Database("durable intake requires an encrypted session".into()));
        }
        Ok(())
    }

    /// Deduplicate only the identical full source; retain every distinct edit or
    /// variant. No acknowledgment or pruning happens here.
    pub async fn persist_inbound_envelope(&self, kind: &str, source_id: &str, payload: &[u8]) -> Result<String> {
        self.require_encrypted_journal()?;
        if kind.is_empty() || kind.len() > 64 || source_id.len() > 4096 || payload.len() > 8 * 1024 * 1024 {
            return Err(StoreError::Database("inbound envelope exceeds its bounds".into()));
        }
        let mut hash = Sha256::new();
        for bytes in [kind.as_bytes(), source_id.as_bytes(), payload] {
            hash.update((bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
        }
        let id: String = hash.finalize().iter().map(|b| format!("{b:02x}")).collect();
        let kind = kind.to_owned();
        let source_id = source_id.to_owned();
        let payload = payload.to_vec();
        self.run(move |conn| {
            conn.execute(
                "INSERT INTO inbound_envelopes (id,kind,source_id,payload,created_at)
                 VALUES (?1,?2,?3,?4,?5) ON CONFLICT(id) DO UPDATE SET received_count=received_count+1",
                params![id, kind, source_id, payload, now_secs()],
            ).map_err(db_err)?;
            Ok(id)
        }).await
    }

    /// Replay from durable storage, including after a process restart. A full
    /// channel or disconnected consumer cannot discard these records.
    pub async fn pending_inbound_envelopes(&self, limit: u32) -> Result<Vec<InboundEnvelope>> {
        self.require_encrypted_journal()?;
        if limit == 0 || limit > 100 {
            return Err(StoreError::Database("invalid inbox page size".into()));
        }
        self.run(move |conn| {
            let mut statement = conn.prepare(
                "SELECT id,kind,source_id,payload,received_count FROM inbound_envelopes
                 WHERE acknowledged=0 ORDER BY created_at,id LIMIT ?1",
            ).map_err(db_err)?;
            let rows = statement.query_map([limit], |r| Ok(InboundEnvelope {
                id:r.get(0)?, kind:r.get(1)?, source_id:r.get(2)?, payload:r.get(3)?, received_count:r.get(4)?,
            })).map_err(db_err)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db_err)
        }).await
    }

    /// Called only after the application has durably accepted this exact source.
    /// Repeated ACKs are idempotent; unknown references never report acceptance.
    pub async fn acknowledge_inbound_envelope(&self, id: &str) -> Result<bool> {
        self.require_encrypted_journal()?;
        let id = id.to_owned();
        self.run(move |conn| {
            conn.execute("UPDATE inbound_envelopes SET acknowledged=1 WHERE id=?1", [id])
                .map(|count| count == 1).map_err(db_err)
        }).await
    }
}
