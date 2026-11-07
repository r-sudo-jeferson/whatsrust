//! Immutable application intents. Claim commits uncertainty before network work.
use super::{now_secs, Store};
use rusqlite::{params, OptionalExtension};
use wacore::store::error::{db_err, Result, StoreError};
use wacore_binary::jid::{Jid, DEFAULT_USER_SERVER, HIDDEN_USER_SERVER};

#[derive(Clone, serde::Serialize)]
pub struct PrivateOutcome {
    pub id: String,
    pub session_id: String,
    pub message_id: String,
    pub state: String,
}
pub struct PrivateAttempt {
    pub outcome: PrivateOutcome,
    pub recipient: String,
    pub body: String,
}
fn invalid() -> StoreError {
    StoreError::Database("invalid_outbound_intent".into())
}
fn outcome(conn: &rusqlite::Connection, id: &str) -> Result<PrivateOutcome> {
    conn.query_row("SELECT o.id,i.incarnation,o.message_id,o.state FROM private_outbound o CROSS JOIN private_identity i WHERE o.id=?1 AND i.id=1", [id], |r| Ok(PrivateOutcome {id:r.get(0)?,session_id:r.get(1)?,message_id:r.get(2)?,state:r.get(3)?})).map_err(db_err)
}
impl Store {
    pub async fn prepare_private_outbound(
        &self,
        id: &str,
        recipient: &str,
        body: &str,
    ) -> Result<PrivateOutcome> {
        self.require_encrypted_journal()?;
        if id.len() != 36
            || !id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-')
            || body.is_empty()
            || body.len() > 2048
            || recipient.len() > 128
        {
            return Err(invalid());
        }
        let jid: Jid = recipient.parse().map_err(|_| invalid())?;
        // Private direct notifications only. Groups/newsletters cannot receive scoped status.
        if !matches!(
            jid.server.as_ref(),
            DEFAULT_USER_SERVER | HIDDEN_USER_SERVER
        ) || jid.user.is_empty()
            || !jid.user.bytes().all(|b| b.is_ascii_digit())
            || jid.agent != 0
            || jid.device != 0
            || jid.integrator != 0
        {
            return Err(invalid());
        }
        let id = id.to_owned();
        let recipient = recipient.to_owned();
        let body = body.to_owned();
        let message_id =
            wacore::request::RequestUtils::new(String::new()).generate_message_id(None);
        self.run(move |conn| {
            conn.execute("INSERT INTO private_outbound VALUES(?1,?2,?3,?4,'prepared',?5) ON CONFLICT(id) DO NOTHING",params![id,recipient,body,message_id,now_secs()]).map_err(db_err)?;
            let same: bool=conn.query_row("SELECT recipient=?2 AND body=?3 FROM private_outbound WHERE id=?1",params![id,recipient,body],|r|r.get(0)).map_err(db_err)?;
            if !same {return Err(StoreError::Database("outbound_intent_conflict".into()));}
            outcome(conn,&id)
        }).await
    }
    pub async fn private_outcome(&self, id: &str) -> Result<PrivateOutcome> {
        self.require_encrypted_journal()?;
        let id = id.to_owned();
        self.run(move |conn| outcome(conn, &id)).await
    }
    pub async fn claim_private_outbound(&self, id: &str) -> Result<Option<PrivateAttempt>> {
        self.require_encrypted_journal()?;
        let id = id.to_owned();
        self.run(move |conn| {
            let payload=conn.query_row("UPDATE private_outbound SET state='uncertain' WHERE id=?1 AND state='prepared' RETURNING recipient,body",[&id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).optional().map_err(db_err)?;
            match payload {Some((recipient,body))=>Ok(Some(PrivateAttempt {outcome:outcome(conn,&id)?,recipient,body})),None=>Ok(None)}
        }).await
    }
    pub async fn record_private_sent(&self, id: &str) -> Result<()> {
        let id = id.to_owned();
        self.run(move |conn| {
            conn.execute(
                "UPDATE private_outbound SET state='sent' WHERE id=?1 AND state='uncertain'",
                [id],
            )
            .map_err(db_err)?;
            Ok(())
        })
        .await
    }
    pub async fn record_private_receipt(
        &self,
        message_id: &str,
        recipient: &str,
        state: &str,
    ) -> Result<()> {
        if state != "read" && state != "delivered" {
            return Err(invalid());
        }
        let message_id = message_id.to_owned();
        let recipient = recipient.to_owned();
        let state = state.to_owned();
        self.run(move |conn| {
            conn.execute("UPDATE private_outbound SET state=?3 WHERE message_id=?1 AND recipient=?2 AND (state IN ('uncertain','sent') OR (state='delivered' AND ?3='read'))",params![message_id,recipient,state]).map_err(db_err)?;
            Ok(())
        }).await
    }
}
