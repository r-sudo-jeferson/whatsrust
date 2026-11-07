//! Private library surface: no legacy HTTP/MCP, terminal QR, automatic outbound,
//! simulated typing, read marking, or environment-controlled logging setup.
use crate::{instance_lock::InstanceLock, storage::{Store, ProtocolJournal}};
use anyhow::{anyhow, bail, ensure, Result};
use parking_lot::Mutex;
use std::{path::Path, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};
use whatsapp_rust::{bot::{Bot, BotHandle}, pair_code::PairCodeOptions, types::events::Event, Client};
use whatsapp_rust_tokio_transport::TokioWebSocketTransportFactory;
use whatsapp_rust_ureq_http_client::UreqHttpClient;
use crate::storage::PrivateOutcome;
use wacore::types::presence::ReceiptType;

/// The seam is only the external send, never the durable intent/state machine.
#[async_trait::async_trait]
pub trait SendTransport: Send + Sync {
    async fn send(&self, message_id: &str, recipient: &str, body: &str) -> Result<()>;
}
#[async_trait::async_trait]
impl SendTransport for Arc<Client> {
    async fn send(&self, message_id: &str, recipient: &str, body: &str) -> Result<()> {
        let recipient=recipient.parse().map_err(|_| anyhow!("invalid_recipient"))?;
        self.send_message_with_options(recipient, waproto::whatsapp::Message {conversation:Some(body.into()), ..Default::default()}, whatsapp_rust::send::SendOptions {message_id:Some(message_id.into()), ..Default::default()})
            .await.map_err(|_| anyhow!("outcome_uncertain"))?;
        Ok(())
    }
}
impl Store {
    pub async fn dispatch_private(&self, transport: &dyn SendTransport, id: &str) -> Result<PrivateOutcome> {
        let Some(attempt)=self.claim_private_outbound(id).await.map_err(|_| anyhow!("outbound_storage_failed"))? else {
            return self.private_outcome(id).await.map_err(|_| anyhow!("intent_unavailable"));
        };
        transport.send(&attempt.outcome.message_id,&attempt.recipient,&attempt.body).await.map_err(|_| anyhow!("outcome_uncertain"))?;
        self.record_private_sent(id).await.map_err(|_| anyhow!("outcome_uncertain"))?;
        self.private_outcome(id).await.map_err(|_| anyhow!("outcome_uncertain"))
    }
}

// No Debug on any private challenge/configuration/event payload.
#[derive(Clone, serde::Serialize)]
pub struct Challenge {
    pub kind: &'static str,
    pub value: String,
    pub expires_at: u64,
}
#[derive(Clone, serde::Serialize)]
pub struct Snapshot {
    pub state: &'static str,
    pub challenge: Option<Challenge>,
}
pub struct PrivateSession {
    store: Store,
    state: Mutex<Snapshot>,
    client: Mutex<Option<Arc<Client>>>,
    task: Mutex<Option<BotHandle>>,
    _lock: InstanceLock,
}
fn epoch() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}
fn terminal(state: &str) -> bool {
    matches!(state, "pairing_required" | "suspended" | "stopped" | "storage_failed")
}
impl PrivateSession {
    pub fn open(path: &Path, key: [u8;32]) -> Result<Arc<Self>> {
        let lock = InstanceLock::acquire(path, Duration::ZERO).map_err(|_| anyhow!("session_unavailable"))?;
        let store = Store::new_encrypted(path, key).map_err(|_| anyhow!("session_unavailable"))?;
        let state = match store.session_terminal().map_err(|_| anyhow!("session_unavailable"))?.as_str() {
            "" => "disconnected", "pairing_required" => "pairing_required", _ => "suspended",
        };
        Ok(Arc::new(Self {store, state:Mutex::new(Snapshot {state, challenge:None}), client:Mutex::new(None), task:Mutex::new(None), _lock:lock}))
    }
    pub fn snapshot(&self) -> Snapshot {
        let mut state = self.state.lock();
        if state.challenge.as_ref().is_some_and(|c| c.expires_at <= epoch()) {state.challenge = None;}
        state.clone()
    }
    pub fn store(&self) -> &Store { &self.store }

    /// Apply only typed protocol events from this session's Client. Not an IPC
    /// command: untrusted callers cannot assert connection/delivery state.
    pub async fn observe(&self, event: Event) -> Result<()> {
        // Reconciliation evidence remains useful after revocation; it confers no
        // right to send or to disclose the private event to company users.
        if let Event::Receipt(ref receipt)=event {
            let state=match receipt.r#type {ReceiptType::Delivered=>Some("delivered"),ReceiptType::Read=>Some("read"),_=>None};
            if let Some(state)=state {
                for id in &receipt.message_ids {
                    self.store.record_private_receipt(id,&receipt.source.chat.to_string(),state).await.map_err(|_| anyhow!("receipt_storage_failed"))?;
                }
            }
            return Ok(());
        }
        let mut persist = None;
        {
            let mut state = self.state.lock();
            if terminal(state.state) { return Ok(()); }
            match event {
                Event::PairingQrCode {code, timeout} => {
                    state.state = "pairing";
                    state.challenge = Some(Challenge {kind:"qr", value:code, expires_at:epoch().saturating_add(timeout.as_secs())});
                }
                Event::PairingCode {code, timeout} => {
                    state.state = "pairing";
                    state.challenge = Some(Challenge {kind:"pair_code", value:code, expires_at:epoch().saturating_add(timeout.as_secs())});
                }
                Event::Connected(_) => {state.state = "active"; state.challenge = None;}
                Event::Disconnected(_) | Event::StreamError(_) => {state.state = "recovering"; state.challenge = None;}
                Event::LoggedOut(_) | Event::PairError(_) => {persist = Some("pairing_required");}
                Event::ConnectFailure(f) => {
                    if f.reason.is_logged_out() {persist = Some("pairing_required");}
                    else if f.reason.should_reconnect() {state.state="recovering"; state.challenge=None;}
                    else {persist=Some("suspended");}
                }
                Event::TemporaryBan(_) | Event::ClientOutdated(_) | Event::StreamReplaced(_) => {persist = Some("suspended");}
                _ => {}
            }
            if let Some(reason) = persist {state.state=reason; state.challenge=None;}
        }
        if let Some(reason) = persist {
            self.store.set_session_terminal(reason).await.map_err(|_| anyhow!("session_storage_failed"))?;
        }
        Ok(())
    }

    pub async fn connect(self: &Arc<Self>, method: &str, phone: Option<String>, live_authorized: bool) -> Result<()> {
        ensure!(live_authorized, "live_channel_not_authorized");
        ensure!(method == "qr" || method == "pair_code", "invalid_pairing_method");
        if method == "pair_code" {
            ensure!(phone.as_ref().is_some_and(|p| (9..=15).contains(&p.len()) && p.bytes().all(|b| b.is_ascii_digit())), "invalid_pairing_input");
        } else {ensure!(phone.is_none(), "invalid_pairing_input");}
        {
            let mut state = self.state.lock();
            ensure!(state.state == "disconnected", "session_generation_unavailable");
            state.state="connecting";
        }
        let weak = Arc::downgrade(self);
        let mut builder = Bot::builder()
            .with_backend(Arc::new(self.store.clone()))
            .with_transport_factory(TokioWebSocketTransportFactory::new())
            .with_http_client(UreqHttpClient::new())
            .with_runtime(whatsapp_rust::TokioRuntime)
            .skip_history_sync()
            .with_device_props(Some("SAAS_SIMPLES office connector".into()), None, None)
            .on_event(move |event, client| {
                let weak = weak.clone();
                async move {
                    if let Some(session) = weak.upgrade() {
                        if session.observe(event).await.is_err() {
                            *session.state.lock()=Snapshot {state:"storage_failed",challenge:None};
                        }
                        if terminal(session.snapshot().state) {client.disconnect().await;}
                    } else {client.disconnect().await;}
                }
            });
        if let Some(phone_number) = phone {
            builder=builder.with_pair_code(PairCodeOptions {phone_number, ..Default::default()});
        }
        let result = async {
            let mut bot = builder.build().await?;
            let client = bot.client();
            client.set_inbound_journal(Arc::new(ProtocolJournal::new(self.store.clone())))?;
            *self.client.lock()=Some(client);
            let handle=bot.run().await?;
            *self.task.lock()=Some(handle);
            Ok::<_, anyhow::Error>(())
        }.await;
        if result.is_err() {
            *self.state.lock()=Snapshot {state:"storage_failed",challenge:None};
            bail!("session_start_failed");
        }
        Ok(())
    }
    pub async fn stop(&self) {
        *self.state.lock()=Snapshot {state:"stopped",challenge:None};
        let client=self.client.lock().take();
        if let Some(client)=client {client.disconnect().await;}
        if let Some(task)=self.task.lock().take() {task.abort();}
    }

    pub async fn dispatch(&self,id:&str)->Result<PrivateOutcome> {
        ensure!(self.snapshot().state=="active","session_not_active");
        let client=self.client.lock().clone().ok_or_else(|| anyhow!("session_not_active"))?;
        self.store.dispatch_private(&client,id).await
    }
}
impl Drop for PrivateSession {
    fn drop(&mut self) {
        if let Some(task)=self.task.get_mut().take() {task.abort();}
    }
}
