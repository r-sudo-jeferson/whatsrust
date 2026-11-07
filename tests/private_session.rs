use std::{path::PathBuf, time::Duration};
use whatsapp_rust::types::events::{Connected, Disconnected, Event, LoggedOut, ConnectFailureReason};
use whatsrust::private::PrivateSession;

fn path() -> PathBuf {
    let root = std::env::temp_dir().join(format!("private-session-{}-{}", std::process::id(), rand::random::<u64>()));
    std::fs::create_dir(&root).unwrap();
    root.join("session.db")
}

#[tokio::test]
async fn ac_m0_11_pairing_reconnect_repair_isolated_private_challenges() {
    let a = path(); let b = path();
    let first = PrivateSession::open(&a, [0x61;32]).unwrap();
    let second = PrivateSession::open(&b, [0x62;32]).unwrap();
    assert!(PrivateSession::open(&a, [0x61;32]).is_err(), "two processes may not own one session");
    assert_eq!(first.connect("pair_code", Some("15550000009".into()), false).await.unwrap_err().to_string(), "live_channel_not_authorized");
    assert_eq!(first.snapshot().state, "disconnected");
    first.observe(Event::PairingQrCode { code:"synthetic-private-qr".into(), timeout:Duration::from_secs(60) }).await.unwrap();
    let snapshot = first.snapshot();
    assert_eq!(snapshot.state, "pairing");
    assert_eq!(snapshot.challenge.as_ref().unwrap().kind, "qr");
    assert_eq!(snapshot.challenge.as_ref().unwrap().value, "synthetic-private-qr");
    assert!(second.snapshot().challenge.is_none());
    first.observe(Event::PairingCode { code:"3210-9876".into(), timeout:Duration::ZERO }).await.unwrap();
    assert!(first.snapshot().challenge.is_none(), "expired pairing material remains visible");
    first.observe(Event::PairingCode { code:"3210-9876".into(), timeout:Duration::from_secs(60) }).await.unwrap();
    assert_eq!(first.snapshot().challenge.as_ref().unwrap().value, "3210-9876");
    first.observe(Event::Connected(Connected)).await.unwrap();
    assert_eq!(first.snapshot().state, "active");
    assert!(first.snapshot().challenge.is_none());
    first.observe(Event::Disconnected(Disconnected)).await.unwrap();
    assert_eq!(first.snapshot().state, "recovering");
    assert_eq!(second.snapshot().state, "disconnected");
    first.observe(Event::Connected(Connected)).await.unwrap();
    assert_eq!(first.snapshot().state, "active");
    first.observe(Event::LoggedOut(LoggedOut { on_connect:false, reason:ConnectFailureReason::LoggedOut })).await.unwrap();
    assert_eq!(first.snapshot().state, "pairing_required");
    first.observe(Event::Disconnected(Disconnected)).await.unwrap();
    assert_eq!(first.snapshot().state, "pairing_required", "logout was changed to an endless reconnect");
    drop(first);
    let reopened = PrivateSession::open(&a, [0x61;32]).unwrap();
    assert_eq!(reopened.snapshot().state, "pairing_required");
    assert!(reopened.snapshot().challenge.is_none());
    second.stop().await;
    assert_eq!(second.snapshot().state, "stopped");
    assert!(second.snapshot().challenge.is_none());
    std::fs::remove_dir_all(a.parent().unwrap()).unwrap();
    std::fs::remove_dir_all(b.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn ac_m0_11_outbound_private_intent_is_immutable_and_uncertain_is_never_reexecuted() {
    let a=path();
    let session=PrivateSession::open(&a,[0x71;32]).unwrap();
    let id="73000000-0000-0000-0000-000000000001";
    let destination="15550000010@s.whatsapp.net";
    let first=session.store().prepare_private_outbound(id,destination,"Open the authenticated portal.").await.unwrap();
    assert_eq!(first.state,"prepared");
    let same=session.store().prepare_private_outbound(id,destination,"Open the authenticated portal.").await.unwrap();
    assert_eq!(first.message_id,same.message_id);
    assert!(session.store().prepare_private_outbound(id,destination,"different text").await.is_err());
    assert!(session.store().prepare_private_outbound(id,"15550000011@s.whatsapp.net","Open the authenticated portal.").await.is_err());
    let claimed=session.store().claim_private_outbound(id).await.unwrap().unwrap();
    assert_eq!(claimed.outcome.state,"uncertain", "network attempt started without durable uncertainty");
    assert_eq!(claimed.outcome.message_id,first.message_id);
    assert!(session.store().claim_private_outbound(id).await.unwrap().is_none());
    drop(session);
    let resumed=PrivateSession::open(&a,[0x71;32]).unwrap();
    assert_eq!(resumed.store().private_outcome(id).await.unwrap().state,"uncertain");
    assert!(resumed.store().claim_private_outbound(id).await.unwrap().is_none(),"restart authorized a duplicate send");
    assert!(resumed.store().private_outcome("unknown-intent").await.is_err());
    // A receipt for another conversation cannot upgrade this intent.
    resumed.store().record_private_receipt(&first.message_id,"15550000011@s.whatsapp.net","delivered").await.unwrap();
    assert_eq!(resumed.store().private_outcome(id).await.unwrap().state,"uncertain");
    resumed.store().record_private_receipt(&first.message_id,destination,"read").await.unwrap();
    assert_eq!(resumed.store().private_outcome(id).await.unwrap().state,"read");
    resumed.store().record_private_sent(id).await.unwrap();
    resumed.store().record_private_receipt(&first.message_id,destination,"delivered").await.unwrap();
    assert_eq!(resumed.store().private_outcome(id).await.unwrap().state,"read", "late weaker evidence downgraded read");
    std::fs::remove_dir_all(a.parent().unwrap()).unwrap();
}
