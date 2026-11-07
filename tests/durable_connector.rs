use std::path::PathBuf;
use whatsrust::storage::Store;

fn path(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("whatsrust-contract-{name}-{}-{}", std::process::id(), rand::random::<u64>()));
    std::fs::create_dir(&root).unwrap();
    root.join("session.db")
}

#[tokio::test]
async fn ac_m0_11_outbound_uncertain_failure_and_restart_never_requeue() {
    let db = path("uncertain");
    let store = Store::new(&db).unwrap();
    let failed = store.enqueue_job("15550000001@s.whatsapp.net", "text", r#"{"text":"synthetic"}"#, None).await.unwrap();
    assert_eq!(store.claim_next_job().await.unwrap().unwrap().id, failed);
    store.mark_outbound_failed(failed, 3).await.unwrap();
    // A connection error does not establish whether WhatsApp accepted the send.
    store.requeue_outbound(failed).await.unwrap();
    assert!(store.claim_next_job().await.unwrap().is_none(), "uncertain send became eligible for a blind resend");
    let interrupted = store.enqueue_job("15550000002@s.whatsapp.net", "text", r#"{"text":"interrupted"}"#, None).await.unwrap();
    assert_eq!(store.claim_next_job().await.unwrap().unwrap().id, interrupted);
    drop(store);
    let resumed = Store::new(&db).unwrap();
    resumed.requeue_stale_inflight(-1).await.unwrap();
    assert!(resumed.claim_next_job().await.unwrap().is_none(), "restart blindly requeued an in-flight send");
    std::fs::remove_dir_all(db.parent().unwrap()).unwrap();
}
