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

#[tokio::test]
async fn ac_m0_11_privacy_session_and_backup_require_the_number_key() {
    let db = path("encrypted");
    let key = [0x31; 32]; // Synthetic test key, never used by a real session.
    let store = Store::new_encrypted(&db, key).unwrap();
    let private = "synthetic-confidential-message-15550000003";
    store.insert_inbound("15550000003@s.whatsapp.net", "15550000003@s.whatsapp.net", "fixture-a", "text", Some(private), 1).await.unwrap();
    let backup = store.perform_backup(&db.parent().unwrap().join("backups"), 2).unwrap();
    for entry in std::fs::read_dir(db.parent().unwrap()).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            let bytes = std::fs::read(path).unwrap();
            assert!(!bytes.windows(private.len()).any(|w| w == private.as_bytes()), "session file contains plaintext");
        }
    }
    let bytes = std::fs::read(&backup).unwrap();
    assert!(!bytes.starts_with(b"SQLite format 3"), "backup is unencrypted SQLite");
    assert!(!bytes.windows(private.len()).any(|w| w == private.as_bytes()));
    drop(store);
    assert!(Store::new(&db).is_err(), "unkeyed session opened");
    assert!(Store::new_encrypted(&db, [0x32; 32]).is_err(), "another number's key opened the session");
    assert!(Store::new_encrypted(&backup, [0x32; 32]).is_err(), "another number's key opened the backup");
    for file in [&db, &backup] {
        let resumed = Store::new_encrypted(file, key).unwrap();
        let messages = resumed.search_inbound(None, None, 10, None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].body_text.as_deref(), Some(private));
    }
    std::fs::remove_dir_all(db.parent().unwrap()).unwrap();
}
