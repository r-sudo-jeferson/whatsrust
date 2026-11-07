use std::path::PathBuf;
use whatsrust::storage::Store;

fn path(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "whatsrust-contract-{name}-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir(&root).unwrap();
    root.join("session.db")
}

#[tokio::test]
async fn ac_m0_11_outbound_uncertain_failure_and_restart_never_requeue() {
    let db = path("uncertain");
    let store = Store::new(&db).unwrap();
    let failed = store
        .enqueue_job(
            "15550000001@s.whatsapp.net",
            "text",
            r#"{"text":"synthetic"}"#,
            None,
        )
        .await
        .unwrap();
    assert_eq!(store.claim_next_job().await.unwrap().unwrap().id, failed);
    store.mark_outbound_failed(failed, 3).await.unwrap();
    // A connection error does not establish whether WhatsApp accepted the send.
    store.requeue_outbound(failed).await.unwrap();
    assert!(
        store.claim_next_job().await.unwrap().is_none(),
        "uncertain send became eligible for a blind resend"
    );
    let interrupted = store
        .enqueue_job(
            "15550000002@s.whatsapp.net",
            "text",
            r#"{"text":"interrupted"}"#,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        store.claim_next_job().await.unwrap().unwrap().id,
        interrupted
    );
    drop(store);
    let resumed = Store::new(&db).unwrap();
    resumed.requeue_stale_inflight(-1).await.unwrap();
    assert!(
        resumed.claim_next_job().await.unwrap().is_none(),
        "restart blindly requeued an in-flight send"
    );
    std::fs::remove_dir_all(db.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn ac_m0_11_privacy_session_and_backup_require_the_number_key() {
    let db = path("encrypted");
    let key = [0x31; 32]; // Synthetic test key, never used by a real session.
    let store = Store::new_encrypted(&db, key).unwrap();
    let private = "synthetic-confidential-message-15550000003";
    store
        .insert_inbound(
            "15550000003@s.whatsapp.net",
            "15550000003@s.whatsapp.net",
            "fixture-a",
            "text",
            Some(private),
            1,
        )
        .await
        .unwrap();
    let backup = store
        .perform_backup(&db.parent().unwrap().join("backups"), 2)
        .unwrap();
    for entry in std::fs::read_dir(db.parent().unwrap()).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            let bytes = std::fs::read(path).unwrap();
            assert!(
                !bytes
                    .windows(private.len())
                    .any(|w| w == private.as_bytes()),
                "session file contains plaintext"
            );
        }
    }
    let bytes = std::fs::read(&backup).unwrap();
    assert!(
        !bytes.starts_with(b"SQLite format 3"),
        "backup is unencrypted SQLite"
    );
    assert!(!bytes
        .windows(private.len())
        .any(|w| w == private.as_bytes()));
    drop(store);
    assert!(Store::new(&db).is_err(), "unkeyed session opened");
    assert!(
        Store::new_encrypted(&db, [0x32; 32]).is_err(),
        "another number's key opened the session"
    );
    assert!(
        Store::new_encrypted(&backup, [0x32; 32]).is_err(),
        "another number's key opened the backup"
    );
    for file in [&db, &backup] {
        let resumed = Store::new_encrypted(file, key).unwrap();
        let messages = resumed.search_inbound(None, None, 10, None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].body_text.as_deref(), Some(private));
    }
    std::fs::remove_dir_all(db.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn ac_m0_11_ingress_replays_unacknowledged_sources_and_preserves_edits() {
    let db = path("inbox");
    let key = [0x41; 32];
    let source = "15550000004@s.whatsapp.net/message-a";
    let original = br#"{"text":"synthetic original","media_key":[1,2,3]}"#;
    let edit = br#"{"text":"synthetic edit","media_key":[4,5,6]}"#;
    let store = Store::new_encrypted(&db, key).unwrap();
    let first = store
        .persist_inbound_envelope("decoded", source, original)
        .await
        .unwrap();
    let duplicate = store
        .persist_inbound_envelope("decoded", source, original)
        .await
        .unwrap();
    let changed = store
        .persist_inbound_envelope("decoded", source, edit)
        .await
        .unwrap();
    assert_eq!(first, duplicate);
    assert_ne!(first, changed, "an edit was collapsed into its original");
    drop(store); // Crash boundary: durable commit, but no application ACK.
    let resumed = Store::new_encrypted(&db, key).unwrap();
    let pending = resumed.pending_inbound_envelopes(100).await.unwrap();
    assert_eq!(pending.len(), 2);
    let saved = pending.iter().find(|e| e.id == first).unwrap();
    assert_eq!(saved.payload, original);
    assert_eq!(saved.received_count, 2);
    assert_eq!(saved.source_id, source);
    assert_eq!(saved.kind, "decoded");
    assert_eq!(
        pending.iter().find(|e| e.id == changed).unwrap().payload,
        edit
    );
    assert!(!resumed
        .acknowledge_inbound_envelope("unknown")
        .await
        .unwrap());
    assert!(resumed.acknowledge_inbound_envelope(&first).await.unwrap());
    drop(resumed); // Crash boundary: application ACK committed.
    let resumed = Store::new_encrypted(&db, key).unwrap();
    let pending = resumed.pending_inbound_envelopes(100).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, changed);
    assert_eq!(
        resumed
            .persist_inbound_envelope("decoded", source, original)
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        resumed.pending_inbound_envelopes(100).await.unwrap().len(),
        1,
        "ACKed duplicate was offered for business ingestion again"
    );
    let other = path("other-inbox");
    let other_store = Store::new_encrypted(&other, [0x42; 32]).unwrap();
    assert!(other_store
        .pending_inbound_envelopes(100)
        .await
        .unwrap()
        .is_empty());
    assert!(!other_store
        .acknowledge_inbound_envelope(&changed)
        .await
        .unwrap());
    std::fs::remove_dir_all(db.parent().unwrap()).unwrap();
    std::fs::remove_dir_all(other.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn ac_m0_11_ingress_protocol_journal_requires_decoded_commit_and_survives_restart() {
    use prost::Message;
    use wacore_binary::builder::NodeBuilder;
    use whatsapp_rust::{inbound_journal::InboundJournal, types::message::MessageInfo};
    use whatsrust::storage::ProtocolJournal;
    let db = path("protocol-journal");
    let key = [0x51; 32];
    let info = MessageInfo {
        id: "source-a".into(),
        ..Default::default()
    };
    let node = NodeBuilder::new("message")
        .attr("id", "source-a")
        .children([NodeBuilder::new("enc").bytes(vec![1, 2, 3]).build()])
        .build();
    let full = waproto::whatsapp::Message {
        conversation: Some("private retained original".into()),
        ..Default::default()
    };
    let store = Store::new_encrypted(&db, key).unwrap();
    let journal = ProtocolJournal::new(store.clone());
    assert!(!journal.begin(&node, Some(&info)).await.unwrap());
    assert!(
        journal.complete(&node).await.is_err(),
        "encrypted source ACKed without a durable decoded message"
    );
    journal.decoded(&info, &full).await.unwrap();
    // A restart between decoded commit and completion must not require a consumed ratchet again.
    drop(journal);
    drop(store);
    let resumed = Store::new_encrypted(&db, key).unwrap();
    let journal = ProtocolJournal::new(resumed.clone());
    assert!(journal.begin(&node, Some(&info)).await.unwrap());
    assert!(journal.complete(&node).await.unwrap());
    let pending = resumed.pending_inbound_envelopes(100).await.unwrap();
    assert_eq!(
        pending.len(),
        2,
        "raw source or full decoded message was lost"
    );
    let decoded = pending.iter().find(|e| e.kind == "decoded").unwrap();
    let body: serde_json::Value = serde_json::from_slice(&decoded.payload).unwrap();
    let bytes: Vec<u8> = serde_json::from_value(body["protobuf"].clone()).unwrap();
    assert_eq!(
        waproto::whatsapp::Message::decode(bytes.as_slice()).unwrap(),
        full
    );
    assert_eq!(body["info"]["id"], "source-a");
    assert!(resumed
        .acknowledge_inbound_envelope(&decoded.id)
        .await
        .unwrap());
    assert!(journal.begin(&node, Some(&info)).await.unwrap());
    assert!(journal.complete(&node).await.unwrap());
    assert_eq!(
        resumed.pending_inbound_envelopes(100).await.unwrap().len(),
        1
    );
    // A distinct raw variant is never declared complete from another variant's decoded data.
    let variant = NodeBuilder::new("message")
        .attr("id", "source-a")
        .children([NodeBuilder::new("enc").bytes(vec![4, 5, 6]).build()])
        .build();
    assert!(!journal.begin(&variant, Some(&info)).await.unwrap());
    assert!(journal.complete(&variant).await.is_err());
    let unsupported = NodeBuilder::new("message")
        .attr("id", "unsupported-a")
        .build();
    assert!(!journal.begin(&unsupported, None).await.unwrap());
    assert!(!journal.complete(&unsupported).await.unwrap());
    assert!(resumed
        .pending_inbound_envelopes(100)
        .await
        .unwrap()
        .iter()
        .any(|e| e.kind == "raw" && e.payload.windows(13).any(|w| w == b"unsupported-a")));
    std::fs::remove_dir_all(db.parent().unwrap()).unwrap();
}
