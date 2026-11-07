use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    io::Write,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
use waproto::whatsapp as wa;
use whatsapp_rust::types::message::{MessageInfo, MessageSource};
use whatsrust::private::{MediaBuffer, MediaTransport, PrivateSession};
fn path() -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "private-source-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir(&root).unwrap();
    root.join("session.db")
}
struct Download {
    bytes: Vec<u8>,
    calls: AtomicUsize,
}
#[async_trait::async_trait]
impl MediaTransport for Download {
    async fn fetch(&self, _: &wa::message::DocumentMessage) -> anyhow::Result<Vec<u8>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.bytes.clone())
    }
}
#[tokio::test]
async fn ac_m0_11_ingress_retains_supported_document_bytes_and_exposes_unsupported_sources() {
    let db = path();
    let session = PrivateSession::open(&db, [0x73; 32]).unwrap();
    let bytes = b"synthetic original document".to_vec();
    let info = MessageInfo {
        id: "document-a".into(),
        source: MessageSource {
            chat: "15550000013@s.whatsapp.net".parse().unwrap(),
            sender: "15550000013:7@s.whatsapp.net".parse().unwrap(),
            ..Default::default()
        },
        ..Default::default()
    };
    let document = wa::message::DocumentMessage {
        file_length: Some(bytes.len() as u64),
        file_sha256: Some(Sha256::digest(&bytes).to_vec()),
        mimetype: Some("application/pdf".into()),
        ..Default::default()
    };
    let message = wa::Message {
        document_message: Some(Box::new(document)),
        ..Default::default()
    };
    let payload =
        serde_json::to_vec(&serde_json::json!({"info":info,"protobuf":message.encode_to_vec()}))
            .unwrap();
    let id = session
        .store()
        .persist_inbound_envelope("decoded", "raw-a", &payload)
        .await
        .unwrap();
    let source = session.store().private_source(&id).await.unwrap();
    assert_eq!(source.kind, "document");
    assert_eq!(source.sender, "15550000013@s.whatsapp.net");
    assert_eq!(source.message_id, "document-a");
    assert!(!source.is_group);
    let wrong = Download {
        bytes: b"incorrect response".to_vec(),
        calls: AtomicUsize::new(0),
    };
    assert!(
        session.store().download_private(&wrong, &id).await.is_err(),
        "corrupt media was accepted"
    );
    let wire = Download {
        bytes: bytes.clone(),
        calls: AtomicUsize::new(0),
    };
    let media = session.store().download_private(&wire, &id).await.unwrap();
    assert_eq!(media.payload, bytes);
    assert_eq!(media.source_id, id);
    assert_eq!(media.kind, "media");
    drop(session);
    let resumed = PrivateSession::open(&db, [0x73; 32]).unwrap();
    assert_eq!(
        resumed
            .store()
            .download_private(&wrong, &id)
            .await
            .unwrap()
            .payload,
        bytes,
        "restart did not retain downloaded originals"
    );
    assert_eq!(
        wrong.calls.load(Ordering::SeqCst),
        1,
        "durable media unexpectedly downloaded again"
    );
    let audio = wa::Message {
        audio_message: Some(Box::new(wa::message::AudioMessage::default())),
        ..Default::default()
    };
    let payload =
        serde_json::to_vec(&serde_json::json!({"info":info,"protobuf":audio.encode_to_vec()}))
            .unwrap();
    let unsupported = resumed
        .store()
        .persist_inbound_envelope("decoded", "raw-b", &payload)
        .await
        .unwrap();
    assert_eq!(
        resumed
            .store()
            .private_source(&unsupported)
            .await
            .unwrap()
            .kind,
        "unsupported"
    );
    assert!(resumed
        .store()
        .download_private(&wire, &unsupported)
        .await
        .is_err());
    assert_eq!(wire.calls.load(Ordering::SeqCst), 1);
    assert!(resumed
        .store()
        .pending_inbound_envelopes(100)
        .await
        .unwrap()
        .iter()
        .any(|e| e.id == unsupported));
    std::fs::remove_dir_all(db.parent().unwrap()).unwrap();
}
#[test]
fn ac_m0_11_ingress_media_stream_rejects_excess_without_allocating_it() {
    let mut buffer = MediaBuffer::new(8);
    buffer.write_all(b"original").unwrap();
    assert!(buffer.write_all(b"extra").is_err());
    assert_eq!(buffer.into_bytes(), b"original");
}

#[tokio::test]
async fn ac_m0_11_ingress_document_retention_preserves_the_existing_intake_limit() {
    // SAAS_SIMPLES internal/intake/store.go: the existing approved original-intake
    // boundary is 10 MiB. Protocol stanza bounds must not narrow that document limit.
    let db = path();
    let session = PrivateSession::open(&db, [0x74; 32]).unwrap();
    let bytes = vec![b'x'; 9 * 1024 * 1024];
    let message = wa::Message {
        document_message: Some(Box::new(wa::message::DocumentMessage {
            file_length: Some(bytes.len() as u64),
            file_sha256: Some(Sha256::digest(&bytes).to_vec()),
            ..Default::default()
        })),
        ..Default::default()
    };
    let payload = serde_json::to_vec(
        &serde_json::json!({"info":MessageInfo::default(),"protobuf":message.encode_to_vec()}),
    )
    .unwrap();
    let id = session
        .store()
        .persist_inbound_envelope("decoded", "raw-large", &payload)
        .await
        .unwrap();
    let wire = Download {
        bytes: bytes.clone(),
        calls: AtomicUsize::new(0),
    };
    let media = session.store().download_private(&wire, &id).await.unwrap();
    assert_eq!(media.payload, bytes);
    std::fs::remove_dir_all(db.parent().unwrap()).unwrap();
}
