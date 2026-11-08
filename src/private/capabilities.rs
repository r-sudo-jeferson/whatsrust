//! Capability projection, never authority. Unknown fields remain visible.
use serde_json::Value;
use whatsapp_rust::types::events::Event;

pub struct EventPolicy {
    pub message_type: &'static str,
    pub handling: &'static str,
    pub reason: &'static str,
}

// No wildcard: upgrading the actual protocol Event enum requires an explicit
// custody/visibility decision before the connector can compile.
pub fn event_policy(event: &Event) -> EventPolicy {
    let kind = match event {
        Event::Connected(_) => "connected",
        Event::Disconnected(_) => "disconnected",
        Event::PairSuccess(_) => "pair_success",
        Event::PairError(_) => "pair_error",
        Event::LoggedOut(_) => "logged_out",
        Event::PairingQrCode { .. } => "pairing_qr",
        Event::PairingCode { .. } => "pairing_code",
        Event::QrScannedWithoutMultidevice(_) => "qr_scanned_without_multidevice",
        Event::ClientOutdated(_) => "client_outdated",
        Event::Message(_, _) => "message",
        Event::Receipt(_) => "receipt",
        Event::UndecryptableMessage(_) => "undecryptable_message",
        Event::Notification(_) => "notification",
        Event::ChatPresence(_) => "chat_presence",
        Event::Presence(_) => "presence",
        Event::PictureUpdate(_) => "picture_update",
        Event::UserAboutUpdate(_) => "user_about_update",
        Event::ContactUpdated(_) => "contact_updated",
        Event::ContactNumberChanged(_) => "contact_number_changed",
        Event::ContactSyncRequested(_) => "contact_sync_requested",
        Event::JoinedGroup(_) => "joined_group",
        Event::GroupUpdate(_) => "group_update",
        Event::ContactUpdate(_) => "contact_update",
        Event::PushNameUpdate(_) => "push_name_update",
        Event::SelfPushNameUpdated(_) => "self_push_name_updated",
        Event::PinUpdate(_) => "pin_update",
        Event::MuteUpdate(_) => "mute_update",
        Event::ArchiveUpdate(_) => "archive_update",
        Event::StarUpdate(_) => "star_update",
        Event::MarkChatAsReadUpdate(_) => "mark_chat_as_read_update",
        Event::DeleteChatUpdate(_) => "delete_chat_update",
        Event::DeleteMessageForMeUpdate(_) => "delete_message_for_me_update",
        Event::HistorySync(_) => "history_sync",
        Event::OfflineSyncPreview(_) => "offline_sync_preview",
        Event::OfflineSyncCompleted(_) => "offline_sync_completed",
        Event::DeviceListUpdate(_) => "device_list_update",
        Event::BusinessStatusUpdate(_) => "business_status_update",
        Event::StreamReplaced(_) => "stream_replaced",
        Event::TemporaryBan(_) => "temporary_ban",
        Event::ConnectFailure(_) => "connect_failure",
        Event::StreamError(_) => "stream_error",
        Event::DisappearingModeChanged(_) => "disappearing_mode_changed",
        Event::NewsletterLiveUpdate(_) => "newsletter_live_update",
    };
    event_type_policy(kind)
}

pub fn event_type_policy(kind: &str) -> EventPolicy {
    let (kind, handling, reason) = match kind {
        "pairing_qr" => ("pairing_qr", "handled", "private_pairing_material_omitted"),
        "pairing_code" => (
            "pairing_code",
            "handled",
            "private_pairing_material_omitted",
        ),
        "pair_error" => ("pair_error", "handled", "private_pairing_material_omitted"),
        "message" => ("message", "handled", "decoded_source_journal"),
        "receipt" => (
            "receipt",
            "handled",
            "receipt_retained_delivery_is_not_completion",
        ),
        "history_sync" => (
            "history_sync",
            "visible_unsupported",
            "history_import_unavailable",
        ),
        // Session handling remains the existing private state machine. Other
        // events are retained observations, never inferred company requests.
        other => {
            let known = match other {
                "connected" => "connected",
                "disconnected" => "disconnected",
                "pair_success" => "pair_success",
                "logged_out" => "logged_out",
                "qr_scanned_without_multidevice" => "qr_scanned_without_multidevice",
                "client_outdated" => "client_outdated",
                "undecryptable_message" => "undecryptable_message",
                "notification" => "notification",
                "chat_presence" => "chat_presence",
                "presence" => "presence",
                "picture_update" => "picture_update",
                "user_about_update" => "user_about_update",
                "contact_updated" => "contact_updated",
                "contact_number_changed" => "contact_number_changed",
                "contact_sync_requested" => "contact_sync_requested",
                "joined_group" => "joined_group",
                "group_update" => "group_update",
                "contact_update" => "contact_update",
                "push_name_update" => "push_name_update",
                "self_push_name_updated" => "self_push_name_updated",
                "pin_update" => "pin_update",
                "mute_update" => "mute_update",
                "archive_update" => "archive_update",
                "star_update" => "star_update",
                "mark_chat_as_read_update" => "mark_chat_as_read_update",
                "delete_chat_update" => "delete_chat_update",
                "delete_message_for_me_update" => "delete_message_for_me_update",
                "offline_sync_preview" => "offline_sync_preview",
                "offline_sync_completed" => "offline_sync_completed",
                "device_list_update" => "device_list_update",
                "business_status_update" => "business_status_update",
                "stream_replaced" => "stream_replaced",
                "temporary_ban" => "temporary_ban",
                "connect_failure" => "connect_failure",
                "stream_error" => "stream_error",
                "disappearing_mode_changed" => "disappearing_mode_changed",
                "newsletter_live_update" => "newsletter_live_update",
                _ => {
                    return EventPolicy {
                        message_type: "unknown_event",
                        handling: "visible_unsupported",
                        reason: "unmapped_protocol_event",
                    }
                }
            };
            (known, "visible_unsupported", "specialized_handling_pending")
        }
    };
    EventPolicy {
        message_type: kind,
        handling,
        reason,
    }
}
#[derive(serde::Serialize)]
pub struct FieldPolicy {
    pub field: String,
    pub message_type: String,
    pub handling: &'static str,
    pub reason: &'static str,
}
pub fn message_field_policy(field: &str) -> FieldPolicy {
    let (kind, reason) = match field {
        "conversation" | "extended_text_message" => ("text", "specialized_handling_pending"),
        "document_message" | "document_with_caption_message" => {
            ("document", "company_attribution_required")
        }
        "image_message" => ("image", "media_metadata_or_company_context_required"),
        "audio_message" => ("audio", "media_metadata_or_company_context_required"),
        "video_message" | "ptv_message" => ("video", "specialized_handling_pending"),
        "contact_message" | "contacts_array_message" => ("contact", "specialized_handling_pending"),
        "location_message" => ("location", "specialized_handling_pending"),
        "live_location_message" => ("live_location", "specialized_handling_pending"),
        "view_once_message"
        | "view_once_message_v2"
        | "view_once_message_v2_extension"
        | "conditional_reveal_message"
        | "ephemeral_message"
        | "keep_in_chat_message"
        | "limit_sharing_message" => (field, "retention_policy_required"),
        "message_context_info" => ("context", "context_metadata"),
        "protocol_message" => ("protocol_update", "specialized_handling_pending"),
        "sender_key_distribution_message" | "fast_ratchet_key_sender_key_distribution_message" => {
            ("encryption_metadata", "protocol_metadata")
        }
        "reaction_message" | "enc_reaction_message" => ("reaction", "specialized_handling_pending"),
        "edited_message" => ("edit", "specialized_handling_pending"),
        "group_mentioned_message" => ("group_mention", "specialized_handling_pending"),
        "sticker_message"
        | "lottie_sticker_message"
        | "sticker_sync_rmr_message"
        | "sticker_pack_message" => ("sticker", "specialized_handling_pending"),
        "poll_creation_message"
        | "poll_creation_message_v2"
        | "poll_creation_message_v3"
        | "poll_creation_message_v4"
        | "poll_creation_message_v5"
        | "poll_creation_message_v6"
        | "poll_creation_option_image_message"
        | "poll_add_option_message" => ("poll", "specialized_handling_pending"),
        "poll_update_message" => ("poll_vote", "specialized_handling_pending"),
        "poll_result_snapshot_message" | "poll_result_snapshot_message_v3" => {
            ("poll_result", "specialized_handling_pending")
        }
        "call"
        | "bcall_message"
        | "call_log_messsage"
        | "scheduled_call_creation_message"
        | "scheduled_call_edit_message" => ("call", "call_handling_unavailable"),
        "status_mention_message"
        | "group_status_mention_message"
        | "status_add_yours"
        | "group_status_message"
        | "group_status_message_v2"
        | "status_notification_message"
        | "status_question_answer_message"
        | "status_quoted_message"
        | "status_sticker_interaction_message" => ("status", "specialized_handling_pending"),
        "newsletter_admin_invite_message"
        | "newsletter_follower_invite_message_v2"
        | "newsletter_admin_profile_message"
        | "newsletter_admin_profile_message_v2" => ("newsletter", "specialized_handling_pending"),
        "chat"
        | "highly_structured_message"
        | "send_payment_message"
        | "request_payment_message"
        | "decline_payment_request_message"
        | "cancel_payment_request_message"
        | "template_message"
        | "group_invite_message"
        | "template_button_reply_message"
        | "product_message"
        | "device_sent_message"
        | "list_message"
        | "order_message"
        | "list_response_message"
        | "invoice_message"
        | "buttons_message"
        | "buttons_response_message"
        | "payment_invite_message"
        | "interactive_message"
        | "interactive_response_message"
        | "request_phone_number_message"
        | "pin_in_chat_message"
        | "bot_invoke_message"
        | "message_history_bundle"
        | "enc_comment_message"
        | "event_message"
        | "enc_event_response_message"
        | "comment_message"
        | "placeholder_message"
        | "secret_encrypted_message"
        | "album_message"
        | "event_cover_image"
        | "associated_child_message"
        | "rich_response_message"
        | "bot_task_message"
        | "question_message"
        | "message_history_notice"
        | "bot_forwarded_message"
        | "question_reply_message"
        | "question_response_message"
        | "spoiler_message"
        | "event_invite_message" => (field, "specialized_handling_pending"),
        _ => (field, "unmapped_schema_field"),
    };
    FieldPolicy {
        field: field.into(),
        message_type: kind.into(),
        handling: "visible_unsupported",
        reason,
    }
}
pub fn classify_message_fields(shape: &Value) -> Vec<FieldPolicy> {
    shape
        .as_object()
        .map(|fields| {
            fields
                .iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(name, _)| message_field_policy(name))
                .collect()
        })
        .unwrap_or_default()
}
