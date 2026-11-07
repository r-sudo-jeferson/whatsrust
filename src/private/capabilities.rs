//! Capability projection, never authority. Unknown fields remain visible.
use serde_json::Value;
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
