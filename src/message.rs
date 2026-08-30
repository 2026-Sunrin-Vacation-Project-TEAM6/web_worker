use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

/// Structured messages a client may send over a room's WebSocket.
/// Mirrors the `doc_updates` (CRDT) and `canvas_presence` (cursor) tables.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    DocUpdate {
        /// Base64-encoded Yjs update blob, matches doc_updates.blob.
        blob: String,
    },
    Presence {
        cursor_x: Option<f64>,
        cursor_y: Option<f64>,
        selection: Option<Value>,
        color: Option<String>,
        /// Stamped by the server with the sender's user id before broadcast
        /// so peers know whose cursor this is; absent on client-sent frames.
        #[serde(default)]
        user_id: Option<i64>,
    },
    /// Broadcast of a code block's execution result. Published by the backend
    /// after it calls the code_runner sandbox and persists a CodeRun row, so
    /// this variant is never persisted again here — it's relay-only.
    CodeResult {
        block_id: i64,
        stdout: String,
        stderr: String,
        #[serde(default)]
        compile_error: Option<String>,
        exit_code: i32,
        duration_ms: u64,
    },
}

pub fn parse_client_message(raw: &str) -> Result<ClientMessage, serde_json::Error> {
    serde_json::from_str(raw)
}

/// Re-serializes a `Presence` message with the sender's user id stamped in,
/// so peers receiving the broadcast know whose cursor it is. Other variants
/// are returned unchanged (as their original wire text).
pub fn stamp_presence_sender(
    raw: &str,
    message: ClientMessage,
    sender_user_id: Option<i64>,
) -> String {
    match message {
        ClientMessage::Presence {
            cursor_x,
            cursor_y,
            selection,
            color,
            ..
        } => serde_json::to_string(&ClientMessage::Presence {
            cursor_x,
            cursor_y,
            selection,
            color,
            user_id: sender_user_id,
        })
        .unwrap_or_else(|_| raw.to_string()),
        _ => raw.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presence_without_user_id_parses_with_none() {
        let raw = r#"{"type":"presence","cursor_x":1.0,"cursor_y":2.0,"selection":null,"color":"red"}"#;
        let parsed = parse_client_message(raw).unwrap();
        match parsed {
            ClientMessage::Presence { user_id, .. } => assert_eq!(user_id, None),
            _ => panic!("expected Presence"),
        }
    }

    #[test]
    fn stamp_presence_sender_overwrites_client_supplied_user_id() {
        let raw = r#"{"type":"presence","cursor_x":1.0,"cursor_y":2.0,"selection":null,"color":"red","user_id":999}"#;
        let parsed = parse_client_message(raw).unwrap();
        let stamped = stamp_presence_sender(raw, parsed, Some(42));
        let restamped = parse_client_message(&stamped).unwrap();
        match restamped {
            ClientMessage::Presence { user_id, .. } => assert_eq!(user_id, Some(42)),
            _ => panic!("expected Presence"),
        }
    }

    #[test]
    fn stamp_presence_sender_leaves_other_variants_untouched() {
        let raw = r#"{"type":"doc_update","blob":"YWJj"}"#;
        let parsed = parse_client_message(raw).unwrap();
        let stamped = stamp_presence_sender(raw, parsed, Some(7));
        assert_eq!(stamped, raw);
    }

    #[test]
    fn stamp_presence_sender_with_none_sender_clears_user_id() {
        let raw = r#"{"type":"presence","cursor_x":null,"cursor_y":null,"selection":null,"color":null,"user_id":5}"#;
        let parsed = parse_client_message(raw).unwrap();
        let stamped = stamp_presence_sender(raw, parsed, None);
        let restamped = parse_client_message(&stamped).unwrap();
        match restamped {
            ClientMessage::Presence { user_id, .. } => assert_eq!(user_id, None),
            _ => panic!("expected Presence"),
        }
    }
}
