use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Structured messages a client may send over a room's WebSocket.
/// Mirrors the `doc_updates` (CRDT) and `canvas_presence` (cursor) tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    DocUpdate {
        /// Base64-encoded Yjs update blob, matches doc_updates.blob.
        blob: String,
    },
    Presence {
        user_id: i64,
        cursor_x: Option<f64>,
        cursor_y: Option<f64>,
        selection: Option<Value>,
        color: Option<String>,
    },
}

pub fn parse_client_message(raw: &str) -> Result<ClientMessage, serde_json::Error> {
    serde_json::from_str(raw)
}
