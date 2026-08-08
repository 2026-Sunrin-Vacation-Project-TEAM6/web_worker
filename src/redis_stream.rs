use std::time::Duration;

use base64::Engine;
use redis::aio::ConnectionManager;
use redis::RedisResult;
use sqlx::PgPool;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::message::{self, ClientMessage};

pub async fn connect(redis_url: &str) -> RedisResult<ConnectionManager> {
    let client = redis::Client::open(redis_url)?;
    ConnectionManager::new(client).await
}

pub fn room_stream_key(prefix: &str, stack_box_id: i64) -> String {
    format!("{prefix}:{stack_box_id}")
}

pub async fn ensure_consumer_group(
    conn: &mut ConnectionManager,
    stream_key: &str,
    group: &str,
) -> RedisResult<()> {
    let result = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream_key)
        .arg(group)
        .arg("$")
        .arg("MKSTREAM")
        .query_async::<()>(conn)
        .await;

    match result {
        Ok(()) => {
            info!("created redis stream group: {} on {}", group, stream_key);
            Ok(())
        }
        Err(err) if err.to_string().contains("BUSYGROUP") => Ok(()),
        Err(err) => Err(err),
    }
}

pub async fn publish_message(
    conn: &mut ConnectionManager,
    stream_key: &str,
    message: &str,
    user_id: Option<i64>,
) -> RedisResult<String> {
    let entry_id: String = redis::cmd("XADD")
        .arg(stream_key)
        .arg("*")
        .arg("message")
        .arg(message)
        .arg("user_id")
        .arg(user_id.map(|v| v.to_string()).unwrap_or_default())
        .query_async(conn)
        .await?;
    Ok(entry_id)
}

pub async fn run_consumer(
    mut conn: ConnectionManager,
    stream_key: String,
    group: String,
    consumer: String,
    outbound: broadcast::Sender<String>,
    db: PgPool,
    stack_box_id: i64,
    mut next_seq: i64,
) {
    loop {
        let response: RedisResult<redis::Value> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&group)
            .arg(&consumer)
            .arg("COUNT")
            .arg(10)
            .arg("BLOCK")
            .arg(2000)
            .arg("STREAMS")
            .arg(&stream_key)
            .arg(">")
            .query_async(&mut conn)
            .await;

        match response {
            Ok(value) => {
                if let Some(messages) = parse_stream_messages(&value) {
                    for (entry_id, message, sender_user_id) in messages {
                        info!("stream message {}: {}", entry_id, message);

                        if let Ok(client_message) = message::parse_client_message(&message) {
                            if let Err(err) = persist_message(
                                &db,
                                stack_box_id,
                                client_message,
                                sender_user_id,
                                &mut next_seq,
                            )
                            .await
                            {
                                warn!(
                                    "failed to persist message in room {}: {}",
                                    stack_box_id, err
                                );
                            }
                        }

                        if outbound.receiver_count() > 0 {
                            let _ = outbound.send(message);
                        }

                        if let Err(err) = redis::cmd("XACK")
                            .arg(&stream_key)
                            .arg(&group)
                            .arg(&entry_id)
                            .query_async::<i64>(&mut conn)
                            .await
                        {
                            warn!("failed to ack {}: {}", entry_id, err);
                        }
                    }
                }
            }
            Err(err) => {
                error!("stream read failed: {}", err);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Persists a parsed client message to Postgres. Best-effort: failures are
/// returned to the caller to log, but never block the XACK/broadcast so a
/// transient DB issue can't stall the live relay.
async fn persist_message(
    db: &PgPool,
    stack_box_id: i64,
    message: ClientMessage,
    sender_user_id: Option<i64>,
    next_seq: &mut i64,
) -> Result<(), sqlx::Error> {
    match message {
        ClientMessage::DocUpdate { blob } => {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(&blob)
                .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
            let seq = *next_seq;
            sqlx::query(
                "INSERT INTO doc_updates (stack_box_id, blob, seq, created_by) VALUES ($1, $2, $3, $4)",
            )
            .bind(stack_box_id)
            .bind(bytes)
            .bind(seq)
            .bind(sender_user_id)
            .execute(db)
            .await?;
            *next_seq += 1;
            Ok(())
        }
        ClientMessage::Presence {
            user_id,
            cursor_x,
            cursor_y,
            selection,
            color,
        } => {
            sqlx::query(
                "INSERT INTO canvas_presence (stack_box_id, user_id, cursor_x, cursor_y, selection, color, last_seen_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, NOW()) \
                 ON CONFLICT (stack_box_id, user_id) DO UPDATE SET \
                 cursor_x = EXCLUDED.cursor_x, cursor_y = EXCLUDED.cursor_y, \
                 selection = EXCLUDED.selection, color = EXCLUDED.color, last_seen_at = NOW()",
            )
            .bind(stack_box_id)
            .bind(user_id)
            .bind(cursor_x)
            .bind(cursor_y)
            .bind(selection)
            .bind(color)
            .execute(db)
            .await?;
            Ok(())
        }
    }
}

fn parse_stream_messages(value: &redis::Value) -> Option<Vec<(String, String, Option<i64>)>> {
    let mut results = Vec::new();

    if let redis::Value::Array(streams) = value {
        for stream in streams {
            if let redis::Value::Array(stream_parts) = stream {
                let entries = stream_parts.get(1)?;
                if let redis::Value::Array(entry_list) = entries {
                    for entry in entry_list {
                        if let redis::Value::Array(items) = entry {
                            if let Some(redis::Value::BulkString(id_bytes)) = items.first() {
                                let entry_id = String::from_utf8_lossy(id_bytes).to_string();
                                if let Some(message) = extract_field(items, "message") {
                                    let sender_user_id = extract_field(items, "user_id")
                                        .and_then(|s| s.parse::<i64>().ok());
                                    results.push((entry_id, message, sender_user_id));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if results.is_empty() {
        None
    } else {
        Some(results)
    }
}

fn extract_field(items: &[redis::Value], field_name: &str) -> Option<String> {
    let fields = items.get(1)?;
    if let redis::Value::Array(pairs) = fields {
        let mut iter = pairs.iter();
        while let Some(key) = iter.next() {
            let value = iter.next()?;
            if let redis::Value::BulkString(key_bytes) = key {
                if key_bytes == field_name.as_bytes() {
                    if let redis::Value::BulkString(value_bytes) = value {
                        return Some(String::from_utf8_lossy(value_bytes).to_string());
                    }
                }
            }
        }
    }
    None
}
