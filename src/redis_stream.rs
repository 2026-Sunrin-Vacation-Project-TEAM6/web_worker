use std::time::Duration;

use redis::aio::ConnectionManager;
use redis::RedisResult;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

pub async fn connect(redis_url: &str) -> RedisResult<ConnectionManager> {
    let client = redis::Client::open(redis_url)?;
    ConnectionManager::new(client).await
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
) -> RedisResult<String> {
    let entry_id: String = redis::cmd("XADD")
        .arg(stream_key)
        .arg("*")
        .arg("message")
        .arg(message)
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
                    for (entry_id, message) in messages {
                        info!("stream message {}: {}", entry_id, message);
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

fn parse_stream_messages(value: &redis::Value) -> Option<Vec<(String, String)>> {
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
                                if let Some(message) = extract_message_field(items) {
                                    results.push((entry_id, message));
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

fn extract_message_field(items: &[redis::Value]) -> Option<String> {
    let fields = items.get(1)?;
    if let redis::Value::Array(pairs) = fields {
        let mut iter = pairs.iter();
        while let Some(key) = iter.next() {
            let value = iter.next()?;
            if let redis::Value::BulkString(key_bytes) = key {
                if key_bytes == b"message" {
                    if let redis::Value::BulkString(value_bytes) = value {
                        return Some(String::from_utf8_lossy(value_bytes).to_string());
                    }
                }
            }
        }
    }
    None
}
