mod config;
mod message;
mod redis_stream;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::StreamExt;
use redis::aio::ConnectionManager;
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use config::Config;
use redis_stream::{connect, ensure_consumer_group, publish_message, room_stream_key, run_consumer};

struct Room {
    tx: broadcast::Sender<String>,
    consumer_handle: JoinHandle<()>,
}

struct AppState {
    redis: ConnectionManager,
    db: PgPool,
    stream_prefix: String,
    consumer_group: String,
    consumer_name: String,
    rooms: Mutex<HashMap<i64, Room>>,
}

#[derive(Debug, Deserialize)]
struct WsParams {
    user_id: Option<i64>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let config = Config::from_env();
    let redis = connect(&config.redis_url)
        .await
        .expect("failed to connect redis");
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await
        .expect("failed to connect to postgres");

    let state = Arc::new(AppState {
        redis,
        db,
        stream_prefix: config.stream_prefix.clone(),
        consumer_group: config.consumer_group.clone(),
        consumer_name: config.consumer_name.clone(),
        rooms: Mutex::new(HashMap::new()),
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/ws/{stack_box_id}", get(ws_handler))
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .expect("invalid address");

    info!(
        "web_worker listening on {} (stream prefix: {})",
        addr, config.stream_prefix
    );

    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind failed");
    axum::serve(listener, app).await.expect("server failed");
}

async fn health() -> impl IntoResponse {
    "ok"
}

async fn ws_handler(
    Path(stack_box_id): Path<i64>,
    Query(params): Query<WsParams>,
    State(state): State<Arc<AppState>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state, stack_box_id, params.user_id))
}

/// Returns the room's broadcast sender, lazily creating the room (and its
/// backing Redis consumer group + consumer task) on first connection.
async fn get_or_create_room(state: &Arc<AppState>, stack_box_id: i64) -> broadcast::Sender<String> {
    let mut rooms = state.rooms.lock().await;
    if let Some(room) = rooms.get(&stack_box_id) {
        return room.tx.clone();
    }

    let (tx, _rx) = broadcast::channel(256);
    let stream_key = room_stream_key(&state.stream_prefix, stack_box_id);
    let mut conn = state.redis.clone();

    if let Err(err) = ensure_consumer_group(&mut conn, &stream_key, &state.consumer_group).await {
        error!(
            "failed to ensure consumer group for room {}: {}",
            stack_box_id, err
        );
    }

    let next_seq: i64 =
        sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0) FROM doc_updates WHERE stack_box_id = $1")
            .bind(stack_box_id)
            .fetch_one(&state.db)
            .await
            .unwrap_or_else(|err| {
                error!(
                    "failed to load starting seq for room {}: {}",
                    stack_box_id, err
                );
                0
            });

    let consumer_handle = tokio::spawn(run_consumer(
        conn,
        stream_key,
        state.consumer_group.clone(),
        state.consumer_name.clone(),
        tx.clone(),
        state.db.clone(),
        stack_box_id,
        next_seq,
    ));

    rooms.insert(
        stack_box_id,
        Room {
            tx: tx.clone(),
            consumer_handle,
        },
    );

    info!("room {} opened", stack_box_id);
    tx
}

/// Tears down a room's consumer task once its last client has disconnected,
/// so idle rooms don't accumulate forever-polling Redis consumer tasks.
async fn cleanup_room_if_empty(state: &Arc<AppState>, stack_box_id: i64) {
    let mut rooms = state.rooms.lock().await;
    if let Some(room) = rooms.get(&stack_box_id) {
        if room.tx.receiver_count() == 0 {
            if let Some(room) = rooms.remove(&stack_box_id) {
                room.consumer_handle.abort();
                info!("room {} closed", stack_box_id);
            }
        }
    }
}

async fn handle_socket(
    mut socket: WebSocket,
    state: Arc<AppState>,
    stack_box_id: i64,
    user_id: Option<i64>,
) {
    info!("websocket client connected to room {}", stack_box_id);

    let tx = get_or_create_room(&state, stack_box_id).await;
    let mut stream_rx = tx.subscribe();
    let stream_key = room_stream_key(&state.stream_prefix, stack_box_id);
    let mut interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        tokio::select! {
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        match message::parse_client_message(&text) {
                            Ok(_) => {
                                let mut redis = state.redis.clone();
                                match publish_message(&mut redis, &stream_key, &text, user_id).await {
                                    Ok(entry_id) => info!("published to stream: {}", entry_id),
                                    Err(err) => error!("failed to publish to stream: {}", err),
                                }
                            }
                            Err(err) => {
                                warn!("dropping malformed message in room {}: {}", stack_box_id, err);
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
            stream_msg = stream_rx.recv() => {
                match stream_msg {
                    Ok(message) => {
                        if socket.send(Message::Text(message.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = interval.tick() => {
                if socket.send(Message::Text("ping".into())).await.is_err() {
                    break;
                }
            }
        }
    }

    drop(stream_rx);
    cleanup_room_if_empty(&state, stack_box_id).await;
    info!("websocket client disconnected from room {}", stack_box_id);
}
