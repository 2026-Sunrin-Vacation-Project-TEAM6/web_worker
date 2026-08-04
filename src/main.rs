mod config;
mod redis_stream;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::StreamExt;
use redis::aio::ConnectionManager;
use tokio::sync::broadcast;
use tracing::{error, info};

use config::Config;
use redis_stream::{connect, ensure_consumer_group, publish_message, run_consumer};

#[derive(Clone)]
struct AppState {
    redis: ConnectionManager,
    stream_key: String,
    outbound: broadcast::Sender<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let config = Config::from_env();
    let mut redis = connect(&config.redis_url)
        .await
        .expect("failed to connect redis");

    ensure_consumer_group(&mut redis, &config.stream_key, &config.consumer_group)
        .await
        .expect("failed to ensure consumer group");

    let (outbound, _) = broadcast::channel(256);
    let consumer_redis = connect(&config.redis_url)
        .await
        .expect("failed to connect redis for consumer");

    tokio::spawn(run_consumer(
        consumer_redis,
        config.stream_key.clone(),
        config.consumer_group.clone(),
        config.consumer_name.clone(),
        outbound.clone(),
    ));

    let state = Arc::new(AppState {
        redis,
        stream_key: config.stream_key.clone(),
        outbound,
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws_handler))
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .expect("invalid address");

    info!(
        "web_worker listening on {} (stream: {})",
        addr, config.stream_key
    );

    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind failed");
    axum::serve(listener, app).await.expect("server failed");
}

async fn health() -> impl IntoResponse {
    "ok"
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    info!("websocket client connected");

    let mut stream_rx = state.outbound.subscribe();
    let mut interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        tokio::select! {
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        info!("received: {}", text);
                        let mut redis = state.redis.clone();
                        match publish_message(&mut redis, &state.stream_key, &text).await {
                            Ok(entry_id) => info!("published to stream: {}", entry_id),
                            Err(err) => error!("failed to publish to stream: {}", err),
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
                        if socket.send(Message::Text(format!("stream: {}", message).into())).await.is_err() {
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

    info!("websocket client disconnected");
}
