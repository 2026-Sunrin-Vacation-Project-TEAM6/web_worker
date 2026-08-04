use std::net::SocketAddr;
use std::time::Duration;

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::StreamExt;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let app = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws_handler));

    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    let addr: SocketAddr = format!("{}:{}", host, port).parse().expect("invalid address");

    info!("web_worker listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind failed");
    axum::serve(listener, app).await.expect("server failed");
}

async fn health() -> impl IntoResponse {
    "ok"
}

async fn ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
    info!("websocket client connected");

    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        info!("received: {}", text);
                        if socket
                            .send(Message::Text(format!("echo: {}", text).into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => {}
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
