//! Standalone sandbox service: runs untrusted code snippets with rlimits and
//! a wall-clock timeout, returning captured output. This is a process-level
//! sandbox (rlimits + timeout + scratch dir), not container/VM isolation —
//! it is a reasonable boundary for trusted/internal use only.
use std::net::SocketAddr;
use std::process::Stdio;
use std::time::{Duration, Instant};

use axum::extract::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

const MAX_OUTPUT_BYTES: usize = 64 * 1024;
const WALL_CLOCK_TIMEOUT: Duration = Duration::from_secs(10);
const CPU_TIME_LIMIT_SECS: u64 = 5;
const MEMORY_LIMIT_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Deserialize)]
struct ExecuteRequest {
    language: String,
    code: String,
    #[serde(default)]
    stdin: Option<String>,
}

#[derive(Debug, Serialize)]
struct ExecuteResponse {
    stdout: String,
    stderr: String,
    exit_code: i32,
    duration_ms: u64,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let host = std::env::var("CODE_RUNNER_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port: u16 = std::env::var("CODE_RUNNER_PORT")
        .unwrap_or_else(|_| "3001".to_string())
        .parse()
        .expect("CODE_RUNNER_PORT must be a valid port number");

    let app = Router::new()
        .route("/health", get(health))
        .route("/execute", post(execute));

    let addr: SocketAddr = format!("{host}:{port}").parse().expect("invalid bind address");
    tracing::info!("code_runner listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind code_runner listener");
    axum::serve(listener, app)
        .await
        .expect("code_runner server failed");
}

async fn health() -> &'static str {
    "ok"
}

async fn execute(Json(req): Json<ExecuteRequest>) -> impl IntoResponse {
    match run_sandboxed(req).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": err })),
        )
            .into_response(),
    }
}

fn interpreter_for(language: &str) -> Result<(&'static str, Vec<&'static str>), String> {
    match language {
        "python" | "python3" => Ok(("python3", vec!["-I", "-c"])),
        "javascript" | "js" | "node" => Ok(("node", vec!["-e"])),
        other => Err(format!("unsupported language: {other}")),
    }
}

async fn run_sandboxed(req: ExecuteRequest) -> Result<ExecuteResponse, String> {
    let (program, base_args) = interpreter_for(&req.language)?;

    let dir = std::env::temp_dir().join(format!("stackbox-run-{}", unique_suffix()));
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| format!("failed to create sandbox dir: {e}"))?;

    let mut command = Command::new(program);
    command
        .args(&base_args)
        .arg(&req.code)
        .current_dir(&dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    unsafe {
        command.pre_exec(|| {
            apply_rlimits();
            Ok(())
        });
    }

    let start = Instant::now();
    let spawn_result = command.spawn();
    let mut child = match spawn_result {
        Ok(child) => child,
        Err(e) => {
            let _ = tokio::fs::remove_dir_all(&dir).await;
            return Err(format!("failed to spawn interpreter: {e}"));
        }
    };

    if let Some(stdin_data) = req.stdin.as_deref() {
        if let Some(mut stdin) = child.stdin.take() {
            let _ = stdin.write_all(stdin_data.as_bytes()).await;
        }
    } else {
        drop(child.stdin.take());
    }

    let awaited = tokio::time::timeout(WALL_CLOCK_TIMEOUT, child.wait_with_output()).await;
    let _ = tokio::fs::remove_dir_all(&dir).await;
    let duration_ms = start.elapsed().as_millis() as u64;

    match awaited {
        Ok(Ok(output)) => Ok(ExecuteResponse {
            stdout: truncate_utf8(&output.stdout, MAX_OUTPUT_BYTES),
            stderr: truncate_utf8(&output.stderr, MAX_OUTPUT_BYTES),
            exit_code: output.status.code().unwrap_or(-1),
            duration_ms,
        }),
        Ok(Err(e)) => Err(format!("execution failed: {e}")),
        Err(_) => Err("execution timed out".to_string()),
    }
}

/// Applies CPU-time and address-space limits to the child process. Runs
/// after fork(), before exec() — must stay async-signal-safe.
fn apply_rlimits() {
    let _ = rlimit::setrlimit(rlimit::Resource::CPU, CPU_TIME_LIMIT_SECS, CPU_TIME_LIMIT_SECS);
    let _ = rlimit::setrlimit(rlimit::Resource::AS, MEMORY_LIMIT_BYTES, MEMORY_LIMIT_BYTES);
}

fn truncate_utf8(bytes: &[u8], max_bytes: usize) -> String {
    let slice = if bytes.len() > max_bytes {
        &bytes[..max_bytes]
    } else {
        bytes
    };
    String::from_utf8_lossy(slice).to_string()
}

fn unique_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{nanos:x}-{:x}", std::process::id())
}
