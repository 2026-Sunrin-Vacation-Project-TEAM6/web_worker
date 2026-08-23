//! Standalone sandbox service: runs untrusted code snippets with rlimits and
//! a wall-clock timeout, returning captured output. This is a process-level
//! sandbox (rlimits + timeout + scratch dir), not container/VM isolation —
//! it is a reasonable boundary for trusted/internal use only.
use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Json, State};
use axum::http::{HeaderMap, StatusCode};
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
// RLIMIT_NPROC is enforced per real UID for the whole container, not just the
// spawned child: this service's own Tokio runtime alone already holds ~18
// threads under the `app` user before any code even runs. Node.js needs
// several more threads at startup (libuv threadpool + V8 platform threads),
// so 32 left no headroom and made every JS run crash with
// `uv_thread_create` assertion failures. Sized well above steady-state usage
// while still bounding a fork bomb.
const NPROC_LIMIT: u64 = 160;
const NOFILE_LIMIT: u64 = 64;
const FSIZE_LIMIT_BYTES: u64 = 10 * 1024 * 1024;
const AUTH_HEADER: &str = "x-code-runner-token";

#[derive(Clone)]
struct AppState {
    auth_token: Arc<String>,
}

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

    let host = std::env::var("CODE_RUNNER_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("CODE_RUNNER_PORT")
        .unwrap_or_else(|_| "3001".to_string())
        .parse()
        .expect("CODE_RUNNER_PORT must be a valid port number");
    let auth_token = std::env::var("CODE_RUNNER_AUTH_TOKEN").unwrap_or_default();
    if auth_token.is_empty() {
        panic!(
            "CODE_RUNNER_AUTH_TOKEN must be set to a non-empty shared secret \
             before code_runner can accept requests to /execute"
        );
    }
    let state = AppState {
        auth_token: Arc::new(auth_token),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/execute", post(execute))
        .with_state(state);

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

async fn execute(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ExecuteRequest>,
) -> impl IntoResponse {
    let provided = headers
        .get(AUTH_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !constant_time_eq(provided.as_bytes(), state.auth_token.as_bytes()) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "unauthorized" })),
        )
            .into_response();
    }

    match run_sandboxed(req).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": err })),
        )
            .into_response(),
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
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
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .env("LANG", "C.UTF-8")
        .args(&base_args)
        .arg(&req.code)
        .current_dir(&dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true);

    let is_node = program == "node";
    unsafe {
        command.pre_exec(move || {
            apply_rlimits(is_node);
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
    let pid = child.id();

    if let Some(stdin_data) = req.stdin.as_deref() {
        if let Some(mut stdin) = child.stdin.take() {
            let _ = stdin.write_all(stdin_data.as_bytes()).await;
        }
    } else {
        drop(child.stdin.take());
    }

    let awaited = tokio::time::timeout(WALL_CLOCK_TIMEOUT, child.wait_with_output()).await;
    if awaited.is_err() {
        // Timed out: the interpreter is its own process-group leader (see
        // `process_group(0)` above), so kill the whole group — not just the
        // leader — to catch any subprocesses it spawned before we give up.
        if let Some(pid) = pid {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
    }
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

/// Applies CPU-time, memory, process-count, open-file, and output-size
/// limits to the child process. Runs after fork(), before exec() — must
/// stay async-signal-safe.
///
/// The `AS` (virtual address space) limit is skipped for Node: V8 reserves a
/// large virtual region up front (the pointer-compression cage / JIT code
/// range, several GB) regardless of how much memory the script actually
/// touches, so any tight `RLIMIT_AS` makes V8 itself fail to start with
/// "Failed to reserve virtual memory for CodeRange" before user code ever
/// runs. Python has no equivalent up-front reservation, so it keeps the real
/// limit. The other limits (CPU time, wall-clock timeout, process count,
/// open files, output size) still bound a runaway Node script.
fn apply_rlimits(is_node: bool) {
    let _ = rlimit::setrlimit(rlimit::Resource::CPU, CPU_TIME_LIMIT_SECS, CPU_TIME_LIMIT_SECS);
    if !is_node {
        let _ = rlimit::setrlimit(rlimit::Resource::AS, MEMORY_LIMIT_BYTES, MEMORY_LIMIT_BYTES);
    }
    let _ = rlimit::setrlimit(rlimit::Resource::NPROC, NPROC_LIMIT, NPROC_LIMIT);
    let _ = rlimit::setrlimit(rlimit::Resource::NOFILE, NOFILE_LIMIT, NOFILE_LIMIT);
    let _ = rlimit::setrlimit(rlimit::Resource::FSIZE, FSIZE_LIMIT_BYTES, FSIZE_LIMIT_BYTES);
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
