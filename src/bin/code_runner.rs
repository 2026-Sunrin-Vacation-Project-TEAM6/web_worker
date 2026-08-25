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
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

const MAX_OUTPUT_BYTES: usize = 64 * 1024;
const WALL_CLOCK_TIMEOUT: Duration = Duration::from_secs(10);
const CPU_TIME_LIMIT_SECS: u64 = 5;
const CACHE_TTL_SECS: u64 = 3600;
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
    cache: Arc<RedisCodeCache>,
}

#[derive(Debug, Deserialize)]
struct ExecuteRequest {
    language: String,
    code: String,
    #[serde(default)]
    stdin: Option<String>,
    /// Optional caller-supplied identity (e.g. a user or workspace id) used
    /// to namespace the cache key so one caller's execution result can never
    /// be served to another. code_runner has no per-user auth today — it's
    /// authenticated by a single shared secret for the whole backend
    /// service (see `AUTH_HEADER`), and the current caller
    /// (backend/app/routers/code_exec.py) never sends one, so this is
    /// `None` in practice. The field exists so cache isolation is automatic
    /// the moment a caller-specific identity is threaded through, instead
    /// of silently sharing cache entries across callers.
    #[serde(default)]
    cache_namespace: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ExecuteResponse {
    stdout: String,
    stderr: String,
    exit_code: i32,
    duration_ms: u64,
    cache_key: String,
    cached: bool,
}

/// Abstraction over the code-execution result cache. Lets production code
/// use Redis while unit tests use an in-memory fake, so the caching control
/// flow (hit/miss, key sensitivity) is testable without a live Redis.
trait CodeResultCache: Send + Sync {
    /// Returns the cached, JSON-serialized `ExecuteResponse` for `key`, if any.
    async fn get(&self, key: &str) -> Option<String>;
    /// Stores `value` under `key`, expiring after `ttl_secs` seconds.
    async fn set(&self, key: &str, value: &str, ttl_secs: u64);
}

/// Redis-backed `CodeResultCache` used in production.
struct RedisCodeCache {
    conn: redis::aio::ConnectionManager,
}

impl CodeResultCache for RedisCodeCache {
    async fn get(&self, key: &str) -> Option<String> {
        let mut conn = self.conn.clone();
        match conn.get::<_, Option<String>>(key).await {
            Ok(value) => value,
            Err(e) => {
                tracing::warn!("code_runner cache get failed: {e}");
                None
            }
        }
    }

    async fn set(&self, key: &str, value: &str, ttl_secs: u64) {
        let mut conn = self.conn.clone();
        if let Err(e) = conn.set_ex::<_, _, ()>(key, value, ttl_secs).await {
            tracing::warn!("code_runner cache set failed: {e}");
        }
    }
}

/// Deterministically derives a cache key from the parts of a request that
/// affect its output, plus `namespace` for caller isolation (see
/// `ExecuteRequest::cache_namespace`). Each part is length-prefixed before
/// hashing so field boundaries can't be confused by concatenation (e.g.
/// language "ab" + code "c" must not hash the same as language "a" + code
/// "bc").
fn compute_cache_key(namespace: &str, language: &str, code: &str, stdin: &str) -> String {
    let mut hasher = Sha256::new();
    for part in [namespace, language, code, stdin] {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    let digest = hasher.finalize();
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    format!("code_runner:cache:{hex}")
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
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let redis_client =
        redis::Client::open(redis_url.as_str()).expect("REDIS_URL must be a valid connection string");
    let redis_conn = redis::aio::ConnectionManager::new(redis_client)
        .await
        .expect("failed to connect to redis for code_runner cache");

    let state = AppState {
        auth_token: Arc::new(auth_token),
        cache: Arc::new(RedisCodeCache { conn: redis_conn }),
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

    match execute_with_cache(state.cache.as_ref(), req).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": err })),
        )
            .into_response(),
    }
}

/// Checks `cache` for a prior result before running the sandbox, and stores
/// a fresh result on miss. Kept generic over `CodeResultCache` (rather than
/// hard-coding Redis) so this control flow — the part actually worth unit
/// testing — can run against an in-memory fake in tests.
async fn execute_with_cache<C: CodeResultCache>(
    cache: &C,
    req: ExecuteRequest,
) -> Result<ExecuteResponse, String> {
    let cache_key = compute_cache_key(
        req.cache_namespace.as_deref().unwrap_or(""),
        &req.language,
        &req.code,
        req.stdin.as_deref().unwrap_or(""),
    );

    if let Some(cached_json) = cache.get(&cache_key).await
        && let Ok(mut cached) = serde_json::from_str::<ExecuteResponse>(&cached_json)
    {
        cached.cached = true;
        return Ok(cached);
    }

    let mut response = run_sandboxed(req).await?;
    response.cached = false;
    response.cache_key = cache_key;
    if let Ok(serialized) = serde_json::to_string(&response) {
        cache.set(&response.cache_key, &serialized, CACHE_TTL_SECS).await;
    }
    Ok(response)
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
            // Filled in by `execute_with_cache` once the key is known; this
            // function only knows how to run code, not how it's cached.
            cache_key: String::new(),
            cached: false,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// In-memory `CodeResultCache` fake so the caching control flow can be
    /// exercised without a live Redis instance.
    #[derive(Default)]
    struct FakeCache {
        store: Mutex<HashMap<String, String>>,
    }

    impl CodeResultCache for FakeCache {
        async fn get(&self, key: &str) -> Option<String> {
            self.store.lock().unwrap().get(key).cloned()
        }

        async fn set(&self, key: &str, value: &str, _ttl_secs: u64) {
            self.store.lock().unwrap().insert(key.to_string(), value.to_string());
        }
    }

    fn make_request(
        language: &str,
        code: &str,
        stdin: Option<&str>,
        cache_namespace: Option<&str>,
    ) -> ExecuteRequest {
        ExecuteRequest {
            language: language.to_string(),
            code: code.to_string(),
            stdin: stdin.map(str::to_string),
            cache_namespace: cache_namespace.map(str::to_string),
        }
    }

    #[test]
    fn cache_key_is_deterministic() {
        let a = compute_cache_key("", "python", "print(1)", "");
        let b = compute_cache_key("", "python", "print(1)", "");
        assert_eq!(a, b);
    }

    #[test]
    fn cache_key_differs_by_code() {
        let a = compute_cache_key("", "python", "print(1)", "");
        let b = compute_cache_key("", "python", "print(2)", "");
        assert_ne!(a, b);
    }

    #[test]
    fn cache_key_differs_by_language() {
        let a = compute_cache_key("", "python", "print(1)", "");
        let b = compute_cache_key("", "javascript", "print(1)", "");
        assert_ne!(a, b);
    }

    #[test]
    fn cache_key_differs_by_stdin() {
        let a = compute_cache_key("", "python", "code", "one");
        let b = compute_cache_key("", "python", "code", "two");
        assert_ne!(a, b);
    }

    #[test]
    fn cache_key_differs_by_namespace() {
        // Simulates isolation between two users/workspaces running
        // identical code with identical stdin.
        let a = compute_cache_key("user-1", "python", "print(1)", "");
        let b = compute_cache_key("user-2", "python", "print(1)", "");
        assert_ne!(a, b);
    }

    #[test]
    fn cache_key_has_no_field_boundary_ambiguity() {
        // Without length-prefixing, ("ab", "c") and ("a", "bc") would hash
        // identically under naive concatenation.
        let a = compute_cache_key("", "ab", "c", "");
        let b = compute_cache_key("", "a", "bc", "");
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn cache_hit_short_circuits_execution_and_marks_cached() {
        let cache = FakeCache::default();
        let key = compute_cache_key("", "python", "this is not valid python", "");
        let stored = ExecuteResponse {
            stdout: "from-cache".to_string(),
            stderr: String::new(),
            exit_code: 0,
            duration_ms: 1,
            cache_key: key.clone(),
            cached: false,
        };
        cache
            .set(&key, &serde_json::to_string(&stored).unwrap(), 3600)
            .await;

        // If this ever fell through to execution, running invalid Python
        // would surface as an error or non-zero exit code, not this value.
        let resp = execute_with_cache(
            &cache,
            make_request("python", "this is not valid python", None, None),
        )
        .await
        .expect("cache hit should short-circuit execution");

        assert!(resp.cached);
        assert_eq!(resp.stdout, "from-cache");
        assert_eq!(resp.cache_key, key);
    }

    #[tokio::test]
    async fn cache_miss_executes_and_populates_cache() {
        let cache = FakeCache::default();
        let key = compute_cache_key("", "python", "print(21 * 2)", "");
        assert!(cache.get(&key).await.is_none());

        let first = execute_with_cache(&cache, make_request("python", "print(21 * 2)", None, None))
            .await
            .expect("execution should succeed");
        assert!(!first.cached);
        assert_eq!(first.stdout.trim(), "42");
        assert!(cache.get(&key).await.is_some());

        let second = execute_with_cache(&cache, make_request("python", "print(21 * 2)", None, None))
            .await
            .expect("second call should hit the now-populated cache");
        assert!(second.cached);
        assert_eq!(second.stdout, first.stdout);
    }

    #[tokio::test]
    async fn unsupported_language_is_not_cached() {
        let cache = FakeCache::default();
        let result = execute_with_cache(&cache, make_request("cobol", "PRINT 1", None, None)).await;
        assert!(result.is_err());

        let key = compute_cache_key("", "cobol", "PRINT 1", "");
        assert!(cache.get(&key).await.is_none());
    }
}
