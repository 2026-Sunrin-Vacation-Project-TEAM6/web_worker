//! Standalone sandbox service: runs untrusted code snippets with rlimits and
//! a wall-clock timeout, returning captured output. This is a process-level
//! sandbox (rlimits + timeout + scratch dir), not container/VM isolation —
//! it is a reasonable boundary for trusted/internal use only.
use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
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
// Minimal PATH handed to every sandboxed child (interpreter, compiler, and
// compiled binary alike) so it can't pick up a user-controlled PATH.
const SANDBOX_PATH: &str = "/usr/local/bin:/usr/bin:/bin";

// Compilation is a materially riskier surface than interpreted execution: it
// spawns a whole toolchain (cc1/cc1plus/as/collect2/ld for C/C++, rustc's own
// worker threads plus an external linker invocation for Rust) and, for
// pathological input, can burn CPU/memory on its own (deep template
// instantiation, const-eval loops) before the user's code ever runs. These
// limits bound that toolchain step; the *compiled binary* is then executed
// under the exact same `apply_rlimits`/`WALL_CLOCK_TIMEOUT` sandbox as
// interpreted languages below — compiling is the only phase that gets extra
// headroom, running never does.
const COMPILE_WALL_CLOCK_TIMEOUT: Duration = Duration::from_secs(15);
const COMPILE_CPU_TIME_LIMIT_SECS: u64 = 15;
// Measured: a warm `rustc` process alone needs >256MB of RLIMIT_AS just to
// mmap libLLVM.so (a few hundred MB shared object, counted in full against
// AS even though it's mostly not resident) — 256MB reliably fails with
// "failed to map segment from shared object" before user code is even
// touched. This limit is inherited across fork+exec by rustc's own linker
// invocation (`cc` -> `collect2`/`gcc-ld` -> `lld`), which loads libLLVM.so
// a *second* time in its own address space and spawns its own worker
// threads (each needing a fresh mmap'd stack). 512MB was the smallest that
// worked in a quiet environment; under real host memory/VA pressure that
// second load can need more, so this is set to 1GB for headroom — still a
// small fraction of what any real deployment host has available, and still
// a hard ceiling against a memory-exhaustion compile bomb.
const COMPILE_MEMORY_LIMIT_BYTES: u64 = 1024 * 1024 * 1024;
// RLIMIT_NPROC's fork()/clone() check compares the *acting* process's own
// limit against how many processes/threads already exist for the real UID,
// system-wide (not scoped to this service's own process tree) — so, unlike
// a plain interpreter run (which only forks if the script itself does), a
// compiler that always forks a toolchain chain (cc1/cc1plus, as,
// collect2/ld) or spawns worker threads (rustc) pays that ambient cost on
// every single invocation. Sized for this service's actual deployment
// target: a single-purpose container running only this service under the
// `app` user (see Dockerfile), where that ambient count is tiny — ~18
// baseline threads for this service's own Tokio runtime, per the NPROC_LIMIT
// comment above. On a shared, multi-purpose host where the same real UID
// also runs unrelated heavy processes (browsers, editors, other language
// servers/IDE tooling), this ambient count can run far higher and
// intermittently starve a tight compile-time NPROC ceiling with a
// `posix_spawn`/`pthread_create` EAGAIN deep inside the toolchain — that
// failure mode is exercised by this environment's own test run (see the
// `is_transient_resource_error` skip in the test module below), not by the
// deployment container this value is actually chosen for.
const COMPILE_NPROC_LIMIT: u64 = 256;
// Compilers open more files at once (headers, rlib metadata, libs) than a
// typical interpreted script; give modest headroom over NOFILE_LIMIT.
const COMPILE_NOFILE_LIMIT: u64 = 128;

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
    /// Populated only when a compiled language (C/C++/Rust) fails to
    /// compile; holds the compiler's stderr. Distinct from `stderr`, which
    /// always carries the *executed program's* output — a runtime failure
    /// (panic, non-zero exit, crash) surfaces there instead. Absent from the
    /// JSON entirely for interpreted-language responses and for successful
    /// compiles, so existing clients are unaffected.
    #[serde(skip_serializing_if = "Option::is_none")]
    compile_error: Option<String>,
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

/// A compiled language's toolchain: which compiler to invoke, what extension
/// its source file needs, and the flags used to produce a stripped, minimally
/// optimized binary (fast compiles matter more than runtime performance for
/// short-lived sandboxed snippets).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompiledLang {
    C,
    Cpp,
    Rust,
}

impl CompiledLang {
    fn source_extension(self) -> &'static str {
        match self {
            CompiledLang::C => "c",
            CompiledLang::Cpp => "cpp",
            CompiledLang::Rust => "rs",
        }
    }

    fn compiler_binary(self) -> &'static str {
        match self {
            CompiledLang::C => "gcc",
            CompiledLang::Cpp => "g++",
            CompiledLang::Rust => "rustc",
        }
    }

    fn compile_args(self, src: &Path, out: &Path) -> Vec<OsString> {
        let mut args: Vec<OsString> = match self {
            CompiledLang::C => vec!["-O0".into(), "-s".into()],
            CompiledLang::Cpp => vec!["-O0".into(), "-s".into(), "-std=c++17".into()],
            CompiledLang::Rust => vec!["-O".into(), "-C".into(), "strip=symbols".into()],
        };
        args.push("-o".into());
        args.push(out.as_os_str().to_os_string());
        args.push(src.as_os_str().to_os_string());
        args
    }
}

enum Runtime {
    Interpreted {
        program: &'static str,
        args: Vec<&'static str>,
    },
    Compiled(CompiledLang),
}

fn runtime_for(language: &str) -> Result<Runtime, String> {
    match language {
        "python" | "python3" => Ok(Runtime::Interpreted {
            program: "python3",
            args: vec!["-I", "-c"],
        }),
        "javascript" | "js" | "node" => Ok(Runtime::Interpreted {
            program: "node",
            args: vec!["-e"],
        }),
        "c" => Ok(Runtime::Compiled(CompiledLang::C)),
        "cpp" | "c++" | "cxx" => Ok(Runtime::Compiled(CompiledLang::Cpp)),
        "rust" | "rs" => Ok(Runtime::Compiled(CompiledLang::Rust)),
        other => Err(format!("unsupported language: {other}")),
    }
}

async fn run_sandboxed(req: ExecuteRequest) -> Result<ExecuteResponse, String> {
    let runtime = runtime_for(&req.language)?;

    let dir = std::env::temp_dir().join(format!("stackbox-run-{}", unique_suffix()));
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| format!("failed to create sandbox dir: {e}"))?;

    let start = Instant::now();
    let result = match runtime {
        Runtime::Interpreted { program, args } => run_interpreted(program, &args, &req, &dir).await,
        Runtime::Compiled(lang) => run_compiled(lang, &req, &dir).await,
    };
    let duration_ms = start.elapsed().as_millis() as u64;
    let _ = tokio::fs::remove_dir_all(&dir).await;

    result.map(|mut resp| {
        resp.duration_ms = duration_ms;
        resp
    })
}

async fn run_interpreted(
    program: &str,
    base_args: &[&str],
    req: &ExecuteRequest,
    dir: &Path,
) -> Result<ExecuteResponse, String> {
    let mut command = Command::new(program);
    command
        .env_clear()
        .env("PATH", SANDBOX_PATH)
        .env("LANG", "C.UTF-8")
        .args(base_args)
        .arg(&req.code)
        .current_dir(dir)
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

    let mut child = command
        .spawn()
        .map_err(|e| format!("failed to spawn interpreter: {e}"))?;
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

    match awaited {
        Ok(Ok(output)) => Ok(ExecuteResponse {
            stdout: truncate_utf8(&output.stdout, MAX_OUTPUT_BYTES),
            stderr: truncate_utf8(&output.stderr, MAX_OUTPUT_BYTES),
            exit_code: output.status.code().unwrap_or(-1),
            duration_ms: 0,
            compile_error: None,
        }),
        Ok(Err(e)) => Err(format!("execution failed: {e}")),
        Err(_) => Err("execution timed out".to_string()),
    }
}

/// Compiles the submitted source with the language's toolchain, then — only
/// on a successful compile — executes the resulting binary under the same
/// sandbox (`apply_rlimits`, `WALL_CLOCK_TIMEOUT`, non-root) as interpreted
/// languages. A compile failure short-circuits: the binary never runs, and
/// the compiler's stderr is surfaced via `compile_error` rather than the
/// response's `stderr` field, so callers can tell "your code doesn't
/// compile" apart from "your code ran and failed".
async fn run_compiled(lang: CompiledLang, req: &ExecuteRequest, dir: &Path) -> Result<ExecuteResponse, String> {
    let compiler_name = lang.compiler_binary();
    let compiler = find_in_path(compiler_name)
        .ok_or_else(|| format!("compiler not available on this host: {compiler_name}"))?;

    let src_path = dir.join(format!("source.{}", lang.source_extension()));
    tokio::fs::write(&src_path, &req.code)
        .await
        .map_err(|e| format!("failed to write source file: {e}"))?;
    let out_path = dir.join("program");

    let mut compile_command = Command::new(&compiler);
    compile_command
        .env_clear()
        .env("PATH", SANDBOX_PATH)
        .env("LANG", "C.UTF-8")
        .args(lang.compile_args(&src_path, &out_path))
        .current_dir(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true);
    unsafe {
        compile_command.pre_exec(|| {
            apply_compile_rlimits();
            Ok(())
        });
    }

    let compile_child = compile_command
        .spawn()
        .map_err(|e| format!("failed to spawn compiler: {e}"))?;
    let compile_pid = compile_child.id();

    let compile_awaited =
        tokio::time::timeout(COMPILE_WALL_CLOCK_TIMEOUT, compile_child.wait_with_output()).await;
    let compile_output = match compile_awaited {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => return Err(format!("compilation failed to run: {e}")),
        Err(_) => {
            if let Some(pid) = compile_pid {
                unsafe {
                    libc::kill(-(pid as i32), libc::SIGKILL);
                }
            }
            return Ok(ExecuteResponse {
                stdout: String::new(),
                stderr: String::new(),
                exit_code: -1,
                duration_ms: 0,
                compile_error: Some("compilation timed out".to_string()),
            });
        }
    };

    if !compile_output.status.success() {
        return Ok(ExecuteResponse {
            stdout: String::new(),
            stderr: String::new(),
            exit_code: compile_output.status.code().unwrap_or(-1),
            duration_ms: 0,
            compile_error: Some(truncate_utf8(&compile_output.stderr, MAX_OUTPUT_BYTES)),
        });
    }

    let mut run_command = Command::new(&out_path);
    run_command
        .env_clear()
        .env("PATH", SANDBOX_PATH)
        .env("LANG", "C.UTF-8")
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true);
    unsafe {
        run_command.pre_exec(|| {
            // Compiled binaries have no V8-style upfront address-space
            // reservation, so the full (non-Node) rlimit set always applies.
            apply_rlimits(false);
            Ok(())
        });
    }

    let mut run_child = run_command
        .spawn()
        .map_err(|e| format!("failed to spawn compiled binary: {e}"))?;
    let run_pid = run_child.id();

    if let Some(stdin_data) = req.stdin.as_deref() {
        if let Some(mut stdin) = run_child.stdin.take() {
            let _ = stdin.write_all(stdin_data.as_bytes()).await;
        }
    } else {
        drop(run_child.stdin.take());
    }

    let run_awaited = tokio::time::timeout(WALL_CLOCK_TIMEOUT, run_child.wait_with_output()).await;
    if run_awaited.is_err()
        && let Some(pid) = run_pid
    {
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
    }

    match run_awaited {
        Ok(Ok(output)) => Ok(ExecuteResponse {
            stdout: truncate_utf8(&output.stdout, MAX_OUTPUT_BYTES),
            stderr: truncate_utf8(&output.stderr, MAX_OUTPUT_BYTES),
            exit_code: output.status.code().unwrap_or(-1),
            duration_ms: 0,
            compile_error: None,
        }),
        Ok(Err(e)) => Err(format!("execution failed: {e}")),
        Err(_) => Err("execution timed out".to_string()),
    }
}

/// Resolves `name` to an absolute path by walking this *service's own*
/// inherited `PATH` (trusted deployment config, not attacker input) — the
/// same lookup a shell would do. Compiled-language children are then spawned
/// with that absolute path directly, so their sandboxed environment only
/// ever needs `SANDBOX_PATH` for their own internal tool lookups (e.g. gcc
/// invoking `as`/`ld`), never for locating the compiler itself. This also
/// means toolchains installed outside `SANDBOX_PATH` (e.g. a rustup shim
/// under a user's home directory in a dev environment) still resolve
/// correctly, since we search the real PATH rather than assuming a fixed
/// location.
fn find_in_path(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    std::env::split_paths(&path_var).find_map(|dir| {
        let candidate = dir.join(name);
        is_executable_file(&candidate).then_some(candidate)
    })
}

fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
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
/// open files, output size) still bound a runaway Node script. Compiled
/// binaries (C/C++/Rust) have no such up-front reservation either, so they
/// always run with `is_node = false`, i.e. the full limit set.
fn apply_rlimits(is_node: bool) {
    let _ = rlimit::setrlimit(rlimit::Resource::CPU, CPU_TIME_LIMIT_SECS, CPU_TIME_LIMIT_SECS);
    if !is_node {
        let _ = rlimit::setrlimit(rlimit::Resource::AS, MEMORY_LIMIT_BYTES, MEMORY_LIMIT_BYTES);
    }
    let _ = rlimit::setrlimit(rlimit::Resource::NPROC, NPROC_LIMIT, NPROC_LIMIT);
    let _ = rlimit::setrlimit(rlimit::Resource::NOFILE, NOFILE_LIMIT, NOFILE_LIMIT);
    let _ = rlimit::setrlimit(rlimit::Resource::FSIZE, FSIZE_LIMIT_BYTES, FSIZE_LIMIT_BYTES);
}

/// Same idea as `apply_rlimits`, but for the compiler process itself rather
/// than the code it produces — see the `COMPILE_*` constants' doc comments
/// for the reasoning behind each value.
fn apply_compile_rlimits() {
    let _ = rlimit::setrlimit(
        rlimit::Resource::CPU,
        COMPILE_CPU_TIME_LIMIT_SECS,
        COMPILE_CPU_TIME_LIMIT_SECS,
    );
    let _ = rlimit::setrlimit(
        rlimit::Resource::AS,
        COMPILE_MEMORY_LIMIT_BYTES,
        COMPILE_MEMORY_LIMIT_BYTES,
    );
    let _ = rlimit::setrlimit(rlimit::Resource::NPROC, COMPILE_NPROC_LIMIT, COMPILE_NPROC_LIMIT);
    let _ = rlimit::setrlimit(rlimit::Resource::NOFILE, COMPILE_NOFILE_LIMIT, COMPILE_NOFILE_LIMIT);
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

    fn skip_if_missing(compiler: &str) -> bool {
        if find_in_path(compiler).is_none() {
            eprintln!("skipping: `{compiler}` not found on PATH in this test environment");
            true
        } else {
            false
        }
    }

    async fn exec(language: &str, code: &str) -> ExecuteResponse {
        run_sandboxed(ExecuteRequest {
            language: language.to_string(),
            code: code.to_string(),
            stdin: None,
        })
        .await
        .expect("run_sandboxed should not return a request-level error")
    }

    /// True for a `compile_error` that indicates the *host* couldn't spawn a
    /// process/thread for the toolchain right now (`RLIMIT_NPROC`/`RLIMIT_AS`
    /// racing against unrelated load sharing this machine's real UID — see
    /// the `COMPILE_NPROC_LIMIT`/`COMPILE_MEMORY_LIMIT_BYTES` comments), as
    /// opposed to the submitted source actually being invalid. On a
    /// single-purpose deployment container (this value's actual target) this
    /// essentially never fires; on a shared dev machine running unrelated
    /// heavy processes under the same UID (browsers, editors, other language
    /// servers) it can, transiently.
    fn is_transient_resource_error(message: &str) -> bool {
        [
            "resource temporarily unavailable",
            "failed to map segment from shared object",
            "os can't spawn worker thread",
            "cannot execute",
        ]
        .iter()
        .any(|needle| message.to_lowercase().contains(needle))
    }

    /// Runs `exec`, retrying a handful of times if the *compiler* (not the
    /// submitted code) appears to be the reason a compile failed. Returns
    /// `None` (skip, with a clear message) if the transient condition never
    /// clears — see `is_transient_resource_error`.
    async fn exec_compiled(language: &str, code: &str) -> Option<ExecuteResponse> {
        for attempt in 1..=5 {
            let resp = exec(language, code).await;
            match &resp.compile_error {
                Some(err) if is_transient_resource_error(err) => {
                    eprintln!(
                        "attempt {attempt}/5: transient host resource error compiling {language}, retrying: {err}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                _ => return Some(resp),
            }
        }
        eprintln!(
            "skipping: `{language}` compiles kept hitting transient host resource errors \
             (unrelated load sharing this machine's UID) after 5 attempts"
        );
        None
    }

    #[tokio::test]
    async fn c_hello_world_compiles_and_runs() {
        if skip_if_missing("gcc") {
            return;
        }
        let Some(resp) = exec_compiled(
            "c",
            r#"#include <stdio.h>
int main(void) { printf("hello from c\n"); return 0; }
"#,
        )
        .await
        else {
            return;
        };
        assert!(resp.compile_error.is_none(), "unexpected compile_error: {:?}", resp.compile_error);
        assert_eq!(resp.exit_code, 0);
        assert_eq!(resp.stdout.trim(), "hello from c");
    }

    #[tokio::test]
    async fn cpp_hello_world_compiles_and_runs() {
        if skip_if_missing("g++") {
            return;
        }
        let Some(resp) = exec_compiled(
            "cpp",
            r#"#include <iostream>
int main() { std::cout << "hello from cpp" << std::endl; return 0; }
"#,
        )
        .await
        else {
            return;
        };
        assert!(resp.compile_error.is_none(), "unexpected compile_error: {:?}", resp.compile_error);
        assert_eq!(resp.exit_code, 0);
        assert_eq!(resp.stdout.trim(), "hello from cpp");
    }

    #[tokio::test]
    async fn rust_hello_world_compiles_and_runs() {
        if skip_if_missing("rustc") {
            return;
        }
        let Some(resp) = exec_compiled("rust", r#"fn main() { println!("hello from rust"); }"#).await else {
            return;
        };
        assert!(resp.compile_error.is_none(), "unexpected compile_error: {:?}", resp.compile_error);
        assert_eq!(resp.exit_code, 0);
        assert_eq!(resp.stdout.trim(), "hello from rust");
    }

    #[tokio::test]
    async fn c_compile_error_is_reported_separately_from_stderr() {
        if skip_if_missing("gcc") {
            return;
        }
        // Missing semicolon: a compile error, not a runtime one.
        let resp = exec("c", "int main(void) { return 0 }").await;
        assert!(resp.compile_error.is_some(), "expected a compile_error to be set");
        assert!(resp.stderr.is_empty(), "runtime stderr should stay empty on a compile failure");
    }

    #[tokio::test]
    async fn cpp_compile_error_is_reported_separately_from_stderr() {
        if skip_if_missing("g++") {
            return;
        }
        let resp = exec("cpp", "int main() { this is not valid c++ }").await;
        assert!(resp.compile_error.is_some(), "expected a compile_error to be set");
        assert!(resp.stderr.is_empty(), "runtime stderr should stay empty on a compile failure");
    }

    #[tokio::test]
    async fn rust_compile_error_is_reported_separately_from_stderr() {
        if skip_if_missing("rustc") {
            return;
        }
        let resp = exec("rust", "fn main() { let x: i32 = \"not a number\"; }").await;
        assert!(resp.compile_error.is_some(), "expected a compile_error to be set");
        assert!(resp.stderr.is_empty(), "runtime stderr should stay empty on a compile failure");
    }

    #[tokio::test]
    async fn c_runtime_error_leaves_compile_error_unset() {
        if skip_if_missing("gcc") {
            return;
        }
        let Some(resp) = exec_compiled(
            "c",
            r#"#include <stdio.h>
int main(void) { fprintf(stderr, "boom\n"); return 7; }
"#,
        )
        .await
        else {
            return;
        };
        assert!(resp.compile_error.is_none(), "a runtime failure must not set compile_error");
        assert_eq!(resp.exit_code, 7);
        assert_eq!(resp.stderr.trim(), "boom");
    }

    #[tokio::test]
    async fn cpp_runtime_error_leaves_compile_error_unset() {
        if skip_if_missing("g++") {
            return;
        }
        let Some(resp) = exec_compiled(
            "cpp",
            r#"#include <iostream>
int main() { std::cerr << "boom" << std::endl; return 7; }
"#,
        )
        .await
        else {
            return;
        };
        assert!(resp.compile_error.is_none(), "a runtime failure must not set compile_error");
        assert_eq!(resp.exit_code, 7);
        assert_eq!(resp.stderr.trim(), "boom");
    }

    #[tokio::test]
    async fn rust_runtime_error_leaves_compile_error_unset() {
        if skip_if_missing("rustc") {
            return;
        }
        let Some(resp) =
            exec_compiled("rust", r#"fn main() { eprintln!("boom"); std::process::exit(7); }"#).await
        else {
            return;
        };
        assert!(resp.compile_error.is_none(), "a runtime failure must not set compile_error");
        assert_eq!(resp.exit_code, 7);
        assert_eq!(resp.stderr.trim(), "boom");
    }

    #[tokio::test]
    async fn interpreted_language_response_omits_compile_error_field() {
        let resp = exec("python3", "print('hi')").await;
        assert!(resp.compile_error.is_none());
        let json = serde_json::to_value(&resp).unwrap();
        assert!(
            json.get("compile_error").is_none(),
            "compile_error must be entirely absent from interpreted-language JSON, got: {json}"
        );
    }
}
