//! Standalone HTTP service: takes a natural-language prompt, asks an
//! OpenAI-compatible chat completions endpoint for a structured slide
//! outline (JSON mode), then renders that outline into a real `.pptx` file
//! and returns it as a binary HTTP response.
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Json, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use ppt_rs::SlideContent;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const AUTH_HEADER: &str = "x-ppt-builder-token";
const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
const OPENAI_TIMEOUT: Duration = Duration::from_secs(90);
const MIN_SLIDES: u32 = 1;
const MAX_SLIDES: u32 = 30;

#[derive(Clone)]
struct AppState {
    auth_token: Arc<String>,
    openai_api_key: Arc<String>,
    openai_base_url: Arc<String>,
    openai_model: Arc<String>,
    http_client: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct PptRequest {
    prompt: String,
    #[serde(default = "default_num_slides")]
    num_slides: u32,
}

fn default_num_slides() -> u32 {
    5
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Slide {
    title: String,
    #[serde(default)]
    bullets: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Outline {
    slides: Vec<Slide>,
}

/// Errors that can occur while handling a build request. Each variant maps
/// to a specific HTTP status code so callers get a meaningful signal instead
/// of a blanket 500.
#[derive(Debug, Error)]
enum BuildError {
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("timed out waiting for OpenAI")]
    UpstreamTimeout,
    #[error("rate limited by OpenAI: {0}")]
    RateLimited(String),
    #[error("OpenAI request failed: {0}")]
    UpstreamRequest(String),
    #[error("OpenAI returned an error response ({status}): {body}")]
    UpstreamStatus { status: u16, body: String },
    #[error("could not parse OpenAI response: {0}")]
    MalformedOutline(String),
    #[error("failed to build pptx: {0}")]
    PptxBuild(String),
}

impl BuildError {
    fn status_code(&self) -> StatusCode {
        match self {
            BuildError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            BuildError::UpstreamTimeout => StatusCode::GATEWAY_TIMEOUT,
            BuildError::RateLimited(_) => StatusCode::TOO_MANY_REQUESTS,
            BuildError::UpstreamRequest(_) | BuildError::UpstreamStatus { .. } => {
                StatusCode::BAD_GATEWAY
            }
            BuildError::MalformedOutline(_) => StatusCode::BAD_GATEWAY,
            BuildError::PptxBuild(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for BuildError {
    fn into_response(self) -> axum::response::Response {
        let status = self.status_code();
        tracing::error!("ppt_builder request failed: {}", self);
        (status, Json(serde_json::json!({ "error": self.to_string() }))).into_response()
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let host = std::env::var("PPT_BUILDER_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("PPT_BUILDER_PORT")
        .unwrap_or_else(|_| "3002".to_string())
        .parse()
        .expect("PPT_BUILDER_PORT must be a valid port number");

    let auth_token = std::env::var("PPT_BUILDER_AUTH_TOKEN").unwrap_or_default();
    if auth_token.is_empty() {
        panic!(
            "PPT_BUILDER_AUTH_TOKEN must be set to a non-empty shared secret \
             before ppt_builder can accept requests to /build"
        );
    }

    let openai_api_key = std::env::var("OPENAI_API_KEY").unwrap_or_default();
    if openai_api_key.is_empty() {
        panic!("OPENAI_API_KEY must be set before ppt_builder can accept requests to /build");
    }

    // Mirrors backend/app/ai_client.py: an empty OPENAI_BASE_URL falls back
    // to OpenAI's default endpoint, a non-empty value overrides it.
    let openai_base_url = std::env::var("OPENAI_BASE_URL").unwrap_or_default();
    let openai_base_url = if openai_base_url.is_empty() {
        DEFAULT_OPENAI_BASE_URL.to_string()
    } else {
        openai_base_url.trim_end_matches('/').to_string()
    };
    let openai_model = std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_string());

    let http_client = reqwest::Client::builder()
        .timeout(OPENAI_TIMEOUT)
        .build()
        .expect("failed to build reqwest client");

    let state = AppState {
        auth_token: Arc::new(auth_token),
        openai_api_key: Arc::new(openai_api_key),
        openai_base_url: Arc::new(openai_base_url),
        openai_model: Arc::new(openai_model),
        http_client,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/build", post(build))
        .with_state(state);

    let addr: SocketAddr = format!("{host}:{port}").parse().expect("invalid bind address");
    tracing::info!("ppt_builder listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind ppt_builder listener");
    axum::serve(listener, app)
        .await
        .expect("ppt_builder server failed");
}

async fn health() -> &'static str {
    "ok"
}

async fn build(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<PptRequest>,
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

    match build_presentation(&state, req).await {
        Ok(bytes) => (
            StatusCode::OK,
            [
                (
                    header::CONTENT_TYPE,
                    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                ),
                (
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"presentation.pptx\"",
                ),
            ],
            bytes,
        )
            .into_response(),
        Err(err) => err.into_response(),
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn build_presentation(state: &AppState, req: PptRequest) -> Result<Vec<u8>, BuildError> {
    validate_request(&req)?;

    let raw_response = request_outline_completion(
        &state.http_client,
        &state.openai_base_url,
        &state.openai_api_key,
        &state.openai_model,
        &req.prompt,
        req.num_slides,
    )
    .await?;

    let outline = parse_outline_response(&raw_response)?;
    build_pptx(&req.prompt, &outline)
}

fn validate_request(req: &PptRequest) -> Result<(), BuildError> {
    if req.prompt.trim().is_empty() {
        return Err(BuildError::InvalidRequest("prompt must not be empty".to_string()));
    }
    if !(MIN_SLIDES..=MAX_SLIDES).contains(&req.num_slides) {
        return Err(BuildError::InvalidRequest(format!(
            "num_slides must be between {MIN_SLIDES} and {MAX_SLIDES}"
        )));
    }
    Ok(())
}

fn outline_system_prompt(num_slides: u32) -> String {
    format!(
        "You are a presentation outline generator. Given a topic, respond with a JSON \
         object matching exactly this shape: {{\"slides\": [{{\"title\": string, \"bullets\": \
         [string, ...]}}, ...]}}. Produce exactly {num_slides} slides. Each slide must have a \
         short title and 2-5 concise bullet points. Respond with JSON only, no prose, no \
         markdown fences."
    )
}

/// Calls the chat completions endpoint in JSON mode and returns the raw
/// response body (as text) for `parse_outline_response` to interpret. Kept
/// separate from parsing so the parsing logic can be unit-tested without a
/// network call, and so `base_url` can be pointed at a mock server in tests.
async fn request_outline_completion(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    model: &str,
    prompt: &str,
    num_slides: u32,
) -> Result<String, BuildError> {
    let url = format!("{base_url}/chat/completions");
    let body = serde_json::json!({
        "model": model,
        "response_format": { "type": "json_object" },
        "temperature": 0.7,
        "messages": [
            { "role": "system", "content": outline_system_prompt(num_slides) },
            { "role": "user", "content": prompt },
        ],
    });

    let response = client
        .post(&url)
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await
        .map_err(|err| {
            if err.is_timeout() {
                BuildError::UpstreamTimeout
            } else {
                BuildError::UpstreamRequest(err.to_string())
            }
        })?;

    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| BuildError::UpstreamRequest(err.to_string()))?;

    if status == StatusCode::TOO_MANY_REQUESTS {
        return Err(BuildError::RateLimited(text));
    }
    if !status.is_success() {
        return Err(BuildError::UpstreamStatus {
            status: status.as_u16(),
            body: text,
        });
    }

    Ok(text)
}

/// Extracts `choices[0].message.content` from a chat completions response
/// body and parses it as an `Outline`. Pure/sync so it can be unit-tested
/// against static fixture strings without any network access.
fn parse_outline_response(raw_response: &str) -> Result<Outline, BuildError> {
    let envelope: serde_json::Value = serde_json::from_str(raw_response)
        .map_err(|err| BuildError::MalformedOutline(format!("response was not valid JSON: {err}")))?;

    let content = envelope
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("message"))
        .and_then(|m| m.get("content"))
        .and_then(|c| c.as_str())
        .ok_or_else(|| {
            BuildError::MalformedOutline("missing choices[0].message.content".to_string())
        })?;

    let outline: Outline = serde_json::from_str(content).map_err(|err| {
        BuildError::MalformedOutline(format!("message content was not a valid outline: {err}"))
    })?;

    if outline.slides.is_empty() {
        return Err(BuildError::MalformedOutline("outline had no slides".to_string()));
    }

    Ok(outline)
}

fn build_pptx(deck_title: &str, outline: &Outline) -> Result<Vec<u8>, BuildError> {
    let slides: Vec<SlideContent> = outline
        .slides
        .iter()
        .map(|slide| {
            slide
                .bullets
                .iter()
                .fold(SlideContent::new(&slide.title), |content, bullet| {
                    content.add_bullet(bullet)
                })
        })
        .collect();

    ppt_rs::create_pptx_with_content(deck_title, slides)
        .map_err(|err| BuildError::PptxBuild(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_well_formed_outline() {
        let raw = serde_json::json!({
            "choices": [{
                "message": {
                    "content": serde_json::json!({
                        "slides": [
                            { "title": "Intro", "bullets": ["Point A", "Point B"] },
                            { "title": "Conclusion", "bullets": ["Wrap up"] },
                        ]
                    }).to_string()
                }
            }]
        })
        .to_string();

        let outline = parse_outline_response(&raw).expect("should parse");
        assert_eq!(outline.slides.len(), 2);
        assert_eq!(outline.slides[0].title, "Intro");
        assert_eq!(outline.slides[0].bullets, vec!["Point A", "Point B"]);
        assert_eq!(outline.slides[1].title, "Conclusion");
    }

    #[test]
    fn parses_outline_with_missing_bullets_as_empty() {
        let raw = serde_json::json!({
            "choices": [{
                "message": {
                    "content": serde_json::json!({
                        "slides": [{ "title": "Only title" }]
                    }).to_string()
                }
            }]
        })
        .to_string();

        let outline = parse_outline_response(&raw).expect("should parse");
        assert_eq!(outline.slides[0].bullets, Vec::<String>::new());
    }

    #[test]
    fn rejects_non_json_envelope() {
        let err = parse_outline_response("not json at all").unwrap_err();
        assert!(matches!(err, BuildError::MalformedOutline(_)));
    }

    #[test]
    fn rejects_missing_message_content() {
        let raw = serde_json::json!({ "choices": [{ "message": {} }] }).to_string();
        let err = parse_outline_response(&raw).unwrap_err();
        assert!(matches!(err, BuildError::MalformedOutline(_)));
    }

    #[test]
    fn rejects_content_that_is_not_json() {
        let raw = serde_json::json!({
            "choices": [{ "message": { "content": "Sure, here is your outline: ..." } }]
        })
        .to_string();
        let err = parse_outline_response(&raw).unwrap_err();
        assert!(matches!(err, BuildError::MalformedOutline(_)));
    }

    #[test]
    fn rejects_empty_slide_list() {
        let raw = serde_json::json!({
            "choices": [{
                "message": { "content": serde_json::json!({ "slides": [] }).to_string() }
            }]
        })
        .to_string();
        let err = parse_outline_response(&raw).unwrap_err();
        assert!(matches!(err, BuildError::MalformedOutline(_)));
    }

    #[test]
    fn validates_empty_prompt() {
        let req = PptRequest {
            prompt: "   ".to_string(),
            num_slides: 5,
        };
        assert!(matches!(
            validate_request(&req),
            Err(BuildError::InvalidRequest(_))
        ));
    }

    #[test]
    fn validates_num_slides_bounds() {
        let too_few = PptRequest {
            prompt: "topic".to_string(),
            num_slides: 0,
        };
        let too_many = PptRequest {
            prompt: "topic".to_string(),
            num_slides: 999,
        };
        assert!(validate_request(&too_few).is_err());
        assert!(validate_request(&too_many).is_err());
    }

    #[test]
    fn builds_valid_pptx_bytes_from_outline() {
        let outline = Outline {
            slides: vec![
                Slide {
                    title: "Welcome".to_string(),
                    bullets: vec!["First point".to_string(), "Second point".to_string()],
                },
                Slide {
                    title: "Thanks".to_string(),
                    bullets: vec![],
                },
            ],
        };

        let bytes = build_pptx("My Deck", &outline).expect("should build pptx");
        // A .pptx is a zip archive; a real one starts with the local file
        // header signature "PK\x03\x04".
        assert_eq!(&bytes[0..4], b"PK\x03\x04");
    }

    #[tokio::test]
    async fn request_outline_completion_returns_body_on_success() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;
        let mock_body = serde_json::json!({
            "choices": [{
                "message": {
                    "content": serde_json::json!({
                        "slides": [{ "title": "Hi", "bullets": ["a"] }]
                    }).to_string()
                }
            }]
        });

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(header("authorization", "Bearer test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&mock_body))
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let raw = request_outline_completion(
            &client,
            &mock_server.uri(),
            "test-key",
            "gpt-4o-mini",
            "a topic",
            1,
        )
        .await
        .expect("request should succeed");

        let outline = parse_outline_response(&raw).expect("should parse");
        assert_eq!(outline.slides[0].title, "Hi");
    }

    #[tokio::test]
    async fn request_outline_completion_maps_429_to_rate_limited() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(429).set_body_string("rate limit exceeded"))
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let err = request_outline_completion(
            &client,
            &mock_server.uri(),
            "test-key",
            "gpt-4o-mini",
            "a topic",
            1,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, BuildError::RateLimited(_)));
        assert_eq!(err.status_code(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn request_outline_completion_maps_500_to_upstream_status() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let err = request_outline_completion(
            &client,
            &mock_server.uri(),
            "test-key",
            "gpt-4o-mini",
            "a topic",
            1,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, BuildError::UpstreamStatus { status: 500, .. }));
        assert_eq!(err.status_code(), StatusCode::BAD_GATEWAY);
    }
}
