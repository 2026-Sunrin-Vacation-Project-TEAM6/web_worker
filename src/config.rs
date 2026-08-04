pub struct Config {
    pub host: String,
    pub port: u16,
    pub redis_url: String,
    pub stream_key: String,
    pub consumer_group: String,
    pub consumer_name: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            host: std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            port: std::env::var("PORT")
                .unwrap_or_else(|_| "3000".to_string())
                .parse()
                .expect("PORT must be a number"),
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
            stream_key: std::env::var("REDIS_STREAM_KEY")
                .unwrap_or_else(|_| "stackbox:events".to_string()),
            consumer_group: std::env::var("REDIS_STREAM_GROUP")
                .unwrap_or_else(|_| "web_worker".to_string()),
            consumer_name: std::env::var("REDIS_STREAM_CONSUMER")
                .unwrap_or_else(|_| "worker-1".to_string()),
        }
    }
}
