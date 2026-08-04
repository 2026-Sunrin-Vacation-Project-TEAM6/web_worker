# Build stage
FROM rust:1-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV HOST=0.0.0.0
ENV PORT=3000
ENV REDIS_URL=redis://127.0.0.1:6379
ENV REDIS_STREAM_KEY=stackbox:events
ENV REDIS_STREAM_GROUP=web_worker
ENV REDIS_STREAM_CONSUMER=worker-1

EXPOSE 3000

COPY --from=builder /app/target/release/web_worker /usr/local/bin/web_worker

CMD ["web_worker"]
