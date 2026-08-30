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

RUN groupadd -r app && useradd -r -g app app

EXPOSE 3000

COPY --from=builder /app/target/release/web_worker /usr/local/bin/web_worker

USER app
CMD ["web_worker"]

# code_runner runtime stage: same build, sandbox HTTP service binary only.
# Deployment note: run this container with a restricted network (e.g.
# --network=none or an isolated compose network with no egress) and a
# non-root user for a stronger boundary — the binary itself only applies
# process-level rlimits/timeouts, not container/VM isolation.
FROM debian:bookworm-slim AS code_runner_runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates python3 nodejs gcc g++ rustc \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r app && useradd -r -g app app

ENV CODE_RUNNER_HOST=0.0.0.0
ENV CODE_RUNNER_PORT=3001

EXPOSE 3001

COPY --from=builder /app/target/release/code_runner /usr/local/bin/code_runner

USER app
CMD ["code_runner"]

# ppt_builder runtime stage: unlike code_runner/web_worker, this service
# needs network egress (it calls the OpenAI API) — do NOT run it with
# --network=none or on an isolated no-egress network like code_runner.
FROM debian:bookworm-slim AS ppt_builder_runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r app && useradd -r -g app app

ENV PPT_BUILDER_HOST=0.0.0.0
ENV PPT_BUILDER_PORT=3002

EXPOSE 3002

COPY --from=builder /app/target/release/ppt_builder /usr/local/bin/ppt_builder

USER app
CMD ["ppt_builder"]
