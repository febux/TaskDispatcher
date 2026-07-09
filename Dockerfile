# syntax=docker/dockerfile:1.7

# ---- chef base: cargo-chef for layer-cached dependency builds ----
FROM rust:1.95-bookworm AS chef
RUN cargo install cargo-chef --locked && rm -rf /usr/local/cargo/registry
WORKDIR /app

# ---- planner: produce a recipe.json from the manifest only ----
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- builder: cook dependencies first (cached), then build the app ----
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --locked --release

# ---- runtime: slim Debian with only CA bundle + binary ----
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/taskmanager /app/taskmanager

ENV LISTEN_ADDR=0.0.0.0:8080 \
    LOG_FORMAT=json \
    RUST_LOG=info,taskmanager=info,sqlx=warn,redis=warn

EXPOSE 8080

# Non-root user for defense-in-depth.
RUN useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /app
USER appuser

ENTRYPOINT ["/app/taskmanager"]
