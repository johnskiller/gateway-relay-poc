# ============================================================
# Multi-stage Dockerfile for zenoh-gateway-poc
# Targets: gateway, producer, consumer
# ============================================================

# ---- Stage 1: Build ----
FROM rust:1.87-slim AS builder

WORKDIR /app

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "" > src/lib.rs && echo "fn main() {}" > src/main.rs
RUN cargo build --release 2>/dev/null || true

# Copy actual source and build all binaries
COPY . .
RUN touch src/lib.rs src/main.rs
RUN cargo build --release

# ---- Stage 2: Runtime (common base) ----
FROM debian:bookworm-slim AS runtime-base

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ---- Target: gateway ----
FROM runtime-base AS gateway

COPY --from=builder /app/target/release/gateway /usr/local/bin/gateway
COPY gateway-config.json5 /etc/zenoh-gateway/config.json5

# GATEWAY_ID can be overridden via env var (K8s Downward API)
ENV GATEWAY_ID=""
ENV GATEWAY_CONFIG="/etc/zenoh-gateway/config.json5"

ENTRYPOINT ["gateway"]
CMD ["--config", "/etc/zenoh-gateway/config.json5"]

# ---- Target: producer ----
FROM runtime-base AS producer

COPY --from=builder /app/target/release/producer-perf /usr/local/bin/producer-perf
COPY --from=builder /app/target/release/producer-sim /usr/local/bin/producer-sim

ENTRYPOINT ["producer-perf"]
CMD ["100", "100"]

# ---- Target: consumer ----
FROM runtime-base AS consumer

COPY --from=builder /app/target/release/consumer-perf /usr/local/bin/consumer-perf
COPY --from=builder /app/target/release/consumer-sim /usr/local/bin/consumer-sim

ENTRYPOINT ["consumer-perf"]
CMD ["client-1", "100"]
