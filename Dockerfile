# syntax=docker/dockerfile:1.6
# Edition 2024 requires the nightly toolchain until stabilized; allow overrides via ARG.
ARG RUST_VERSION=nightly

FROM rust:${RUST_VERSION}-bookworm AS builder

WORKDIR /app

ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        build-essential \
        pkg-config \
        libssl-dev \
        libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --release --locked -p barffine-server

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        ca-certificates \
        curl \
        libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

ENV BARFFINE_DATABASE_PATH=/app/data/barffine.db \
    BARFFINE_BIND_ADDRESS=0.0.0.0:8081 \
    BARFFINE_SERVER_HOST=0.0.0.0 \
    BARFFINE_SERVER_PORT=8081

WORKDIR /app

RUN useradd --create-home --uid 10001 barffine \
    && mkdir -p /app/data \
    && chown -R barffine:barffine /app

COPY --from=builder /app/target/release/barffine-server /usr/local/bin/barffine-server

EXPOSE 8081
VOLUME ["/app/data"]

USER barffine

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8081/health || exit 1

ENTRYPOINT ["/usr/local/bin/barffine-server"]
CMD ["serve"]
