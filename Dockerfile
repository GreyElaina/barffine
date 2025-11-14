#
# Dockerfile for barffine-server
# - Multi-stage build keeps the final image lean.
# - Builder installs the requested Rust toolchain and compiles the release binary.
# - Runtime stage contains only the stripped binary and the shared libs it needs.
#

# Builder stage: compile the Rust workspace
ARG RUST_VERSION=1.81
FROM rust:${RUST_VERSION}-slim AS builder

ARG RUST_TOOLCHAIN=nightly
ENV CARGO_TERM_COLOR=never \
    CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

WORKDIR /app

# Install build dependencies for RocksDB/sqlite/sqlx and tooling
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    clang \
    cmake \
    curl \
    git \
    libclang-dev \
    liblz4-dev \
    libsqlite3-dev \
    pkg-config \
 && rm -rf /var/lib/apt/lists/*

# Install the requested Rust toolchain (defaults to nightly)
RUN rustup toolchain install "${RUST_TOOLCHAIN}" --profile minimal --component rustfmt \
 && rustup default "${RUST_TOOLCHAIN}"

# Bring in the full workspace
COPY . .

# Fetch dependencies (kept as a separate layer for cache reuse)
RUN cargo fetch --locked

# Build the server binary
RUN cargo build --locked --release -p barffine-server \
 && strip target/release/barffine-server

# Prepare runtime filesystem (owned by nonroot UID 65532 used by distroless images)
RUN mkdir -p /runtime/app/data \
 && chown -R 65532:65532 /runtime/app

# Runtime stage: distroless base keeps the final image much smaller
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

LABEL org.opencontainers.image.source="https://github.com/GreyElaina/barffine" \
      org.opencontainers.image.title="barffine-server"

ENV APP_HOME=/app \
    BARFFINE_DATA_PATH=/app/data

# Copy pre-created runtime tree so the nonroot user can write to /app
COPY --from=builder --chown=nonroot:nonroot /runtime/app ${APP_HOME}

# Bring over CA bundle for TLS usage
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

WORKDIR ${APP_HOME}

COPY --from=builder /app/target/release/barffine-server /usr/local/bin/barffine

VOLUME ["${BARFFINE_DATA_PATH}"]

EXPOSE 8081

USER nonroot

ENTRYPOINT ["/usr/local/bin/barffine"]
CMD ["serve"]
