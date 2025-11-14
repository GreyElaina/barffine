# syntax=docker/dockerfile:1.6
ARG RUST_VERSION=1.81
ARG RUST_TOOLCHAIN=nightly

FROM rust:${RUST_VERSION}-alpine3.20 AS builder
ARG RUST_TOOLCHAIN
ARG TARGETARCH
ARG CARGO_FEATURES

WORKDIR /app

ENV CARGO_NET_GIT_FETCH_WITH_CLI=true \
    PKG_CONFIG_ALLOW_CROSS=1

RUN apk add --no-cache \
        build-base \
        git \
        pkgconf \
        cmake \
        sqlite-dev \
        clang \
        clang-dev \
        lld \
        llvm-dev \
        zlib-dev \
        zstd-dev \
        lz4-dev \
        snappy-dev \
        bzip2-dev

RUN rustup toolchain install "${RUST_TOOLCHAIN}" \
    && rustup default "${RUST_TOOLCHAIN}"

COPY . .

RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
        arm64) target_triple="aarch64-unknown-linux-musl" ;; \
        amd64|x86_64) target_triple="x86_64-unknown-linux-musl" ;; \
        *) echo "unsupported TARGETARCH ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    rustup target add "${target_triple}"; \
    if [ -n "${CARGO_FEATURES:-}" ]; then \
        cargo build --release --locked -p barffine-server --target "${target_triple}" ${CARGO_FEATURES}; \
    else \
        cargo build --release --locked -p barffine-server --target "${target_triple}"; \
    fi; \
    strip "target/${target_triple}/release/barffine-server"; \
    cp "target/${target_triple}/release/barffine-server" /tmp/barffine-server

FROM alpine:3.20 AS runtime-prep

RUN apk add --no-cache ca-certificates \
    && addgroup -S barffine \
    && adduser -S -G barffine -u 10001 barffine \
    && mkdir -p /app/data /app/data/doc-kv /app/data/blob-store \
    && chown -R barffine:barffine /app

COPY --from=builder /tmp/barffine-server /usr/local/bin/barffine-server

FROM scratch AS runtime

ENV BARFFINE_DATABASE_PATH=/app/data \
    BARFFINE_DOC_DATA_PATH=/app/data/doc-kv \
    BARFFINE_BLOB_STORE_PATH=/app/data/blob-store \
    BARFFINE_DOC_DATA_BACKEND=rocksdb \
    BARFFINE_BLOB_STORE_BACKEND=rocksdb \
    BARFFINE_SERVER_NAME="Barffine Server" \
    BARFFINE_BIND_ADDRESS=0.0.0.0:8081 \
    BARFFINE_SERVER_HOST=0.0.0.0 \
    BARFFINE_SERVER_PORT=8081

WORKDIR /app

COPY --from=runtime-prep /etc/passwd /etc/passwd
COPY --from=runtime-prep /etc/group /etc/group
COPY --from=runtime-prep /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=runtime-prep /usr/local/bin/barffine-server /usr/local/bin/barffine-server
COPY --from=runtime-prep /app /app

EXPOSE 8081
VOLUME ["/app/data"]

USER barffine

ENTRYPOINT ["/usr/local/bin/barffine-server"]
CMD ["serve"]
