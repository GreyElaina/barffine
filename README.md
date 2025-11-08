# Barffine

Barffine is an experimental Rust backend tailored for self-hosted or resource-limited deployments of [AFFiNE](https://github.com/toeverything/AFFiNE). The goal is to keep the collaboration surface compatible with existing clients while dramatically shrinking the operational footprint.

## Running locally

```shell
cargo run -p barffine-server
```

The server exposes simple health and document snapshot endpoints on `127.0.0.1:8081` by default.

## Add Account

```shell
cargo run -p barffine-server -- create-admin <username> <password>
```

## Runtime Configuration

Barffine's runtime configuration is mainly managed through environment variables, and it also supports loading through optional TOML configuration files (specified by `BARFFINE_CONFIG_FILE`) and `.env` files (the project uses `dotenvy`). The configuration priority is usually: default values -> configuration files -> environment variables -> `.env` (if it exists).

Common environment variables and default values (grouped descriptions):

- `BARFFINE_CONFIG_FILE`: Specifies the path to an additional TOML configuration file (overriding the default settings).
- `BARFFINE_DATABASE_PATH`: Database path (default `./data/barffine.db`; also appears as `BARFFINE_DATABASE_URL` in repository examples).
- `BARFFINE_BIND_ADDRESS`: Full binding address (e.g., `127.0.0.1:8081`).
- `BARFFINE_SERVER_HOST` / `AFFINE_SERVER_HOST` (default `127.0.0.1`)
- `BARFFINE_SERVER_PORT` / `AFFINE_SERVER_PORT` (default `8081`)
- `BARFFINE_SERVER_PATH`, `BARFFINE_SERVER_SUB_PATH`, `AFFINE_SERVER_PATH`, etc.: Set the service's URL prefix/subpath.
- `BARFFINE_BASE_URL`, `BARFFINE_PUBLIC_BASE_URL`, `AFFINE_BASE_URL`, `AFFINE_SERVER_BASE_URL`: Used to build the basic address for external access; if not set, it will be automatically generated based on host/port/https.
- `AFFINE_VERSION`: AFFiNE version (used for compatibility display).
- `DEPLOYMENT_TYPE` / `BARFFINE_DEPLOYMENT_TYPE` (e.g., `selfhosted`).
- `SERVER_FLAVOR` / `BARFFINE_FLAVOR` (e.g., `allinone`).
- `AFFINE_SERVER_MESSAGE` / `BARFFINE_SERVER_MESSAGE`: Used to display server messages in the UI.
- `BARFFINE_ENVIRONMENT` / `BARFFINE_ENV`: Runtime environment identifier (e.g., development/production).
- `BARFFINE_CRYPTO_PRIVATE_KEY` / `AFFINE_CRYPTO_PRIVATE_KEY`: Private key for RPC tokens (if not provided, the service will generate a temporary private key and record a warning).
- `BARFFINE_SERVER_FEATURES`: A list of features separated by commas or semicolons (e.g., indexer, comment).
- `BARFFINE_ALLOW_GUEST_DEMO_WORKSPACE`: Whether to allow guest demo workspace (boolean).
- `BARFFINE_PASSWORD_MIN`, `BARFFINE_PASSWORD_MAX`: Password length limits.
- `RUST_LOG`: Rust log filter (e.g., `info,tower_http=trace`).
- `LOGFIRE_TOKEN`: Logfire log service token (optional).

- Tracking/Sampling Related: `BARFFINE_TRACE_DEFAULT_RATE`, `BARFFINE_TRACE_FORCE_ALL`, `BARFFINE_TRACE_DEBUG`, `BARFFINE_TRACE_FORCE_PATHS`, `BARFFINE_TRACE_DROP_PATHS`, `BARFFINE_TRACE_RULES` (used to control sampling and tracking behavior).

Example `.env` (examples already exist in the repository, can be used directly or adjusted):

```dotenv
AFFINE_VERSION=0.25.0
DEPLOYMENT_TYPE=selfhosted
SERVER_FLAVOR=allinone
BARFFINE_DATABASE_URL=./barffine.db
# BARFFINE_BASE_URL=http://localhost:8081
# BARFFINE_PUBLIC_BASE_URL=http://localhost:8081
BARFFINE_BASE_URL=http://127.0.0.1:8081
BARFFINE_PUBLIC_BASE_URL=http://127.0.0.1:8081
BARFFINE_SERVER_NAME="Barffine Server"
BARFFINE_SERVER_HOST=127.0.0.1
BARFFINE_SERVER_PORT=8081

RUST_LOG=info,tower_http=trace,axum=debug
```

References (view more configuration items and default values):

- `server/src/config.rs`, `core/src/config.rs` (binding address, database path, configuration file parsing)
- `server/src/state.rs`, `server/src/observability.rs`, `server/src/crypto.rs` (logic and default strategies for reading other environment variables)

Note: Most `BARFFINE_*` variables in Barffine provide corresponding `AFFINE_*` backup names to ensure compatibility with the official AFFiNE implementation.
