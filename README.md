# Barffine

Barffine is an [AFFiNE](https://github.com/toeverything/AFFiNE) compatible backend written in Rust, which provides a REST/GraphQL/Socket.IO/CRDT collaboration interface consistent with the official Node/TS backend with a single binary. It uses only SQLite storage, can be self-hosted in resource-constrained environments, and includes built-in migration, admin creation, document caching, and observability tools.

## Quick start

1. Install the latest stable Rust toolchain (Edition 2024 is enabled in `Cargo.toml`).
2. Copy `.env` or create a new one based on the snippet below to describe your bind address and public URL.
3. Run the server:
   ```shell
   cargo run -p barffine-server
   ```
   The default bind address is `127.0.0.1:8081` and the SQLite file is created at `./data/barffine.db`.
4. Bootstrap an admin account so AFFiNE can log in:
   ```shell
   cargo run -p barffine-server -- create-admin <email> <password>
   ```
   The command is idempotent; rerunning it rotates the password if the user already exists.
5. Verify the instance with `curl http://127.0.0.1:8081/health` and point your AFFiNE client at the same base URL.

## Docker deployment

If you prefer containers over a local Rust toolchain, use the published image at `ghcr.io/greyelaina/barffine`:

1. Pull the image (update the tag if you need a specific release):
   ```shell
   docker pull ghcr.io/greyelaina/barffine:latest
   ```
2. Prepare a `.env` file (see below) and a data directory on the host for the SQLite + doc-data files, then start the container:
   ```shell
   docker run -d \
     --name barffine \
     -p 8081:8081 \
     --env-file .env \
     -v $(pwd)/data:/app/data \
     ghcr.io/greyelaina/barffine:latest
   ```
   The example mounts `./data`, which now contains both `barffine.db` and the RocksDB column families under `data/doc-kv`. Adjust the port mapping when reverse proxies or different hosts are involved, and make sure the host volume has enough inodes/space for the additional KV files.
3. Use `docker logs -f barffine` to monitor startup and reuse the operational commands below via `docker exec barffine <command>` when needed.

`docker-compose.ghcr.yml` mirrors these steps and can be used as a template for multi-container deployments.

## Operational commands

| Command | Description |
| --- | --- |
| `cargo run -p barffine-server -- serve` | Start the HTTP, GraphQL, and Socket.IO services (default subcommand). |
| `cargo run -p barffine-server -- create-admin <email> <password>` | Create or update an administrator and ensure the `admin` role is applied. |

Planned CLI parity with the Node backend (see `server/src/cli/mod.rs`) documents future subcommands such as `data-migrate` and workspace inspection.

## Configuration

Barffine resolves configuration in the following order (later items win): built-in defaults → environment variables → `.env` (via `dotenvy`). Keep a `.env` file for local runs and mirror the same values into your process manager or orchestration layer when deploying.

Key variables:

| Variable | Default / Example | Notes |
| --- | --- | --- |
| `BARFFINE_BIND_ADDRESS` | `127.0.0.1:8081` | Socket address for the HTTP listener. |
| `BARFFINE_DATABASE_PATH` | `./data` | Directory that stores the SQLite metadata database (Barffine creates `barffine.db` inside) and co-locates RocksDB column families. Keep this pointing at a directory; file paths are best-effort supported for backward compatibility. |
| `BARFFINE_DATABASE_BACKEND` | `sqlite` | Choose `sqlite` or `postgres`. |
| `BARFFINE_DATABASE_URL` | `postgres://user:pass@localhost:5432/barffine` | Required when `BARFFINE_DATABASE_BACKEND=postgres`; ignored otherwise. |
| `BARFFINE_DATABASE_MAX_CONNECTIONS` | `16` | Controls the metadata connection pool size. Increase for higher concurrency. |
| `BARFFINE_DOC_DATA_BACKEND` | `sqlite` / `rocksdb` | Selects where doc updates, snapshots, notifications, and other large KV payloads live. |
| `BARFFINE_DOC_DATA_PATH` | `./data/doc-kv` | Directory for RocksDB column families (used when `BARFFINE_DOC_DATA_BACKEND=rocksdb`). |
| `BARFFINE_DOC_STORE_BACKEND` | unset | Selects where full document exports are stored (e.g., for downloads). |
| `BARFFINE_NOTIFICATION_CENTER_BACKEND` | unset / `auto` | Optional override for the notification backend. See “Notification center backend” below for supported values and defaults. |
| `BARFFINE_BASE_URL` | Derived from host/port | Used to build absolute links returned to clients. Set when exposing Barffine behind a proxy. |
| `BARFFINE_SERVER_HOST` / `BARFFINE_SERVER_PORT` | `127.0.0.1` / `8081` | Populate compatibility metadata surfaced to AFFiNE clients / Proxy. |
| `BARFFINE_SERVER_PATH` / `BARFFINE_SERVER_SUB_PATH` | unset | Mount the API under a prefix when sitting behind a reverse proxy. |
| `AFFINE_VERSION`, `DEPLOYMENT_TYPE`, `SERVER_FLAVOR` | `0.25.0`, `selfhosted`, `allinone` | Advertise compatibility and deployment flavour in `/info`. |
| `BARFFINE_SERVER_FEATURES` | empty | Comma or semicolon separated list enabling feature flags (e.g., `comment,indexer`). |
| `BARFFINE_CRYPTO_PRIVATE_KEY` (`AFFINE_CRYPTO_PRIVATE_KEY`) | autogenerated if missing | Used by `DocTokenSigner` to mint RPC tokens; a random key is generated at startup when not provided. |
| `BARFFINE_ALLOW_GUEST_DEMO_WORKSPACE` | `false` | Allow anonymous demo workspaces. |
| `BARFFINE_PASSWORD_MIN` / `BARFFINE_PASSWORD_MAX` | upstream defaults | Enforce password length guards. |
| `BARFFINE_ENVIRONMENT` (`BARFFINE_ENV`) | unset | Emitted with log events and exported to Logfire. |
| `LOGFIRE_TOKEN` | unset | Enables Logfire export; tracing falls back to `tracing_subscriber` when absent. |
| `RUST_LOG` | `info` | Standard Rust log filter. |
| `BARFFINE_TRACE_*` | unset | Control sampling behaviour for custom tracing (see `server/src/observability.rs`). |

### Example & Recommended `.env`

```dotenv
AFFINE_VERSION=0.25.0
DEPLOYMENT_TYPE=selfhosted
SERVER_FLAVOR=allinone

BARFFINE_SERVER_NAME="Barffine Server"

BARFFINE_SERVER_HOST=127.0.0.1
BARFFINE_SERVER_PORT=8081
BARFFINE_BASE_URL=http://127.0.0.1:8081

BARFFINE_DATABASE_PATH=./data
BARFFINE_DOC_DATA_BACKEND=rocksdb
BARFFINE_DOC_STORE_BACKEND=rocksdb
BARFFINE_BLOB_STORE_BACKEND=rocksdb
BARFFINE_NOTIFICATION_CENTER_BACKEND=rocksdb

RUST_LOG=info,tower_http=trace,axum=debug
```

### Notification center backend

Barffine uses a `NotificationCenter` to store and fan out comment/mention/workspace notifications.

By default (when `BARFFINE_NOTIFICATION_CENTER_BACKEND` is unset or set to `auto`):

- If `BARFFINE_DOC_DATA_BACKEND=rocksdb`, Barffine uses `RocksNotificationCenter` backed by the RocksDB doc-data store (`notifications` column family).
- Otherwise, Barffine picks a backend that matches the metadata database:
  - `DatabaseBackend::Sqlite` → `SqliteNotificationCenter`
  - `DatabaseBackend::Postgres` → `PostgresNotificationCenter`

You can override this selection with `BARFFINE_NOTIFICATION_CENTER_BACKEND`:

- `auto` (or unset): use the default detection described above.
- `rocksdb` / `rocks`: force `RocksNotificationCenter`. Requires `BARFFINE_DOC_DATA_BACKEND=rocksdb`, otherwise the server will fail to start.
- `sqlite`: force `SqliteNotificationCenter`. Requires `BARFFINE_DATABASE_BACKEND=sqlite`.
- `postgres` / `postgresql` / `pg`: force `PostgresNotificationCenter`. Requires `BARFFINE_DATABASE_BACKEND=postgres`.

Unsupported values are ignored with a warning and Barffine falls back to the default backend. For most deployments, leaving this setting unset is recommended.

### OAuth providers

Barffine now exposes the same `/api/oauth/*` endpoints and GraphQL metadata as the official AFFiNE backend. Each provider is enabled when both the client ID and secret are present (values are read from the process environment as well as `.env` via `dotenvy`):

| Provider | Required variables | Optional variables |
| --- | --- | --- |
| Google | `BARFFINE_OAUTH_GOOGLE_CLIENT_ID`, `BARFFINE_OAUTH_GOOGLE_CLIENT_SECRET` | `BARFFINE_OAUTH_GOOGLE_SCOPE`, `BARFFINE_OAUTH_GOOGLE_PROMPT`, `BARFFINE_OAUTH_GOOGLE_ACCESS_TYPE` |
| GitHub | `BARFFINE_OAUTH_GITHUB_CLIENT_ID`, `BARFFINE_OAUTH_GITHUB_CLIENT_SECRET` | `BARFFINE_OAUTH_GITHUB_SCOPE` |
| Apple | `BARFFINE_OAUTH_APPLE_CLIENT_ID`, `BARFFINE_OAUTH_APPLE_CLIENT_SECRET` | — (redirect path automatically set to `/api/oauth/callback`) |
| Generic OIDC | `BARFFINE_OAUTH_OIDC_CLIENT_ID`, `BARFFINE_OAUTH_OIDC_CLIENT_SECRET`, `BARFFINE_OAUTH_OIDC_ISSUER` | `BARFFINE_OAUTH_OIDC_SCOPE`, `BARFFINE_OAUTH_OIDC_CLAIM_ID`, `BARFFINE_OAUTH_OIDC_CLAIM_EMAIL`, `BARFFINE_OAUTH_OIDC_CLAIM_NAME` |

Use `BARFFINE_ALLOW_OAUTH_SIGNUP` (defaults to `true`) to forbid new users from being created through OAuth flows while still allowing existing linked accounts to sign in.
