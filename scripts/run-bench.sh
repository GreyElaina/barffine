#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENV_FILE="${BARFFINE_BENCH_ENV:-.env.bench}"
SCENARIOS=("$@")
if [[ ${#SCENARIOS[@]} -eq 0 ]]; then
  SCENARIOS=(low balanced high)
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "[bench] missing env file: $ENV_FILE" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

ADMIN_EMAIL="${BARFFINE_ADMIN_EMAIL:-admin@example.com}"
ADMIN_PASSWORD="${BARFFINE_ADMIN_PASSWORD:-password}"
DATA_ROOT="${BARFFINE_DATABASE_PATH:-/tmp/barffine_bench}"
DATA_ROOT="${DATA_ROOT%/}"
DATABASE_PATH="$DATA_ROOT/barffine.db"
DOC_KV_DIR="$DATA_ROOT/doc-kv"
BLOB_STORE_DIR="$DATA_ROOT/blob-store"
WORKSPACE_NAME="${BARFFINE_BENCH_WORKSPACE_NAME:-bench-$(date -u +%Y%m%dT%H%M%SZ)}"

mkdir -p "$DATA_ROOT" bench-results

if [[ -f "bench-results/dataset.json" ]]; then
  echo "[bench] deleting existing dataset at bench-results/dataset.json"
  rm -f "bench-results/dataset.json"
fi

if [[ "${BARFFINE_BENCH_RESET_DB:-1}" == "1" ]]; then
  if [[ -e "$DATABASE_PATH" || -e "${DATABASE_PATH}-wal" || -e "${DATABASE_PATH}-shm" || -d "$DOC_KV_DIR" || -d "$BLOB_STORE_DIR" ]]; then
    echo "[bench] deleting existing bench data under $DATA_ROOT"
  fi
  rm -f "$DATABASE_PATH" "${DATABASE_PATH}-wal" "${DATABASE_PATH}-shm"
  rm -rf "$DOC_KV_DIR" "$BLOB_STORE_DIR"
fi

function cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && ps -p "$SERVER_PID" >/dev/null 2>&1; then
    echo "[bench] stopping barffine-server (pid=$SERVER_PID)"
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi

  # Remove the pid file
  if [[ -f barffine.pid ]]; then
    rm -f barffine.pid
  fi
}
trap cleanup EXIT

if pgrep -f "barffine-server" >/dev/null 2>&1; then
  echo "[bench] killing existing barffine-server"
  pkill -f "barffine-server" || true
  sleep 2
fi

echo "[bench] creating admin $ADMIN_EMAIL"
BARFFINE_ENV_FILE="$ENV_FILE" \
  cargo run -p barffine-server --release --bin barffine-server -- \
  create-admin "$ADMIN_EMAIL" "$ADMIN_PASSWORD"

echo "[bench] creating workspace $WORKSPACE_NAME"
if ! CREATE_WORKSPACE_OUTPUT=$(BARFFINE_ENV_FILE="$ENV_FILE" \
  cargo run -p barffine-server --release --bin barffine-server -- \
  create-workspace --owner-email "$ADMIN_EMAIL" --name "$WORKSPACE_NAME" 2>&1); then
  echo "$CREATE_WORKSPACE_OUTPUT"
  echo "[bench] failed to create workspace" >&2
  exit 1
fi
echo "$CREATE_WORKSPACE_OUTPUT"
WORKSPACE_ID=$(echo "$CREATE_WORKSPACE_OUTPUT" | sed -n 's/.*(\([0-9a-fA-F-]\{32,36\}\)) for owner.*/\1/p' | head -n 1)
if [[ -z "$WORKSPACE_ID" ]]; then
  echo "[bench] unable to parse workspace id" >&2
  exit 1
fi

SEED_DOCS=${BARFFINE_BENCH_WORKSPACE_SEED_DOCS:-32}
if [[ "$SEED_DOCS" -gt 0 ]]; then
  echo "[bench] seeding workspace $WORKSPACE_ID with $SEED_DOCS doc(s)"
  SEED_ARGS=(--workspace-id "$WORKSPACE_ID" --doc-count "$SEED_DOCS")
  if [[ -n "${BARFFINE_BENCH_WORKSPACE_DOC_PREFIX:-}" ]]; then
    SEED_ARGS+=(--doc-prefix "${BARFFINE_BENCH_WORKSPACE_DOC_PREFIX}")
  fi
  BARFFINE_ENV_FILE="$ENV_FILE" \
    cargo run -p barffine-server --release --bin barffine-server -- \
    seed-workspace "${SEED_ARGS[@]}"
fi

echo "[bench] starting barffine-server with $ENV_FILE"
BARFFINE_ENV_FILE="$ENV_FILE" \
RUST_LOG="${RUST_LOG:-info}" \
NO_COLOR=1 \
TERM=dumb \
cargo run -p barffine-server --release --bin barffine-server \
  >/tmp/barffine-server.log 2>&1 &
SERVER_PID=$!
echo $SERVER_PID > barffine.pid

for attempt in {1..30}; do
  if curl -sf http://127.0.0.1:8081/health >/dev/null 2>&1; then
    echo "[bench] server is up"
    break
  fi
  sleep 1
  if [[ $attempt -eq 30 ]]; then
    echo "[bench] server failed to start" >&2
    exit 1
  fi
done

echo "[bench] generating dataset"
declare -a DATASET_ARGS=()
if [[ -n "${BARFFINE_BENCH_DATASET_WORKSPACE_LIMIT:-}" ]]; then
  DATASET_ARGS+=(--workspace-limit "${BARFFINE_BENCH_DATASET_WORKSPACE_LIMIT}")
fi
if [[ -n "${BARFFINE_BENCH_DATASET_DOCS_PER_WORKSPACE:-}" ]]; then
  DATASET_ARGS+=(--docs-per-workspace "${BARFFINE_BENCH_DATASET_DOCS_PER_WORKSPACE}")
fi
if [[ -n "${BARFFINE_BENCH_DATASET_PAGE_SIZE:-}" ]]; then
  DATASET_ARGS+=(--page-size "${BARFFINE_BENCH_DATASET_PAGE_SIZE}")
fi

if [[ ${DATASET_ARGS+set} == set ]]; then
  cargo run -p bench-runner --release -- generate "${DATASET_ARGS[@]}"
else
  cargo run -p bench-runner --release -- generate
fi

for scenario in "${SCENARIOS[@]}"; do
  echo "[bench] running scenario: $scenario"
  cargo run -p bench-runner --release -- run "$scenario"
done
