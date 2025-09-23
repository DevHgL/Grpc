#!/usr/bin/env bash
set -Eeuo pipefail

# Resolve repo root (directory of this script)
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="$ROOT/venv/bin/python"

if [[ ! -x "$PY" ]]; then
  echo "Python venv not found or not executable at: $PY" >&2
  echo "Create/activate the venv first, or adjust PY path in this script." >&2
  exit 1
fi

LOG_DIR="$ROOT/.logs"
mkdir -p "$LOG_DIR"

declare -a PIDS

start_service() {
  local cmd="$1"
  local name="$2"
  local log_file="$LOG_DIR/${name}.log"

  echo "Starting ${name}... (logs: $log_file)"
  nohup bash -c "$cmd" >"$log_file" 2>&1 &
  local pid=$!
  PIDS+=("$pid")
  echo "$pid" > "$LOG_DIR/${name}.pid"
}

cleanup() {
  echo
  echo "Stopping services..."
  # First try graceful termination
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done

  # Give them a moment to exit
  sleep 1

  # Force kill any remaining
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  done

  echo "All services stopped."
}

trap cleanup INT TERM EXIT

(
  cd "$ROOT"
  start_service "$PY -m server.broker" broker
)
sleep 0.2
(
  cd "$ROOT"
  start_service "$PY -m frontend.app" frontend
)
sleep 0.2
(
  cd "$ROOT"
  start_service "$PY -m workers.simulator" simulator
)

echo "Services started: broker, frontend, simulator"
echo "Press Ctrl-C to stop and clean up. Logs in $LOG_DIR"

# Wait for all background jobs
wait


