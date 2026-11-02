#!/usr/bin/env bash
# Runs the arq worker and the API in ONE container.
#
# Only for the Render free-tier demo, which has no free Background Worker. In production (and in
# docker-compose.yml) the api and worker run as separate services so they can scale and restart independently.
#
# - Uvicorn binds to $PORT (set by Render; defaults to 8000 locally).
# - If either process exits, the other is stopped and the container exits non-zero, so Render restarts it.
# - SIGTERM/SIGINT are forwarded to both and we wait for them, so the worker's drain logic finishes
#   in-flight upstream requests before the container stops (keep maxShutdownDelaySeconds > WORKER_DRAIN_SECONDS).
set -uo pipefail

PORT="${PORT:-8000}"

arq app.worker.WorkerSettings &
worker_pid=$!

uvicorn app.main:app --host 0.0.0.0 --port "$PORT" &
api_pid=$!

echo "start.sh: worker pid=${worker_pid}, api pid=${api_pid}, port=${PORT}"

stop_children() {
  kill -TERM "$api_pid" "$worker_pid" 2>/dev/null
  wait "$api_pid" 2>/dev/null
  wait "$worker_pid" 2>/dev/null
}

on_signal() {
  trap - TERM INT
  echo "start.sh: received shutdown signal, stopping api and draining worker"
  stop_children
  exit 0
}
trap on_signal TERM INT

# Returns when the first of the two processes exits (or a trapped signal interrupts the wait).
wait -n "$api_pid" "$worker_pid"
status=$?

trap - TERM INT
if kill -0 "$api_pid" 2>/dev/null; then exited="worker"; else exited="api"; fi
echo "start.sh: ${exited} exited with status ${status}; stopping the other process"
stop_children
# Any exit here is unexpected, so never report success.
exit $(( status == 0 ? 1 : status ))
