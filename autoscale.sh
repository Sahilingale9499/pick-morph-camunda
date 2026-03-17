#!/usr/bin/env bash
# autoscale.sh — monitors spring-camunda container memory and scales via Docker Swarm
# Usage: ./autoscale.sh [--service <service_name>] [--threshold-gb <n>] [--max-replicas <n>]

set -euo pipefail

# ── Config (override via flags or env vars) ───────────────────────────────────
SERVICE_NAME="${SERVICE_NAME:-spring-camunda_spring-camunda}"
CONTAINER_NAME="${CONTAINER_NAME:-spring-camunda}"
THRESHOLD_GB="${THRESHOLD_GB:-5}"          # scale up above this many GB
SCALE_DOWN_GB="${SCALE_DOWN_GB:-3}"        # scale down below this many GB
MAX_REPLICAS="${MAX_REPLICAS:-5}"
MIN_REPLICAS="${MIN_REPLICAS:-1}"
POLL_INTERVAL="${POLL_INTERVAL:-30}"       # seconds between checks
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-120}" # wait after a scale event

# ── Parse flags ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --service)      SERVICE_NAME="$2";    shift 2 ;;
    --threshold-gb) THRESHOLD_GB="$2";   shift 2 ;;
    --max-replicas) MAX_REPLICAS="$2";   shift 2 ;;
    *) echo "Unknown flag: $1"; exit 1 ;;
  esac
done

THRESHOLD_BYTES=$(( THRESHOLD_GB * 1024 * 1024 * 1024 ))
SCALE_DOWN_BYTES=$(( SCALE_DOWN_GB * 1024 * 1024 * 1024 ))

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# ── Detect orchestration mode ─────────────────────────────────────────────────
is_swarm() { docker info --format '{{.Swarm.LocalNodeState}}' 2>/dev/null | grep -q active; }

get_current_replicas() {
  docker service ls --filter "name=${SERVICE_NAME}" --format '{{.Replicas}}' 2>/dev/null \
    | awk -F'/' '{print $1}'
}

scale_service() {
  local replicas=$1
  log "Scaling ${SERVICE_NAME} to ${replicas} replica(s)"
  docker service scale "${SERVICE_NAME}=${replicas}"
}

# ── Memory parsing ─────────────────────────────────────────────────────────────
# docker stats returns values like "1.2GiB", "500MiB", "4.8GiB"
parse_mem_bytes() {
  local raw=$1
  local num unit
  num=$(echo "$raw" | grep -oE '[0-9]+(\.[0-9]+)?')
  unit=$(echo "$raw" | grep -oE '[A-Za-z]+')
  case "$unit" in
    B)            echo "${num%.*}" ;;
    KiB|kB|KB)    echo "$(echo "$num * 1024" | bc | cut -d. -f1)" ;;
    MiB|MB)       echo "$(echo "$num * 1048576" | bc | cut -d. -f1)" ;;
    GiB|GB)       echo "$(echo "$num * 1073741824" | bc | cut -d. -f1)" ;;
    TiB|TB)       echo "$(echo "$num * 1099511627776" | bc | cut -d. -f1)" ;;
    *)            echo "0" ;;
  esac
}

get_container_mem_bytes() {
  local raw
  raw=$(docker stats --no-stream --format '{{.MemUsage}}' \
        $(docker ps -q --filter "name=${CONTAINER_NAME}") 2>/dev/null \
        | awk -F'/' '{print $1}' | tr -d ' ' | head -1)
  [[ -z "$raw" ]] && echo "0" && return
  parse_mem_bytes "$raw"
}

# ── Main loop ─────────────────────────────────────────────────────────────────
log "Starting autoscaler: service=${SERVICE_NAME}, threshold=${THRESHOLD_GB}GB, max_replicas=${MAX_REPLICAS}"

if ! is_swarm; then
  log "WARNING: Docker Swarm is not active. Swarm-mode scaling (docker service scale) is unavailable."
  log "Running in monitor-only mode. To enable scaling, initialize Swarm: docker swarm init"
fi

last_scale=0

while true; do
  mem_bytes=$(get_container_mem_bytes)
  mem_gb=$(echo "scale=2; $mem_bytes / 1073741824" | bc)
  log "Memory usage: ${mem_gb} GB (threshold: ${THRESHOLD_GB} GB)"

  now=$(date +%s)
  cooldown_elapsed=$(( now - last_scale ))

  if (( mem_bytes >= THRESHOLD_BYTES )) && (( cooldown_elapsed >= COOLDOWN_SECONDS )); then
    if is_swarm; then
      current=$(get_current_replicas)
      current=${current:-1}
      new=$(( current + 1 ))
      if (( new <= MAX_REPLICAS )); then
        log "Memory at ${mem_gb} GB >= ${THRESHOLD_GB} GB — scaling UP: ${current} → ${new}"
        scale_service "$new"
        last_scale=$(date +%s)
      else
        log "Memory at ${mem_gb} GB >= ${THRESHOLD_GB} GB — already at max replicas (${MAX_REPLICAS})"
      fi
    else
      log "ALERT: Memory at ${mem_gb} GB >= ${THRESHOLD_GB} GB. Start Docker Swarm to enable auto-scaling."
    fi

  elif (( mem_bytes < SCALE_DOWN_BYTES )) && (( cooldown_elapsed >= COOLDOWN_SECONDS )); then
    if is_swarm; then
      current=$(get_current_replicas)
      current=${current:-1}
      if (( current > MIN_REPLICAS )); then
        new=$(( current - 1 ))
        log "Memory at ${mem_gb} GB < ${SCALE_DOWN_GB} GB — scaling DOWN: ${current} → ${new}"
        scale_service "$new"
        last_scale=$(date +%s)
      fi
    fi
  fi

  sleep "${POLL_INTERVAL}"
done
