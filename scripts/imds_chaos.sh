#!/bin/bash
# IMDS chaos script — blocks and unblocks IMDS in a loop to simulate transient outages.
# Logs to both stdout and a file with timestamps for correlation with worker agent logs.

LOG_FILE="/tmp/imds_chaos.log"
BLOCK_DURATION=10
UNBLOCK_DURATION=30

log() {
    echo "$(date -u '+%Y-%m-%dT%H:%M:%S.%3NZ') $1" | tee -a "$LOG_FILE"
}

log "=== IMDS chaos script started ==="
log "Block duration: ${BLOCK_DURATION}s, Unblock duration: ${UNBLOCK_DURATION}s"
log "Logging to: $LOG_FILE"

while true; do
    log "BLOCKING IMDS (169.254.169.254)"
    sudo iptables -A OUTPUT -d 169.254.169.254 -j DROP
    sleep "$BLOCK_DURATION"

    log "UNBLOCKING IMDS (169.254.169.254)"
    sudo iptables -D OUTPUT -d 169.254.169.254 -j DROP
    sleep "$UNBLOCK_DURATION"
done
