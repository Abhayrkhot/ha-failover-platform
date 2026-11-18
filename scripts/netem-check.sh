#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# Proves the Docker VM kernel supports netem: add delay on one node's egress
# and confirm RTT from a peer rises by about that much. M3's fault injector
# depends on this (slow replicas, lossy links).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

FROM=${1:-mysql-2}
TARGET=${2:-mysql-3}
DELAY_MS=${DELAY_MS:-100}

nettools() { # nettools NODE [--cap-add ...] CMD...
  local node=$1; shift
  docker run --rm --network "container:hafo-$node" "$@"
}

avg_rtt() {
  nettools "$FROM" hafo-nettools:local ping -c 5 -i 0.2 -q "$TARGET" \
    | awk -F'/' '/^rtt/ { printf "%.2f", $5 }'
}

clear_qdisc() {
  nettools "$TARGET" --cap-add NET_ADMIN hafo-nettools:local tc qdisc del dev eth0 root 2>/dev/null || true
}
trap clear_qdisc EXIT

clear_qdisc
before=$(avg_rtt)
log "baseline $FROM -> $TARGET avg RTT: ${before} ms"

nettools "$TARGET" --cap-add NET_ADMIN hafo-nettools:local tc qdisc add dev eth0 root netem delay "${DELAY_MS}ms"
after=$(avg_rtt)
log "with ${DELAY_MS}ms netem delay on $TARGET: ${after} ms"

awk -v a="$after" -v b="$before" -v d="$DELAY_MS" 'BEGIN { exit !((a - b) >= 0.8 * d) }' \
  || die "RTT rose by less than 80% of the injected delay; netem is not taking effect"
log "netem works"
