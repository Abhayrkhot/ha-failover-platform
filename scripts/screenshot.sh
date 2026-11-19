#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# Render the Grafana dashboard to docs/img/dashboard.png.
#
# Usage: scripts/screenshot.sh [minutes]   (window ending now; default 10)
set -euo pipefail
source "$(dirname "$0")/lib.sh"

minutes=${1:-10}
cd "$ROOT"
docker compose --profile screenshots up -d --wait renderer >/dev/null 2>&1 || docker compose --profile screenshots up -d renderer >/dev/null
# Grafana looks for the renderer at startup; restart it if it gave up.
curl -fsS -u admin:hafo-grafana-dev "http://127.0.0.1:19500/api/plugins/grafana-image-renderer/settings" >/dev/null 2>&1 \
  || { docker compose restart grafana >/dev/null; sleep 8; }

to=$(( $(now_ms) )); from=$(( to - minutes * 60 * 1000 ))
mkdir -p docs/img
log "rendering the last ${minutes} minutes"
curl -fsS -u admin:hafo-grafana-dev -o docs/img/dashboard.png \
  "http://127.0.0.1:19500/render/d/hafo/hafo?orgId=1&from=${from}&to=${to}&width=1800&height=1800&tz=UTC&kiosk=1&timeout=60"
log "wrote docs/img/dashboard.png ($(wc -c < docs/img/dashboard.png) bytes)"
