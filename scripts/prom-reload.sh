#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# Apply edits under deploy/prometheus/ and verify Prometheus is really running them.
#
# On Docker Desktop, a running container can keep seeing a bind-mounted file's
# *old size* after the file is edited on the host, so it reads the new content
# cut off at the old length. If the cut still parses as YAML, Prometheus loads
# it without complaint and silently loses rule groups or whole scrape jobs.
# (M1 lost the agents job this way: no targets, so no alert could fire.)
# So: if the container's copy differs from the host's, recreate the container
# (a fresh mount reads the real file) instead of reloading. Then verify rules
# and targets.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

PROM=${PROM:-http://127.0.0.1:19090}
FILES=(prometheus.yml rules.yml)

stale=0
for f in "${FILES[@]}"; do
  host=$(shasum -a 256 "$ROOT/deploy/prometheus/$f" | cut -d' ' -f1)
  inside=$(docker exec hafo-prometheus sha256sum "/etc/prometheus/$f" | cut -d' ' -f1)
  [[ "$host" == "$inside" ]] || { log "container sees a stale copy of $f"; stale=1; }
done

if ((stale)); then
  log "recreating prometheus"
  (cd "$ROOT" && docker compose up -d --wait --force-recreate prometheus)
else
  curl -fsS -X POST "$PROM/-/reload" >/dev/null
fi

want_rules=$(docker exec hafo-prometheus promtool check rules /etc/prometheus/rules.yml | awk '/SUCCESS/ { print $2 }')
[[ -n "$want_rules" ]] || die "promtool rejected rules.yml"

for _ in $(seq 20); do
  read -r rules agents_up < <(python3 - "$PROM" <<'EOF'
import json, sys, urllib.request
base = sys.argv[1]
get = lambda p: json.load(urllib.request.urlopen(base + p))["data"]
rules = sum(len(g["rules"]) for g in get("/api/v1/rules")["groups"])
up = sum(1 for t in get("/api/v1/targets")["activeTargets"] if t["labels"]["job"] == "agents" and t["health"] == "up")
print(rules, up)
EOF
  )
  if [[ "$rules" == "$want_rules" && "$agents_up" == "3" ]]; then
    log "Prometheus running $rules/$want_rules rules, 3/3 agent targets up"
    exit 0
  fi
  sleep 0.5
done
die "Prometheus has $rules/$want_rules rules and $agents_up/3 agent targets up"
