COMPOSE := docker compose

# Lab credentials, for hafoctl's checks, faults and write load.
include deploy/dev.env
export MYSQL_ROOT_PASSWORD APP_PASSWORD

.PHONY: up bootstrap status health test e2e bench-detect bench-failover seed backup drill rebuild prom-reload netem-check logs down clean

up:            ## build agents, start everything, wait until healthy
	$(COMPOSE) up -d --wait --build

bootstrap: up  ## one-time replication setup on a fresh cluster
	scripts/bootstrap.sh

status:        ## topology, read-only flags, replication state per node (from MySQL)
	scripts/status.sh

health:        ## agents' verdicts as a Redis majority sees them
	go run ./cmd/hafoctl health

test:          ## unit tests
	go test -race ./...

bench-detect:  ## break a replica repeatedly and time detection; ARGS=-trials=3
	go run ./cmd/hafoctl bench detect $(ARGS)

bench-failover: ## fail the primary under write load and time recovery; ARGS=-trials=3
	go run ./cmd/hafoctl bench failover $(ARGS)

e2e:           ## scenario suite: breaks and repairs the cluster, asserts invariants; ARGS=-only=steady
	go run ./cmd/hafoctl e2e $(ARGS)

seed:          ## load app.payload with random rows through the router; ARGS=-mib=1024
	go run ./cmd/hafoctl seed $(ARGS)

drill:         ## DESTROYS all MySQL data, restores it from backup + binlog archive, times it
	go run ./cmd/hafoctl drill $(ARGS)

backup:        ## hot physical backup (Clone) of a healthy replica into the backups volume; KEEP=3
	scripts/backup.sh $(NODE)

rebuild:       ## replace a fenced node's data and let the coordinator rejoin it; NODE=mysql-1
	scripts/rebuild.sh $(NODE)

prom-reload:   ## apply deploy/prometheus edits; verify rules and agent targets
	scripts/prom-reload.sh

netem-check:   ## confirm tc netem works inside the Docker VM
	$(COMPOSE) build nettools
	scripts/netem-check.sh

logs:
	$(COMPOSE) logs -f

down:          ## stop containers, keep data (nodes restart read-only)
	$(COMPOSE) down

clean:         ## stop and delete all data volumes
	$(COMPOSE) down -v --remove-orphans
