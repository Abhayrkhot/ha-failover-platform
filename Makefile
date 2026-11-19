COMPOSE := docker compose

# Lab credentials, for hafoctl's SQL-level faults.
include deploy/dev.env
export MYSQL_ROOT_PASSWORD

.PHONY: up bootstrap status health test bench-detect prom-reload netem-check logs down clean

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
