COMPOSE := docker compose

.PHONY: up bootstrap status netem-check logs down clean

up:            ## start all nodes and wait until healthy
	$(COMPOSE) up -d --wait

bootstrap: up  ## one-time replication setup on a fresh cluster
	scripts/bootstrap.sh

status:        ## topology, read-only flags, replication state per node
	scripts/status.sh

netem-check:   ## confirm tc netem works inside the Docker VM
	$(COMPOSE) build nettools
	scripts/netem-check.sh

logs:
	$(COMPOSE) logs -f

down:          ## stop containers, keep data (nodes restart read-only)
	$(COMPOSE) down

clean:         ## stop and delete all data volumes
	$(COMPOSE) down -v --remove-orphans
