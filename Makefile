# Tether. `make db && make install && make migrate && make test`

PY      ?= .venv/bin/python
VENV_PY ?= python3.12          # NOT python3: that is 3.6 on some machines
PG      ?= tether-pg
PGPORT  ?= 5433

.PHONY: help db db-stop db-logs install migrate test test-fast test-chaos bench bench-recovery plan stats clean

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-16s %s\n", $$1, $$2}'

db: ## start Postgres on $(PGPORT) in Docker
	@docker rm -f $(PG) >/dev/null 2>&1 || true
	docker run -d --name $(PG) --restart unless-stopped \
		-e POSTGRES_PASSWORD=tether -e POSTGRES_USER=tether \
		-e POSTGRES_DB=tether -p $(PGPORT):5432 postgres:16-alpine
	@until docker exec $(PG) pg_isready -U tether -d tether >/dev/null 2>&1; do sleep 1; done
	@echo "postgres ready on localhost:$(PGPORT)"

db-stop: ## remove the container
	docker rm -f $(PG)

db-logs:
	docker logs -f $(PG)

install: ## create .venv and install everything
	$(VENV_PY) -m venv .venv
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -q -r requirements-dev.txt

migrate: ## apply the schema
	$(PY) -m tether.cli migrate

test: ## the whole suite, chaos included
	$(PY) -m pytest -q

test-fast: ## everything except the chaos suite
	$(PY) -m pytest -q --ignore=tests/test_chaos.py

test-chaos: ## only the tests that break something
	$(PY) -m pytest -q tests/test_chaos.py

bench: ## throughput and latency
	$(PY) -m bench.loadgen --rate 2000 --duration 20 --workers 4 --concurrency 8 --batch 20

bench-saturate: ## find the ceiling
	$(PY) -m bench.loadgen --rate 6000 --duration 15 --workers 4 --concurrency 8 --batch 20

bench-recovery: ## redelivery latency and restart recovery
	$(PY) -m bench.recovery --lease-ttl-ms 2000 --tasks 3000

plan: ## show the query plan for the lease statement
	$(PY) -m bench.explain

stats:
	$(PY) -m tether.cli stats

clean:
	rm -rf .venv .pytest_cache **/__pycache__
