SHELL := /bin/bash

COMPOSE := docker compose --env-file .env
RUN_WITH_ENV := set -a; source .env; set +a;
SYSTEM_PYTHON ?= python3
VENV ?= .venv
PYTHON := $(VENV)/bin/python
RUN_PYTHON = $(RUN_WITH_ENV) taskset -c "$${COORDINATOR_CPUS:-$${HARNESS_CPUS:-2,3}}" $(PYTHON)
RUNS_DIR ?= runs

MESSAGES ?= 1000000
TOPICS ?= 30000
USERS ?= 1000
SCHEMA_FILE ?= /bench/schema_zero.sql

QUERY_CLASS ?= window
NEEDLE ?= gamma
LIMIT ?= 10
CLIENTS ?= 2
QUERIES ?= 250
LOAD_RATE ?= 250
PROBES ?= 100
SETTLE ?= 30
MEASURE_SECONDS ?= 60
DRAIN_SECONDS ?= 30
TIMEOUT ?= 30
HARNESS_CORES ?= 2
REGISTRATION_RATE ?= 200
QUERY_DIVERSITY ?= unique
SEED ?= 42
APPARATUS_RETRIES ?= 1

QUERY_STAGES ?= 250,500,750,1000
WRITE_STAGES ?= 250,500,750,1000
REPEATS ?= 3

INIT_SUBSCRIBERS ?= 100
INIT_CLIENTS ?= 100
DENSITY_SUBSCRIBERS ?= 1000
DENSITY_CLIENTS ?= 100

.PHONY: help doctor prepare-output venv db-up db-seed \
        hasura-up hasura-setup hasura-preflight hasura-smoke \
        hasura-query-series hasura-write-series hasura-init \
        hasura-density-init hasura-expressivity hasura-down \
        zero-up zero-preflight zero-smoke zero-query-series \
        zero-write-series zero-init zero-density-init \
        zero-expressivity zero-vacuum zero-down

help:
	@echo "Pruefung und Einrichtung:"
	@echo "  make doctor               Voraussetzungen und .env pruefen"
	@echo ""
	@echo "Einrichtung:"
	@echo "  make venv                 Python-Umgebung und Abhaengigkeiten anlegen"
	@echo "  make db-seed              gemeinsames Schema und Seed erzeugen"
	@echo ""
	@echo "Hasura:"
	@echo "  make hasura-up"
	@echo "  make hasura-preflight"
	@echo "  make hasura-smoke"
	@echo "  make hasura-query-series"
	@echo "  make hasura-write-series"
	@echo "  make hasura-init"
	@echo "  make hasura-density-init"
	@echo "  make hasura-expressivity"
	@echo "  make hasura-down"
	@echo ""
	@echo "Zero:"
	@echo "  make zero-up"
	@echo "  make zero-preflight"
	@echo "  make zero-smoke"
	@echo "  make zero-query-series"
	@echo "  make zero-write-series"
	@echo "  make zero-init"
	@echo "  make zero-density-init"
	@echo "  make zero-expressivity"
	@echo "  make zero-down"
	@echo ""
	@echo "Neue Messergebnisse werden unter $(RUNS_DIR)/ gespeichert."

doctor:
	@test -f .env || { echo "FEHLER: .env fehlt; zuerst 'cp .env.example .env' ausfuehren."; exit 1; }
	@command -v docker >/dev/null || { echo "FEHLER: docker fehlt."; exit 1; }
	@docker compose version >/dev/null || { echo "FEHLER: docker compose ist nicht verfuegbar."; exit 1; }
	@command -v $(SYSTEM_PYTHON) >/dev/null || { echo "FEHLER: $(SYSTEM_PYTHON) fehlt."; exit 1; }
	@command -v taskset >/dev/null || { echo "FEHLER: taskset fehlt."; exit 1; }
	@test -f zero/package-lock.json || { echo "FEHLER: zero/package-lock.json fehlt."; exit 1; }
	@$(RUN_WITH_ENV) test -n "$${PG_CPUS:-}" -a -n "$${HASURA_CPUS:-}" -a -n "$${ZERO_CPUS:-}" -a -n "$${HARNESS_CPUS:-}" || { echo "FEHLER: CPU-Zuordnung in .env unvollstaendig."; exit 1; }
	@$(RUN_WITH_ENV) taskset -c "$${COORDINATOR_CPUS:-$$HARNESS_CPUS}" true || { echo "FEHLER: Coordinator-CPU-Zuordnung ist auf diesem Host ungueltig."; exit 1; }
	@echo "Docker: $$(docker --version)"
	@echo "Compose: $$(docker compose version)"
	@echo "Python: $$($(SYSTEM_PYTHON) --version)"
	@$(RUN_WITH_ENV) echo "CPU: PostgreSQL=$$PG_CPUS Hasura=$$HASURA_CPUS Zero=$$ZERO_CPUS Harness=$$HARNESS_CPUS Coordinator=$${COORDINATOR_CPUS:-$$HARNESS_CPUS}"

prepare-output:
	mkdir -p $(RUNS_DIR)

venv:
	$(SYSTEM_PYTHON) -m venv $(VENV)
	$(PYTHON) -m pip install -r benchmark/requirements.txt

db-up:
	$(COMPOSE) -f compose/zero.yml up -d postgres

db-seed: db-up
	$(RUN_WITH_ENV) $(COMPOSE) -f compose/zero.yml exec -T postgres \
		psql -v ON_ERROR_STOP=1 \
		     -U $${POSTGRES_USER:-benchmark} \
		     -d $${POSTGRES_DB:-chat} \
		     -f $(SCHEMA_FILE)
	$(RUN_WITH_ENV) $(COMPOSE) -f compose/zero.yml exec -T postgres \
		psql -v ON_ERROR_STOP=1 \
		     -v messages=$(MESSAGES) \
		     -v topics=$(TOPICS) \
		     -v users=$(USERS) \
		     -U $${POSTGRES_USER:-benchmark} \
		     -d $${POSTGRES_DB:-chat} \
		     -f /bench/seed_zero_1m.sql

hasura-up:
	-$(COMPOSE) -f compose/zero.yml stop zero-cache zero-query-api postgres-meta
	$(COMPOSE) -f compose/hasura.yml up -d postgres hasura
	@echo "Warte auf Hasura ..."
	@sleep 10
	$(MAKE) hasura-setup

hasura-setup:
	$(RUN_PYTHON) benchmark/hasura_setup.py

hasura-preflight:
	$(RUN_PYTHON) benchmark/hasura_preflight.py \
		--expect-messages $(MESSAGES) \
		--expect-topics $(TOPICS) \
		--expect-users $(USERS)

hasura-smoke: prepare-output
	$(RUN_PYTHON) benchmark/hasura_smoke.py \
		--queries $(QUERIES) \
		--clients $(CLIENTS) \
		--query-class $(QUERY_CLASS) \
		--query-diversity $(QUERY_DIVERSITY) \
		--registration-rate $(REGISTRATION_RATE) \
		--needle $(NEEDLE) \
		--limit $(LIMIT) \
		--topics $(TOPICS) \
		--users $(USERS) \
		--probes $(PROBES) \
		--settle $(SETTLE) \
		--measure-seconds $(MEASURE_SECONDS) \
		--drain-seconds $(DRAIN_SECONDS) \
		--timeout $(TIMEOUT) \
		--load-rate $(LOAD_RATE) \
		--harness-cores $(HARNESS_CORES) \
		--seed $(SEED) \
		--json-out $(RUNS_DIR)/hasura_smoke.json

hasura-query-series: prepare-output
	$(RUN_PYTHON) benchmark/hasura_series.py \
		--mode query \
		--query-class $(QUERY_CLASS) \
		--queries $(QUERY_STAGES) \
		--load-rate $(LOAD_RATE) \
		--clients $(CLIENTS) \
		--registration-rate $(REGISTRATION_RATE) \
		--harness-cores $(HARNESS_CORES) \
		--repeats $(REPEATS) \
		--apparatus-retries $(APPARATUS_RETRIES) \
		--shuffle-seed $(SEED) \
		--out $(RUNS_DIR)/hasura_$(QUERY_CLASS)_query_series.json

hasura-write-series: prepare-output
	$(RUN_PYTHON) benchmark/hasura_series.py \
		--mode write \
		--query-class $(QUERY_CLASS) \
		--queries $(QUERIES) \
		--rates $(WRITE_STAGES) \
		--clients $(CLIENTS) \
		--registration-rate $(REGISTRATION_RATE) \
		--harness-cores $(HARNESS_CORES) \
		--repeats $(REPEATS) \
		--apparatus-retries $(APPARATUS_RETRIES) \
		--shuffle-seed $(SEED) \
		--out $(RUNS_DIR)/hasura_$(QUERY_CLASS)_write_series.json

hasura-init: prepare-output
	$(RUN_PYTHON) benchmark/hasura_init.py \
		--subscribers $(INIT_SUBSCRIBERS) \
		--clients $(INIT_CLIENTS) \
		--query-class $(QUERY_CLASS) \
		--topics $(TOPICS) \
		--limit $(LIMIT) \
		--needle $(NEEDLE) \
		--barrier \
		--json-out $(RUNS_DIR)/hasura_$(QUERY_CLASS)_init_n$(INIT_SUBSCRIBERS)_c$(INIT_CLIENTS).json

hasura-density-init: prepare-output
	$(RUN_PYTHON) benchmark/hasura_batch_init.py \
		--subscribers $(DENSITY_SUBSCRIBERS) \
		--clients $(DENSITY_CLIENTS) \
		--query-class $(QUERY_CLASS) \
		--topics $(TOPICS) \
		--limit $(LIMIT) \
		--needle $(NEEDLE) \
		--barrier \
		--json-out $(RUNS_DIR)/hasura_$(QUERY_CLASS)_density_n$(DENSITY_SUBSCRIBERS)_c$(DENSITY_CLIENTS).json

hasura-expressivity: prepare-output
	$(RUN_PYTHON) benchmark/hasura_expressivity.py \
		--json-out $(RUNS_DIR)/expr_hasura.json

hasura-down:
	$(COMPOSE) -f compose/hasura.yml down

zero-up:
	-$(COMPOSE) -f compose/hasura.yml stop hasura
	$(COMPOSE) -f compose/zero.yml up -d --build \
		postgres postgres-meta zero-query-api zero-cache
	$(COMPOSE) -f compose/zero.yml build zero-client

zero-preflight:
	$(RUN_PYTHON) benchmark/zero_preflight.py \
		--expect-messages $(MESSAGES) \
		--expect-topics $(TOPICS) \
		--expect-users $(USERS)

zero-smoke: prepare-output
	$(RUN_PYTHON) benchmark/zero_smoke.py \
		--queries $(QUERIES) \
		--clients $(CLIENTS) \
		--query-class $(QUERY_CLASS) \
		--needle $(NEEDLE) \
		--limit $(LIMIT) \
		--topics $(TOPICS) \
		--users $(USERS) \
		--probes $(PROBES) \
		--settle $(SETTLE) \
		--measure-seconds $(MEASURE_SECONDS) \
		--drain-seconds $(DRAIN_SECONDS) \
		--timeout $(TIMEOUT) \
		--load-rate $(LOAD_RATE) \
		--harness-cores $(HARNESS_CORES) \
		--seed $(SEED) \
		--json-out $(RUNS_DIR)/zero_smoke.json

zero-query-series: prepare-output
	$(RUN_PYTHON) benchmark/zero_series.py \
		--mode query \
		--query-class $(QUERY_CLASS) \
		--queries $(QUERY_STAGES) \
		--load-rate $(LOAD_RATE) \
		--clients $(CLIENTS) \
		--harness-cores $(HARNESS_CORES) \
		--repeats $(REPEATS) \
		--apparatus-retries $(APPARATUS_RETRIES) \
		--shuffle-seed $(SEED) \
		--out $(RUNS_DIR)/zero_$(QUERY_CLASS)_query_series.json

zero-write-series: prepare-output
	$(RUN_PYTHON) benchmark/zero_series.py \
		--mode write \
		--query-class $(QUERY_CLASS) \
		--queries $(QUERIES) \
		--rates $(WRITE_STAGES) \
		--clients $(CLIENTS) \
		--harness-cores $(HARNESS_CORES) \
		--repeats $(REPEATS) \
		--apparatus-retries $(APPARATUS_RETRIES) \
		--shuffle-seed $(SEED) \
		--out $(RUNS_DIR)/zero_$(QUERY_CLASS)_write_series.json

zero-init: prepare-output
	$(RUN_PYTHON) benchmark/zero_init.py \
		--subscribers $(INIT_SUBSCRIBERS) \
		--clients $(INIT_CLIENTS) \
		--query-class $(QUERY_CLASS) \
		--topics $(TOPICS) \
		--limit $(LIMIT) \
		--needle $(NEEDLE) \
		--barrier \
		--json-out $(RUNS_DIR)/zero_$(QUERY_CLASS)_init_n$(INIT_SUBSCRIBERS)_c$(INIT_CLIENTS).json

zero-density-init: prepare-output
	$(RUN_PYTHON) benchmark/zero_query_density_init.py \
		--subscribers $(DENSITY_SUBSCRIBERS) \
		--clients $(DENSITY_CLIENTS) \
		--query-class $(QUERY_CLASS) \
		--topics $(TOPICS) \
		--limit $(LIMIT) \
		--needle $(NEEDLE) \
		--barrier \
		--json-out $(RUNS_DIR)/zero_$(QUERY_CLASS)_density_n$(DENSITY_SUBSCRIBERS)_c$(DENSITY_CLIENTS).json

zero-expressivity: prepare-output
	$(RUN_PYTHON) benchmark/zero_expressivity.py \
		--topics $(TOPICS) \
		--json-out $(RUNS_DIR)/expr_zero.json

zero-vacuum:
	$(RUN_WITH_ENV) $(COMPOSE) -f compose/zero.yml exec -T postgres \
		psql -U $${POSTGRES_USER:-benchmark} \
		     -d $${POSTGRES_DB:-chat} \
		     -c "VACUUM (ANALYZE) messages"

zero-down:
	$(COMPOSE) -f compose/zero.yml down
