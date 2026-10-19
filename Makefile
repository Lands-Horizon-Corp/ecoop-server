# ecoop-server
#
# Tests come in two kinds:
#   unit         no build tag; need nothing but Go (tests/utils/...).
#   integration  `//go:build integration`; run against the docker compose stack
#                (Postgres, Kafka, Debezium, ...), which the suites start
#                themselves with `docker compose up -d --wait <service>`.
#
# Every test target runs with the race detector and verbose output, and with
# -count=1 so a cached "ok" never hides a run. Override on the command line:
#   make test-unit RACE=            # without -race
#   make test-pagination RUN=TestBank_ TIMEOUT=5m
#   make test-integration PKG=./tests/packages/sql/...

GO       ?= go
RACE     ?= -race
VERBOSE  ?= -v
TIMEOUT  ?= 30m
RUN      ?=
# PARALLEL is how many packages go test runs at once (-p); empty = Go's default.
PARALLEL ?=
# SQL_PARALLEL caps how many sql tests run at once. Its migration tests are
# heavy, and at Go's default (one per CPU) they starve each other of the shared
# Postgres and time out; 3 passes cleanly.
SQL_PARALLEL ?= 3
PKG      ?=
# LOGDIR is where the *-errors targets keep the full log.
LOGDIR   ?= .test-logs

TEST_FLAGS = $(RACE) $(VERBOSE) -count=1 -timeout $(TIMEOUT) \
	$(if $(RUN),-run '$(RUN)') $(if $(PARALLEL),-p $(PARALLEL))

.DEFAULT_GOAL := help
.PHONY: help build vet fmt fmt-check check \
	test test-unit test-integration test-pagination test-sql test-cqrs test-heavy test-all \
	test-observability test-telemetry-e2e telemetry-up telemetry-down test-light verify \
	test-errors test-unit-errors test-pagination-errors report \
	services-up services-down lint lint-fix lint-config

help: ## list the targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

# ---- build & static checks -------------------------------------------------

build: ## compile every package
	$(GO) build ./...

vet: ## go vet, with and without the integration tag
	$(GO) vet ./...
	$(GO) vet -tags integration ./...

fmt: ## format all Go files
	gofmt -w .

fmt-check: ## fail if any Go file is not gofmt-formatted
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "not formatted:"; echo "$$out"; exit 1; fi

# The version the .golangci.yml schema was written and checked against. Run it
# with `go run`, so nothing is installed and everyone uses the same one.
GOLANGCI_LINT ?= github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0

lint: ## golangci-lint over every package, including the integration-tagged tests
	$(GO) run $(GOLANGCI_LINT) run ./...

lint-fix: ## golangci-lint with --fix: repairs what it can (comment periods, imports)
	$(GO) run $(GOLANGCI_LINT) run --fix ./...

lint-config: ## check .golangci.yml against the golangci-lint schema
	$(GO) run $(GOLANGCI_LINT) config verify

check: build vet fmt-check lint ## build + vet + format check + lint

# ---- tests -----------------------------------------------------------------

test-unit: ## unit tests only (no integration tag), -race -v
	$(GO) test $(TEST_FLAGS) $(if $(PKG),$(PKG),./...)

test-integration: ## integration tests only (tests/packages/...), -race -v; needs docker
	$(GO) test -tags integration -p $(or $(PARALLEL),1) $(TEST_FLAGS) $(if $(PKG),$(PKG),./tests/packages/...)

test-all: ## everything: unit + integration in one run, -race -v; needs docker
	$(GO) test -tags integration -p $(or $(PARALLEL),1) $(TEST_FLAGS) $(if $(PKG),$(PKG),./...)

test: test-all ## alias for test-all

test-pagination: ## pagination integration suite, -race -v
	$(GO) test -tags integration $(TEST_FLAGS) ./tests/packages/pagination/...

test-sql: ## sql service integration suite, -race -v, -parallel $(SQL_PARALLEL)
	$(GO) test -tags integration $(TEST_FLAGS) -parallel $(SQL_PARALLEL) ./tests/packages/sql/...

# The cqrs, sql and pagination suites hammer one shared Postgres. Their TestMain
# takes a lock (tests/internal/suitelock) so they queue even under `go test ./...`;
# -p 1 also keeps them from competing for CPU while they wait.
test-heavy: ## cqrs + sql + pagination integration suites, one at a time (sql capped at -parallel $(SQL_PARALLEL))
	$(MAKE) --no-print-directory test-cqrs
	$(MAKE) --no-print-directory test-sql
	$(MAKE) --no-print-directory test-pagination

test-cqrs: ## cqrs integration suite, -race -v
	$(GO) test -tags integration $(TEST_FLAGS) ./tests/packages/cqrs/...

# The observability package, its shared test helpers, the wiring test and the
# dashboard check need nothing but Go, so this is the fast one to run often.
test-observability: ## observability package, shared helpers, wiring and dashboard checks (no docker)
	$(GO) test $(TEST_FLAGS) ./tests/packages/observability/... ./tests/internal/...

# The telemetry stack: a collector, Prometheus, Tempo and Grafana (profile
# "telemetry", not started by the default stack). Grafana is on :3000 with the
# dashboard loaded; the collector is on :14318.
telemetry-up: ## start the collector, Prometheus, Tempo and Grafana
	docker compose --profile telemetry up -d otel-collector prometheus tempo grafana

telemetry-down: ## stop them
	docker compose --profile telemetry stop otel-collector prometheus tempo grafana

# Runs the app's own Setup against the real stack and reads the result back from
# Prometheus, Tempo and Grafana. Skips when the stack is not running.
test-telemetry-e2e: ## export through the real collector and read it back (needs make telemetry-up)
	$(GO) test -tags integration $(TEST_FLAGS) -run 'TestE2E' ./tests/packages/observability/otelobs/

# Every integration suite except the three that hammer the shared Postgres
# (cqrs, sql, pagination). They run one package at a time (-p 1) so none of
# them times out waiting for another.
test-light: ## every integration suite except cqrs, sql, pagination; -p 1
	$(GO) test -tags integration -p 1 $(TEST_FLAGS) \
		$$($(GO) list -tags integration ./tests/packages/... | grep -v -E '/(cqrs|sql|pagination)$$')

# verify is what to run before a commit that touches telemetry or the services:
# static checks, the fast suite, the light suites, then the heavy ones, one at a
# time. The first failure stops it.
verify: check test-observability test-light test-heavy ## check + every suite, one at a time (needs docker; long)

# ---- failures only ---------------------------------------------------------
# Same runs as above, but the full -v output goes to $(LOGDIR)/<name>.log and
# only what went wrong is printed: failing tests with their messages, panics,
# data races and build errors, then the counts. The exit status is the test
# run's, so these work in CI too.

test-errors: ## everything (unit + integration), print only failures; full log in .test-logs/
	@mkdir -p $(LOGDIR)
	@$(GO) test -tags integration -p $(or $(PARALLEL),1) $(TEST_FLAGS) $(if $(PKG),$(PKG),./...) > $(LOGDIR)/all.log 2>&1; status=$$?; \
		$(MAKE) --no-print-directory report LOG=$(LOGDIR)/all.log; exit $$status

test-unit-errors: ## unit tests only, print only failures
	@mkdir -p $(LOGDIR)
	@$(GO) test $(TEST_FLAGS) $(if $(PKG),$(PKG),./...) > $(LOGDIR)/unit.log 2>&1; status=$$?; \
		$(MAKE) --no-print-directory report LOG=$(LOGDIR)/unit.log; exit $$status

test-pagination-errors: ## pagination suite only, print only failures
	@mkdir -p $(LOGDIR)
	@$(GO) test -tags integration $(TEST_FLAGS) ./tests/packages/pagination/... > $(LOGDIR)/pagination.log 2>&1; status=$$?; \
		$(MAKE) --no-print-directory report LOG=$(LOGDIR)/pagination.log; exit $$status

# report LOG=<file> summarizes a `go test -v` log.
report:
	@test -f "$(LOG)" || { echo "usage: make report LOG=<go test -v log>"; exit 2; }
	@echo "== build errors =="; grep -E '^# |\[build failed\]|\[setup failed\]|^[^ ]+\.go:[0-9]+:[0-9]+: ' "$(LOG)" | grep -v -E '^\s+[A-Za-z_]+\.go' || echo "none"
	@echo; echo "== failing tests (with their messages) =="; grep -A6 -E '^\s*--- FAIL' "$(LOG)" | grep -v -E '^\s*(--- PASS|=== )' || echo "none"
	@echo; echo "== panics =="; grep -A14 -E '^(panic:|--- FAIL.*panic|.*test timed out)' "$(LOG)" || echo "none"
	@echo; echo "== data races =="; grep -A22 'WARNING: DATA RACE' "$(LOG)" || echo "none"
	@echo; echo "== failing packages =="; grep -E '^(FAIL|--- FAIL: Test[A-Za-z0-9_]+ )' "$(LOG)" | grep -E '^FAIL\s' || echo "none"
	@echo; echo "== counts =="; \
		echo "passed: $$(grep -cE '^\s*--- PASS' "$(LOG)")  failed: $$(grep -cE '^\s*--- FAIL' "$(LOG)")  skipped: $$(grep -cE '^\s*--- SKIP' "$(LOG)")  races: $$(grep -c 'WARNING: DATA RACE' "$(LOG)")"; \
		echo "full log: $(LOG)"

# ---- local services --------------------------------------------------------

services-up: ## start the whole docker compose stack (the suites also start what they need)
	docker compose up -d --wait

services-down: ## stop the docker compose stack
	docker compose down
