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
PKG      ?=
# LOGDIR is where the *-errors targets keep the full log.
LOGDIR   ?= .test-logs

TEST_FLAGS = $(RACE) $(VERBOSE) -count=1 -timeout $(TIMEOUT) \
	$(if $(RUN),-run '$(RUN)') $(if $(PARALLEL),-p $(PARALLEL))

.DEFAULT_GOAL := help
.PHONY: help build vet fmt fmt-check check \
	test test-unit test-integration test-pagination test-sql test-all \
	test-errors test-unit-errors test-pagination-errors report \
	services-up services-down

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

check: build vet fmt-check ## build + vet + format check

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

test-sql: ## sql service integration suite, -race -v
	$(GO) test -tags integration $(TEST_FLAGS) ./tests/packages/sql/...

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
