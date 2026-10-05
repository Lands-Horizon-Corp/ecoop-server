.PHONY: test test-up test-down test-default test-race test-all test-v vet migrate-up migrate-down migrate-status migrate-create migrate-diff migrate-watch atlas-diff

# Services the tests need: Redis, Redis Sentinel and Postgres (see docker-compose.yml).
TEST_SERVICES := redis redis-master redis-sentinel postgres
TESTFLAGS ?= -count=1 -timeout 10m

# `make test-all` runs the suite under every build-tag variant: the default build (!race) and the
# race detector (-race, which sets the `race` tag). Some tests only exist or relax under one of them.
test-all: vet test-default test-race

test: test-default

test-up:
	docker compose up -d --wait $(TEST_SERVICES)

test-down:
	docker compose stop $(TEST_SERVICES)

test-default: test-up
	go test $(TESTFLAGS) ./...

test-race: test-up
	go test -race $(TESTFLAGS) ./...

vet:
	go vet ./...

# Like test-all, but lists every test's result and a passed/failed/skipped count per build variant.
test-v: vet test-up
	@final=0; for flags in "" "-race"; do \
		echo "== go test $$flags =="; \
		out=$$(mktemp); code=0; \
		go test $$flags $(TESTFLAGS) -v ./... > $$out 2>&1 || code=$$?; \
		grep -E '^--- (PASS|FAIL|SKIP)' $$out; \
		echo "passed=$$(grep -cE '^[[:space:]]*--- PASS' $$out) failed=$$(grep -cE '^[[:space:]]*--- FAIL' $$out) skipped=$$(grep -cE '^[[:space:]]*--- SKIP' $$out)"; \
		grep -E '^(ok|FAIL)[[:space:]]' $$out; \
		rm -f $$out; \
		[ $$code -eq 0 ] || final=$$code; \
		echo; \
	done; exit $$final

# Migrations use DATABASE_URL (see .env). `make migrate-diff name=add_orders` writes a goose file
# from your model changes (models are listed in src/models/models.go).
migrate-up:
	go run ./cmd/migrate up

migrate-down:
	go run ./cmd/migrate down

migrate-status:
	go run ./cmd/migrate status

migrate-create:
	go run ./cmd/migrate create $(name)

migrate-diff:
	go run ./cmd/migrate diff $(name)

# Re-runs the diff whenever a file in src/models changes. Install watchexec for instant triggers;
# without it this polls every 2s. A new file is only written when the models differ from the database.
migrate-watch:
	@echo "watching src/models (Ctrl-C to stop)"
	@if command -v watchexec >/dev/null 2>&1; then \
		watchexec -w src/models -e go --debounce 500ms -- go run ./cmd/migrate diff auto; \
	else \
		prev=""; \
		while true; do \
			cur=$$(cat src/models/*.go 2>/dev/null | cksum); \
			if [ -n "$$prev" ] && [ "$$cur" != "$$prev" ]; then go run ./cmd/migrate diff auto || true; fi; \
			prev=$$cur; sleep 2; \
		done; \
	fi

# Alternative generator (needs the atlas CLI): handles numeric(p,s), string defaults and more.
atlas-diff:
	atlas migrate diff $(name) --env local
