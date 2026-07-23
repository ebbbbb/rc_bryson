SHELL := /bin/sh
GO_IMAGE := golang:1.26.2-bookworm@sha256:47ce5636e9936b2c5cbf708925578ef386b4f8872aec74a67bd13a627d242b19
SKILL_VALIDATOR_IMAGE := reliable-notifier-skill-validator:2.6.1
GO_CONTAINER := docker run --rm \
	-v "$(CURDIR):/src" \
	-v reliable-notifier-go-mod:/go/pkg/mod \
	-v reliable-notifier-go-build:/root/.cache/go-build \
	-w /src $(GO_IMAGE)
GO_RUN := $(GO_CONTAINER) go

.PHONY: preflight format format-check lint test test-race integration integration-isolation-test verify-gate verify-slice verify validate-skills migrate up down source-export-check

preflight:
	./scripts/preflight.sh

format:
	gofmt -w $$(find . -type f -name '*.go' -not -path './vendor/*')

format-check:
	@unformatted="$$(gofmt -l $$(find . -type f -name '*.go' -not -path './vendor/*'))" || exit $$?; \
		test -z "$$unformatted" || { printf '%s\n' "$$unformatted"; exit 1; }

lint: format-check
	go vet ./...
	go mod verify
	docker compose config --quiet

test:
	$(GO_RUN) test ./...
	bash ./.codex/hooks/tests/hooks_test.sh

test-race:
	$(GO_RUN) test -race ./...

integration: preflight
	./scripts/compose-test.sh integration

integration-isolation-test: preflight
	./scripts/compose-isolation-fixture.sh

verify-gate: preflight
	./scripts/compose-test.sh gate

verify-slice:
	@test -n "$(SLICE)" || (echo "SLICE is required" >&2; exit 2)
	@case "$(SLICE)" in \
		0) $(MAKE) verify-gate ;; \
		1) $(GO_RUN) test ./internal/delivery && $(MAKE) integration ;; \
		2) $(GO_RUN) test ./internal/delivery ./internal/dispatch && $(MAKE) integration ;; \
		*) echo "slice $(SLICE) is not implemented" >&2; exit 2 ;; \
	esac

validate-skills:
	docker build --quiet -f .agents/tools/Dockerfile -t $(SKILL_VALIDATOR_IMAGE) .
	@for skill in implement-slice verify reliability-review; do \
		docker run --rm -v "$(CURDIR)/.agents/skills:/skills:ro" \
			$(SKILL_VALIDATOR_IMAGE) "/skills/$$skill"; \
	done

migrate: preflight
	$(GO_RUN) run -tags postgres -ldflags '-X main.Version=v4.19.1' \
		github.com/golang-migrate/migrate/v4/cmd/migrate $(ARGS)

verify:
	$(MAKE) preflight
	$(MAKE) lint
	$(MAKE) validate-skills
	$(MAKE) test
	$(MAKE) test-race
	./scripts/compose-test.sh gate
	./scripts/compose-test.sh integration

up:
	docker compose up -d --build --wait --wait-timeout 120

down:
	docker compose down

source-export-check:
	./scripts/source-export-check.sh
