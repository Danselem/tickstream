# tickstream — the Makefile is the only entry point (see AGENTS.md).
# Phase map: install/lint/format/typecheck/test + infra-* land in Phase 0.
# up/down/logs/restart land in Phase 1. quality/dbt-build land in Phase 2.

UV := uv run
TOFU := tofu
INFRA_DIR := infra
BOOTSTRAP_DIR := infra/bootstrap
BACKEND_CONFIG := $(INFRA_DIR)/backend.hcl

ALERT_EMAIL ?= budget-alerts@example.com

.PHONY: help install up down logs restart lint format typecheck test quality dbt-build infra-bootstrap infra-plan infra-apply

help: ## Show targets
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS=":.*?## "} {printf "  %-16s %s\n", $$1, $$2}'

install: ## Install Python deps with uv (writes uv.lock)
	uv sync

up: ## Start all local services (Phase 1+)
	docker compose up -d --build

down: ## Stop all local services
	docker compose down

logs: ## Tail all service logs
	docker compose logs -f

restart: ## Rebuild and restart all services
	docker compose up -d --build --force-recreate

lint: ## Ruff lint
	$(UV) ruff check .

format: ## Ruff format (rewrites files)
	$(UV) ruff format .

typecheck: ## Mypy on src and tests
	$(UV) mypy src tests

test: ## Pytest unit suite (integration tests auto-skip without env)
	$(UV) pytest -m "not integration"

quality: ## Run Great Expectations checks once (Phase 2+)
	$(UV) python -m pipeline.scheduler --once --quality-only

dbt-build: ## Run dbt build (Phase 2+; scheduler runs this only after GE passes)
	$(UV) dbt build --project-dir dbt --profiles-dir dbt

infra-bootstrap: ## One-time: create S3+DynamoDB tofu state backend, write infra/backend.hcl
	cd $(BOOTSTRAP_DIR) && $(TOFU) init && $(TOFU) apply -auto-approve
	@ACCOUNT_ID=$$(aws sts get-caller-identity --query Account --output text); \
	REGION=$${AWS_REGION:-us-east-1}; \
	printf 'bucket = "tickstream-tfstate-%s"\nkey = "tickstream/terraform.tfstate"\nregion = "%s"\ndynamodb_table = "tickstream-tfstate-lock"\nencrypt = true\n' "$$ACCOUNT_ID" "$$REGION" > $(BACKEND_CONFIG)
	@echo "Wrote $(BACKEND_CONFIG)"

infra-plan: ## Show tofu plan (needs infra/backend.hcl; run infra-bootstrap first)
	@if [ ! -f $(BACKEND_CONFIG) ]; then echo "ERROR: $(BACKEND_CONFIG) missing — run 'make infra-bootstrap' first."; exit 1; fi
	@if [ "$(ALERT_EMAIL)" = "budget-alerts@example.com" ]; then echo "WARNING: placeholder ALERT_EMAIL in use — override at apply time (ALERT_EMAIL=you@example.com)."; fi
	cd $(INFRA_DIR) && $(TOFU) init -backend-config=backend.hcl && $(TOFU) plan -out=plan.tfplan -var="alert_email=$(ALERT_EMAIL)"

infra-apply: ## Apply the saved plan — REQUIRES EXPLICIT HUMAN APPROVAL
	cd $(INFRA_DIR) && $(TOFU) apply plan.tfplan
