
# ═══════════════════════════════════════════════════════════════════════════
# Makefile — Financial Data Platform
# ═══════════════════════════════════════════════════════════════════════════
# Usage: make <target>
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: help install lint format test test-unit test-integration security \
        terraform-init terraform-plan terraform-apply deploy-workflows \
        trigger-daily trigger-reprocessing clean

# Default target
help: ## Show this help message
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "  Financial Data Platform — Available Commands"
	@echo "═══════════════════════════════════════════════════════════════"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'
	@echo "═══════════════════════════════════════════════════════════════"

# ─── Setup ──────────────────────────────────────────────────────────────

install: ## Install all dependencies
	pip install -r requirements.txt
	pip install -r requirements-dev.txt
	pre-commit install
	@echo "✓ Dependencies installed and pre-commit hooks configured"

# ─── Code Quality ──────────────────────────────────────────────────────

lint: ## Run linters (ruff + mypy)
	ruff check .
	mypy scripts/ --ignore-missing-imports || true

format: ## Auto-format code (black + isort)
	black .
	isort .
	ruff check --fix .
	@echo "✓ Code formatted"

check: ## Run all code quality checks (no auto-fix)
	black --check .
	isort --check-only .
	ruff check .
	@echo "✓ All checks passed"

# ─── Testing ───────────────────────────────────────────────────────────

test: ## Run all tests with coverage
	pytest tests/ \
		--verbose \
		--tb=short \
		--cov=scripts \
		--cov=databricks \
		--cov-report=term-missing \
		-x

test-unit: ## Run unit tests only
	pytest tests/ -m unit \
		--verbose \
		--tb=short \
		-x

test-integration: ## Run integration tests (requires AWS/Databricks)
	pytest tests/ -m integration \
		--verbose \
		--tb=short

test-fast: ## Run tests in parallel
	pytest tests/ -m unit \
		--verbose \
		-n auto \
		-x

# ─── Security ──────────────────────────────────────────────────────────

security: ## Run security scans
	bandit -r scripts/ databricks/ -ll
	pip-audit
	@echo "✓ Security scan complete"

# ─── Terraform ─────────────────────────────────────────────────────────

terraform-init: ## Initialize Terraform
	cd terraform/environments/dev && terraform init

terraform-plan: ## Plan Terraform changes
	cd terraform/environments/dev && terraform plan

terraform-apply: ## Apply Terraform changes
	cd terraform/environments/dev && terraform apply

terraform-destroy: ## Destroy Terraform resources (DANGER)
	@echo "⚠️  WARNING: This will destroy all infrastructure!"
	@read -p "Type 'destroy' to confirm: " confirm; \
	if [ "$$confirm" = "destroy" ]; then \
		cd terraform/environments/dev && terraform destroy; \
	else \
		echo "Cancelled."; \
	fi

# ─── Databricks ────────────────────────────────────────────────────────

deploy-workflows: ## Deploy Databricks workflows
	bash scripts/deploy_workflows.sh dev

trigger-daily: ## Trigger daily pipeline
	bash scripts/trigger_pipeline.sh daily --wait

trigger-reprocessing: ## Trigger quarantine reprocessing
	bash scripts/trigger_pipeline.sh reprocessing --wait

trigger-maintenance: ## Trigger table maintenance
	bash scripts/trigger_pipeline.sh maintenance

# ─── Data Generation ──────────────────────────────────────────────────

generate-data: ## Generate sample transaction data
	python scripts/01_generate_transactions.py
	python scripts/02_inject_data_defects.py
	python scripts/03_upload_to_s3.py

# ─── Cleanup ──────────────────────────────────────────────────────────

clean: ## Remove generated files and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf coverage.xml test-results.xml htmlcov/ .coverage
	rm -rf data/generated/*.csv
	@echo "✓ Cleaned up"

