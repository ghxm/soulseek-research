.PHONY: help install test-local deploy monitor destroy clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install package in development mode
	pip install -e .

test-local: ## Test local setup with Docker
	@chmod +x scripts/test-local.sh
	./scripts/test-local.sh

deploy: ## Deploy production infrastructure 
	@chmod +x scripts/deploy.sh
	./scripts/deploy.sh

monitor: ## Monitor production deployment
	@chmod +x scripts/monitor.sh
	./scripts/monitor.sh

destroy: ## Destroy production infrastructure
	@chmod +x scripts/destroy.sh
	./scripts/destroy.sh

clean: ## Clean up cache and temporary files
	rm -rf src/soulseek_research/__pycache__/
	rm -rf build/ dist/ *.egg-info/
	rm -f .env.test
	docker system prune -f

check: ## Check code with basic validation
	python -m py_compile src/soulseek_research/*.py
	@echo "✅ Code validation passed"

docker-build: ## Build Docker image
	docker build -t soulseek-research:latest .

docker-test: ## Quick test of Docker image
	docker run --rm soulseek-research:latest soulseek-research --help