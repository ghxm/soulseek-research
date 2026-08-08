.PHONY: help install test-local deploy monitor destroy clean docker-build docker-test

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install package in development mode
	pip install -e ".[dev]"

test-local: ## Test local setup with Docker
	@chmod +x ops/test-local.sh
	./ops/test-local.sh

deploy: ## Deploy production infrastructure 
	@chmod +x ops/deploy.sh
	./ops/deploy.sh

monitor: ## Monitor production deployment
	@chmod +x ops/monitor.sh
	./ops/monitor.sh

destroy: ## Destroy production infrastructure
	@chmod +x ops/destroy.sh
	./ops/destroy.sh

clean: ## Clean up cache and temporary files
	rm -rf src/soulseek_research/__pycache__/
	rm -rf build/ dist/ *.egg-info/
	rm -f .env.test
	docker system prune -f

docker-build: ## Build Docker image
	docker build -t soulseek-research:latest .

docker-test: ## Quick test of Docker image
	docker run --rm soulseek-research:latest soulseek-research --help