.PHONY: setup start stop restart status test logs clean help airflow-cli airflow-logs airflow-test-dag airflow-connections airflow-reset

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup: ## Initial project setup: copy env file and create required directories
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo ".env file created from .env.example"; \
	else \
		echo ".env file already exists, skipping"; \
	fi
	@mkdir -p pipelines/dags pipelines/plugins pipelines/logs

start: ## Start all services
	docker compose up -d

stop: ## Stop all services
	docker compose down

restart: ## Restart all services
	docker compose down
	docker compose up -d

status: ## Show status of all services
	docker compose ps

test: ## Run all tests
	@echo "Running infrastructure tests..."
	docker compose config --quiet && echo "docker-compose.yml is valid"
	@echo "Running pipeline tests..."
	@if [ -d pipelines/tests ]; then \
		python -m pytest pipelines/tests -v; \
	else \
		echo "No pipeline tests found (pipelines/tests/)"; \
	fi
	@echo "Running transformation tests..."
	@if [ -d transformations ] && [ -f transformations/dbt_project.yml ]; then \
		cd transformations && dbt test; \
	else \
		echo "No transformation tests found"; \
	fi
	@echo "Running data quality tests..."
	@if [ -d data-quality/tests ]; then \
		python -m pytest data-quality/tests -v; \
	else \
		echo "No data quality tests found (data-quality/tests/)"; \
	fi

logs: ## Tail logs from all services
	docker compose logs -f

clean: ## Stop services and remove volumes
	docker compose down -v

airflow-cli: ## Execute Airflow CLI command (usage: make airflow-cli CMD="dags list")
	docker compose exec airflow-webserver airflow $(CMD)

airflow-logs: ## Tail Airflow scheduler and webserver logs
	docker compose logs -f airflow-scheduler airflow-webserver

airflow-test-dag: ## Test DAG file (usage: make airflow-test-dag DAG=example_hello_world)
	docker compose exec airflow-webserver airflow dags test $(DAG)

airflow-connections: ## List all Airflow connections
	docker compose exec airflow-webserver airflow connections list

airflow-reset: ## Reset Airflow database (development only)
	docker compose exec airflow-webserver airflow db reset -y
