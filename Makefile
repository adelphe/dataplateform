.PHONY: setup start stop restart status test logs clean help airflow-cli airflow-logs airflow-test-dag airflow-connections airflow-reset minio-buckets minio-upload-sample minio-verify minio-console superset-logs superset-shell superset-init superset-reset superset-console

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup: ## Initial project setup: copy env file and create required directories
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo ".env file created from .env.example"; \
	else \
		echo ".env file already exists, skipping"; \
	fi
	@mkdir -p pipelines/dags pipelines/plugins pipelines/logs scripts

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

minio-buckets: ## List all MinIO buckets and their contents
	docker compose exec minio mc ls local/

minio-upload-sample: ## Upload sample test data to bronze layer
	docker compose exec airflow-webserver python /opt/airflow/scripts/upload_sample_data.py

minio-verify: ## Verify MinIO bucket structure and connectivity
	docker compose exec airflow-webserver python /opt/airflow/scripts/verify_buckets.py

minio-console: ## Show MinIO console URL
	@echo "MinIO Console: http://localhost:9001 (minio / minio123)"

# ==============================
# Superset Commands
# ==============================

superset-logs: ## Tail Superset container logs
	docker compose logs -f superset

superset-shell: ## Access Superset container shell for debugging
	docker compose exec superset /bin/bash

superset-init: ## Re-run Superset initialization scripts
	docker compose exec superset python /app/superset_init/setup_database.py
	docker compose exec superset python /app/superset_init/setup_roles.py
	docker compose exec superset python /app/superset_init/import_dashboards.py

superset-reset: ## Reset Superset database and re-initialize
	docker compose stop superset
	docker compose rm -f superset
	docker compose up -d superset
	@echo "Superset is restarting with fresh initialization..."

superset-console: ## Show Superset console URL and credentials
	@echo "Superset Dashboard: http://localhost:8088"
	@echo "Admin: admin / admin"
	@echo "Analyst: analyst / analyst123"
	@echo "Viewer: viewer / viewer123"
