FROM apache/airflow:2.9.3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    curl \
    jq \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY infrastructure/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy utility scripts and make them executable
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/
RUN chmod +x /opt/airflow/scripts/*.py 2>/dev/null || true
