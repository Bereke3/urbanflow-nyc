
FROM apache/airflow:2.10.5

USER root

RUN python -m venv /opt/nyc-venv \
    && chown -R airflow:0 /opt/nyc-venv

USER airflow

RUN /opt/nyc-venv/bin/pip install --no-cache-dir \
    "dbt-core==1.12.6" \
    "dbt-duckdb==1.11.1"
