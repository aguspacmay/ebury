FROM apache/airflow:3.1.2

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    dbt-core==1.10.15 \
    dbt-adapters==1.19.0 \
    dbt-postgres==1.9.1 \
    polars==1.35.2
