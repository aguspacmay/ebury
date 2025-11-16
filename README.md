# Ebury Data Pipeline

## Project Description

This project implements a modern **data pipeline** for processing customer transaction data using a **Medallion Architecture** (Bronze, Silver, Gold layers). The pipeline is orchestrated with **Apache Airflow** and uses **dbt (Data Build Tool)** for data transformations, all running in a containerized environment with **Docker**.

### Requirements 

Build a scalable data platform using Airflow, dbt, and PostgreSQL, orchestrated via Docker Compose. Ingest CSV data and apply dimensional modeling with robust data quality checks. Implement an Airflow DAG for automated ingestion, transformation, and monitoring. Deliver a well-documented GitHub repository with seamless deployment.

### Key Features

- **Bronze Layer**: Incremental ingestion of raw customer transaction data from CSV files into PostgreSQL
- **Silver Layer**: Data validation, cleansing, and quality checks with error handling and routing
- **Gold Layer**: Business-ready dimensional models including:
  - Star schema with fact and dimension tables
  - Customer insights and analytics aggregations
  - Monthly transaction summaries
- **Data-Driven Orchestration**: Uses Airflow Datasets to create event-driven pipelines that automatically trigger downstream processing
- **Quality Assurance**: Implements data validation macros and error logging for problematic records
- **Scalable Architecture**: Built with Docker Compose using Celery Executor for distributed task processing

## Architecture

### System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Docker Environment                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐      ┌─────────────────────────────────────────────┐  │
│  │   CSV Data   │      │         Apache Airflow (Celery)             │  │
│  │  /data/*.csv │      │  ┌─────────────────────────────────────┐    │  │
│  └──────┬───────┘      │  │   API Server / Scheduler            │    │  │
│         │              │  │   DAG Processor / Triggerer         │    │  │
│         │              │  │   Celery Worker                     │    │  │
│         │              │  └─────────────────────────────────────┘    │  │
│         │              │            │                                │  │
│         │              │            │ Orchestration                  │  │
│         │              │            ▼                                │  │
│         │              │  ┌─────────────────────────────────────┐    │  │
│         │              │  │      Dataset-Driven DAGs            │    │  │
│         │              │  │                                     │    │  │
│         │              │  │  ┌─────────────────────────────┐    │    │  │
│         └──────────────┼──┼─▶│  Bronze Layer (Ingestion)   │    │    │  │
│                        │  │  │  - bronze_customer_trans... │    │    │  │
│                        │  │  └──────────┬──────────────────┘    │    │  │
│                        │  │             │ Trigger (Dataset)     │    │  │
│                        │  │             ▼                       │    │  │
│                        │  │  ┌─────────────────────────────┐    │    │  │
│                        │  │  │  Silver Layer (Transform)   │    │    │  │
│                        │  │  │  - silver_customer_trans... │    │    │  │
│                        │  │  │  - Validation & Cleansing   │    │    │  │
│                        │  │  └──────────┬──────────────────┘    │    │  │
│                        │  │             │ Trigger (Dataset)     │    │  │
│                        │  │             ▼                       │    │  │
│                        │  │  ┌─────────────────────────────┐    │    │  │
│                        │  │  │  Gold Layer (Analytics)     │    │    │  │
│                        │  │  │  - Star Schema              │    │    │  │
│                        │  │  │  - Customer Insights        │    │    │  │
│                        │  │  └─────────────────────────────┘    │    │  │
│                        │  └─────────────────────────────────────┘    │  │
│                        │                                             │  │
│                        │  ┌─────────────────────────────────────┐    │  │
│                        │  │            dbt Core                 │    │  │
│                        │  │  - Data Transformations             │    │  │
│                        │  │  - SQL Models                       │    │  │
│                        │  │  - Macros & Tests                   │    │  │
│                        │  └──────────┬──────────────────────────┘    │  │
│                        └─────────────┼───────────────────────────────┘  │
│                                      │                                  │
│  ┌───────────────────────────────────▼──────────────────────────────┐   │
│  │              PostgreSQL Database (analytics)                     │   │
│  │  ┌──────────────┐  ┌───────────────┐  ┌──────────────────────┐   │   │
│  │  │ebury_bronze  │  │ ebury_silver  │  │    ebury_gold        │   │   │
│  │  │- customer_   │  │- validated_   │  │- facts_transactions  │   │   │
│  │  │  transactions│  │  transactions │  │- dim_product         │   │   │
│  │  │              │  │- error_       │  │- customer_insights   │   │   │
│  │  │              │  │  transactions │  │- customer_totals     │   │   │
│  │  └──────────────┘  └───────────────┘  └──────────────────────┘   │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌────────────────┐                                                     │
│  │  Redis Cache   │ ← Celery Message Broker                             │
│  └────────────────┘                                                     │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Ingestion (Bronze)**: CSV files are loaded incrementally into the `ebury_bronze.customer_transactions` table
2. **Validation (Silver)**: dbt validates and cleanses data, routing valid records to `ebury_silver.validated_transactions` and errors to `ebury_silver.error_transactions`
3. **Analytics (Gold)**: Business models are built from validated data:
   - **Star Schema**: `facts_transactions` and `dim_product` for dimensional analysis
   - **Aggregations**: Customer insights and monthly totals for reporting

### Dataset-Driven Orchestration

The pipeline uses Airflow Datasets for event-driven triggers:
- Bronze DAG produces -> `postgres://postgres/analytics/ebury_bronze/customer_transactions`
- Silver DAG consumes Bronze dataset and produces -> `postgres://postgres/analytics/ebury_silver/customer_transactions`
- Gold DAGs consume Silver dataset and produce final analytics tables

## Project Structure

```
ebury/
│
├── docker-compose.yaml          # Docker orchestration configuration
├── Dockerfile                   # Custom Airflow image with dependencies
├── init-db.sql                  # PostgreSQL initialization script
├── .env                         # Environment variables
├── README.md                    # Project documentation
│
├── airflow/                     # Airflow workspace
│   ├── dags/                    # Airflow DAG definitions
│   │   ├── bronze_customer_transactions_dag.py
│   │   ├── silver_customer_transactions_dag.py
│   │   ├── gold_customer_transactions_star_schema_dag.py
│   │   └── gold_customer_insights_dag.py
│   │
│   ├── data/                    # Source data files
│   │   └── customer_transactions.csv
│   │
│   ├── config/                  # Airflow configuration
│   ├── logs/                    # Airflow execution logs
│   ├── plugins/                 # Airflow custom plugins
│   └── dbt/                     # dbt symlink (for container access)
│
└── dbt/                         # dbt project root
    ├── dbt_project.yml          # dbt project configuration
    ├── profiles.yml             # Database connection profiles
    │
    ├── models/                  # SQL transformation models
    │   ├── sources.yml          # Source table definitions
    │   ├── bronze/              # Raw data models
    │   ├── silver/              # Validated & cleansed data
    │   │   ├── validated_transactions.sql
    │   │   └── error_transactions.sql
    │   └── gold/                # Analytics & business models
    │       ├── facts_transactions.sql
    │       ├── dim_product.sql
    │       ├── customer_insights_by_transactions.sql
    │       └── customer_totals_by_month.sql
    │
    ├── macros/                  # Reusable SQL macros
    │   ├── apply_routing.sql
    │   ├── apply_transformations.sql
    │   ├── clean_date.sql
    │   ├── clean_decimal.sql
    │   ├── clean_integer.sql
    │   ├── row_validation.sql
    │   └── utils/
    │
    ├── seeds/                   # Static reference data
    ├── target/                  # dbt compiled artifacts (gitignored)
    └── dbt_packages/            # dbt dependencies
```

### Key Directories

- **`/airflow/dags`**: Airflow DAG definitions that orchestrate the data pipeline
- **`/airflow/data`**: Source CSV files for ingestion
- **`/dbt/models`**: SQL models organized by layer (bronze/silver/gold)
- **`/dbt/macros`**: Reusable SQL functions for validation and transformation

## Airflow DAGs

The pipeline consists of **4 DAGs** implementing the medallion architecture with event-driven orchestration:

### 1. `bronze_customer_transactions` 
- **Layer**: Bronze | **Schedule**: `@daily`
- Incremental CSV ingestion into `ebury_bronze.customer_transactions`
- Produces dataset to trigger Silver layer

### 2. `silver_customer_transactions`
- **Layer**: Silver | **Schedule**: Triggered by Bronze dataset
- Validates and cleanses data using dbt
- Outputs: `validated_transactions` (silver) and `error_transactions` (error schema)
- Produces dataset to trigger Gold layers

### 3. `gold_customer_transactions_star_schema`
- **Layer**: Gold | **Schedule**: Triggered by Silver dataset
- Builds star schema: `facts_transactions` and `dim_product`

### 4. `gold_customer_insights`
- **Layer**: Gold | **Schedule**: Triggered by Silver dataset
- Generates analytics: customer insights and monthly totals

**DAG Flow**: Bronze (daily) -> Silver (triggered) -> Gold layers (triggered: star_schema -> insights)

## dbt Models

### Silver Layer
- **`validated_transactions`**: Validates and transforms raw data using custom macros (`apply_routing`, `clean_*` functions)
- **`error_transactions`**: Captures invalid records with error messages and timestamps

### Gold Layer
- **`dim_product`**: Product dimension table (incremental)
- **`facts_transactions`**: Transaction fact table (incremental)
- **`customer_insights_by_transactions`**: Customer spending patterns and product preferences
- **`customer_totals_by_month`**: Monthly aggregated metrics per customer

All models use **incremental materialization** to optimize performance.

## Useful Commands

### Docker
```bash
# stop docker compose 
docker compose down

# build images
docker compose build

# init airflow
docker compose up airflow-init

# Init docker compose servicies
docker compose up -d
```

### dbt
```bash
# exe specific model
docker compose exec airflow-worker bash -c "cd /opt/airflow/dbt && dbt run -s validated_transactions customer_transactions"

```
