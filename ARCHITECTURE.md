# System Architecture

## Overview

This project implements a layered data pipeline for healthcare claims analytics, modeled after production CMS/Medicaid data workflows.

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                            │
│   Raw Claims CSV / S3 Landing Zone / GCS Bucket             │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                     ETL LAYER                               │
│   notebooks/01_claims_etl.ipynb  (Python/pandas)            │
│   pyspark/claims_transformation.py  (PySpark/Databricks)    │
│                                                             │
│   • Schema validation                                       │
│   • Null / domain / type checks                             │
│   • Transformation + enrichment                             │
│   • Anomaly detection                                       │
│   • Error logging (audit trail)                             │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   CURATED DATA LAYER                        │
│   data/output/curated_claims.csv                            │
│   (Production: Delta tables / Partitioned Parquet in S3)    │
│                                                             │
│   • Clean, typed, validated                                 │
│   • Partitioned by state_code                               │
│   • Includes derived fields (payment_rate, flags)           │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                  SQL ANALYTICS LAYER                        │
│   notebooks/02_sql_analytics.ipynb                          │
│   sql/medicaid_analytics.sql                                │
│                                                             │
│   • Cost analysis (KPIs, PMPM, trends)                      │
│   • Utilization analysis (BH, denial rates)                 │
│   • Risk stratification (high-cost members)                 │
│   • Data quality monitoring (anomaly queries)               │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                     LLM LAYER                               │
│   notebooks/03_llm_automation.ipynb                         │
│                                                             │
│   • Executive summary generation                            │
│   • Denial reason classification                            │
│   • Natural language → SQL                                  │
│   • Anomaly explanation                                     │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│               REPORTING / VISUALIZATION                     │
│   Amazon QuickSight / Tableau / PowerBI                     │
│   (connected to curated data layer)                         │
└─────────────────────────────────────────────────────────────┘
```

---

## Layer Details

### ETL Layer

**Purpose:** Transform raw claims data into a clean, validated, analysis-ready dataset.

**Python version** (`notebooks/01_claims_etl.ipynb`):
- Uses pandas for fast local development and prototyping
- Ideal for datasets up to ~1GB
- Output: `data/output/curated_claims.csv`

**PySpark version** (`pyspark/claims_transformation.py`):
- Distributed processing across a Spark cluster
- Handles datasets of any size (terabytes of claims data)
- Partitioned output by `state_code` for query efficiency
- In Databricks: remove the `SparkSession` block, paste into a notebook

Both versions implement identical transformation logic — only the execution engine differs.

---

### Curated Data Layer

The curated layer is the single source of truth for all downstream analytics.

| Field | Description |
|-------|-------------|
| `curated_claims.csv` | Clean, validated claims |
| `pipeline_run_summary.json` | Audit log: row counts, anomaly count, run timestamp |
| `validation_errors.csv` | Rows that failed validation (for remediation) |

In production this would be a **Delta table** in Databricks or **partitioned Parquet** in S3/GCS.

---

### SQL Analytics Layer

Business-facing queries organized by use case:

- **Cost Analysis** — spend KPIs, monthly trends, PMPM by plan
- **Utilization Analysis** — behavioral health, provider denial rates
- **Risk Stratification** — high-cost member identification, readmission proxy
- **Data Quality** — anomaly detection, integrity checks

Compatible with: Databricks SQL, Snowflake, BigQuery, SQLite (local).

---

### LLM Layer

Augments the analytics layer with natural language capabilities:

- **Input:** Structured metrics from the SQL layer
- **Output:** Plain-English summaries, classifications, generated SQL
- **Model:** GPT-4o-mini (cost-efficient, production-suitable)
- **Validation:** All outputs require human review before use in official reporting

---

## Scalability

### From Prototype to Production

| Component | Prototype (this repo) | Production |
|-----------|----------------------|------------|
| ETL | Python/pandas | PySpark on Databricks |
| Storage | Local CSV | Delta tables / S3 Parquet |
| SQL | SQLite in-memory | Databricks SQL / Snowflake |
| Scheduling | Manual notebook run | Databricks Jobs / Airflow |
| LLM | GPT-4o-mini API call | Same, with output validation layer |

### Databricks Deployment Pattern

```
1. Upload raw data to S3/GCS (or read from T-MSIS API)
2. ETL notebook runs as a scheduled Databricks Job
3. Curated output written to Delta table
4. SQL analytics run as Databricks SQL queries
5. Results loaded into QuickSight / Tableau for dashboards
6. LLM layer triggered post-ETL for automated summaries
```

---

## Production Considerations

### Data Quality
- Validation failures are written to an error log, not silently dropped
- Anomalies are flagged with specific codes for audit and remediation
- Pipeline run summary JSON provides a timestamped audit trail

### Reproducibility
- All transformations are deterministic
- Explicit schema definitions prevent type inference variability
- Version-controlled code ensures consistent reruns

### Auditability
- Error log captures which rows failed and why
- Pipeline summary records row counts before and after validation
- Git history tracks all code changes

### Security
- API keys stored in `.env` (excluded from version control via `.gitignore`)
- In production: secrets managed via AWS Secrets Manager or Databricks secrets
