# Scalable Healthcare Claims Data Pipeline with Analytics and LLM Augmentation

An end-to-end pipeline prototype simulating CMS/Medicaid data workflows — from raw claims ingestion through curated analytics layers to LLM-driven insight generation.

---

## Overview

This project demonstrates a production-style data pipeline for healthcare claims analysis, modeled after the kinds of workflows used in CMS Medicaid programs (T-MSIS, managed care reporting). It covers the full data lifecycle: ingestion, validation, transformation, SQL-based analytics, and LLM augmentation for automated insight generation.

The pipeline is implemented in Python and PySpark, with SQL analytics designed to be compatible with Databricks SQL, Snowflake, and BigQuery.

---

## Architecture

```
Raw Claims Data (CSV / GCS / S3)
        │
        ▼
┌───────────────────┐
│   ETL Layer       │  Python (pandas) for prototyping
│   (Notebook 01 /  │  PySpark for production scale
│   PySpark script) │  - Schema validation
│                   │  - Null / anomaly checks
│                   │  - Transformation + enrichment
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│  Curated Tables   │  Clean, validated, partitioned output
│                   │  Ready for downstream consumers
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│   SQL Analytics   │  Business metrics, cost analysis,
│   Layer           │  utilization, risk stratification
│   (Notebook 02)   │
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│   LLM Layer       │  GPT-4o-mini for executive summaries,
│   (Notebook 03)   │  denial classification, NL→SQL,
│                   │  anomaly explanation
└───────────────────┘
```

---

## Key Features

- **Scalable ETL design** — Python (pandas) for prototyping, PySpark for distributed scale
- **Data quality validation** — null checks, schema enforcement, row count validation, anomaly detection
- **SQL analytics layer** — cost analysis, utilization metrics, risk stratification, readmission proxy
- **LLM integration** — automated executive summaries, denial reason classification, natural language to SQL, anomaly explanation
- **Production patterns** — partitioned output, explicit schemas, auditability, reproducibility

---

## Notebooks (start here)

| Notebook | Layer | What it covers |
|----------|-------|---------------|
| `notebooks/01_claims_etl.ipynb` | ETL | Ingestion, validation, transformation, curated output |
| `notebooks/02_sql_analytics.ipynb` | Analytics | Cost, utilization, risk, data quality queries |
| `notebooks/03_llm_automation.ipynb` | LLM | Executive summaries, denial classification, NL→SQL |

---

## Scalability Considerations

The Python ETL pipeline (`notebooks/01`) is designed with the same logic structure as the PySpark version (`pyspark/claims_transformation.py`). This means:

- **Prototyping** happens in Python/pandas (fast iteration, no cluster needed)
- **Production scale** uses the PySpark version on a Databricks cluster or EMR
- Partitioned output by `state_code` allows downstream consumers to query only relevant partitions
- Explicit schema definitions (rather than `inferSchema`) ensure consistent types at scale

In a Databricks environment, the ETL and analytics notebooks would run as **scheduled jobs** against Delta tables, with the LLM layer triggered on completion.

---

## Production Considerations

- **Auditability** — validation errors are logged to a separate output file, not silently dropped
- **Reproducibility** — all transformations are deterministic and documented
- **Data quality** — anomalies (e.g. denied claims with paid amount > 0) are flagged and reported rather than corrected silently
- **Version control** — all code is Git-versioned following collaborative workflow practices

---

## Tools

| Category | Tools |
|----------|-------|
| Languages | Python, SQL, PySpark |
| Platforms | Databricks (conceptual), Snowflake-compatible SQL |
| Cloud | AWS S3 / GCS (storage layer) |
| LLM | OpenAI GPT-4o-mini |
| Version Control | Git |

---

## How to Run

```bash
pip install -r requirements.txt
jupyter notebook
```

See `ARCHITECTURE.md` for full system design details.

---

*github.com/kuanchao*
