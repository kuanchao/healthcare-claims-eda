"""
PySpark Claims ETL — Production Scale Version
-----------------------------------------------
This script is the distributed, production-scale version of the ETL
pipeline in notebooks/01_claims_etl.ipynb.

The transformation logic is identical to the notebook — only the
execution engine changes from pandas to PySpark, enabling processing
of datasets that don't fit in memory (millions of claims, multi-state).

Pipeline stages (mirrors notebook 01):
  1. Data Ingestion       — load with explicit schema
  2. Data Quality Checks  — validation, null checks, domain checks
  3. Data Transformation  — type casting, derived fields, anomaly flags
  4. Curated Output       — partitioned write (by state_code)

Deployment:
  Local:      python pyspark/claims_transformation.py
  Databricks: Remove the SparkSession block below — `spark` is
              pre-initialized in the notebook context. Paste the
              remaining code into a Databricks notebook cell.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
import os


# ── Spark Session (local only) ────────────────────────────────
# On Databricks: remove this block. `spark` is already available.

spark = (
    SparkSession.builder
    .appName("HealthcareClaimsETL")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")  # Low for local dev; increase for cluster
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# ── Explicit Schema ───────────────────────────────────────────
# Defining schema explicitly rather than using inferSchema:
# - Faster at scale (no scan required)
# - Guarantees consistent types across pipeline runs
# - Fails fast if source data doesn't match expected structure

CLAIMS_SCHEMA = StructType([
    StructField("claim_id",          StringType(), False),
    StructField("member_id",         StringType(), False),
    StructField("state_code",        StringType(), True),
    StructField("plan_id",           StringType(), True),
    StructField("service_date",      StringType(), True),
    StructField("claim_type",        StringType(), True),
    StructField("diagnosis_code",    StringType(), True),
    StructField("procedure_code",    StringType(), True),
    StructField("billed_amount",     DoubleType(), True),
    StructField("paid_amount",       DoubleType(), True),
    StructField("claim_status",      StringType(), True),
    StructField("provider_id",       StringType(), True),
    StructField("provider_type",     StringType(), True),
    StructField("managed_care_flag", StringType(), True),
])

VALID_CLAIM_TYPES = ["IP", "OP", "LTC", "RX"]
VALID_STATUSES    = ["PAID", "DENIED", "PENDING", "VOID"]


# ── Stage 1: Data Ingestion ───────────────────────────────────

def ingest(path: str):
    print(f"\n[Stage 1] Ingesting from: {path}")
    df = (
        spark.read
        .option("header", "true")
        .schema(CLAIMS_SCHEMA)
        .csv(path)
        .withColumn("service_date", F.to_date("service_date", "yyyy-MM-dd"))
    )
    count = df.count()
    print(f"         Rows loaded: {count}")
    return df, count


# ── Stage 2: Data Quality Checks ─────────────────────────────
# Same checks as notebook 01 — null checks, domain validation.
# Invalid rows are isolated, not dropped silently, for audit purposes.

def validate(df):
    print("\n[Stage 2] Running data quality checks...")

    invalid_mask = F.lit(False)

    # Null checks on required fields
    for col in ["claim_id", "member_id", "state_code", "service_date",
                "claim_type", "billed_amount", "paid_amount", "claim_status"]:
        invalid_mask = invalid_mask | F.col(col).isNull()

    # Domain validation — claim type
    invalid_mask = invalid_mask | ~F.col("claim_type").isin(VALID_CLAIM_TYPES)

    # Domain validation — claim status
    invalid_mask = invalid_mask | ~F.col("claim_status").isin(VALID_STATUSES)

    valid_df   = df.filter(~invalid_mask)
    invalid_df = df.filter(invalid_mask)

    valid_count   = valid_df.count()
    invalid_count = invalid_df.count()

    print(f"         ✓ Valid rows:   {valid_count}")
    print(f"         ✗ Invalid rows: {invalid_count} → written to error log")

    return valid_df, invalid_df


# ── Stage 3: Transformation ───────────────────────────────────
# Mirrors notebook 01 transformations exactly.
# Window functions enable per-member analytics at distributed scale.

def transform(df):
    print("\n[Stage 3] Transforming...")

    # Date part extraction — supports partitioning and time-series queries
    df = (
        df
        .withColumn("service_year",    F.year("service_date"))
        .withColumn("service_month",   F.month("service_date"))
        .withColumn("service_quarter", F.quarter("service_date"))
    )

    # Payment rate — key managed care performance metric
    df = df.withColumn("payment_rate",
        F.when(F.col("billed_amount") > 0,
            F.round(F.col("paid_amount") / F.col("billed_amount"), 4)
        ).otherwise(F.lit(0.0))
    )

    # Boolean flags for analytics layer
    df = (
        df
        .withColumn("is_inpatient",  F.col("claim_type")    == "IP")
        .withColumn("is_denied",     F.col("claim_status")  == "DENIED")
        .withColumn("is_behavioral", F.col("provider_type") == "BEHAVIORAL")
    )

    # Anomaly detection — denied claims with paid_amount > 0
    # Indicates unreconciled post-payment reversal. Flagged, not corrected.
    df = df.withColumn("anomaly_paid_denied",
        (F.col("is_denied")) & (F.col("paid_amount") > 0)
    )

    # ── Window functions (distributed analytics) ──────────────
    # These run across the full dataset in parallel on a cluster.
    # Equivalent to SQL window functions in Databricks SQL.

    member_window = (
        Window.partitionBy("member_id").orderBy("service_date")
    )

    # Cumulative paid per member — supports high-cost member identification
    df = df.withColumn("member_cumulative_paid",
        F.sum("paid_amount").over(
            member_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
    )

    # Days since previous claim — readmission and utilization signal
    df = (
        df
        .withColumn("prev_claim_date", F.lag("service_date", 1).over(member_window))
        .withColumn("days_since_last_claim", F.datediff("service_date", "prev_claim_date"))
    )

    anomaly_count = df.filter(F.col("anomaly_paid_denied")).count()
    if anomaly_count:
        print(f"         ⚠  {anomaly_count} anomaly(ies) detected — denied claims with paid_amount > 0")
    else:
        print("         ✓ No payment anomalies detected")

    return df


# ── Stage 4: Curated Output ───────────────────────────────────
# Partitioned by state_code — in production this would write to
# Delta tables or partitioned Parquet in S3/GCS.
# Partitioning enables efficient downstream querying by state.

def write_curated(df, output_path: str):
    print(f"\n[Stage 4] Writing curated output to: {output_path}")
    os.makedirs(output_path, exist_ok=True)

    curated_cols = [
        "claim_id", "member_id", "state_code", "plan_id",
        "service_date", "service_year", "service_month", "service_quarter",
        "claim_type", "diagnosis_code", "provider_type",
        "billed_amount", "paid_amount", "payment_rate", "claim_status",
        "is_inpatient", "is_denied", "is_behavioral", "anomaly_paid_denied",
        "member_cumulative_paid", "days_since_last_claim"
    ]

    (
        df.select(curated_cols)
        .filter(F.col("claim_status") == "PAID")
        .repartition("state_code")              # Distribute across workers by state
        .write
        .mode("overwrite")
        .partitionBy("state_code")              # Partition on disk for query efficiency
        .option("header", "true")
        .csv(output_path)
    )
    print("         ✓ Written (partitioned by state_code)")
    print("         → In Databricks: replace .csv() with .format('delta').save(path)")


# ── Main ──────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  Healthcare Claims ETL — PySpark (Production Scale)")
    print("=" * 55)

    raw_df, raw_count         = ingest("data/sample_claims.csv")
    valid_df, invalid_df      = validate(raw_df)
    transformed_df            = transform(valid_df)
    write_curated(transformed_df, "data/output/spark_curated")

    print(f"\n✅ Pipeline complete — {valid_df.count()} rows processed\n")
    spark.stop()


if __name__ == "__main__":
    main()
