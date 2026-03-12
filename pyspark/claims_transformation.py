"""
PySpark Claims Transformation
-------------------------------
I wanted to learn how to do proper large-scale data transformations
in PySpark — specifically the things that are painful in pandas at
scale: window functions, partitioned writes, and explicit schemas.

This processes the same claims dataset as the Python pipeline but
using Spark patterns I'd use on a Databricks cluster or EMR.

Key things I was figuring out:
- Explicit schema definition (avoids inferSchema cost at scale)
- Window functions: running totals, rankings, lag for time-series
- Partitioned output by state (mirrors how you'd land data in S3/GCS)
- Keeping it readable so a teammate could pick it up

Run locally:  pip install pyspark
              python pyspark/claims_transformation.py

On Databricks: remove the SparkSession block at the top —
               `spark` is already available in the notebook context.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)
import os


# ── Spark Session ─────────────────────────────────────────────
# Local mode only — remove this block on Databricks

spark = (
    SparkSession.builder
    .appName("ClaimsTransformation")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# ── Schema ────────────────────────────────────────────────────
# Defining schema explicitly is faster and safer than inferSchema —
# learned this the hard way when amount columns got read as strings.

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


# ── 1. Load ───────────────────────────────────────────────────

def load(path: str):
    print(f"\n[1] Loading from {path}...")
    df = (
        spark.read
        .option("header", "true")
        .schema(CLAIMS_SCHEMA)
        .csv(path)
        .withColumn("service_date", F.to_date("service_date", "yyyy-MM-dd"))
    )
    print(f"    Rows: {df.count()}")
    df.printSchema()
    return df


# ── 2. Enrich ─────────────────────────────────────────────────
# Adding derived columns I always want downstream —
# date parts, payment rate, and a few boolean flags.

def enrich(df):
    print("\n[2] Enriching...")
    return (
        df
        .withColumn("service_year",    F.year("service_date"))
        .withColumn("service_month",   F.month("service_date"))
        .withColumn("service_quarter", F.quarter("service_date"))
        .withColumn("payment_rate",
            F.when(F.col("billed_amount") > 0,
                F.round(F.col("paid_amount") / F.col("billed_amount"), 4)
            ).otherwise(F.lit(0.0))
        )
        .withColumn("is_inpatient",   F.col("claim_type")    == "IP")
        .withColumn("is_denied",      F.col("claim_status")  == "DENIED")
        .withColumn("is_behavioral",  F.col("provider_type") == "BEHAVIORAL")
        .withColumn("anomaly_paid_denied",
            (F.col("is_denied")) & (F.col("paid_amount") > 0)
        )
    )


# ── 3. Window Functions ───────────────────────────────────────
# This was the part I most wanted to get comfortable with.
# Window functions in Spark feel similar to SQL but the API
# takes some getting used to.

def apply_windows(df):
    print("\n[3] Applying window functions...")

    # Per-member timeline (sorted by date)
    by_member = (
        Window
        .partitionBy("member_id")
        .orderBy("service_date")
    )

    # Per state+month (for % of total)
    by_state_month = Window.partitionBy("state_code", "service_year", "service_month")

    return (
        df
        # Cumulative paid per member over time
        .withColumn("member_cumulative_paid",
            F.sum("paid_amount").over(
                by_member.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            )
        )
        # Rank by paid amount within state+month
        .withColumn("rank_in_state_month",
            F.rank().over(
                Window
                .partitionBy("state_code", "service_year", "service_month")
                .orderBy(F.desc("paid_amount"))
            )
        )
        # Days since previous claim for this member
        .withColumn("prev_claim_date",
            F.lag("service_date", 1).over(by_member)
        )
        .withColumn("days_since_last_claim",
            F.datediff("service_date", "prev_claim_date")
        )
        # This member's claim as % of their state's monthly spend
        .withColumn("state_month_total",
            F.sum("paid_amount").over(by_state_month)
        )
        .withColumn("pct_of_state_month",
            F.round(F.col("paid_amount") / F.col("state_month_total") * 100, 2)
        )
    )


# ── 4. Summaries ──────────────────────────────────────────────

def summarize(df):
    print("\n[4] Summarizing...")

    print("\n    Spend by state + claim type:")
    (
        df.filter(F.col("claim_status") == "PAID")
        .groupBy("state_code", "claim_type")
        .agg(
            F.count("claim_id").alias("claims"),
            F.countDistinct("member_id").alias("members"),
            F.round(F.sum("paid_amount"), 2).alias("total_paid"),
            F.round(F.avg("paid_amount"), 2).alias("avg_paid"),
        )
        .orderBy(F.desc("total_paid"))
        .show(truncate=False)
    )

    print("\n    Denial rate by provider type:")
    (
        df.groupBy("provider_type")
        .agg(
            F.count("claim_id").alias("total"),
            F.sum(F.when(F.col("claim_status") == "DENIED", 1).otherwise(0)).alias("denied"),
        )
        .withColumn("denial_rate_pct",
            F.round(F.col("denied") / F.col("total") * 100, 2)
        )
        .orderBy(F.desc("denial_rate_pct"))
        .show(truncate=False)
    )


# ── 5. Write ──────────────────────────────────────────────────
# Partitioning by state_code mirrors how you'd land data in S3
# for downstream consumers to query only the partitions they need.

def write(df, output_path: str):
    print(f"\n[5] Writing partitioned output to {output_path}...")
    os.makedirs(output_path, exist_ok=True)

    (
        df.filter(F.col("claim_status") == "PAID")
        .select(
            "claim_id", "member_id", "state_code", "plan_id",
            "service_date", "service_year", "service_month",
            "claim_type", "diagnosis_code", "provider_type",
            "billed_amount", "paid_amount", "payment_rate",
            "is_inpatient", "is_behavioral",
            "member_cumulative_paid", "days_since_last_claim",
            "pct_of_state_month"
        )
        .repartition("state_code")
        .write.mode("overwrite")
        .partitionBy("state_code")
        .option("header", "true")
        .csv(output_path)
    )
    print("    ✓ Done (partitioned by state_code)")


# ── Main ──────────────────────────────────────────────────────

def main():
    print("=" * 50)
    print("  PySpark Claims Transformation")
    print("=" * 50)

    df = load("data/sample_claims.csv")
    df = enrich(df)
    df = apply_windows(df)
    summarize(df)
    write(df, "data/output/spark_output")

    print("\n✅ Done.\n")
    spark.stop()


if __name__ == "__main__":
    main()
