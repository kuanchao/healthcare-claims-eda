"""
Healthcare Claims ETL Pipeline
--------------------------------
Personal project exploring public healthcare claims data patterns.

I got interested in healthcare data after noticing how messy real-world
claims data tends to be — lots of nulls, invalid codes, and edge cases
that break naive pipelines. This is my attempt at building something
robust enough to handle that.

Pipeline stages:
  1. Ingest raw CSV
  2. Validate against known code lists (claim types, statuses)
  3. Transform and derive analytical fields
  4. Generate a summary report
  5. Export clean dataset + QA log

Run:  python python/medicaid_etl_pipeline.py
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime


# ── Config ────────────────────────────────────────────────────

INPUT_FILE  = "data/sample_claims.csv"
OUTPUT_DIR  = "data/output"

VALID_CLAIM_TYPES = {"IP", "OP", "LTC", "RX"}
VALID_STATUSES    = {"PAID", "DENIED", "PENDING", "VOID"}

REQUIRED_COLUMNS = [
    "claim_id", "member_id", "state_code", "service_date",
    "claim_type", "billed_amount", "paid_amount", "claim_status"
]


# ── Step 1: Ingest ────────────────────────────────────────────

def ingest(filepath: str) -> pd.DataFrame:
    print(f"\n[1/5] Ingesting: {filepath}")
    df = pd.read_csv(filepath, dtype=str)
    print(f"      {len(df)} rows, {len(df.columns)} columns")
    return df


# ── Step 2: Validate ─────────────────────────────────────────
# Real claims data is surprisingly dirty — this catches the most
# common issues I kept running into: nulls, invalid type codes,
# and amounts that aren't actually numeric.

def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("\n[2/5] Validating...")
    errors      = []
    invalid_mask = pd.Series(False, index=df.index)

    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Null checks
    for col in REQUIRED_COLUMNS:
        null_rows = df[col].isna()
        if null_rows.any():
            for idx in df[null_rows].index:
                errors.append({
                    "row": idx,
                    "claim_id": df.loc[idx, "claim_id"],
                    "error": f"Null in {col}"
                })
            invalid_mask |= null_rows

    # Claim type
    bad_type = ~df["claim_type"].isin(VALID_CLAIM_TYPES)
    for idx in df[bad_type].index:
        errors.append({
            "row": idx,
            "claim_id": df.loc[idx, "claim_id"],
            "error": f"Unknown claim_type: {df.loc[idx, 'claim_type']}"
        })
    invalid_mask |= bad_type

    # Status
    bad_status = ~df["claim_status"].isin(VALID_STATUSES)
    for idx in df[bad_status].index:
        errors.append({
            "row": idx,
            "claim_id": df.loc[idx, "claim_id"],
            "error": f"Unknown claim_status: {df.loc[idx, 'claim_status']}"
        })
    invalid_mask |= bad_status

    # Numeric amounts
    for col in ["billed_amount", "paid_amount"]:
        non_numeric = pd.to_numeric(df[col], errors="coerce").isna()
        for idx in df[non_numeric].index:
            errors.append({
                "row": idx,
                "claim_id": df.loc[idx, "claim_id"],
                "error": f"Non-numeric {col}"
            })
        invalid_mask |= non_numeric

    valid_df   = df[~invalid_mask].copy()
    invalid_df = df[invalid_mask].copy()

    print(f"      Valid: {len(valid_df)} | Invalid: {len(invalid_df)}")
    for e in errors[:5]:
        print(f"        → {e}")

    return valid_df, pd.DataFrame(errors)


# ── Step 3: Transform ─────────────────────────────────────────
# I added the anomaly flag after noticing denied claims sometimes
# still had paid_amount > 0 — turns out this is a real data quality
# issue worth flagging rather than silently dropping.

def transform(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[3/5] Transforming...")

    df["service_date"]    = pd.to_datetime(df["service_date"])
    df["billed_amount"]   = pd.to_numeric(df["billed_amount"])
    df["paid_amount"]     = pd.to_numeric(df["paid_amount"])

    df["service_year"]    = df["service_date"].dt.year
    df["service_month"]   = df["service_date"].dt.month
    df["service_quarter"] = df["service_date"].dt.quarter

    df["payment_rate"] = np.where(
        df["billed_amount"] > 0,
        (df["paid_amount"] / df["billed_amount"]).round(4),
        0.0
    )

    df["is_inpatient"]        = df["claim_type"]    == "IP"
    df["is_behavioral_health"] = df["provider_type"] == "BEHAVIORAL"
    df["is_denied"]            = df["claim_status"]  == "DENIED"

    # Anomaly: denied but still shows a paid amount — worth surfacing
    df["anomaly_paid_denied"] = df["is_denied"] & (df["paid_amount"] > 0)

    n = df["anomaly_paid_denied"].sum()
    if n:
        print(f"      ⚠  {n} denied claims with paid_amount > 0 — flagged for review")

    print(f"      Output shape: {df.shape}")
    return df


# ── Step 4: Analyze ───────────────────────────────────────────

def analyze(df: pd.DataFrame) -> dict:
    print("\n[4/5] Summarizing...")

    paid = df[df["claim_status"] == "PAID"]

    summary = {
        "run_timestamp":        datetime.utcnow().isoformat(),
        "total_claims":         len(df),
        "paid_claims":          int((df["claim_status"] == "PAID").sum()),
        "denied_claims":        int((df["claim_status"] == "DENIED").sum()),
        "denial_rate_pct":      round((df["claim_status"] == "DENIED").mean() * 100, 2),
        "total_billed":         round(df["billed_amount"].sum(), 2),
        "total_paid":           round(df["paid_amount"].sum(), 2),
        "avg_payment_rate_pct": round(paid["payment_rate"].mean() * 100, 2),
        "spend_by_claim_type":  df.groupby("claim_type")["paid_amount"].sum().round(2).to_dict(),
        "spend_by_state":       df.groupby("state_code")["paid_amount"].sum().round(2).to_dict(),
        "spend_by_provider":    df.groupby("provider_type")["paid_amount"].sum().round(2).to_dict(),
        "top_diagnoses":        (
            df.groupby("diagnosis_code")["claim_id"]
            .count().sort_values(ascending=False).head(5).to_dict()
        ),
        "inpatient_avg_paid":       round(df[df["is_inpatient"]]["paid_amount"].mean(), 2),
        "behavioral_health_spend":  round(df[df["is_behavioral_health"]]["paid_amount"].sum(), 2),
    }

    print(f"      Claims: {summary['total_claims']} | Denial rate: {summary['denial_rate_pct']}%")
    print(f"      Total paid: ${summary['total_paid']:,.2f} | Avg pay rate: {summary['avg_payment_rate_pct']}%")
    return summary


# ── Step 5: Export ────────────────────────────────────────────

def export(df: pd.DataFrame, summary: dict, errors: pd.DataFrame):
    print(f"\n[5/5] Exporting to {OUTPUT_DIR}/")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df.to_csv(f"{OUTPUT_DIR}/claims_clean.csv", index=False)
    print(f"      ✓ claims_clean.csv")

    with open(f"{OUTPUT_DIR}/analytics_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print(f"      ✓ analytics_summary.json")

    if not errors.empty:
        errors.to_csv(f"{OUTPUT_DIR}/validation_errors.csv", index=False)
        print(f"      ✓ validation_errors.csv ({len(errors)} rows)")


# ── Main ──────────────────────────────────────────────────────

def run_pipeline():
    print("=" * 50)
    print("  Healthcare Claims ETL Pipeline")
    print("=" * 50)
    raw_df                     = ingest(INPUT_FILE)
    clean_df, error_df         = validate(raw_df)
    transformed_df             = transform(clean_df)
    summary                    = analyze(transformed_df)
    export(transformed_df, summary, error_df)
    print("\n✅ Done.\n")
    return transformed_df, summary


if __name__ == "__main__":
    run_pipeline()
