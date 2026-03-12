"""
LLM Automation for Healthcare Claims Analysis
-----------------------------------------------
I've been applying LLMs to data workflows in production, and wanted
to explore whether the same approach works for claims analytics —
specifically for tasks that are currently done manually:

  1. Writing plain-English summaries of analytics for non-technical stakeholders
  2. Classifying free-text denial reason codes automatically
  3. Generating SQL from natural language questions
  4. Flagging and explaining data anomalies

The interesting part is that most of these tasks don't need a perfect
LLM output — they need a "good enough" draft that a human can quickly
review and confirm. That's where LLMs genuinely save time.

Run:  pip install openai pandas
      $env:OPENAI_API_KEY="your-key"   # Windows PowerShell
      python python/llm_claims_insights.py

      DEMO_MODE = True by default — runs without any API key.
      Set to False and add your key to use live GPT-4.
"""

import os
import json
import pandas as pd

# ── Toggle this to use a real OpenAI key ─────────────────────
DEMO_MODE = True

try:
    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY", ""))
except ImportError:
    client     = None
    DEMO_MODE  = True


# ── LLM Helper ────────────────────────────────────────────────

def call_llm(system: str, user: str, mock: str = "") -> str:
    if DEMO_MODE or not client:
        print("    [demo mode — mock response]")
        return mock
    resp = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
        temperature=0.2,
        max_tokens=600,
    )
    return resp.choices[0].message.content.strip()


# ── 1. Executive Summary Generation ──────────────────────────
# The goal here is to replace a manual "write up the monthly numbers"
# task. Analysts spend hours on these — a good LLM draft cuts that
# to a 5-minute review.

def generate_executive_summary(metrics: dict) -> str:
    print("\n[1] Generating executive summary...")

    system = (
        "You are a healthcare data analyst. Translate the following metrics "
        "into a concise plain-English executive briefing for non-technical "
        "program managers. Be specific with numbers. Flag anything that "
        "warrants attention. Keep it under 200 words."
    )
    user = f"Summarize these claims analytics:\n{json.dumps(metrics, indent=2)}"

    mock = """
Executive Summary

For the period analyzed, 20 claims were processed across TX, FL, and CA,
with $159,100 paid against $201,300 billed (79% average payment rate).

Notable findings:
- Denial rate is 10% (2 claims). One anomaly requires review: a denied claim
  showing a non-zero paid amount, suggesting a possible post-payment reversal
  that wasn't properly reconciled.
- Inpatient claims average $11,200 each — the primary cost driver.
  Hospital and nursing facility providers account for the majority of spend.
- Behavioral health spend is $450 across 3 claims, relatively low but worth
  tracking as utilization tends to grow quarter-over-quarter.
- Texas (MCO_TX_01) has the highest claim volume; payment rates are
  consistent across states (78–80%).

Recommended next steps:
1. Investigate the denied claim with paid_amount > 0.
2. Flag the two high-cost inpatient members for care management review.
3. Set up month-over-month BH trending for next reporting cycle.
"""
    result = call_llm(system, user, mock)
    print(result)
    return result


# ── 2. Denial Reason Classification ──────────────────────────
# Denial reason codes in raw claims data are often free text or
# inconsistent short codes. This auto-classifies them into a
# standard taxonomy — useful for trend analysis and provider feedback.

def classify_denial_reasons(notes: list[str]) -> list[dict]:
    print("\n[2] Classifying denial reasons...")

    system = (
        "You are a claims analyst. Classify each denial reason note into "
        "one of: AUTHORIZATION_MISSING, DUPLICATE_CLAIM, ELIGIBILITY_ISSUE, "
        "CODING_ERROR, TIMELY_FILING, MEDICAL_NECESSITY, OTHER. "
        "Return a JSON array: [{note, category, confidence}]."
    )
    user = f"Classify:\n{json.dumps(notes, indent=2)}"

    mock = json.dumps([
        {"note": "Service not authorized prior to date of service",
         "category": "AUTHORIZATION_MISSING", "confidence": 0.97},
        {"note": "Claim submitted beyond 90-day filing limit",
         "category": "TIMELY_FILING",         "confidence": 0.99},
        {"note": "Member not enrolled on date of service",
         "category": "ELIGIBILITY_ISSUE",     "confidence": 0.95},
        {"note": "Procedure code does not match diagnosis",
         "category": "CODING_ERROR",          "confidence": 0.91},
    ], indent=2)

    result = call_llm(system, user, mock)
    parsed = json.loads(result) if not DEMO_MODE else json.loads(mock)
    print(f"\n    Results:")
    for r in parsed:
        print(f"      [{r['category']}] ({r['confidence']:.0%}) — {r['note']}")
    return parsed


# ── 3. Natural Language to SQL ────────────────────────────────
# This is the one I'm most excited about for real workflows.
# Analysts who know what they want but aren't fluent in SQL
# can describe the question in plain English and get a working
# query to review and run.

def nl_to_sql(question: str) -> str:
    print(f"\n[3] SQL from natural language...")
    print(f"    Q: \"{question}\"")

    schema_hint = (
        "Table: claims(claim_id, member_id, state_code, plan_id, "
        "service_date, claim_type, diagnosis_code, procedure_code, "
        "billed_amount, paid_amount, claim_status, provider_id, "
        "provider_type, managed_care_flag). "
        "Return only the SQL, no explanation."
    )

    mock_map = {
        "Which members had more than one inpatient stay?": """
SELECT   member_id,
         COUNT(*) AS inpatient_stays
FROM     claims
WHERE    claim_type   = 'IP'
  AND    claim_status = 'PAID'
GROUP BY member_id
HAVING   COUNT(*) > 1
ORDER BY inpatient_stays DESC;""",

        "What is total behavioral health spend by state?": """
SELECT   state_code,
         ROUND(SUM(paid_amount), 2) AS bh_spend
FROM     claims
WHERE    provider_type = 'BEHAVIORAL'
  AND    claim_status  = 'PAID'
GROUP BY state_code
ORDER BY bh_spend DESC;""",
    }

    mock = mock_map.get(question, "-- (run with DEMO_MODE=False for live generation)")
    result = call_llm(schema_hint, question, mock)
    print(f"\n    Generated SQL:\n{result}")
    return result


# ── 4. Anomaly Explanation ────────────────────────────────────
# Rather than just flagging anomalies, this explains them in plain
# language and suggests a remediation — useful for audit reports.

def explain_anomalies(anomalies: list[dict]) -> str:
    print("\n[4] Explaining anomalies...")

    if not anomalies:
        print("    No anomalies to explain.")
        return ""

    system = (
        "You are a claims data quality analyst. For each anomaly, briefly "
        "explain the likely root cause and suggest a remediation action. "
        "Be concise — one paragraph per anomaly."
    )
    user = f"Explain these claims anomalies:\n{json.dumps(anomalies, indent=2)}"

    mock = """
Anomaly — Denied claim with paid_amount > 0:

This typically occurs when a payment is processed and then the claim is
retroactively denied during a post-payment audit (e.g. eligibility review
or coordination of benefits check). The paid amount was not subsequently
voided in the system.

Recommended action: Cross-reference with remittance data to confirm whether
a 835 reversal transaction was generated. If not, initiate a manual
adjustment or void transaction to reconcile the record.
"""
    result = call_llm(system, user, mock)
    print(result)
    return result


# ── Main ──────────────────────────────────────────────────────

def main():
    print("=" * 50)
    print("  LLM Claims Automation")
    print("=" * 50)
    if DEMO_MODE:
        print("  ⚡ Demo mode — set DEMO_MODE=False + OPENAI_API_KEY for live GPT-4\n")

    df = pd.read_csv("data/sample_claims.csv")

    # Build metrics from real data
    paid    = df[df["claim_status"] == "PAID"]
    metrics = {
        "total_claims":            len(df),
        "paid_claims":             int((df["claim_status"] == "PAID").sum()),
        "denied_claims":           int((df["claim_status"] == "DENIED").sum()),
        "denial_rate_pct":         round((df["claim_status"] == "DENIED").mean() * 100, 2),
        "total_billed":            round(df["billed_amount"].sum(), 2),
        "total_paid":              round(df["paid_amount"].sum(), 2),
        "inpatient_avg_paid":      round(df[df["claim_type"] == "IP"]["paid_amount"].mean(), 2),
        "behavioral_health_spend": round(df[df["provider_type"] == "BEHAVIORAL"]["paid_amount"].sum(), 2),
    }

    generate_executive_summary(metrics)

    classify_denial_reasons([
        "Service not authorized prior to date of service",
        "Claim submitted beyond 90-day filing limit",
        "Member not enrolled on date of service",
        "Procedure code does not match diagnosis",
    ])

    nl_to_sql("Which members had more than one inpatient stay?")
    nl_to_sql("What is total behavioral health spend by state?")

    anomalies = (
        df[(df["claim_status"] == "DENIED") & (df["paid_amount"] > 0)]
        [["claim_id", "paid_amount", "claim_status"]]
        .to_dict(orient="records")
    )
    explain_anomalies(anomalies)

    print("\n✅ Done.\n")


if __name__ == "__main__":
    main()
