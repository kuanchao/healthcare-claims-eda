-- ============================================================
-- Healthcare Claims Analytics
-- ============================================================
-- Exploratory SQL I wrote while digging into claims data.
-- Started simple (spend summaries) and kept adding queries
-- as I found interesting patterns — high-cost members,
-- readmission signals, denial anomalies, etc.
--
-- Compatible with Databricks SQL, Snowflake, BigQuery,
-- and SQLite (for local testing — see README).
-- ============================================================


-- ── Table Definition (SQLite local testing) ──────────────────

CREATE TABLE IF NOT EXISTS claims (
    claim_id          TEXT,
    member_id         TEXT,
    state_code        TEXT,
    plan_id           TEXT,
    service_date      DATE,
    claim_type        TEXT,   -- IP, OP, LTC, RX
    diagnosis_code    TEXT,   -- ICD-10
    procedure_code    TEXT,
    billed_amount     REAL,
    paid_amount       REAL,
    claim_status      TEXT,   -- PAID, DENIED, PENDING, VOID
    provider_id       TEXT,
    provider_type     TEXT,
    managed_care_flag TEXT
);


-- ── Q1: Baseline KPIs ─────────────────────────────────────────
-- First thing I always run — get a feel for the data before
-- going deeper. Denial rate is usually the most telling signal.

SELECT
    COUNT(*)                                                        AS total_claims,
    COUNT(CASE WHEN claim_status = 'PAID'   THEN 1 END)            AS paid_claims,
    COUNT(CASE WHEN claim_status = 'DENIED' THEN 1 END)            AS denied_claims,
    ROUND(
        100.0 * COUNT(CASE WHEN claim_status = 'DENIED' THEN 1 END)
              / COUNT(*), 2
    )                                                               AS denial_rate_pct,
    ROUND(SUM(billed_amount), 2)                                    AS total_billed,
    ROUND(SUM(paid_amount), 2)                                      AS total_paid,
    ROUND(100.0 * SUM(paid_amount) / NULLIF(SUM(billed_amount),0), 2)
                                                                    AS avg_payment_rate_pct
FROM claims;


-- ── Q2: Monthly Spend Trend ───────────────────────────────────
-- Useful for spotting seasonal patterns or sudden spikes.
-- Grouping by claim_type helps separate inpatient cost drivers
-- from routine outpatient volume.

SELECT
    STRFTIME('%Y-%m', service_date)   AS service_month,
    claim_type,
    COUNT(*)                          AS claim_count,
    ROUND(SUM(paid_amount), 2)        AS total_paid,
    ROUND(AVG(paid_amount), 2)        AS avg_paid_per_claim
FROM claims
WHERE claim_status = 'PAID'
GROUP BY 1, 2
ORDER BY 1, 2;


-- ── Q3: Spend by State and Plan ───────────────────────────────
-- PMPM (per-member per-month) is the standard managed care metric.
-- Large PMPM variation across plans often flags utilization issues
-- or enrollment mix differences worth investigating.

SELECT
    state_code,
    plan_id,
    COUNT(DISTINCT member_id)                                        AS unique_members,
    COUNT(*)                                                         AS total_claims,
    ROUND(SUM(paid_amount), 2)                                       AS total_paid,
    ROUND(SUM(paid_amount) / NULLIF(COUNT(DISTINCT member_id), 0), 2) AS pmpm_proxy
FROM claims
WHERE claim_status = 'PAID'
GROUP BY 1, 2
ORDER BY total_paid DESC;


-- ── Q4: High-Cost Member Identification ──────────────────────
-- A small percentage of members typically drives a large share
-- of spend — this surfaces them. In a real program, these would
-- feed into care management outreach workflows.

SELECT
    member_id,
    state_code,
    COUNT(*)                                              AS total_claims,
    COUNT(CASE WHEN claim_type = 'IP' THEN 1 END)        AS inpatient_visits,
    ROUND(SUM(paid_amount), 2)                            AS total_paid,
    ROUND(AVG(paid_amount), 2)                            AS avg_per_claim,
    MAX(service_date)                                     AS most_recent_claim
FROM claims
WHERE claim_status = 'PAID'
GROUP BY 1, 2
HAVING SUM(paid_amount) > 5000
ORDER BY total_paid DESC
LIMIT 20;


-- ── Q5: Behavioral Health Utilization ────────────────────────
-- BH spend is often undercounted because it's spread across
-- multiple provider types. Worth isolating to track properly.
-- The window function gives each state's BH share of total claims.

SELECT
    state_code,
    provider_type,
    COUNT(*)                          AS claim_count,
    COUNT(DISTINCT member_id)         AS unique_members,
    ROUND(SUM(paid_amount), 2)        AS total_paid,
    ROUND(AVG(paid_amount), 2)        AS avg_paid,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY state_code), 2
    )                                 AS pct_of_state_claims
FROM claims
WHERE claim_status  = 'PAID'
  AND provider_type = 'BEHAVIORAL'
GROUP BY 1, 2
ORDER BY total_paid DESC;


-- ── Q6: Denial Rate by Provider Type ─────────────────────────
-- High denial rates for a specific provider type usually point
-- to a billing/coding issue, not a clinical one — good starting
-- point for provider education or audit.

SELECT
    provider_type,
    COUNT(*)                                                          AS total_claims,
    COUNT(CASE WHEN claim_status = 'DENIED' THEN 1 END)              AS denied,
    ROUND(
        100.0 * COUNT(CASE WHEN claim_status = 'DENIED' THEN 1 END)
              / COUNT(*), 2
    )                                                                 AS denial_rate_pct,
    ROUND(SUM(billed_amount) - SUM(paid_amount), 2)                  AS total_underpaid
FROM claims
GROUP BY 1
ORDER BY denial_rate_pct DESC;


-- ── Q7: Top ICD-10 Diagnosis Codes by Spend ──────────────────
-- Simple but often surprising — the top 10 codes can account for
-- a disproportionate share of total spend.

SELECT
    diagnosis_code,
    COUNT(*)                       AS claim_count,
    COUNT(DISTINCT member_id)      AS unique_members,
    ROUND(SUM(paid_amount), 2)     AS total_paid,
    ROUND(AVG(paid_amount), 2)     AS avg_per_claim
FROM claims
WHERE claim_status = 'PAID'
GROUP BY 1
ORDER BY total_paid DESC
LIMIT 15;


-- ── Q8: Data Quality — Denied Claims with Payment ────────────
-- Found this anomaly while exploring: some denied claims still
-- had paid_amount > 0. Could be a system timing issue or a
-- post-payment retroactive denial. Either way, worth flagging.

SELECT
    claim_id,
    member_id,
    service_date,
    claim_type,
    billed_amount,
    paid_amount,
    claim_status,
    'denied but paid_amount > 0' AS anomaly_flag
FROM claims
WHERE claim_status = 'DENIED'
  AND paid_amount  > 0;


-- ── Q9: 30-Day Readmission Proxy ─────────────────────────────
-- Not a perfect readmission measure (would need discharge dates)
-- but a reasonable proxy — members with two IP claims within
-- 30 days are worth flagging for follow-up.

SELECT
    a.member_id,
    a.claim_id                                                  AS first_admit,
    a.service_date                                              AS first_admit_date,
    b.claim_id                                                  AS readmit_claim,
    b.service_date                                              AS readmit_date,
    CAST(JULIANDAY(b.service_date) - JULIANDAY(a.service_date) AS INTEGER)
                                                                AS days_between
FROM  claims a
JOIN  claims b
  ON  a.member_id  = b.member_id
  AND a.claim_id  != b.claim_id
  AND a.claim_type = 'IP'
  AND b.claim_type = 'IP'
  AND b.service_date > a.service_date
  AND JULIANDAY(b.service_date) - JULIANDAY(a.service_date) <= 30
WHERE a.claim_status = 'PAID'
  AND b.claim_status = 'PAID'
ORDER BY a.member_id, days_between;
