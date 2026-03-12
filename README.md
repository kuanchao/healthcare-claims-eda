# Data Analytics Portfolio — Kuan Chao

Personal projects exploring healthcare claims data analysis, pipeline engineering, and LLM-driven automation.

---

## Notebooks (start here)

| Notebook | What it covers |
|----------|---------------|
| `notebooks/01_claims_etl.ipynb` | Ingest → validate → transform → export. Data quality checks, anomaly flagging. |
| `notebooks/02_sql_analytics.ipynb` | 7 analytical SQL queries run live: spend trends, denial rates, high-cost members, readmission proxy. |
| `notebooks/03_llm_automation.ipynb` | GPT-4 for executive summary generation, denial reason classification, NL→SQL, anomaly explanation. |

---

## How to run

```bash
# Install dependencies
pip install -r requirements.txt

# Open Jupyter
jupyter notebook
```

Then open any notebook from the `notebooks/` folder. All notebooks run with the sample data in `data/sample_claims.csv`.

`03_llm_automation.ipynb` runs in demo mode by default (no API key needed).
Set `DEMO_MODE = False` and add `OPENAI_API_KEY` to use live GPT-4.

---

## Other files

```
python/     # Same logic as notebooks, as standalone scripts
pyspark/    # PySpark version with window functions + partitioned output
sql/        # Raw SQL queries (Databricks/Snowflake/BigQuery compatible)
data/       # Sample dataset + generated output
```

---

## Note on AI tooling

I used AI tools to help build parts of this — which is exactly what `03_llm_automation.ipynb` demonstrates.
The point isn't whether AI helped write the code; it's understanding it well enough to extend, debug, and apply it to real problems.

---

*github.com/kuanchao*
