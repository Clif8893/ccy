# IT3388 — Interim Progress Review (Individual, 30%)

**Member A — Clifton Chen Yi** · Project Group 2 · Team FinSight Colombia
**Workstream:** Inactivity & Retention — `inactive_next_60d`

Deliverable: [`IT3388_Interim_Progress_Review_MemberA_Clifton.py`](./IT3388_Interim_Progress_Review_MemberA_Clifton.py) — a Databricks notebook (PySpark + Delta Lake) implementing the retention data pipeline described in Appendix C and Appendix D.

---

## How to run it

1. **Import into Databricks.** Workspace → *Import* → *File*, and select the `.py`. It is in Databricks source format, so it opens as a notebook with all 100 cells intact. (If the repo is linked as a Databricks Repo, it appears as a notebook automatically.)
2. **Upload the two COFINFAD CSVs** from [Mendeley Data](https://data.mendeley.com/datasets/mhb4zn3258) to the folder in the `raw_data_dir` widget — default `dbfs:/FileStore/it3388/raw`:
   - `customer_data.csv` (48,723 × 54)
   - `transactions_data.csv` (3,159,157 × 4)
3. **Attach compute** running DBR 13.3 LTS or later (serverless is fine). No `%pip install` is needed — PySpark, pandas, NumPy and matplotlib are all preinstalled.
4. **Run all.** Every widget has a working default; nothing else needs editing.

Expect roughly 10–20 minutes end to end on a small cluster. Section 1.5 (staging arrival files and streaming them back) and Section 3.10 (four-fraction scalability probe) are the slow parts — both can be switched off with widgets if you only need the modelling path.

### Widgets

| Widget | Default | Notes |
|---|---|---|
| `catalog` | *(blank)* | Blank uses the default catalog / `hive_metastore`. Set it to use Unity Catalog; a managed Volume is then created automatically for working files. |
| `schema` | `it3388_g2_retention` | Created if absent. All ~27 output tables land here. |
| `raw_data_dir` | `dbfs:/FileStore/it3388/raw` | Where the two CSVs are. |
| `work_dir` | *(blank)* | Auto-resolves to a UC Volume when `catalog` is set, otherwise `dbfs:/FileStore/it3388/work`. Holds the streaming landing zone, checkpoints and the saved pipeline. |
| `enable_streaming` | `yes` | Set to `no` to skip Section 1.5. |
| `run_scalability_probe` | `yes` | Set to `no` to skip Section 3.10. |
| `sample_fraction` | `1.00` | Down-samples **customers** (not rows, which would break referential integrity). Use for quick iteration; re-run at `1.00` before submitting. |
| `train_cutoff` / `valid_cutoff` / `test_cutoff` | `2023-07-02` / `2023-08-31` / `2023-10-30` | Validated up front: three non-overlapping 60-day outcome windows, the last ending exactly on the final day of data. |
| `outcome_window_days` | `60` | The target horizon from Appendix D. |
| `eligibility_window_days` | `90` | A customer must have transacted within this many days of the cutoff to be scored. |

---

## Where each rubric component is evidenced

| Component | Marks | Sections | Key evidence tables |
|---|---|---|---|
| **Data Collection** | 10 | 1.1 – 1.6 | `meta_source_inventory`, `ops_collection_summary`, `ops_stream_ingest_audit` |
| **Data Management** | 10 | 2.1 – 2.6 | `meta_table_registry`, `meta_data_dictionary`, `dq_audit_result`, `dq_null_profile`, `insight_key_theme` |
| **Data Preparation** | 15 | 3.1 – 3.10 | `meta_column_decision`, `meta_feature_catalogue`, `dq_leakage_audit`, `gold_retention_features`, `ops_scalability_probe`, `ops_runtime_log` |
| **Data Visualization** | 15 | 4.1 – 4.11 | 11 figures + the joint view in 5.4; `insight_target_driver` |
| **Co-creating** | 10 | 5.1 – 5.4 | `meta_team_decision_log`, `meta_shared_contract`, `meta_stakeholder_feedback`, `gold_retention_priority` |

Section 6.4 writes `ops_interim_evidence_index`, which is this table as queryable data.

---

## The three findings worth leading with in the presentation

1. **Two rival transaction-summary column families exist and only one is real.** `tx_count`, `total_tx_volume`, `first_tx` and `last_tx` reconcile with the raw ledger on 100% of customers. The similarly-named `total_transaction_volume`, `average_transaction_value`, `last_transaction_date` and five siblings correlate ≈ 0.00 with the same ledger. All nine are excluded team-wide, and Member B's value target was changed as a result (decision **D-02**).

2. **24 of the 54 source columns never reach the model** — 9 unreconciled, 8 carrying post-cutoff information, 5 redundant, 2 published to Member B instead. `last_tx` alone almost perfectly determines the target; `churn_probability` turned out to be a near-deterministic function of `active_products` (r ≈ −0.88), i.e. a vendor formula rather than an observed outcome.

3. **The hypothesis holds at the univariate level.** Recency, 30-day frequency, the 30-day trend and "silence relative to the customer's own rhythm" dominate the association with `inactive_next_60d`; age, gender, household size and education sit near zero. Recency and trend also act *independently*, so both earn a place in the model.

---

## What this notebook deliberately does **not** claim

Limitations are listed with IDs in Section 6.2 and are referenced from the code that is affected. The four worth knowing before you present:

- **L-09** — `failed_transactions`, `support_tickets_count` and `international_transactions` are counts with no timestamp, so unlike ledger features they cannot be bounded to a cutoff. Check **LK-07** measures how much of the feature set is cutoff-invariant, and an ablation without them is planned.
- **L-10** — the three splits contain the *same customers* at different cutoffs. The outcome windows genuinely do not overlap, so test performance is a later-period estimate — but not a held-out-*population* estimate.
- **L-11** — the readiness model is class-weighted, so `p_inactive` is a **ranking**, not a calibrated probability. Only rank-based metrics appear anywhere in the notebook.
- **L-02** — the target is *no observed transactions in 60 days*. It is not account closure, and it is not called churn.

The Section 3.9 baseline is a **data-readiness check**, not the modelling deliverable. Model comparison, tuning, SHAP and error analysis belong to the final phase (Section 6.3).

---

## Source documents

- `IT3388 2026S1 Project Guide_V1.pdf` — assessment rubric (§6.2.3 is the one this notebook answers)
- `APPENDIX C – PROJECT PROPOSAL.docx` — team proposal
- `APPENDIX D – EXPLORATORY DATA ANALYSIS.docx` — my individual EDA report
- `EXPLORATORY DATA ANALYSIS.ipynb` — the team's shared pandas EDA, which this notebook supersedes on Spark
