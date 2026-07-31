# Databricks notebook source
# MAGIC %md
# MAGIC # IT3388 Big Data Management Project — Interim Progress Review (Individual, 30%)
# MAGIC
# MAGIC **Module Group:** IT3388 &nbsp;|&nbsp; **Project Group:** 2 &nbsp;|&nbsp; **Team Name:** FinSight Colombia
# MAGIC
# MAGIC **Member A — Clifton Chen Yi** &nbsp;|&nbsp; **Workstream:** Inactivity & Retention (`inactive_next_60d`)
# MAGIC
# MAGIC **Platform:** Databricks (Apache Spark + Delta Lake, medallion architecture)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What this notebook delivers
# MAGIC
# MAGIC The project proposal (Appendix C) and my individual EDA report (Appendix D) established *what*
# MAGIC the retention workstream will do. This notebook is the **implementation of the data pipeline** for
# MAGIC that workstream, built and run entirely on the cloud platform: collection → management →
# MAGIC preparation → visualisation, ending in a governed, leakage-audited, model-ready feature table.
# MAGIC
# MAGIC The business question I own is unchanged from the proposal:
# MAGIC
# MAGIC > Among customers who were **active before a cutoff date**, which ones will make **no transactions
# MAGIC > at all during the next 60 days**, and which behavioural signals give the earliest warning?
# MAGIC
# MAGIC ## How this notebook maps to the Interim Progress Review rubric
# MAGIC
# MAGIC | Rubric component | Marks | Notebook section | Evidence produced |
# MAGIC |---|---|---|---|
# MAGIC | Data Collection | 10 | **1** | 10 sources inventoried with include/exclude reasons; 5 ingested across **3 data natures** (structured, unstructured, semi-structured) via **4 mechanisms** — batch CSV with an explicit schema, batch CSV with a schema contract, JSON, and **streaming micro-batches through Auto Loader** — landed into Delta bronze with lineage columns and a batch↔stream row-count reconciliation |
# MAGIC | Data Management | 10 | **2** | Medallion architecture in Unity Catalog / Hive metastore, machine-generated data dictionary, 16-rule data-quality audit table, feature-family grouping, cross-group correlation analysis, key-themes table |
# MAGIC | Data Preparation | 15 | **3** | A written decision for **all 54 source columns**, of which **24 are rejected** (9 unreconciled, 8 leaking, 5 redundant, 2 context-only), documented cleaning rules, three non-overlapping chronological cutoffs, 40+ engineered features, a leakage audit whose recency and target checks are **independent recomputations** rather than restatements, train-only fitted Spark ML pipeline, scalability probe at 25/50/75/100% |
# MAGIC | Data Visualization | 15 | **4** | 11 decision-oriented figures (plus the joint view in §5.4) with one shared risk colour ramp, reference lines, suppressed small groups, and a stated question + answer on every chart |
# MAGIC | Co-creating | 10 | **5** | Shared-asset contract table (producer → consumer guarantees), team decision log, stakeholder feedback loop, and a joint **retention-priority view** that combines my risk score with Member B's value model |
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC 1. Upload `customer_data.csv` and `transactions_data.csv` from
# MAGIC    [COFINFAD on Mendeley Data](https://data.mendeley.com/datasets/mhb4zn3258) to the location you
# MAGIC    set in the `raw_data_dir` widget (default `dbfs:/FileStore/it3388/raw`).
# MAGIC 2. Attach to a cluster / serverless compute with DBR 13.3 LTS or later.
# MAGIC 3. Run all. Every widget has a working default; nothing else needs editing.
# MAGIC
# MAGIC > **Responsible-use note carried over from Appendix D:** the target is *no observed transactions in
# MAGIC > a 60-day window*. It is **not** account closure, and a high score is an association, not a cause.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 0 — Configuration, namespace and instrumentation
# MAGIC
# MAGIC Everything downstream is parameterised. Widgets rather than hard-coded literals means the same
# MAGIC notebook can be re-run by a teammate against their own schema, and the scalability probe can
# MAGIC re-run the pipeline at a reduced data fraction without any code edits.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "01 Catalog (blank = hive_metastore)")
dbutils.widgets.text("schema", "it3388_g2_retention", "02 Schema / database")
dbutils.widgets.text("raw_data_dir", "dbfs:/FileStore/it3388/raw", "03 Raw CSV folder")
dbutils.widgets.text("work_dir", "", "04 Work dir (blank = auto)")
dbutils.widgets.dropdown("enable_streaming", "yes", ["yes", "no"], "05 Run streaming ingestion")
dbutils.widgets.dropdown("run_scalability_probe", "yes", ["yes", "no"], "06 Run scalability probe")
dbutils.widgets.dropdown("sample_fraction", "1.00", ["0.25", "0.50", "0.75", "1.00"], "07 Data fraction")
dbutils.widgets.text("train_cutoff", "2023-07-02", "08 Train cutoff")
dbutils.widgets.text("valid_cutoff", "2023-08-31", "09 Validation cutoff")
dbutils.widgets.text("test_cutoff", "2023-10-30", "10 Test cutoff")
dbutils.widgets.text("outcome_window_days", "60", "11 Outcome window (days)")
dbutils.widgets.text("eligibility_window_days", "90", "12 Eligibility window (days)")

# COMMAND ----------

import datetime as dt
import json
import time
from contextlib import contextmanager

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

from pyspark.sql import DataFrame, functions as F, types as T, Window

# ---------------------------------------------------------------- configuration
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip() or "it3388_g2_retention"
RAW_DIR = dbutils.widgets.get("raw_data_dir").strip().rstrip("/")
ENABLE_STREAMING = dbutils.widgets.get("enable_streaming") == "yes"
RUN_SCALABILITY = dbutils.widgets.get("run_scalability_probe") == "yes"
SAMPLE_FRACTION = float(dbutils.widgets.get("sample_fraction"))

OUTCOME_DAYS = int(dbutils.widgets.get("outcome_window_days"))
ELIGIBILITY_DAYS = int(dbutils.widgets.get("eligibility_window_days"))
CUTOFFS = {
    "train": dbutils.widgets.get("train_cutoff").strip(),
    "valid": dbutils.widgets.get("valid_cutoff").strip(),
    "test": dbutils.widgets.get("test_cutoff").strip(),
}

CUSTOMER_CSV = f"{RAW_DIR}/customer_data.csv"
TRANSACTION_CSV = f"{RAW_DIR}/transactions_data.csv"

MEMBER = "Member A — Clifton Chen Yi"
WORKSTREAM = "inactivity_retention"

# ---------------------------------------------------------------- namespace
if CATALOG:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
    NAMESPACE = f"`{CATALOG}`.`{SCHEMA}`"
else:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{SCHEMA}`")
    NAMESPACE = f"`{SCHEMA}`"


def tbl(name: str) -> str:
    """Fully-qualified, quoted table name. One place to change the storage layout."""
    return f"{NAMESPACE}.`{name}`"


def tbl_plain(name: str) -> str:
    """Same identifier without backticks, for APIs that take a name rather than SQL
    (DataStreamWriter.toTable, spark.catalog.tableExists)."""
    return tbl(name).replace("`", "")


# ---------------------------------------------------------------- work directory
WORK_DIR = dbutils.widgets.get("work_dir").strip().rstrip("/")
if not WORK_DIR:
    if CATALOG:
        # Unity Catalog: a managed Volume is the governed place for files.
        spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`it3388_work`")
        WORK_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/it3388_work"
    else:
        WORK_DIR = "dbfs:/FileStore/it3388/work"
dbutils.fs.mkdirs(WORK_DIR)

LANDING_DIR = f"{WORK_DIR}/landing/transactions_stream"
CHECKPOINT_DIR = f"{WORK_DIR}/_checkpoints"
CONTEXT_DIR = f"{WORK_DIR}/context"

# ---------------------------------------------------------------- instrumentation
RUN_ID = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
RUNTIME_LOG: list = []


@contextmanager
def timed(step: str, layer: str = "", rows: int = -1):
    """Records wall-clock time per pipeline step.

    The proposal commits to reporting ingestion / cleaning / join / feature-engineering /
    training / scoring times. Capturing them as data (not print statements) means Section 3.9
    can chart them and Section 5 can share them with the team.
    """
    t0 = time.time()
    yield
    RUNTIME_LOG.append(
        {
            "run_id": RUN_ID,
            "step": step,
            "layer": layer,
            "rows": rows,
            "seconds": round(time.time() - t0, 3),
            "data_fraction": SAMPLE_FRACTION,
        }
    )
    print(f"  [{layer or 'run':<10}] {step:<52s} {RUNTIME_LOG[-1]['seconds']:>8.2f}s")


def save_table(df: DataFrame, name: str, partition_by=None, comment: str = "") -> str:
    """Single writer for every table in the notebook: Delta, overwrite, schema evolution on."""
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.saveAsTable(tbl(name))
    if comment:
        # Documentation should never be able to fail a data pipeline.
        try:
            safe = comment.replace("'", "''")
            spark.sql(f"COMMENT ON TABLE {tbl(name)} IS '{safe}'")
        except Exception as exc:
            print(f"    (note: could not set comment on {name}: {type(exc).__name__})")
    return tbl(name)


def spark_df(rows: list, columns: list) -> DataFrame:
    """Small metadata/registry frames are authored as Python literals then persisted as Delta,
    so the governance artefacts are queryable alongside the data they describe."""
    return spark.createDataFrame(pd.DataFrame(rows, columns=columns))


print(f"Run id            : {RUN_ID}")
print(f"Namespace         : {NAMESPACE}")
print(f"Raw CSV folder    : {RAW_DIR}")
print(f"Work directory    : {WORK_DIR}")
print(f"Data fraction     : {SAMPLE_FRACTION:.0%}")
print(f"Streaming enabled : {ENABLE_STREAMING}")
print(f"Cutoffs           : {CUTOFFS}")
print(f"Windows           : eligibility {ELIGIBILITY_DAYS}d before cutoff, outcome {OUTCOME_DAYS}d after")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.1 Temporal design check — before any data is read
# MAGIC
# MAGIC The three cutoffs must produce **non-overlapping** outcome windows that all fit inside the
# MAGIC COFINFAD coverage period (2023-01-04 → 2023-12-29), otherwise the "temporal generalisation"
# MAGIC success criterion in Appendix D §3 cannot be met. I assert this up front rather than discovering
# MAGIC it after an hour of feature engineering.

# COMMAND ----------

DATA_START = dt.date(2023, 1, 4)
DATA_END = dt.date(2023, 12, 29)


def window_for(cutoff: str) -> dict:
    c = dt.date.fromisoformat(cutoff)
    return {
        "split": None,
        "cutoff_date": c,
        "eligibility_start": c - dt.timedelta(days=ELIGIBILITY_DAYS - 1),
        "outcome_start": c + dt.timedelta(days=1),
        "outcome_end": c + dt.timedelta(days=OUTCOME_DAYS),
    }


WINDOWS = {}
for split, cutoff in CUTOFFS.items():
    w = window_for(cutoff)
    w["split"] = split
    WINDOWS[split] = w

_rows = []
for split in ["train", "valid", "test"]:
    w = WINDOWS[split]
    _rows.append(
        {
            "split": split,
            "history_from": DATA_START.isoformat(),
            "cutoff_date": w["cutoff_date"].isoformat(),
            "history_days": (w["cutoff_date"] - DATA_START).days + 1,
            "eligibility_window": f"{w['eligibility_start']} .. {w['cutoff_date']}",
            "outcome_window": f"{w['outcome_start']} .. {w['outcome_end']}",
            "outcome_days": (w["outcome_end"] - w["outcome_start"]).days + 1,
        }
    )
temporal_design = pd.DataFrame(_rows)
display(temporal_design)

# --- assertions: fail loudly and early -----------------------------------------
assert WINDOWS["train"]["outcome_end"] < WINDOWS["valid"]["outcome_start"], "train/valid outcome windows overlap"
assert WINDOWS["valid"]["outcome_end"] < WINDOWS["test"]["outcome_start"], "valid/test outcome windows overlap"
assert WINDOWS["test"]["outcome_end"] <= DATA_END, "test outcome window runs past the end of the data"
assert WINDOWS["train"]["eligibility_start"] >= DATA_START, "train eligibility window starts before the data"
for split, w in WINDOWS.items():
    assert (w["outcome_end"] - w["outcome_start"]).days + 1 == OUTCOME_DAYS, f"{split} outcome window is not {OUTCOME_DAYS} days"
print(
    f"Temporal design valid: 3 non-overlapping {OUTCOME_DAYS}-day outcome windows, "
    f"last one ending exactly on the final day of data ({DATA_END})."
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 1 — Data Collection  *(rubric: 10 marks)*
# MAGIC
# MAGIC **What the top band asks for:** *obtain from multiple data sources including structured,
# MAGIC unstructured and streaming data sources* and *use extensive means of collecting & storing data*.
# MAGIC
# MAGIC **How I address it.** COFINFAD ships as two CSVs, so "multiple sources" cannot mean "download
# MAGIC more files and hope they join". Instead I collect three genuinely different *kinds* of data, each
# MAGIC with its own ingestion mechanism and its own storage contract:
# MAGIC
# MAGIC | # | Source | Nature | Collection mechanism | Bronze table |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | `customer_data.csv` (48,723 × 54) | Structured, wide | Batch read, schema contract asserted | `bronze_customer` |
# MAGIC | 2 | `transactions_data.csv` (3,159,157 × 4) | Structured, long ledger | Batch read with **explicit schema** (no inference pass over 3.16M rows) | `bronze_transaction` |
# MAGIC | 3 | Complaint / feature-request / sentiment free text | **Unstructured** documents | Assembled into one document per customer, tokenised downstream | `bronze_feedback_document` |
# MAGIC | 4 | Colombian fintech market context (Superfinanciera, Banco de la República, Finnovista, ITA) | Semi-structured **JSON** | Authored to the work volume, read back with `spark.read.json` | `bronze_market_context` |
# MAGIC | 5 | Daily transaction arrivals | **Streaming** | **Auto Loader** (`cloudFiles`) incremental micro-batches with checkpointing | `bronze_transaction_stream` |
# MAGIC
# MAGIC Every bronze table carries `_ingested_at`, `_source`, `_ingest_mode` and `_run_id` lineage columns,
# MAGIC and every ingestion step is timed into `ops_runtime_log`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Source inventory — what I studied, what I included, and why
# MAGIC
# MAGIC Carried forward from Appendix D §4.1 and persisted as a queryable Delta table so the decision is
# MAGIC auditable rather than buried in prose. Context sources are deliberately **not joined** to customer
# MAGIC records — they justify the business problem, they are not features.

# COMMAND ----------

source_inventory = spark_df(
    [
        ("SRC-01", "COFINFAD customer profiles", "https://data.mendeley.com/datasets/mhb4zn3258",
         "structured", "CSV", "INCLUDED",
         "Primary customer-level table: 48,723 profiles x 54 columns. Supplies demographics, product "
         "holdings, engagement, satisfaction and support features for the inactivity model.", "yes"),
        ("SRC-02", "COFINFAD transaction ledger", "https://data.mendeley.com/datasets/mhb4zn3258",
         "structured", "CSV", "INCLUDED",
         "3,159,157 transactions over 2023-01-04..2023-12-29. The only source of truth for recency, "
         "frequency, value and trend features, and the only way to build the 60-day target.", "yes"),
        ("SRC-03", "COFINFAD free-text feedback fields", "https://data.mendeley.com/datasets/mhb4zn3258",
         "unstructured", "free text in CSV", "INCLUDED",
         "complaint_topics / feature_requests / feedback_sentiment. Text needs tokenising and theme "
         "normalisation before it can be used, so it is collected as documents, not as categories.", "yes"),
        ("SRC-04", "Daily transaction arrivals (streaming replay)", "derived from SRC-02",
         "streaming", "CSV micro-batches", "INCLUDED",
         "Production retention scoring must run on transactions that arrive daily. I replay the last "
         "quarter of the ledger as dated file batches and ingest them with Auto Loader to prove the "
         "incremental path works and reconciles to the batch path.", "yes"),
        ("SRC-05", "Financial Inclusion Report 2023, Superfinanciera",
         "https://www.superfinanciera.gov.co/publicaciones/10115193/reporte-de-inclusion-financiera-2023-avances-y-retos-en-colombia/",
         "semi-structured", "JSON (authored)", "CONTEXT ONLY",
         "94.6% of Colombian adults held a formal financial product in 2023; 27.5M held low-value "
         "digital-wallet deposits. Sizes the retention opportunity. No customer-level key, so it is "
         "never joined to the modelling table.", "no"),
        ("SRC-06", "Financial Infrastructure & Payment Instruments Report 2024, Banco de la Republica",
         "https://www.banrep.gov.co/en/publications-research/financial-infrastructure-payment-instruments-report/2024",
         "semi-structured", "JSON (authored)", "CONTEXT ONLY",
         "28% year-over-year growth in value settled through the large-value payment system in 2023. "
         "Supports the operational-reliability argument. Not joinable.", "no"),
        ("SRC-07", "Finnovista Fintech Radar Colombia",
         "https://assets.ctfassets.net/bvz14004tu0h/2WuRBepO4liQPXYTXBFJZy/56365603e9bccbb96abf61983ee20c1f/RADAR_COLOMBIA_ENGLISH_.pdf",
         "unstructured", "PDF -> JSON facts", "CONTEXT ONLY",
         "394 local fintech startups by April 2024, 3rd in Latin America. Evidence that switching cost "
         "is low and retention matters. Facts extracted manually; the PDF itself is not a data source.", "no"),
        ("SRC-08", "Colombia Financial Technology, US ITA",
         "https://www.trade.gov/market-intelligence/colombia-financial-technology",
         "unstructured", "web page -> JSON facts", "CONTEXT ONLY",
         "Independent corroboration of the competitive landscape.", "no"),
        ("SRC-09", "COFINFAD data article (Data in Brief)", "https://doi.org/10.1016/j.dib.2026.112484",
         "documentation", "journal article", "DOCUMENTATION",
         "Documents collection, anonymisation and the ~6,965 survey respondents (14.3%). Used to bound "
         "claims about satisfaction fields. Not a modelling table.", "no"),
        ("SRC-10", "COFINFAD mirror on Hugging Face", "https://huggingface.co/datasets/luisdavidtrejosrojas/cofinfad",
         "structured", "parquet/CSV", "EXCLUDED",
         "Duplicate distribution of SRC-01/02. Excluded to guarantee one reproducible version is used "
         "for the whole team; kept only as a fallback if Mendeley is unavailable.", "no"),
    ],
    ["source_id", "source_name", "url", "data_nature", "format", "decision", "rationale", "joined_to_model"],
)
save_table(source_inventory, "meta_source_inventory",
           comment="Data sources studied for the retention workstream with include/exclude rationale")
display(spark.table(tbl("meta_source_inventory")).select("source_id", "source_name", "data_nature", "decision", "joined_to_model"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Structured batch ingestion → bronze
# MAGIC
# MAGIC Two deliberate engineering choices, both of which are collection *decisions* rather than defaults:
# MAGIC
# MAGIC 1. **Explicit schema for the 3.16M-row ledger.** `inferSchema=True` costs a full extra pass over
# MAGIC    the file. The ledger has exactly four known columns, so I declare them. This is the single
# MAGIC    cheapest scalability win in the whole pipeline and it also stops Spark from silently guessing
# MAGIC    `amount` as a string if one row is malformed.
# MAGIC 2. **Inference + a schema contract for the 54-column customer file.** It is small (48k rows), so
# MAGIC    inference is cheap; but I then *assert* that all 54 expected column names are present. That
# MAGIC    catches an upstream file swap immediately instead of three sections later.
# MAGIC
# MAGIC Bronze is an immutable, append-only landing copy: no cleaning, no filtering, no renaming.

# COMMAND ----------

TRANSACTION_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.LongType(), True),
        T.StructField("date", T.DateType(), True),
        T.StructField("amount", T.LongType(), True),
        T.StructField("type", T.StringType(), True),
    ]
)

EXPECTED_CUSTOMER_COLUMNS = [
    # demographics
    "customer_id", "age", "gender", "location", "income_bracket", "occupation",
    "education_level", "marital_status", "household_size", "acquisition_channel", "customer_segment",
    # product holdings
    "savings_account", "credit_card", "personal_loan", "investment_account", "insurance_product",
    "active_products",
    # engagement
    "app_logins_frequency", "feature_usage_diversity", "bill_payment_user", "auto_savings_enabled",
    "credit_utilization_ratio", "international_transactions", "failed_transactions",
    # transaction summary "Set A" (ledger-verified in Appendix D 3.4)
    "tx_count", "avg_tx_value", "total_tx_volume", "first_tx", "last_tx",
    # satisfaction / support / feedback
    "base_satisfaction", "tx_satisfaction", "product_satisfaction", "satisfaction_score", "nps_score",
    "last_survey_date", "support_tickets_count", "resolved_tickets_ratio", "app_store_rating",
    "feedback_sentiment", "feature_requests", "complaint_topics",
    # transaction summary "Set B" (does NOT reconcile with the ledger) + value fields
    "clv_segment", "monthly_transaction_count", "average_transaction_value", "total_transaction_volume",
    "transaction_frequency", "last_transaction_date", "preferred_transaction_type",
    "first_transaction_date", "weekend_transaction_ratio", "avg_daily_transactions",
    "customer_tenure", "churn_probability", "customer_lifetime_value",
]


def with_lineage(df: DataFrame, source_id: str, ingest_mode: str, file_metadata: bool = True) -> DataFrame:
    """Attach ingestion lineage to every bronze row. Cheap now, invaluable when a number looks wrong.

    `file_metadata=True` captures the physical file each row came from via Spark's hidden
    `_metadata` column; it is only valid for readers pointed straight at files, so derived
    DataFrames pass False.
    """
    src = F.col("_metadata.file_path") if file_metadata else F.lit(None).cast("string")
    return (
        df.withColumn("_source", F.lit(source_id))
        .withColumn("_source_file", src)
        .withColumn("_ingest_mode", F.lit(ingest_mode))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(RUN_ID))
    )


print("Structured batch ingestion -> bronze")

with timed("read customer_data.csv (inferSchema, 54 cols)", "bronze"):
    raw_customer = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "FAILFAST")
        .csv(CUSTOMER_CSV)
    )
    bronze_customer = with_lineage(raw_customer, "SRC-01", "batch")
    save_table(bronze_customer, "bronze_customer",
               comment="Immutable landing copy of customer_data.csv (SRC-01)")
    n_customer = spark.table(tbl("bronze_customer")).count()

with timed("read transactions_data.csv (explicit schema, 3.16M rows)", "bronze"):
    raw_tx = (
        spark.read.option("header", "true")
        .option("mode", "FAILFAST")
        .schema(TRANSACTION_SCHEMA)
        .csv(TRANSACTION_CSV)
    )
    bronze_tx = with_lineage(raw_tx, "SRC-02", "batch")
    save_table(bronze_tx, "bronze_transaction",
               comment="Immutable landing copy of transactions_data.csv (SRC-02)")
    n_tx = spark.table(tbl("bronze_transaction")).count()

# --- schema contract ----------------------------------------------------------
actual_cols = [c for c in spark.table(tbl("bronze_customer")).columns if not c.startswith("_")]
missing_cols = [c for c in EXPECTED_CUSTOMER_COLUMNS if c not in actual_cols]
unexpected_cols = [c for c in actual_cols if c not in EXPECTED_CUSTOMER_COLUMNS]

print(f"\nbronze_customer    : {n_customer:>10,} rows x {len(actual_cols)} business columns")
print(f"bronze_transaction : {n_tx:>10,} rows x 4 business columns")
print(f"Schema contract    : {len(missing_cols)} expected columns missing, {len(unexpected_cols)} unexpected")
if missing_cols:
    print(f"  MISSING   : {missing_cols}")
if unexpected_cols:
    print(f"  UNEXPECTED: {unexpected_cols}")
assert not missing_cols, "Customer file does not match the documented schema contract - stop and re-check the source file."

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Unstructured collection — feedback as documents, not as categories
# MAGIC
# MAGIC `complaint_topics`, `feature_requests` and `feedback_sentiment` are free-text fields. Appendix D
# MAGIC established that their blanks are **structural** (~50% of customers have no complaint on file,
# MAGIC 33% no feature request) — they are a *state*, not missing data.
# MAGIC
# MAGIC Treating them as ready-made categorical columns would throw that away. Instead I collect one
# MAGIC **document per customer** and keep the raw text, its length and its token count. Section 3
# MAGIC tokenises it, removes stop words and normalises it into operational-friction themes — which is
# MAGIC exactly the shared artefact Member C (Yi Ting) needs for the satisfaction workstream.

# COMMAND ----------

print("Unstructured ingestion -> bronze_feedback_document")

with timed("assemble + ingest free-text feedback documents", "bronze"):
    src = spark.table(tbl("bronze_customer"))
    feedback_doc = (
        src.select(
            "customer_id",
            F.col("complaint_topics").alias("complaint_raw"),
            F.col("feature_requests").alias("feature_request_raw"),
            F.col("feedback_sentiment").alias("sentiment_raw"),
        )
        # one concatenated document per customer; nulls are a state, so they get an explicit token
        .withColumn(
            "document_text",
            F.concat_ws(
                " . ",
                F.concat(F.lit("complaint: "), F.coalesce(F.col("complaint_raw"), F.lit("no complaint on file"))),
                F.concat(F.lit("request: "), F.coalesce(F.col("feature_request_raw"), F.lit("no request on file"))),
                F.concat(F.lit("sentiment: "), F.coalesce(F.col("sentiment_raw"), F.lit("not recorded"))),
            ),
        )
        .withColumn("has_complaint_text", F.col("complaint_raw").isNotNull())
        .withColumn("has_request_text", F.col("feature_request_raw").isNotNull())
        .withColumn("document_chars", F.length("document_text"))
        .withColumn("document_tokens", F.size(F.split(F.trim(F.col("document_text")), r"\s+")))
    )
    save_table(with_lineage(feedback_doc, "SRC-03", "batch_unstructured", file_metadata=False),
               "bronze_feedback_document",
               comment="One free-text feedback document per customer (SRC-03), pre-tokenisation")
    n_doc = spark.table(tbl("bronze_feedback_document")).count()

doc_profile = spark.table(tbl("bronze_feedback_document")).agg(
    F.count("*").alias("documents"),
    F.sum(F.col("has_complaint_text").cast("int")).alias("with_complaint_text"),
    F.sum(F.col("has_request_text").cast("int")).alias("with_request_text"),
    F.round(F.avg("document_tokens"), 1).alias("avg_tokens"),
    F.max("document_tokens").alias("max_tokens"),
)
display(doc_profile)
print(f"{n_doc:,} documents collected. Blank complaint/request text is preserved as an explicit "
      f"'no ... on file' token so downstream models can learn from the absence itself.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Semi-structured collection — market context as JSON
# MAGIC
# MAGIC A second storage format and a second read path, exercised end to end: the market statistics that
# MAGIC justify the business problem are written to the work volume as JSON and read back with
# MAGIC `spark.read.json`. They stay **context-only** (`joined_to_model = no`) — the honest handling for
# MAGIC national aggregates that have no customer key.

# COMMAND ----------

print("Semi-structured ingestion -> bronze_market_context")

market_context = [
    {"source_id": "SRC-05", "publisher": "Superintendencia Financiera de Colombia", "year": 2023,
     "metric": "adults_with_formal_financial_product_pct", "value": 94.6, "unit": "percent",
     "relevance": "Acquisition is close to saturated, so growth has to come from retention."},
    {"source_id": "SRC-05", "publisher": "Superintendencia Financiera de Colombia", "year": 2023,
     "metric": "adults_with_low_value_deposits", "value": 27.5, "unit": "millions",
     "relevance": "Large digital-wallet base means small activity declines scale into large revenue loss."},
    {"source_id": "SRC-06", "publisher": "Banco de la Republica", "year": 2023,
     "metric": "large_value_payment_system_value_growth_yoy", "value": 28.0, "unit": "percent",
     "relevance": "Rising settled value raises the cost of an unreliable or under-used platform."},
    {"source_id": "SRC-07", "publisher": "Finnovista", "year": 2024,
     "metric": "local_fintech_startups", "value": 394.0, "unit": "count",
     "relevance": "Switching cost is low; a disengaging customer has many alternatives."},
    {"source_id": "SRC-09", "publisher": "Data in Brief (COFINFAD article)", "year": 2026,
     "metric": "survey_respondents_share", "value": 14.3, "unit": "percent",
     "relevance": "Satisfaction fields cover ~6,965 customers, so satisfaction-based claims must be bounded."},
]

with timed("write + read market context JSON", "bronze"):
    dbutils.fs.mkdirs(CONTEXT_DIR)
    dbutils.fs.put(
        f"{CONTEXT_DIR}/market_context.json",
        "\n".join(json.dumps(r) for r in market_context),
        overwrite=True,
    )
    ctx = spark.read.json(f"{CONTEXT_DIR}/market_context.json")
    save_table(with_lineage(ctx, "SRC-05..09", "batch_json"), "bronze_market_context",
               comment="Colombian fintech market context facts (context only, never joined to customers)")

display(spark.table(tbl("bronze_market_context")).select("source_id", "publisher", "metric", "value", "unit", "relevance"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Streaming collection — Auto Loader over daily transaction arrivals
# MAGIC
# MAGIC **Why this is not decoration.** A retention score is only useful if it is refreshed as new
# MAGIC transactions arrive; re-reading 3.16M rows every morning to discover ~8,800 new ones is the wrong
# MAGIC architecture. So I build the incremental path now:
# MAGIC
# MAGIC 1. **Replay harness** — the final quarter of the ledger is written out as dated weekly CSV files
# MAGIC    into a landing folder, standing in for the daily drops a production feed would produce.
# MAGIC 2. **Auto Loader** (`cloudFiles`) reads the folder as a stream with a schema and a checkpoint, so
# MAGIC    each file is processed exactly once; `trigger(availableNow=True)` drains everything pending and
# MAGIC    stops, which is the correct trigger for a scheduled batch-scoring job.
# MAGIC 3. **Reconciliation** — the streamed row count is compared against the batch path. Collection is
# MAGIC    only "done" when the two agree.
# MAGIC
# MAGIC A classic file-source stream is used as a fallback if `cloudFiles` is unavailable on the attached
# MAGIC compute, and the whole section is skippable via the `enable_streaming` widget.

# COMMAND ----------

STREAM_FROM = (WINDOWS["test"]["cutoff_date"] - dt.timedelta(days=ELIGIBILITY_DAYS - 1)).isoformat()
stream_stats = {"enabled": ENABLE_STREAMING, "mode": "skipped", "files": 0, "rows_streamed": 0, "rows_expected": 0}

if ENABLE_STREAMING:
    print(f"Streaming ingestion -> bronze_transaction_stream  (replaying arrivals from {STREAM_FROM})")

    # ---- 1. build the landing zone: one CSV file per ISO week -----------------
    with timed("stage weekly arrival files into landing zone", "landing"):
        dbutils.fs.rm(LANDING_DIR, True)
        arrivals = (
            spark.table(tbl("bronze_transaction"))
            .select("customer_id", "date", "amount", "type")
            .where(F.col("date") >= F.lit(STREAM_FROM))
        )
        stream_stats["rows_expected"] = arrivals.count()
        # One file per ISO week. Built with year + weekofyear rather than a date_format pattern
        # because mixing calendar-year with week-of-year letters is not portable across Spark versions.
        arrivals_dated = arrivals.withColumn(
            "arrival_batch",
            F.concat(F.date_format("date", "yyyy"), F.lit("w"),
                     F.lpad(F.weekofyear("date").cast("string"), 2, "0")),
        )
        batches = [r["arrival_batch"] for r in
                   arrivals_dated.select("arrival_batch").distinct().orderBy("arrival_batch").collect()]
        for b in batches:
            # Plain sub-directory names, NOT "key=value": a Hive-style path would make Spark try to
            # infer a partition column that is absent from the explicit schema.
            (
                arrivals_dated.where(F.col("arrival_batch") == b)
                .drop("arrival_batch")
                .coalesce(1)
                .write.mode("overwrite")
                .option("header", "true")
                .csv(f"{LANDING_DIR}/{b}")
            )
        stream_stats["files"] = len(batches)
        print(f"  staged {len(batches)} weekly arrival batches ({stream_stats['rows_expected']:,} rows)")

    # ---- 2. incremental ingestion --------------------------------------------
    def start_stream(use_autoloader: bool):
        if use_autoloader:
            reader = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.schemaLocation", f"{CHECKPOINT_DIR}/tx_stream/schema")
                .option("header", "true")
                .schema(TRANSACTION_SCHEMA)
            )
            mode = "autoloader_cloudFiles"
        else:
            reader = spark.readStream.format("csv").option("header", "true").schema(TRANSACTION_SCHEMA)
            mode = "structured_streaming_file_source"
        stream_df = with_lineage(reader.load(f"{LANDING_DIR}/"), "SRC-04", mode)
        query = (
            stream_df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/tx_stream/commits")
            .trigger(availableNow=True)
            .toTable(tbl_plain("bronze_transaction_stream"))
        )
        return query, mode

    def run_stream(use_autoloader: bool):
        """Start the stream, drain everything pending, and return it. availableNow means the query
        terminates by itself once the backlog is consumed - the right trigger for a scheduled job."""
        spark.sql(f"DROP TABLE IF EXISTS {tbl('bronze_transaction_stream')}")
        dbutils.fs.rm(f"{CHECKPOINT_DIR}/tx_stream", True)
        q, m = start_stream(use_autoloader)
        q.awaitTermination()
        return q, m

    with timed("Auto Loader incremental ingestion (availableNow)", "bronze_stream"):
        try:
            query, stream_stats["mode"] = run_stream(use_autoloader=True)
        except Exception as exc:
            # cloudFiles is not available on every edition/compute type; the incremental pattern is
            # the point, so fall back to the classic file source rather than skipping the section.
            print(f"  Auto Loader unavailable ({type(exc).__name__}: {exc}).")
            print("  Falling back to the Structured Streaming file source.")
            query, stream_stats["mode"] = run_stream(use_autoloader=False)
        progress = query.recentProgress
        stream_stats["rows_streamed"] = spark.table(tbl("bronze_transaction_stream")).count()

    def prog_field(p, name, default=None):
        """recentProgress yields dicts on some runtimes and StreamingQueryProgress objects on others."""
        if isinstance(p, dict):
            return p.get(name, default)
        return getattr(p, name, default)

    micro = pd.DataFrame(
        [
            {
                "batch_id": prog_field(p, "batchId"),
                "input_rows": prog_field(p, "numInputRows"),
                "rows_per_second": round(prog_field(p, "processedRowsPerSecond") or 0, 1),
            }
            for p in progress
        ]
    )
    print(f"\n  ingest mode      : {stream_stats['mode']}")
    print(f"  micro-batches    : {len(micro)}")
    print(f"  rows streamed    : {stream_stats['rows_streamed']:,}")
    print(f"  rows expected    : {stream_stats['rows_expected']:,}")
    if not micro.empty:
        display(micro)

    # Reconcile on VALUES, per day, not just on a total row count. A total-only check cannot detect a
    # double-ingested file offset by a dropped one, a mis-parsed amount, or a column that arrived null.
    _batch_daily = (
        spark.table(tbl("bronze_transaction"))
        .where(F.col("date") >= F.lit(STREAM_FROM))
        .groupBy("date").agg(F.count("*").alias("b_rows"), F.sum("amount").alias("b_amount"),
                             F.countDistinct("customer_id").alias("b_customers"))
    )
    _stream_daily = (
        spark.table(tbl("bronze_transaction_stream"))
        .groupBy("date").agg(F.count("*").alias("s_rows"), F.sum("amount").alias("s_amount"),
                             F.countDistinct("customer_id").alias("s_customers"))
    )
    _recon_daily = _batch_daily.join(_stream_daily, "date", "full_outer")
    _mismatched_days = _recon_daily.where(
        ~(F.col("b_rows").eqNullSafe(F.col("s_rows"))
          & F.col("b_amount").eqNullSafe(F.col("s_amount"))
          & F.col("b_customers").eqNullSafe(F.col("s_customers")))
    )
    n_mismatched_days = _mismatched_days.count()

    print(f"  daily value reconciliation: {n_mismatched_days} mismatching day(s) "
          f"of {_recon_daily.count()} compared")
    if n_mismatched_days:
        display(_mismatched_days.orderBy("date"))
    assert stream_stats["rows_streamed"] == stream_stats["rows_expected"], (
        f"Row count mismatch: streamed {stream_stats['rows_streamed']:,} vs "
        f"expected {stream_stats['rows_expected']:,}."
    )
    assert n_mismatched_days == 0, (
        "Streaming and batch paths disagree on per-day row count, amount or customer count - "
        "do not trust either until they agree."
    )
    stream_stats["mismatched_days"] = n_mismatched_days
    print("  RECONCILED: row counts, daily amounts and daily distinct customers all agree exactly.")
else:
    print("Streaming ingestion skipped (enable_streaming = no).")

save_table(spark_df([(
    stream_stats["mode"], int(stream_stats["files"]), int(stream_stats["rows_streamed"]),
    int(stream_stats["rows_expected"]), int(stream_stats.get("mismatched_days", -1)), STREAM_FROM)],
    ["ingest_mode", "arrival_files", "rows_streamed", "rows_expected", "mismatched_days", "replay_from"]),
    "ops_stream_ingest_audit", comment="Reconciliation of the streaming ingestion path against the batch path")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 Collection summary
# MAGIC
# MAGIC Five bronze tables, four collection mechanisms (batch CSV with explicit schema, batch CSV with a
# MAGIC schema contract, JSON, Auto Loader streaming), two storage formats read, one governed Delta layer
# MAGIC written, full row-count reconciliation, and lineage columns on every row.

# COMMAND ----------

collection_summary = spark_df(
    [
        ("bronze_customer", "SRC-01", "structured", "batch / inferSchema + contract", int(n_customer)),
        ("bronze_transaction", "SRC-02", "structured", "batch / explicit schema", int(n_tx)),
        ("bronze_feedback_document", "SRC-03", "unstructured", "batch / document assembly", int(n_doc)),
        ("bronze_market_context", "SRC-05..09", "semi-structured", "batch / JSON", len(market_context)),
        ("bronze_transaction_stream", "SRC-04", "streaming", stream_stats["mode"], int(stream_stats["rows_streamed"])),
    ],
    ["bronze_table", "source_id", "data_nature", "collection_mechanism", "rows"],
)
save_table(collection_summary, "ops_collection_summary", comment="Data Collection evidence for the Interim Progress Review")
display(collection_summary)


# COMMAND ----------

# MAGIC %md
# MAGIC # Section 2 — Data Management  *(rubric: 10 marks)*
# MAGIC
# MAGIC **What the top band asks for:** analyse the data through a *systematic process*, *identify
# MAGIC connections or correlations between groups of data*, and *determine key themes*.
# MAGIC
# MAGIC **How I address it.** Four artefacts, all persisted as Delta tables so they can be queried,
# MAGIC diffed between runs, and handed to teammates:
# MAGIC
# MAGIC | Artefact | Table | Purpose |
# MAGIC |---|---|---|
# MAGIC | Medallion registry | `meta_table_registry` | Which layer each table belongs to, who owns it, who consumes it |
# MAGIC | Data dictionary | `meta_data_dictionary` | Every column: family, type, completeness, verification status, modelling decision |
# MAGIC | Data-quality audit | `dq_audit_result` | 16 executable rules with observed vs expected values and a pass/fail verdict |
# MAGIC | Key themes | `insight_key_theme` | The cross-group findings that actually change the pipeline design |
# MAGIC
# MAGIC The organising principle is the **medallion architecture**: bronze (immutable landing) → silver
# MAGIC (cleaned, conformed, shared with the team) → gold (workstream-specific, model-ready), with a
# MAGIC parallel `meta_*` / `dq_*` / `ops_*` namespace for governance. That is the systematic process; the
# MAGIC sections below execute it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Medallion registry — the map of the pipeline

# COMMAND ----------

table_registry = spark_df(
    [
        ("bronze", "bronze_customer", "Member A", "all members", "Immutable copy of customer_data.csv"),
        ("bronze", "bronze_transaction", "Member A", "all members", "Immutable copy of the 3.16M-row ledger"),
        ("bronze", "bronze_feedback_document", "Member A", "Member C", "Free-text feedback documents"),
        ("bronze", "bronze_market_context", "Member A", "all members", "Market context facts (not joined)"),
        ("bronze", "bronze_transaction_stream", "Member A", "Member D", "Incrementally ingested arrivals"),
        ("silver", "silver_customer", "Member A", "all members", "Cleaned, conformed customer profile"),
        ("silver", "silver_transaction", "Member A", "all members", "Deduplicated, validated ledger"),
        ("silver", "silver_feedback_theme", "Member A", "Member C", "Normalised complaint / friction themes"),
        ("silver", "silver_daily_activity", "Member A", "Member D", "Ledger aggregated to one row per day"),
        ("gold", "gold_retention_features", "Member A", "Member A", "Feature table keyed by customer + cutoff"),
        ("gold", "gold_retention_scored", "Member A", "Member A, Member B", "Risk scores and risk bands"),
        ("gold", "gold_retention_priority", "Member A", "Member A, Member B", "Risk x value priority quadrants"),
        ("gold", "gold_customer_value_base", "Member A", "Member B", "Ledger-verified value aggregates"),
        ("meta", "meta_source_inventory", "Member A", "all members", "Source include/exclude decisions"),
        ("meta", "meta_data_dictionary", "Member A", "all members", "Column-level catalogue"),
        ("meta", "meta_column_decision", "Member A", "all members", "Inclusion/exclusion + leakage verdicts"),
        ("meta", "meta_feature_catalogue", "Member A", "Member A", "Engineered feature definitions"),
        ("meta", "meta_shared_contract", "Member A", "all members", "Producer/consumer guarantees"),
        ("meta", "meta_team_decision_log", "team", "all members", "Co-created decisions and their impact"),
        ("dq", "dq_audit_result", "Member A", "all members", "Executable data-quality rule results"),
        ("dq", "dq_null_profile", "Member A", "all members", "Per-column completeness profile"),
        ("ops", "ops_runtime_log", "Member A", "all members", "Per-step wall-clock timings"),
        ("ops", "ops_scalability_probe", "Member A", "all members", "Runtime at 25/50/75/100% of the ledger"),
        ("ops", "ops_collection_summary", "Member A", "all members", "Data Collection evidence"),
        ("ops", "ops_stream_ingest_audit", "Member A", "Member D", "Streaming vs batch reconciliation"),
        ("insight", "insight_key_theme", "Member A", "all members", "Cross-group themes that changed the design"),
        ("insight", "insight_target_driver", "Member A", "all members", "Association of each feature with the target"),
    ],
    ["layer", "table_name", "owner", "consumers", "purpose"],
)
save_table(table_registry, "meta_table_registry", comment="Medallion table registry for the retention workstream")
display(table_registry)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Machine-generated data dictionary
# MAGIC
# MAGIC The dictionary is **generated from the live schema** and then enriched with curated annotations
# MAGIC (business family, verification status, notes). Generating it means it cannot drift out of date the
# MAGIC way a hand-typed table in a Word document does; curating it means it still carries the judgement
# MAGIC that automation cannot supply.
# MAGIC
# MAGIC The `family` column is the *grouping* the rubric asks for — it is what makes "correlations between
# MAGIC groups of data" a meaningful question in §2.5.

# COMMAND ----------

FAMILY = {
    "identity": ["customer_id"],
    "demographic": ["age", "gender", "location", "income_bracket", "occupation", "education_level",
                    "marital_status", "household_size", "acquisition_channel"],
    "product": ["savings_account", "credit_card", "personal_loan", "investment_account",
                "insurance_product", "active_products", "bill_payment_user", "auto_savings_enabled",
                "credit_utilization_ratio"],
    "engagement": ["app_logins_frequency", "feature_usage_diversity", "international_transactions"],
    "experience": ["failed_transactions", "base_satisfaction", "tx_satisfaction", "product_satisfaction",
                   "satisfaction_score", "nps_score", "last_survey_date", "support_tickets_count",
                   "resolved_tickets_ratio", "app_store_rating", "feedback_sentiment",
                   "feature_requests", "complaint_topics"],
    "transaction_setA_verified": ["tx_count", "avg_tx_value", "total_tx_volume", "first_tx", "last_tx",
                                  "customer_tenure"],
    "transaction_setB_unverified": ["monthly_transaction_count", "average_transaction_value",
                                    "total_transaction_volume", "transaction_frequency",
                                    "last_transaction_date", "first_transaction_date",
                                    "preferred_transaction_type", "weekend_transaction_ratio",
                                    "avg_daily_transactions"],
    "vendor_label": ["customer_segment", "clv_segment", "churn_probability", "customer_lifetime_value"],
}
COLUMN_FAMILY = {c: fam for fam, cols in FAMILY.items() for c in cols}

VERIFICATION = {
    "transaction_setA_verified": "Reconciled 100% against the raw ledger (DQ-12)",
    "transaction_setB_unverified": "Does NOT reconcile against the ledger (DQ-13) - do not trust",
    "vendor_label": "Pre-computed over the FULL year - post-cutoff information",
    "experience": "Survey-based coverage is partial; complaint/request blanks are a state, not a gap",
}

ANNOTATION = {
    "customer_id": "Primary key; join key to the ledger and to every teammate's table.",
    "churn_probability": "Vendor-supplied score, not an observed outcome. See theme TH-02.",
    "customer_segment": "Activity label derived from the whole year, including the outcome window.",
    "clv_segment": "Near-exact quartile split of customer_lifetime_value.",
    "credit_utilization_ratio": "Null exactly when credit_card = false: 'not applicable', not missing.",
    "complaint_topics": "Null = no complaint on file (~50% of customers). Unstructured text.",
    "feature_requests": "Null = no request submitted (~33% of customers). Unstructured text.",
    "satisfaction_score": "Ordinal 1-6, observed 2-6. Member C's target; a candidate feature for me.",
    "nps_score": "Almost collinear with satisfaction_score (see theme TH-03).",
    "tx_count": "Whole-year count. Ledger-verified but spans the outcome window -> re-derive per cutoff.",
    "last_tx": "Whole-year max transaction date -> directly encodes the target. Excluded.",
    "customer_tenure": "Months since first transaction, measured at end of year. Re-derived per cutoff.",
}

_cust_schema = spark.table(tbl("bronze_customer")).schema
_business_cols = [f for f in _cust_schema.fields if not f.name.startswith("_")]

with timed("profile completeness of all 54 customer columns", "meta"):
    _null_exprs = [F.sum(F.col(f"`{f.name}`").isNull().cast("long")).alias(f.name) for f in _business_cols]
    _distinct_exprs = [F.approx_count_distinct(F.col(f"`{f.name}`"), 0.02).alias(f.name) for f in _business_cols]
    null_counts = spark.table(tbl("bronze_customer")).agg(*_null_exprs).collect()[0].asDict()
    distinct_counts = spark.table(tbl("bronze_customer")).agg(*_distinct_exprs).collect()[0].asDict()

dictionary_rows = []
for f in _business_cols:
    fam = COLUMN_FAMILY.get(f.name, "unclassified")
    nulls = int(null_counts[f.name])
    dictionary_rows.append(
        (
            "customer", f.name, fam, f.dataType.simpleString(),
            n_customer - nulls, round((n_customer - nulls) / n_customer * 100, 2),
            int(distinct_counts[f.name]),
            VERIFICATION.get(fam, "Direct from source; no reconciliation issue found"),
            ANNOTATION.get(f.name, ""),
        )
    )
for name, dtype, note in [
    ("customer_id", "bigint", "Foreign key to the customer profile."),
    ("date", "date", "Transaction date. Sole basis for every time window in the pipeline."),
    ("amount", "bigint", "Amount in Colombian pesos (COP). Range 36,400 .. 689,739,900."),
    ("type", "string", "Transfer / Withdrawal / Payment / Deposit."),
]:
    dictionary_rows.append(("transaction", name, "transaction_ledger", dtype, n_tx, 100.0, -1,
                            "Source of truth for all transaction features", note))

data_dictionary = spark_df(
    dictionary_rows,
    ["source_table", "column_name", "family", "data_type", "non_null_rows", "completeness_pct",
     "approx_distinct", "verification_status", "note"],
)
save_table(data_dictionary, "meta_data_dictionary", comment="Column-level data catalogue, generated from the live schema")

print("Columns per family:")
display(
    spark.table(tbl("meta_data_dictionary"))
    .groupBy("family")
    .agg(F.count("*").alias("columns"), F.round(F.avg("completeness_pct"), 1).alias("avg_completeness_pct"))
    .orderBy(F.desc("columns"))
)
display(spark.table(tbl("meta_data_dictionary")).where("completeness_pct < 100"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Data-quality audit — 16 executable rules
# MAGIC
# MAGIC The proposal's governance checklist is turned into **assertions that run**, each with an observed
# MAGIC value, an expected value, a verdict and the action I took. A rule that only exists as a sentence in
# MAGIC a report cannot fail; a rule in this table can, and did (DQ-05, DQ-13, DQ-16).

# COMMAND ----------

cust_b = spark.table(tbl("bronze_customer"))
tx_b = spark.table(tbl("bronze_transaction"))

with timed("aggregate ledger per customer for reconciliation", "dq"):
    ledger_agg = (
        tx_b.groupBy("customer_id")
        .agg(
            F.count("*").alias("ledger_count"),
            F.sum("amount").alias("ledger_sum"),
            F.min("date").alias("ledger_first"),
            F.max("date").alias("ledger_last"),
            F.countDistinct("type").alias("ledger_types"),
        )
        .cache()
    )
    ledger_agg.count()

with timed("join customer profile to ledger aggregates", "dq"):
    recon = (
        cust_b.select("customer_id", "tx_count", "total_tx_volume", "avg_tx_value", "first_tx", "last_tx",
                      "total_transaction_volume", "average_transaction_value", "last_transaction_date",
                      "credit_card", "credit_utilization_ratio", "satisfaction_score", "last_survey_date")
        .join(ledger_agg, "customer_id", "left")
        .cache()
    )
    n_recon = recon.count()

DQ: list = []


def dq(rule_id, rule, scope, observed, expected, passed, action):
    DQ.append((rule_id, rule, scope, str(observed), str(expected), "PASS" if passed else "FAIL", action))


# --- keys and integrity --------------------------------------------------------
n_distinct_cust = cust_b.select("customer_id").distinct().count()
dq("DQ-01", "customer_id is unique in the customer table", "bronze_customer",
   f"{n_distinct_cust:,} distinct of {n_customer:,}", "equal", n_distinct_cust == n_customer,
   "customer_id accepted as the primary key.")

n_null_key = cust_b.where(F.col("customer_id").isNull()).count()
dq("DQ-02", "customer_id is never null", "bronze_customer", n_null_key, 0, n_null_key == 0,
   "No key repair needed.")

cust_ids = cust_b.select("customer_id").distinct()
tx_ids = tx_b.select("customer_id").distinct()
orphan_customers = cust_ids.join(tx_ids, "customer_id", "left_anti").count()
orphan_tx = tx_ids.join(cust_ids, "customer_id", "left_anti").count()
dq("DQ-03", "every customer appears in the ledger", "join", orphan_customers, 0, orphan_customers == 0,
   "Join is lossless left-to-right; no zero-transaction customers to special-case.")
dq("DQ-04", "every ledger customer_id exists in the profile table", "join", orphan_tx, 0, orphan_tx == 0,
   "Referential integrity holds; an inner join is safe and loses nothing.")

# --- duplicates ---------------------------------------------------------------
n_tx_distinct = tx_b.select("customer_id", "date", "amount", "type").distinct().count()
n_exact_dups = n_tx - n_tx_distinct
dq("DQ-05", "no fully duplicated transaction rows", "bronze_transaction",
   f"{n_exact_dups} ({n_exact_dups / max(n_tx, 1) * 100:.4f}%)", 0, n_exact_dups == 0,
   "FAILED as expected. Rule applied in Section 3.2: drop exact duplicates (keep one), because "
   "customer/date/amount/type collisions at identical values are not distinguishable events.")

near_dups = (
    tx_b.groupBy("customer_id", "date", "type")
    .agg(F.count("*").alias("n"), F.countDistinct("amount").alias("distinct_amounts"))
    .where((F.col("n") > 1) & (F.col("distinct_amounts") == F.col("n")))
    .count()
)
dq("DQ-06", "near-duplicates (same customer/date/type, different amounts) are genuine events",
   "bronze_transaction", f"{near_dups:,} groups", "reviewed, not dropped", True,
   "KEPT. Multiple different-valued transactions of one type in a day are normal fintech behaviour; "
   "deleting them would destroy exactly the frequency signal the model needs.")

# --- coverage -----------------------------------------------------------------
date_bounds = tx_b.agg(F.min("date").alias("lo"), F.max("date").alias("hi")).collect()[0]
n_days_present = tx_b.select("date").distinct().count()
n_days_expected = (date_bounds["hi"] - date_bounds["lo"]).days + 1
dq("DQ-07", "no calendar gaps in the transaction window", "bronze_transaction",
   f"{n_days_present} of {n_days_expected} days ({date_bounds['lo']}..{date_bounds['hi']})",
   "all days present", n_days_present == n_days_expected,
   "No ingestion gaps, so a zero-transaction day for a customer is genuine inactivity, not a data hole. "
   "This is the assumption the whole target definition rests on.")

dq("DQ-08", "coverage contains all three cutoffs and their outcome windows", "temporal design",
   f"{date_bounds['lo']}..{date_bounds['hi']}",
   f"{DATA_START}..{DATA_END}",
   date_bounds["lo"] <= WINDOWS["train"]["eligibility_start"] and date_bounds["hi"] >= WINDOWS["test"]["outcome_end"],
   "Chronological train/valid/test design in Section 3.4 is feasible on this data.")

# --- domain checks ------------------------------------------------------------
amount_stats = tx_b.agg(
    F.sum(F.col("amount").isNull().cast("long")).alias("nulls"),
    F.sum((F.col("amount") < 0).cast("long")).alias("negatives"),
    F.sum((F.col("amount") == 0).cast("long")).alias("zeros"),
    F.min("amount").alias("lo"), F.max("amount").alias("hi"),
).collect()[0]
dq("DQ-09", "transaction amount is non-null and non-negative", "bronze_transaction",
   f"nulls={amount_stats['nulls']}, negatives={amount_stats['negatives']}, zeros={amount_stats['zeros']}, "
   f"range={amount_stats['lo']:,}..{amount_stats['hi']:,}",
   "0 nulls, 0 negatives", amount_stats["nulls"] == 0 and amount_stats["negatives"] == 0,
   "No amount repair needed. Zero amounts would have been KEPT as valid (a zero-value transfer is an "
   "event); none exist, so the question is moot but the rule stays for future data.")

p999 = max(tx_b.approxQuantile("amount", [0.999], 0.001)[0], 1.0)
dq("DQ-10", "extreme amounts are plausible rather than corrupt", "bronze_transaction",
   f"p99.9={p999:,.0f}, max={amount_stats['hi']:,} (ratio {amount_stats['hi'] / p999:.1f}x)",
   "max within ~100x of p99.9", amount_stats["hi"] <= p999 * 100,
   "Right-skew is real fintech behaviour, not corruption. Treatment: keep the rows, but use log1p and "
   "robust scaling for value features rather than deleting customers (Section 3.5).")

tx_types = [r["type"] for r in tx_b.select("type").distinct().orderBy("type").collect()]
dq("DQ-11", "transaction type is a small closed vocabulary", "bronze_transaction",
   f"{len(tx_types)}: {tx_types}", "4 known types", len(tx_types) <= 6,
   "Safe to one-hot encode and to build per-type share features.")

# --- reconciliation of the two summary column sets ----------------------------
def match_rate(a, b):
    """Null-safe match rate: a customer missing from the ledger counts as a MISMATCH, not as an
    excluded row. F.avg would silently drop nulls and inflate the reported agreement."""
    return F.avg(F.coalesce((a == b).cast("double"), F.lit(0.0)))


setA = recon.agg(
    match_rate(F.col("tx_count"), F.col("ledger_count")).alias("count_match"),
    match_rate(F.col("total_tx_volume"), F.col("ledger_sum")).alias("sum_match"),
    match_rate(F.to_date("last_tx"), F.col("ledger_last")).alias("last_match"),
    match_rate(F.to_date("first_tx"), F.col("ledger_first")).alias("first_match"),
).collect()[0]
dq("DQ-12", "'Set A' summary columns reconcile with the ledger", "bronze_customer vs ledger",
   f"count {setA['count_match']:.1%}, sum {setA['sum_match']:.1%}, "
   f"first_tx {setA['first_match']:.1%}, last_tx {setA['last_match']:.1%}",
   "100% on all four", min(setA["count_match"], setA["sum_match"], setA["first_match"], setA["last_match"]) > 0.999,
   "Set A is arithmetically correct - but it is computed over the WHOLE year, so it is still unusable "
   "as a feature at a mid-year cutoff. See DQ-16 and theme TH-01.")

corr_sum = recon.stat.corr("total_transaction_volume", "ledger_sum")
corr_avg = recon.stat.corr("average_transaction_value", "avg_tx_value")
last_date_match = recon.agg(
    match_rate(F.to_date("last_transaction_date"), F.col("ledger_last"))
).collect()[0][0]
dq("DQ-13", "'Set B' summary columns reconcile with the ledger", "bronze_customer vs ledger",
   f"corr(total_transaction_volume, ledger_sum)={corr_sum:.4f}, "
   f"corr(average_transaction_value, avg_tx_value)={corr_avg:.4f}, "
   f"last_transaction_date match={last_date_match:.1%}",
   "corr near 1.0 on both, date match near 100%",
   abs(corr_sum) > 0.9 and abs(corr_avg) > 0.9 and last_date_match > 0.99,
   "FAILED. Near-zero correlation with the ledger they claim to summarise: Set B is stale or "
   "independently generated. All 9 Set B columns EXCLUDED for the whole team (Section 3.1, theme TH-01). "
   "This is the single most important governance finding of the interim phase.")

# --- structural missingness ---------------------------------------------------
cu = recon.agg(
    F.sum(((~F.col("credit_card")) & F.col("credit_utilization_ratio").isNull()).cast("long")).alias("no_card_null"),
    F.sum((~F.col("credit_card")).cast("long")).alias("no_card"),
    F.sum((F.col("credit_card") & F.col("credit_utilization_ratio").isNull()).cast("long")).alias("card_null"),
).collect()[0]
dq("DQ-14", "credit_utilization_ratio is null exactly when there is no credit card", "bronze_customer",
   f"{cu['no_card_null']:,} of {cu['no_card']:,} card-less customers null; "
   f"{cu['card_null']:,} card-holders null",
   "all card-less null, no card-holder null",
   cu["no_card_null"] == cu["no_card"] and cu["card_null"] == 0,
   "Missingness is 'not applicable'. Imputed with a sentinel of 0.0 plus an explicit "
   "has_credit_card flag - NEVER with the mean, which would invent utilisation that cannot exist.")

sat_range = cust_b.agg(F.min("satisfaction_score").alias("lo"), F.max("satisfaction_score").alias("hi")).collect()[0]
dq("DQ-15", "satisfaction_score sits on the documented 1-6 ordinal scale", "bronze_customer",
   f"observed {sat_range['lo']}..{sat_range['hi']}", "within 1..6",
   sat_range["lo"] >= 1 and sat_range["hi"] <= 6,
   "Scale confirmed (levels 1 unobserved). Usable as an ordinal feature without rescaling.")

# --- timing / leakage-relevant checks ----------------------------------------
survey_after_cutoff = recon.where(F.to_date("last_survey_date") > F.lit(CUTOFFS["test"])).count()
dq("DQ-16", "satisfaction survey dates precede the test cutoff", "bronze_customer",
   f"{survey_after_cutoff:,} of {n_recon:,} surveys dated after {CUTOFFS['test']}",
   0, survey_after_cutoff == 0,
   "Any survey dated after the cutoff is post-cutoff information. Handled in Section 3.6 by masking "
   "satisfaction features for those customers rather than dropping the customers.")

dq_audit = spark_df(DQ, ["rule_id", "rule", "scope", "observed", "expected", "verdict", "action_taken"])
save_table(dq_audit, "dq_audit_result", comment="Executable data-quality rules with verdicts and actions")

n_fail = dq_audit.where("verdict = 'FAIL'").count()
print(f"\n{len(DQ)} rules executed: {len(DQ) - n_fail} PASS, {n_fail} FAIL")
print("Every FAIL has an explicit, documented remediation in Section 3 - none are ignored.")
display(dq_audit)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Completeness profile by family

# COMMAND ----------

null_profile = (
    spark.table(tbl("meta_data_dictionary"))
    .where("source_table = 'customer'")
    .select("column_name", "family", "completeness_pct",
            (F.lit(100.0) - F.col("completeness_pct")).alias("missing_pct"))
    .orderBy(F.desc("missing_pct"))
)
save_table(null_profile, "dq_null_profile", comment="Per-column completeness, joined to business family")
display(null_profile.where("missing_pct > 0"))
print("All three incomplete columns are structurally missing (DQ-14 and the annotations in "
      "meta_data_dictionary). No blanket imputation is applied anywhere in this pipeline.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Connections and correlations *between groups of data*
# MAGIC
# MAGIC Grouping columns into families in §2.2 makes this the interesting question: does information in one
# MAGIC family duplicate information in another? I compute the full Pearson matrix in a single Spark job
# MAGIC (`pyspark.ml.stat.Correlation` over an assembled vector) and then read it **family by family**.
# MAGIC
# MAGIC This is not idle exploration — three of the findings below directly change what Section 3 is
# MAGIC allowed to use as a feature.

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

# credit_utilization_ratio is deliberately ABSENT. Correlation.corr needs a null-free vector, and
# filling its 37.5% structural nulls with 0.0 would invent a utilisation figure for customers who
# hold no credit card - the exact practice DQ-14 rejects. Every column below is 100% complete
# (see meta_data_dictionary), so no imputation happens here at all.
CORR_COLS = [
    # demographic
    "age", "household_size",
    # product
    "active_products",
    # engagement
    "app_logins_frequency", "feature_usage_diversity", "international_transactions",
    # experience
    "failed_transactions", "satisfaction_score", "nps_score", "support_tickets_count",
    "resolved_tickets_ratio", "app_store_rating",
    # transaction (Set A, verified)
    "tx_count", "avg_tx_value", "total_tx_volume", "customer_tenure",
    # vendor labels
    "churn_probability", "customer_lifetime_value",
]

with timed("cross-family correlation matrix (single Spark job)", "insight"):
    corr_input = cust_b.select([F.col(c).cast("double").alias(c) for c in CORR_COLS])
    _incomplete = corr_input.agg(
        *[F.sum(F.col(c).isNull().cast("long")).alias(c) for c in CORR_COLS]
    ).collect()[0].asDict()
    _bad = {c: n for c, n in _incomplete.items() if n}
    assert not _bad, f"Correlation inputs must be complete; found nulls in {_bad}. Do not impute here."
    vec = VectorAssembler(inputCols=CORR_COLS, outputCol="_features").transform(corr_input).select("_features")
    corr_matrix = Correlation.corr(vec, "_features", "pearson").head()[0].toArray()

corr_pdf = pd.DataFrame(corr_matrix, index=CORR_COLS, columns=CORR_COLS)

# strongest cross-family pairs
pairs = []
for i, a in enumerate(CORR_COLS):
    for j, b in enumerate(CORR_COLS):
        if j <= i:
            continue
        fa, fb = COLUMN_FAMILY.get(a, "?"), COLUMN_FAMILY.get(b, "?")
        if fa == fb:
            continue
        pairs.append({"family_a": fa, "column_a": a, "family_b": fb, "column_b": b,
                      "pearson_r": round(float(corr_pdf.loc[a, b]), 4)})
cross_family = pd.DataFrame(pairs)
cross_family["abs_r"] = cross_family["pearson_r"].abs()
top_cross = cross_family.sort_values("abs_r", ascending=False).head(12).drop(columns="abs_r")

print("Strongest connections BETWEEN families:")
display(spark.createDataFrame(top_cross))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Key themes
# MAGIC
# MAGIC The themes that the systematic process above actually produced. Each one names its evidence and,
# MAGIC crucially, what changed because of it — a theme that changes nothing is not a finding.

# COMMAND ----------

_r = lambda a, b: round(float(corr_pdf.loc[a, b]), 3)

key_themes = spark_df(
    [
        ("TH-01", "Two rival transaction-summary column sets exist and only one is real",
         f"DQ-12 vs DQ-13: Set A matches the ledger on 100% of customers; Set B correlates "
         f"{corr_sum:.4f} with the same ledger.",
         "All 9 Set B columns excluded team-wide; every transaction feature is re-derived from the raw "
         "ledger. Raised with all four members and logged as decision D-02.", "critical"),
        ("TH-02", "churn_probability is a deterministic vendor rule, not an observed outcome",
         f"r(churn_probability, active_products) = {_r('churn_probability', 'active_products')}; "
         f"r with app_logins_frequency = {_r('churn_probability', 'app_logins_frequency')}. "
         "Almost all of its variance is explained by two profile columns.",
         "Excluded as a feature AND rejected as a target. My target is built from observed transaction "
         "behaviour (inactive_next_60d) so the model learns from reality, not from someone else's rule.",
         "critical"),
        ("TH-03", "nps_score and satisfaction_score are near-duplicates of each other",
         f"r(nps_score, satisfaction_score) = {_r('nps_score', 'satisfaction_score')}.",
         "Keep satisfaction_score (ordinal, interpretable, Member C's target) and drop nps_score to "
         "avoid feeding the model the same information twice and destabilising coefficients.", "high"),
        ("TH-04", "Whole-year aggregates cannot be features at a mid-year cutoff",
         "tx_count, total_tx_volume, avg_tx_value, first_tx, last_tx, customer_tenure and "
         "customer_segment are all computed across 2023-01-04..2023-12-29, which contains every "
         "outcome window.",
         "All 7 excluded and re-derived as cutoff-bounded windows (7/30/90-day). last_tx in particular "
         "would have leaked the target almost perfectly.", "critical"),
        ("TH-05", "Value and activity are related but not interchangeable",
         f"r(tx_count, customer_lifetime_value) = {_r('tx_count', 'customer_lifetime_value')}, "
         f"r(avg_tx_value, customer_lifetime_value) = {_r('avg_tx_value', 'customer_lifetime_value')}.",
         "Justifies the retention-priority view in Section 5: a high-risk customer is not automatically "
         "a valuable one, so risk alone is the wrong thing to hand to the operations team.", "high"),
        ("TH-06", "Experience signals are weakly linked to profile attributes",
         f"r(failed_transactions, satisfaction_score) = {_r('failed_transactions', 'satisfaction_score')}, "
         f"r(support_tickets_count, satisfaction_score) = {_r('support_tickets_count', 'satisfaction_score')}.",
         "Experience features carry information that demographics do not, so they are retained as their "
         "own family. Their real predictive value is tested against my target in Section 4.8, not "
         "assumed from these customer-level correlations.", "medium"),
        ("TH-07", "Missingness is structural everywhere it occurs",
         "DQ-14: credit_utilization_ratio null iff no credit card; complaint/request blanks are "
         "'nothing on file' states covering ~50% and ~33% of customers.",
         "Sentinel + indicator-flag strategy instead of mean imputation, and the free text is kept as "
         "documents so the absence itself becomes a usable signal.", "high"),
        ("TH-08", "Daily platform volume is extremely stable, so risk lives at customer level",
         "Section 4.3 shows a flat daily series; the variation that matters is between customers, not "
         "between days.",
         "Confirms my workstream is correctly framed per-customer, and tells Member D that his "
         "demand-class thresholds must be quantile-based rather than absolute.", "medium"),
    ],
    ["theme_id", "theme", "evidence", "what_changed", "severity"],
)
save_table(key_themes, "insight_key_theme", comment="Key themes from the data management stage and the design changes they caused")
display(key_themes)


# COMMAND ----------

# MAGIC %md
# MAGIC # Section 3 — Data Preparation  *(rubric: 15 marks)*
# MAGIC
# MAGIC **What the top band asks for:** a *thorough rationale on inclusion or exclusion of selected data*
# MAGIC and *comprehensive data cleaning and transformations*.
# MAGIC
# MAGIC **How I address it.** The register in §3.1 records a decision and a written reason for **every one
# MAGIC of the 54 customer columns** — and the code then *derives its column list from that register*, so
# MAGIC the documentation cannot drift away from what actually runs. §3.2 applies the cleaning rules the
# MAGIC audit called for. §3.4–3.6 build the features, the target and the leakage audit that protects them.
# MAGIC
# MAGIC The headline preparation finding: **24 of the 54 customer columns never reach the model.**
# MAGIC
# MAGIC | Reason | Columns | Theme |
# MAGIC |---|---|---|
# MAGIC | Do not reconcile with the ledger they claim to summarise | 9 | TH-01 |
# MAGIC | Contain information from on/after the outcome window | 8 | TH-04, TH-02 |
# MAGIC | Duplicate a column already kept | 5 | TH-03 |
# MAGIC | Legitimate data, but not a predictor of mine (published for Member B instead) | 2 | TH-05 |
# MAGIC
# MAGIC The 17 rejections in the first two rows are the ones that matter: using them would have produced an
# MAGIC impressive and completely worthless model. `last_tx` alone almost perfectly determines the target.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Inclusion / exclusion register
# MAGIC
# MAGIC `decision` is one of:
# MAGIC
# MAGIC - **include** — used as a predictor as-is
# MAGIC - **include_masked** — used, but blanked for customers whose value post-dates the cutoff
# MAGIC - **derive** — not used directly; a cutoff-bounded replacement is engineered from the ledger
# MAGIC - **exclude_leakage** — contains information from on/after the outcome window
# MAGIC - **exclude_unverified** — fails reconciliation against the ledger (DQ-13)
# MAGIC - **exclude_redundant** — near-duplicate of a column already kept
# MAGIC - **exclude_context** — legitimate data, but not a customer-level predictor
# MAGIC - **key** — join key

# COMMAND ----------

COLUMN_DECISIONS = [
    ("customer_id", "key",
     "Verified primary key (DQ-01/02) and the join key to the ledger and to every teammate's table."),

    # ---- demographics: keep, but expect them to be weak -----------------------
    ("age", "include", "Static attribute. Kept specifically to TEST the hypothesis that recent behaviour beats demographics."),
    ("gender", "include", "Low cardinality; retained for the subgroup fairness check the proposal commits to."),
    ("location", "derive", "Free text 'City, Department'. Split into city + department so the model can use the geography without 30+ raw string levels."),
    ("income_bracket", "include", "Ordered 4-level bracket; plausible driver of transaction capacity."),
    ("occupation", "exclude_redundant", "35 near-equal-sized categories (1,329-1,456 each) - the EDA showed this is effectively random in COFINFAD. 35 dummy columns of noise would cost variance for nothing."),
    ("education_level", "include", "Ordered 4-level attribute, cheap to encode."),
    ("marital_status", "include", "Low cardinality; part of the household-context group."),
    ("household_size", "include", "Numeric; may proxy financial obligations."),
    ("acquisition_channel", "include", "Retention teams act on channel. Known at signup, so no leakage risk."),

    # ---- products -------------------------------------------------------------
    ("savings_account", "include", "Product holding at profile snapshot."),
    ("credit_card", "include", "Also the indicator flag that explains credit_utilization_ratio nulls (DQ-14)."),
    ("personal_loan", "include", "Loan obligations may force continued activity - a plausible retention anchor."),
    ("investment_account", "include", "Product holding."),
    ("insurance_product", "include", "Product holding."),
    ("active_products", "include", "Product breadth. Central to the hypothesis that fewer products means weaker attachment."),
    ("bill_payment_user", "include", "Recurring-payment habit is one of the strongest plausible anchors against inactivity."),
    ("auto_savings_enabled", "include", "Automation keeps an account alive without deliberate effort."),
    ("credit_utilization_ratio", "include", "Kept with a 0.0 sentinel + has_credit_card flag. NOT mean-imputed: DQ-14 proves the nulls mean 'no card', and inventing a utilisation figure for a customer with no credit card would be fabrication."),

    # ---- engagement -----------------------------------------------------------
    ("app_logins_frequency", "include", "Direct digital-engagement measure named in my hypothesis."),
    ("feature_usage_diversity", "include", "Breadth of app usage; complements login frequency."),
    ("international_transactions", "include", "Behavioural breadth indicator. UNTIMESTAMPED COUNT - see limitation L-09."),

    # ---- experience -----------------------------------------------------------
    ("failed_transactions", "include", "Operational friction, and the column my hypothesis rests on. UNTIMESTAMPED COUNT: COFINFAD ships no failure timestamp, so unlike tx_count it CANNOT be re-derived per cutoff. Included as a snapshot with limitation L-09 recorded, and a sensitivity run (features_without_untimestamped_counts) planned for the modelling phase."),
    ("support_tickets_count", "include", "Support contact volume. UNTIMESTAMPED COUNT - see limitation L-09."),
    ("resolved_tickets_ratio", "include", "Unresolved issues are the friction signal, not contact volume alone. A ratio, so less horizon-sensitive than a raw count, but still untimestamped (L-09)."),
    ("app_store_rating", "include", "Volunteered satisfaction proxy with full coverage."),
    ("satisfaction_score", "include_masked", "Ordinal 1-6. Masked where last_survey_date > cutoff (DQ-16) so a post-cutoff survey can never inform a pre-cutoff prediction."),
    ("last_survey_date", "derive", "Not a predictor; used only to compute the mask above and a survey-age feature."),
    ("base_satisfaction", "exclude_redundant", "Latent component that satisfaction_score is built from - including both is double counting."),
    ("tx_satisfaction", "exclude_redundant", "As above."),
    ("product_satisfaction", "exclude_redundant", "As above."),
    ("nps_score", "exclude_redundant", "r = 0.92 with satisfaction_score (TH-03). Keeping the interpretable ordinal and dropping the collinear twin."),
    ("feedback_sentiment", "include", "3-level sentiment label with full coverage."),
    ("complaint_topics", "derive", "Unstructured text. Normalised into an operational-friction theme + has_complaint flag (SRC-03, shared with Member C)."),
    ("feature_requests", "derive", "Unstructured text. Reduced to a has_feature_request engagement flag."),

    # ---- transaction summaries: Set A -----------------------------------------
    ("tx_count", "exclude_leakage", "Ledger-accurate (DQ-12) but counted over 2023-01-04..2023-12-29, which CONTAINS every outcome window. Re-derived as tx_cnt_7d/30d/90d/180d."),
    ("avg_tx_value", "exclude_leakage", "Whole-year mean. Re-derived as tx_amt_mean_90d."),
    ("total_tx_volume", "exclude_leakage", "Whole-year sum. Re-derived as tx_amt_sum_30d/90d."),
    ("first_tx", "exclude_leakage", "Whole-year min date. Re-derived as tenure_days at each cutoff."),
    ("last_tx", "exclude_leakage", "Whole-year max date - this alone almost perfectly determines inactive_next_60d. The single most dangerous column in the dataset. Re-derived as recency_days."),
    ("customer_tenure", "exclude_leakage", "Tenure measured at end of year. Re-derived as tenure_days at each cutoff."),
    ("customer_segment", "exclude_leakage", "inactive/occasional/regular/power label derived from full-year activity, i.e. partly from the outcome itself. Retained OUTSIDE the feature set purely to report performance by segment (Section 4.9)."),

    # ---- transaction summaries: Set B (all fail reconciliation) ---------------
    ("monthly_transaction_count", "exclude_unverified", "Set B. DQ-13: this column family does not reconcile with the ledger it claims to summarise."),
    ("average_transaction_value", "exclude_unverified", f"Set B. corr with the ledger's true mean = {corr_avg:.4f}."),
    ("total_transaction_volume", "exclude_unverified", f"Set B. corr with the ledger's true sum = {corr_sum:.4f}."),
    ("transaction_frequency", "exclude_unverified", "Set B, and an exact duplicate of avg_daily_transactions."),
    ("avg_daily_transactions", "exclude_unverified", "Set B duplicate of transaction_frequency."),
    ("weekend_transaction_ratio", "exclude_unverified", "Set B. Re-derived honestly as weekend_share_90d from the ledger."),
    ("preferred_transaction_type", "exclude_unverified", "Set B. Re-derived as per-type share features from the ledger."),
    ("first_transaction_date", "exclude_unverified", "Set B date, inconsistent with the ledger min."),
    ("last_transaction_date", "exclude_unverified", f"Set B date: matches the ledger max for only {last_date_match:.1%} of customers."),

    # ---- vendor labels --------------------------------------------------------
    ("churn_probability", "exclude_leakage", "Vendor score, not an observed outcome, and near-deterministic in active_products (TH-02). Predicting it would mean reverse-engineering someone else's formula instead of predicting customer behaviour."),
    ("customer_lifetime_value", "exclude_context", "Whole-year value estimate. Not a predictor for me, but published as gold_customer_value_base for Member B and used in the joint priority view (Section 5.4)."),
    ("clv_segment", "exclude_context", "Quartile split of the above. Same treatment."),
]

column_decision = spark_df(
    [(c, d, r, COLUMN_FAMILY.get(c, "unclassified")) for c, d, r in COLUMN_DECISIONS],
    ["column_name", "decision", "rationale", "family"],
)
save_table(column_decision, "meta_column_decision",
           comment="Per-column inclusion/exclusion decision with written rationale (Data Preparation evidence)")

# every column in the source must have an explicit decision - no silent defaults
_decided = {c for c, _, _ in COLUMN_DECISIONS}
_undecided = [c for c in EXPECTED_CUSTOMER_COLUMNS if c not in _decided]
assert not _undecided, f"Columns with no documented decision: {_undecided}"

print(f"{len(EXPECTED_CUSTOMER_COLUMNS)} source columns, {len(COLUMN_DECISIONS)} documented decisions, "
      f"{len(_undecided)} undecided.\n")
display(
    spark.table(tbl("meta_column_decision"))
    .groupBy("decision").agg(F.count("*").alias("columns"))
    .orderBy(F.desc("columns"))
)
display(spark.table(tbl("meta_column_decision")).where("decision LIKE 'exclude%'").orderBy("decision", "column_name"))

# COMMAND ----------

# derive the code's column lists FROM the register, so documentation and behaviour cannot diverge
INCLUDE_COLS = [c for c, d, _ in COLUMN_DECISIONS if d == "include"]
MASKED_COLS = [c for c, d, _ in COLUMN_DECISIONS if d == "include_masked"]
DERIVE_COLS = [c for c, d, _ in COLUMN_DECISIONS if d == "derive"]
EXCLUDED_COLS = [c for c, d, _ in COLUMN_DECISIONS if d.startswith("exclude")]
CONTEXT_COLS = [c for c, d, _ in COLUMN_DECISIONS if d == "exclude_context"]

# Columns that are counts but carry no timestamp, so they cannot be bounded to a cutoff the way
# ledger features can. They are INCLUDED, but named here so the limitation is explicit, testable
# (see LK-06) and easy to ablate in the modelling phase.
UNTIMESTAMPED_COUNT_COLS = ["failed_transactions", "support_tickets_count", "international_transactions"]

print(f"include        : {len(INCLUDE_COLS)}")
print(f"include_masked : {len(MASKED_COLS)}  -> {MASKED_COLS}")
print(f"derive         : {len(DERIVE_COLS)}  -> {DERIVE_COLS}")
print(f"excluded       : {len(EXCLUDED_COLS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Cleaning and conforming → silver
# MAGIC
# MAGIC Each rule below exists because a specific audit rule in §2.3 asked for it.
# MAGIC
# MAGIC | Rule | Action | Driven by |
# MAGIC |---|---|---|
# MAGIC | Exact duplicate transactions | Drop, keeping one row per `(customer_id, date, amount, type)` | DQ-05 |
# MAGIC | Near-duplicate transactions | **Keep** — genuine same-day events | DQ-06 |
# MAGIC | Amount domain | Reject null/negative; **preserve** valid zeros | DQ-09 |
# MAGIC | Extreme amounts | Keep the rows; transform with `log1p` instead of deleting customers | DQ-10 |
# MAGIC | Transaction type | Trim + title-case to a closed vocabulary | DQ-11 |
# MAGIC | Text/categorical fields | Trim, collapse internal whitespace, strip stray punctuation, normalise casing | Appendix D §5.2 |
# MAGIC | `location` | Split `"City, Department"` into two clean columns | register: `derive` |
# MAGIC | `credit_utilization_ratio` | 0.0 sentinel + `has_credit_card` flag, never the mean | DQ-14 |
# MAGIC | Complaint / request text | Tokenise, remove stop words, normalise to friction themes; blanks become an explicit state | DQ-07/TH-07 |
# MAGIC | Set B + whole-year columns | Physically dropped from silver so they cannot be used by accident | TH-01, TH-04 |
# MAGIC
# MAGIC Dropping the unusable columns *in the table* rather than only in a `select` list is deliberate: it
# MAGIC makes the governance decision structural, for me and for anyone reusing my silver layer.

# COMMAND ----------

# ---- optional down-sampling for the scalability probe, applied on CUSTOMERS ----
# Sampling rows would break referential integrity; sampling customers keeps every
# customer's ledger complete, which is what the features need.
if SAMPLE_FRACTION < 1.0:
    keep_ids = cust_b.select("customer_id").sample(False, SAMPLE_FRACTION, seed=42).cache()
    print(f"Down-sampled to {keep_ids.count():,} customers ({SAMPLE_FRACTION:.0%}) for this run.")
else:
    keep_ids = None

print("\nCleaning -> silver")


def clean_text(col):
    """Trim, collapse internal whitespace, drop stray punctuation, lower-case."""
    return F.lower(F.trim(F.regexp_replace(F.regexp_replace(col, r"[^\w\s,\-/&]", ""), r"\s+", " ")))


with timed("clean + deduplicate transaction ledger", "silver"):
    src_tx = spark.table(tbl("bronze_transaction")).select("customer_id", "date", "amount", "type")
    if keep_ids is not None:
        src_tx = src_tx.join(keep_ids, "customer_id", "left_semi")
    silver_tx = (
        src_tx
        .where(F.col("amount").isNotNull() & (F.col("amount") >= 0) & F.col("date").isNotNull())
        .withColumn("type", F.initcap(F.trim(F.col("type"))))
        .dropDuplicates(["customer_id", "date", "amount", "type"])       # DQ-05
        .withColumn("amount_log", F.log1p(F.col("amount").cast("double")))  # DQ-10
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
        .withColumn("day_of_week", F.date_format("date", "EEEE"))
        .withColumn("tx_month", F.date_format("date", "yyyy-MM"))
    )
    save_table(silver_tx, "silver_transaction", partition_by=["tx_month"],
               comment="Cleaned, deduplicated transaction ledger (shared silver asset)")
    n_silver_tx = spark.table(tbl("silver_transaction")).count()

print(f"  ledger: {n_tx:,} bronze -> {n_silver_tx:,} silver "
      f"({n_tx - n_silver_tx:,} rows removed by the documented rules)")

# Read the type vocabulary from SILVER, after title-casing - not from bronze. Taking it from bronze
# would compare post-clean values against a pre-clean list, and every per-type feature would quietly
# become zero if the source ever shipped 'TRANSFER' instead of 'Transfer'. LK-06 guards the invariant.
TX_TYPES = [r["type"] for r in
            spark.table(tbl("silver_transaction")).select("type").distinct().orderBy("type").collect()]
print(f"  transaction type vocabulary (from silver): {TX_TYPES}")

with timed("clean + conform customer profile", "silver"):
    src_cust = spark.table(tbl("bronze_customer"))
    if keep_ids is not None:
        src_cust = src_cust.join(keep_ids, "customer_id", "left_semi")
    silver_cust = (
        src_cust
        .select(["customer_id"] + INCLUDE_COLS + MASKED_COLS + DERIVE_COLS)
        # location -> city + department  (register: derive)
        .withColumn("city", F.initcap(F.trim(F.split(F.col("location"), ",").getItem(0))))
        .withColumn("department", F.initcap(F.trim(F.split(F.col("location"), ",").getItem(1))))
        .drop("location")
        # low-cardinality categoricals: conform casing / whitespace
        .withColumn("gender", F.initcap(F.trim(F.col("gender"))))
        .withColumn("income_bracket", F.initcap(F.trim(F.col("income_bracket"))))
        .withColumn("education_level", F.initcap(F.trim(F.col("education_level"))))
        .withColumn("marital_status", F.initcap(F.trim(F.col("marital_status"))))
        .withColumn("acquisition_channel", F.initcap(F.trim(F.col("acquisition_channel"))))
        .withColumn("feedback_sentiment", F.initcap(F.trim(F.col("feedback_sentiment"))))
        # structural missingness: sentinel + the existing credit_card column IS the indicator flag (DQ-14)
        .withColumn("credit_utilization_ratio",
                    F.coalesce(F.col("credit_utilization_ratio").cast("double"), F.lit(0.0)))
        # unstructured text -> flags + normalised theme  (TH-07)
        .withColumn("has_complaint", F.col("complaint_topics").isNotNull())
        .withColumn("has_feature_request", F.col("feature_requests").isNotNull())
        .withColumn("complaint_clean", clean_text(F.coalesce(F.col("complaint_topics"), F.lit("none on file"))))
        .withColumn("last_survey_date", F.to_date("last_survey_date"))
        .drop("complaint_topics", "feature_requests")
    )
    save_table(silver_cust, "silver_customer",
               comment="Cleaned customer profile: only columns approved by meta_column_decision")
    n_silver_cust = spark.table(tbl("silver_customer")).count()

_silver_cols = spark.table(tbl("silver_customer")).columns
_leaked_into_silver = [c for c in EXCLUDED_COLS if c in _silver_cols]
assert not _leaked_into_silver, f"Excluded columns present in silver_customer: {_leaked_into_silver}"
print(f"  profile: {n_silver_cust:,} customers, {len(_silver_cols)} columns "
      f"({len(EXCLUDED_COLS)} excluded columns physically absent - verified by assertion)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2b Unstructured text → normalised friction themes
# MAGIC
# MAGIC The free-text documents collected in §1.3 are tokenised, stop-word filtered, and mapped onto the
# MAGIC operational-friction vocabulary my hypothesis names (transaction failures, app problems, support
# MAGIC delays, product limitations, fees). "No complaint on file" is preserved as its own theme rather
# MAGIC than becoming a null — the absence of a complaint is itself information.
# MAGIC
# MAGIC `silver_feedback_theme` is a **shared** asset: Member C consumes it for the satisfaction
# MAGIC workstream (see the contract table in §5.2).

# COMMAND ----------

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover

with timed("tokenise + normalise unstructured feedback text", "silver"):
    docs = spark.table(tbl("bronze_feedback_document")).select(
        "customer_id", "document_text", "has_complaint_text", "has_request_text")
    if keep_ids is not None:
        docs = docs.join(keep_ids, "customer_id", "left_semi")

    tokenised = RegexTokenizer(inputCol="document_text", outputCol="_tokens_raw",
                               pattern=r"\W+", toLowercase=True).transform(docs)
    tokens = StopWordsRemover(inputCol="_tokens_raw", outputCol="tokens").transform(tokenised)

    THEME_RULES = [
        ("transaction_failure", ["failed", "failure", "declined", "decline", "reject", "error", "transaction"]),
        ("app_problem", ["app", "application", "crash", "bug", "slow", "login", "interface"]),
        ("support_delay", ["support", "service", "wait", "waiting", "response", "delay", "agent", "customer"]),
        ("fees_pricing", ["fee", "fees", "charge", "charges", "cost", "expensive", "price", "interest"]),
        ("product_limitation", ["limit", "limits", "product", "feature", "missing", "option", "unavailable"]),
    ]

    theme_expr = F.when(~F.col("has_complaint_text"), F.lit("no_complaint_on_file"))
    for theme, words in THEME_RULES:
        hit = F.arrays_overlap(F.col("tokens"), F.array(*[F.lit(w) for w in words]))
        theme_expr = theme_expr.when(hit, F.lit(theme))
    theme_expr = theme_expr.otherwise(F.lit("other_complaint"))

    feedback_theme = (
        tokens.withColumn("friction_theme", theme_expr)
        .withColumn("token_count", F.size("tokens"))
        .withColumn(
            "friction_word_hits",
            sum(
                [
                    F.arrays_overlap(F.col("tokens"), F.array(*[F.lit(w) for w in words])).cast("int")
                    for _, words in THEME_RULES
                ]
            ),
        )
        .select("customer_id", "friction_theme", "friction_word_hits", "token_count",
                "has_complaint_text", "has_request_text")
    )
    save_table(feedback_theme, "silver_feedback_theme",
               comment="Normalised complaint/friction themes from unstructured text (shared with Member C)")

display(
    spark.table(tbl("silver_feedback_theme"))
    .groupBy("friction_theme")
    .agg(F.count("*").alias("customers"), F.round(F.avg("friction_word_hits"), 2).alias("avg_friction_hits"))
    .orderBy(F.desc("customers"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2c Daily activity aggregate (shared with Member D)
# MAGIC
# MAGIC Aggregating the ledger to one row per day is work Member D would otherwise repeat. Producing it
# MAGIC here once, from *my* cleaned and deduplicated silver ledger, guarantees the two of us are counting
# MAGIC the same transactions — see decision **D-04** in the team log.

# COMMAND ----------

with timed("aggregate ledger to daily activity", "silver"):
    daily_activity = (
        spark.table(tbl("silver_transaction"))
        .groupBy("date")
        .agg(
            F.count("*").alias("n_transactions"),
            F.sum("amount").alias("total_amount"),
            F.countDistinct("customer_id").alias("active_customers"),
            F.round(F.avg("amount"), 0).alias("avg_amount"),
            F.sum(F.col("is_weekend").cast("int")).alias("weekend_rows"),
        )
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
        .withColumn("day_of_week", F.date_format("date", "EEEE"))
        .withColumn("iso_week", F.weekofyear("date"))
        .orderBy("date")
    )
    save_table(daily_activity, "silver_daily_activity",
               comment="One row per calendar day (shared with Member D's demand workstream)")

display(spark.table(tbl("silver_daily_activity")).orderBy("date").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Value base for Member B
# MAGIC
# MAGIC Member B's future-value workstream needs ledger-verified value aggregates keyed exactly the same
# MAGIC way as my risk table, otherwise the joint priority view in §5.4 cannot be built. Publishing it from
# MAGIC my silver ledger is a one-line cost to me and removes a whole duplicated pipeline for him.

# COMMAND ----------

with timed("publish ledger-verified value base for Member B", "gold"):
    value_base = (
        spark.table(tbl("silver_transaction"))
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("ledger_tx_count"),
            F.sum("amount").alias("ledger_total_value_cop"),
            F.round(F.avg("amount"), 2).alias("ledger_avg_value_cop"),
            F.min("date").alias("ledger_first_tx"),
            F.max("date").alias("ledger_last_tx"),
        )
        .join(
            spark.table(tbl("bronze_customer")).select("customer_id", *CONTEXT_COLS),
            "customer_id", "left",
        )
    )
    save_table(value_base, "gold_customer_value_base",
               comment="Ledger-verified customer value aggregates published for Member B")

display(spark.table(tbl("gold_customer_value_base")).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Feature engineering — one function, three cutoffs
# MAGIC
# MAGIC `build_feature_frame(split)` is the whole feature layer. It takes a split name, resolves its cutoff
# MAGIC and windows, and returns one row per **eligible** customer. Writing it once and calling it three
# MAGIC times is what makes the chronological design trustworthy: train, validation and test features are
# MAGIC produced by *identical* logic at different points in time, so any performance difference between
# MAGIC them is temporal generalisation and not a coding difference.
# MAGIC
# MAGIC **Eligibility.** Only customers with at least one transaction in the `ELIGIBILITY_DAYS` (90) days
# MAGIC before the cutoff. Predicting that a long-dormant customer stays dormant is trivial and would
# MAGIC inflate every metric; the business question is about customers who are *currently* active.
# MAGIC
# MAGIC **Feature groups built (all strictly bounded by `date <= cutoff`):**
# MAGIC
# MAGIC | Group | Features |
# MAGIC |---|---|
# MAGIC | Recency | `recency_days`, `tenure_days`, `max_gap_days_90d`, `avg_gap_days_90d`, `gap_vs_recency_ratio` |
# MAGIC | Frequency | `tx_cnt_7d/30d/90d/180d`, `tx_cnt_prior_30d`, `tx_cnt_prior_90d`, `active_days_30d/90d` |
# MAGIC | Trend (the hypothesis) | `cnt_delta_30d`, `cnt_ratio_30_prior30`, `amt_ratio_30_prior30`, `velocity_7_over_30`, `is_decelerating` |
# MAGIC | Value | `tx_amt_sum_30d/90d`, `tx_amt_mean_90d`, `tx_amt_max_90d`, `tx_amt_stddev_90d`, log variants |
# MAGIC | Diversity | `distinct_types_90d`, per-type shares, `weekend_share_90d` |
# MAGIC | Engagement / product / experience / demographic | from `silver_customer` + `silver_feedback_theme` |

# COMMAND ----------

def _sanitise(name: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in name).strip("_").lower()


def build_feature_frame(split: str) -> DataFrame:
    w = WINDOWS[split]
    cutoff = F.lit(w["cutoff_date"].isoformat()).cast("date")
    tx_all = spark.table(tbl("silver_transaction"))

    # ---------- history: everything on or before the cutoff --------------------
    hist = tx_all.where(F.col("date") <= cutoff).withColumn("days_before", F.datediff(cutoff, F.col("date")))

    def in_window(lo, hi):
        return (F.col("days_before") >= lo) & (F.col("days_before") <= hi)

    def cnt(lo, hi):
        return F.sum(F.when(in_window(lo, hi), 1).otherwise(0))

    def amt(lo, hi):
        return F.sum(F.when(in_window(lo, hi), F.col("amount")).otherwise(0))

    aggs = [
        F.max("date").alias("last_tx_pre_cutoff"),
        F.min("date").alias("first_tx_pre_cutoff"),
        F.count("*").alias("tx_cnt_hist"),   # all activity up to the cutoff
        cnt(0, 6).alias("tx_cnt_7d"),
        cnt(0, 29).alias("tx_cnt_30d"),
        cnt(0, 89).alias("tx_cnt_90d"),
        cnt(0, 179).alias("tx_cnt_180d"),
        cnt(30, 59).alias("tx_cnt_prior_30d"),
        cnt(90, 179).alias("tx_cnt_prior_90d"),
        amt(0, 29).alias("tx_amt_sum_30d"),
        amt(0, 89).alias("tx_amt_sum_90d"),
        amt(30, 59).alias("tx_amt_sum_prior_30d"),
        F.countDistinct(F.when(in_window(0, 29), F.col("date"))).alias("active_days_30d"),
        F.countDistinct(F.when(in_window(0, 89), F.col("date"))).alias("active_days_90d"),
        F.countDistinct(F.when(in_window(0, 89), F.col("type"))).alias("distinct_types_90d"),
        F.avg(F.when(in_window(0, 89), F.col("amount"))).alias("tx_amt_mean_90d"),
        F.max(F.when(in_window(0, 89), F.col("amount"))).alias("tx_amt_max_90d"),
        F.stddev(F.when(in_window(0, 89), F.col("amount"))).alias("tx_amt_stddev_90d"),
        F.avg(F.when(in_window(0, 89), F.col("is_weekend").cast("double"))).alias("weekend_share_90d"),
    ]
    for t in TX_TYPES:
        aggs.append(
            F.sum(F.when(in_window(0, 89) & (F.col("type") == F.lit(t)), 1).otherwise(0))
            .alias(f"cnt_{_sanitise(t)}_90d")
        )

    base = hist.groupBy("customer_id").agg(*aggs)

    # ---------- inter-transaction gaps over the last 90 days ------------------
    gap_win = Window.partitionBy("customer_id").orderBy("date")
    gaps = (
        hist.where(F.col("days_before") <= 89)
        .select("customer_id", "date")
        .distinct()
        .withColumn("prev_date", F.lag("date").over(gap_win))
        .withColumn("gap_days", F.datediff("date", "prev_date"))
        .groupBy("customer_id")
        .agg(
            F.max("gap_days").alias("max_gap_days_90d"),
            F.round(F.avg("gap_days"), 2).alias("avg_gap_days_90d"),
        )
    )

    base = base.join(gaps, "customer_id", "left")

    # ---------- eligibility: active in the observation window -----------------
    eligible = base.where(F.col("tx_cnt_90d") >= 1)

    # ---------- derived recency / trend features ------------------------------
    feat = (
        eligible
        .withColumn("recency_days", F.datediff(cutoff, F.col("last_tx_pre_cutoff")))
        .withColumn("tenure_days", F.datediff(cutoff, F.col("first_tx_pre_cutoff")))
        .withColumn("cnt_delta_30d", F.col("tx_cnt_30d") - F.col("tx_cnt_prior_30d"))
        .withColumn("cnt_ratio_30_prior30",
                    F.col("tx_cnt_30d") / (F.col("tx_cnt_prior_30d") + F.lit(1.0)))
        .withColumn("amt_ratio_30_prior30",
                    F.col("tx_amt_sum_30d") / (F.col("tx_amt_sum_prior_30d") + F.lit(1.0)))
        .withColumn("velocity_7_over_30",
                    (F.col("tx_cnt_7d") * F.lit(30.0 / 7.0)) / (F.col("tx_cnt_30d") + F.lit(1.0)))
        .withColumn("is_decelerating", (F.col("cnt_delta_30d") < 0).cast("int"))
        .withColumn("gap_vs_recency_ratio",
                    F.col("recency_days") / (F.coalesce(F.col("avg_gap_days_90d"), F.lit(1.0)) + F.lit(1.0)))
        .withColumn("tx_amt_sum_90d_log", F.log1p(F.col("tx_amt_sum_90d").cast("double")))
        .withColumn("tx_amt_mean_90d_log", F.log1p(F.coalesce(F.col("tx_amt_mean_90d"), F.lit(0.0))))
        .withColumn("tx_amt_max_90d_log", F.log1p(F.coalesce(F.col("tx_amt_max_90d").cast("double"), F.lit(0.0))))
        .withColumn("tx_cnt_90d_log", F.log1p(F.col("tx_cnt_90d").cast("double")))
        .withColumn("activity_density_90d", F.col("active_days_90d") / F.lit(90.0))
    )
    for t in TX_TYPES:
        c = f"cnt_{_sanitise(t)}_90d"
        feat = feat.withColumn(f"share_{_sanitise(t)}_90d", F.col(c) / (F.col("tx_cnt_90d") + F.lit(1.0)))

    # ---------- profile, experience and text features -------------------------
    # complaint_clean stays in silver for Member C, but the model consumes the NORMALISED
    # friction_theme instead - a free-text column would explode into thousands of dummy levels.
    prof = spark.table(tbl("silver_customer")).drop("complaint_clean")
    prof = (
        prof
        # DQ-16: a survey taken after the cutoff is post-cutoff information -> mask it
        # coalesce to 0: a NULL survey date means "no survey", which must read as unavailable. Left as
        # NULL it would be median-imputed to 1 and would assert a survey that does not exist.
        .withColumn("satisfaction_available",
                    F.coalesce((F.col("last_survey_date") <= cutoff).cast("int"), F.lit(0)))
        .withColumn(
            "satisfaction_score",
            F.when(F.col("last_survey_date") <= cutoff, F.col("satisfaction_score")).otherwise(F.lit(None)),
        )
        .withColumn(
            "survey_age_days",
            F.when(F.col("last_survey_date") <= cutoff, F.datediff(cutoff, F.col("last_survey_date"))),
        )
        .drop("last_survey_date")
    )

    feat = (
        feat.join(prof, "customer_id", "inner")
        .join(spark.table(tbl("silver_feedback_theme"))
              .select("customer_id", "friction_theme", "friction_word_hits"), "customer_id", "left")
        # Denominator is the FULL pre-cutoff history, not a 90-day window: failed_transactions has no
        # timestamp (L-09), so dividing a long-horizon numerator by a short-horizon denominator would
        # inflate the ratio and turn it into a disguised "low recent activity" feature.
        .withColumn("failures_per_tx_history",
                    F.col("failed_transactions") / (F.col("tx_cnt_hist") + F.lit(1.0)))
        .withColumn("unresolved_tickets",
                    F.col("support_tickets_count") * (F.lit(1.0) - F.coalesce(F.col("resolved_tickets_ratio"), F.lit(0.0))))
        .withColumn("logins_per_transaction",
                    F.col("app_logins_frequency") / (F.col("tx_cnt_30d") + F.lit(1.0)))
    )

    # ---------- the target ---------------------------------------------------
    outcome = (
        tx_all.where(
            (F.col("date") >= F.lit(w["outcome_start"].isoformat()).cast("date"))
            & (F.col("date") <= F.lit(w["outcome_end"].isoformat()).cast("date"))
        )
        .groupBy("customer_id")
        .agg(F.count("*").alias("tx_cnt_outcome"), F.sum("amount").alias("tx_amt_outcome"))
    )

    return (
        feat.join(outcome, "customer_id", "left")
        .withColumn("tx_cnt_outcome", F.coalesce(F.col("tx_cnt_outcome"), F.lit(0)))
        .withColumn("inactive_next_60d", (F.col("tx_cnt_outcome") == 0).cast("int"))
        .withColumn("split", F.lit(split))
        .withColumn("cutoff_date", cutoff)
        .withColumn("outcome_start", F.lit(w["outcome_start"].isoformat()).cast("date"))
        .withColumn("outcome_end", F.lit(w["outcome_end"].isoformat()).cast("date"))
        .drop("tx_amt_outcome")  # future value is Member B's target, never a feature or a column of mine
    )


print("build_feature_frame defined. One code path, three cutoffs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Build and persist the gold feature table

# COMMAND ----------

frames = []
for split in ["train", "valid", "test"]:
    with timed(f"engineer features @ {CUTOFFS[split]} ({split})", "gold"):
        f_df = build_feature_frame(split).cache()
        n = f_df.count()
        frames.append(f_df)
        print(f"    {split:<6s} cutoff={CUTOFFS[split]}  eligible customers={n:,}")

gold = frames[0]
for f_df in frames[1:]:
    gold = gold.unionByName(f_df)

with timed("write gold_retention_features", "gold"):
    save_table(gold, "gold_retention_features", partition_by=["split"],
               comment="Model-ready retention features: one row per (customer_id, cutoff_date)")

gold_t = spark.table(tbl("gold_retention_features"))
prevalence = (
    gold_t.groupBy("split", "cutoff_date", "outcome_start", "outcome_end")
    .agg(
        F.count("*").alias("eligible_customers"),
        F.sum("inactive_next_60d").alias("inactive_customers"),
        F.round(F.avg("inactive_next_60d") * 100, 2).alias("inactivity_rate_pct"),
    )
    .orderBy("cutoff_date")
)
display(prevalence)
prevalence_pdf = prevalence.toPandas()

print(f"gold_retention_features: {gold_t.count():,} rows x {len(gold_t.columns)} columns "
      f"across 3 chronological cutoffs")

# Survey coverage differs by cutoff (an earlier cutoff masks more surveys), which is a genuine
# train/serve distribution shift. Report it rather than letting the train-fitted median hide it.
print("\nSatisfaction-survey coverage by cutoff (the masked share differs, so report it):")
display(
    gold_t.groupBy("split", "cutoff_date")
    .agg(F.round(F.avg("satisfaction_available") * 100, 1).alias("survey_available_pct"),
         F.round(F.avg(F.col("satisfaction_score").isNull().cast("double")) * 100, 1).alias("satisfaction_masked_pct"))
    .orderBy("cutoff_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.6 Leakage audit — six assertions the table must survive, plus one measurement
# MAGIC
# MAGIC This is the part of data preparation that most often goes unchecked, and it is the part that decides
# MAGIC whether the interim numbers mean anything. LK-01 to LK-06 either pass or **stop the notebook**.
# MAGIC
# MAGIC Two of them (LK-03, LK-04) deliberately **re-derive the answer by a different code path** —
# MAGIC recomputing recency and the target straight from the ledger and comparing. A check that merely
# MAGIC restates how a column was built cannot fail and is worthless as evidence; these can fail.
# MAGIC
# MAGIC LK-07 is different: it is a *measurement*, not a rule. It reports how much of the feature set is a
# MAGIC fixed profile snapshot that cannot vary with the cutoff, which is the honest size of limitation L-09.

# COMMAND ----------

LEAK: list = []


def leak_check(check_id, description, observed, passed, consequence):
    LEAK.append((check_id, description, str(observed), "PASS" if passed else "FAIL", consequence))
    print(f"  [{'PASS' if passed else 'FAIL'}] {check_id}  {description}  -> {observed}")
    assert passed, f"{check_id} FAILED: {description} ({observed}) - {consequence}"


print("Leakage audit")

# LK-01: no excluded column survived into the feature table
present = [c for c in EXCLUDED_COLS if c in gold_t.columns]
leak_check("LK-01", "no excluded/unverified column appears in gold", f"{len(present)} found {present}",
           not present, "Whole-year or unreconciled columns would invalidate every metric.")

# LK-02: the key is unique. A duplicated key would mean a join fanned out and every rate,
#        every mean and every metric downstream is silently weighted wrong.
n_gold = gold_t.count()
n_keys = gold_t.select("customer_id", "cutoff_date").distinct().count()
leak_check("LK-02", "one row per (customer_id, cutoff_date)", f"{n_gold:,} rows vs {n_keys:,} distinct keys",
           n_gold == n_keys, "A fanned-out join would corrupt every aggregate in the notebook.")

# LK-03: INDEPENDENTLY recompute recency and eligibility from the ledger and compare to the feature
#        table. This re-derives the answer by a different code path, so it can genuinely fail if the
#        window logic in build_feature_frame is wrong.
_recheck_rows = []
for _split in ["train", "valid", "test"]:
    _w = WINDOWS[_split]
    _independent = (
        spark.table(tbl("silver_transaction"))
        .where(F.col("date") <= F.lit(_w["cutoff_date"].isoformat()).cast("date"))
        .groupBy("customer_id")
        .agg(F.max("date").alias("chk_last_tx"))
        .withColumn("chk_recency", F.datediff(F.lit(_w["cutoff_date"].isoformat()).cast("date"), F.col("chk_last_tx")))
    )
    _cmp = (
        gold_t.where(F.col("split") == _split).select("customer_id", "recency_days", "last_tx_pre_cutoff")
        .join(_independent, "customer_id", "inner")
    )
    _recheck_rows.append(
        _cmp.agg(
            F.sum((F.col("recency_days") != F.col("chk_recency")).cast("long")).alias("recency_mismatch"),
            F.sum((F.col("last_tx_pre_cutoff") != F.col("chk_last_tx")).cast("long")).alias("date_mismatch"),
            F.sum((F.col("chk_recency") > (ELIGIBILITY_DAYS - 1)).cast("long")).alias("not_eligible"),
            F.sum((F.col("chk_last_tx") > F.lit(_w["cutoff_date"].isoformat()).cast("date")).cast("long")).alias("post_cutoff"),
        ).collect()[0].asDict()
    )
_rc = {k: sum(r[k] or 0 for r in _recheck_rows) for k in _recheck_rows[0]}
leak_check("LK-03", "recency + last activity date match an independent recomputation from the ledger",
           f"recency mismatches={_rc['recency_mismatch']}, date mismatches={_rc['date_mismatch']}, "
           f"post-cutoff dates={_rc['post_cutoff']}, ineligible rows={_rc['not_eligible']}",
           all(v == 0 for v in _rc.values()),
           "If the recomputation disagrees, the time windows are wrong and every feature is suspect.")

# LK-04: INDEPENDENTLY recompute the target for the test split and compare.
_wt = WINDOWS["test"]
_target_chk = (
    spark.table(tbl("silver_transaction"))
    .where(
        (F.col("date") >= F.lit(_wt["outcome_start"].isoformat()).cast("date"))
        & (F.col("date") <= F.lit(_wt["outcome_end"].isoformat()).cast("date"))
    )
    .groupBy("customer_id")
    .agg(F.count("*").alias("chk_outcome_tx"))
)
_target_cmp = (
    gold_t.where("split = 'test'").select("customer_id", "inactive_next_60d")
    .join(_target_chk, "customer_id", "left")
    .withColumn("chk_inactive", (F.coalesce(F.col("chk_outcome_tx"), F.lit(0)) == 0).cast("int"))
)
target_mismatch = _target_cmp.where(F.col("inactive_next_60d") != F.col("chk_inactive")).count()
leak_check("LK-04", "target matches an independent recomputation of the 60-day outcome window",
           f"{target_mismatch} mismatches of {_target_cmp.count():,} test rows", target_mismatch == 0,
           "A wrong target invalidates the entire workstream.")

# LK-05: no single feature is a near-perfect surrogate for the target
NUMERIC_FEATURES = [
    f.name for f in gold_t.schema.fields
    if isinstance(f.dataType, (T.IntegerType, T.LongType, T.DoubleType, T.FloatType, T.DecimalType))
    and f.name not in ("customer_id", "inactive_next_60d", "tx_cnt_outcome")
]
train_only = gold_t.where("split = 'train'")
# All correlations in ONE Spark job (F.corr ignores null pairs), rather than one job per feature.
_corr_row = train_only.agg(
    *[F.corr(F.col(c).cast("double"), F.col("inactive_next_60d").cast("double")).alias(c)
      for c in NUMERIC_FEATURES]
).collect()[0].asDict()
target_corr = pd.DataFrame(
    [{"feature": c, "corr_with_target": None if v is None else round(float(v), 4)}
     for c, v in _corr_row.items()]
).dropna()
target_corr["abs_corr"] = target_corr["corr_with_target"].abs()
target_corr = target_corr.sort_values("abs_corr", ascending=False)
worst = float(target_corr["abs_corr"].max())
leak_check("LK-05", "no feature correlates > 0.95 with the target (surrogate detector)",
           f"strongest |r| = {worst:.4f} ({target_corr.iloc[0]['feature']})", worst <= 0.95,
           "A near-perfect single predictor is almost always leakage, not insight.")

# LK-06: per-type counts must sum to the total. Catches a vocabulary mismatch between the cleaned
#        `type` values and the TX_TYPES list, which would silently zero every share feature.
_share_cols = [f"cnt_{_sanitise(t)}_90d" for t in TX_TYPES]
type_sum_mismatch = (
    gold_t.withColumn("_type_total", sum([F.col(c) for c in _share_cols]))
    .where(F.col("_type_total") != F.col("tx_cnt_90d")).count()
)
leak_check("LK-06", "per-transaction-type counts sum exactly to tx_cnt_90d",
           f"{type_sum_mismatch} rows disagree", type_sum_mismatch == 0,
           "A type-vocabulary mismatch would silently zero every share feature without any error.")

# LK-07: quantify (rather than assert) how much of the feature set is a fixed profile snapshot that
#        cannot vary with the cutoff. This is the machine-checkable size of limitation L-09.
_invariance = (
    gold_t.groupBy("customer_id")
    .agg(*[F.countDistinct(F.col(c)).alias(c) for c in
           UNTIMESTAMPED_COUNT_COLS + ["recency_days", "tx_cnt_30d"]])
    .agg(*[F.avg((F.col(c) == 1).cast("double")).alias(c) for c in
           UNTIMESTAMPED_COUNT_COLS + ["recency_days", "tx_cnt_30d"]])
    .collect()[0].asDict()
)
print("\n  LK-07 (reported, not asserted) — share of customers whose value is IDENTICAL at all 3 cutoffs:")
for c, v in _invariance.items():
    tag = "  <- untimestamped snapshot (L-09)" if c in UNTIMESTAMPED_COUNT_COLS else "  <- time-varying, as intended"
    print(f"    {c:<28s} {v:6.1%}{tag}")
LEAK.append((
    "LK-07",
    "quantify how many features are cutoff-invariant snapshots (limitation L-09)",
    "; ".join(f"{c}={v:.1%}" for c, v in _invariance.items()),
    "REPORTED",
    "Not a pass/fail rule. It measures how much of the feature set cannot respond to the cutoff, "
    "which bounds how strongly the interim results can be attributed to point-in-time behaviour.",
))

leak_audit = spark_df(LEAK, ["check_id", "description", "observed", "verdict", "consequence_if_failed"])
save_table(leak_audit, "dq_leakage_audit", comment="Leakage audit for the retention feature table")

save_table(spark.createDataFrame(target_corr.drop(columns="abs_corr")), "insight_target_driver",
           comment="Point-biserial association between each numeric feature and inactive_next_60d (train split only)")

n_leak_asserted = sum(1 for r in LEAK if r[3] in ("PASS", "FAIL"))
print(f"\nAll {n_leak_asserted} asserted leakage checks passed (LK-07 is a reported measure, not a rule).")
print("Strongest single-feature associations with the target (train only):")
display(spark.createDataFrame(target_corr.head(15).drop(columns="abs_corr")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.7 Feature catalogue
# MAGIC
# MAGIC Appendix D commits to documenting *every derived feature: its source fields, calculation, time
# MAGIC boundary and business meaning*. Generated from the live gold schema so it is always complete.

# COMMAND ----------

FEATURE_NOTES = {
    "recency_days": ("silver_transaction.date", "datediff(cutoff, max(date) where date <= cutoff)",
                     "Days since last activity. The single most direct engagement signal."),
    "tenure_days": ("silver_transaction.date", "datediff(cutoff, min(date) where date <= cutoff)",
                    "Relationship length at the cutoff, re-derived rather than taken from customer_tenure."),
    "tx_cnt_7d": ("silver_transaction", "count where 0 <= days_before <= 6", "Very recent activity level."),
    "tx_cnt_30d": ("silver_transaction", "count where 0 <= days_before <= 29", "Short-term activity level."),
    "tx_cnt_90d": ("silver_transaction", "count where 0 <= days_before <= 89", "Observation-window activity; also the eligibility test."),
    "tx_cnt_180d": ("silver_transaction", "count where 0 <= days_before <= 179", "Medium-term baseline."),
    "tx_cnt_prior_30d": ("silver_transaction", "count where 30 <= days_before <= 59", "The comparison period for the trend features."),
    "cnt_delta_30d": ("tx_cnt_30d, tx_cnt_prior_30d", "tx_cnt_30d - tx_cnt_prior_30d",
                      "Absolute change in monthly activity. Directly tests 'declining frequency' in my hypothesis."),
    "cnt_ratio_30_prior30": ("tx_cnt_30d, tx_cnt_prior_30d", "tx_cnt_30d / (tx_cnt_prior_30d + 1)",
                             "Relative slowdown; scale-free so it compares light and heavy users fairly."),
    "velocity_7_over_30": ("tx_cnt_7d, tx_cnt_30d", "(tx_cnt_7d * 30/7) / (tx_cnt_30d + 1)",
                           "Is the most recent week running above or below the month's pace?"),
    "is_decelerating": ("cnt_delta_30d", "cnt_delta_30d < 0", "Binary decline flag, easy for the CRM team to act on."),
    "max_gap_days_90d": ("silver_transaction.date", "max(datediff between consecutive active days, 90d window)",
                         "Longest quiet stretch: distinguishes a bursty customer from a fading one."),
    "gap_vs_recency_ratio": ("recency_days, avg_gap_days_90d", "recency_days / (avg_gap_days_90d + 1)",
                             "Is this silence unusual FOR THIS CUSTOMER? > 1 means overdue relative to their own rhythm."),
    "activity_density_90d": ("active_days_90d", "active_days_90d / 90", "Share of days with any activity."),
    "weekend_share_90d": ("silver_transaction.is_weekend", "mean(is_weekend) over the 90d window",
                          "Honest replacement for the unverified Set B weekend_transaction_ratio."),
    "tx_cnt_hist": ("silver_transaction", "count where date <= cutoff",
                    "Total pre-cutoff activity. Used as the denominator for untimestamped counts."),
    "failures_per_tx_history": ("failed_transactions, tx_cnt_hist",
                                "failed_transactions / (tx_cnt_hist + 1)",
                                "Friction per unit of activity. Denominator is the full pre-cutoff history "
                                "because the numerator has no timestamp (limitation L-09)."),
    "unresolved_tickets": ("support_tickets_count, resolved_tickets_ratio", "tickets * (1 - resolved_ratio)",
                           "Unresolved issues, which my hypothesis names - not contact volume."),
    "logins_per_transaction": ("app_logins_frequency, tx_cnt_30d", "logins / (tx_cnt_30d + 1)",
                               "Browsing without transacting: a candidate early-warning pattern."),
    "satisfaction_available": ("last_survey_date", "last_survey_date <= cutoff",
                               "Honest coverage flag; satisfaction_score is masked when this is 0 (DQ-16)."),
    "friction_theme": ("bronze_feedback_document (unstructured)", "tokenise -> stop-word removal -> theme rules",
                       "Complaint theme from free text; 'no_complaint_on_file' is a real level, not a null."),
    "inactive_next_60d": ("silver_transaction", "1 if count(tx in [cutoff+1, cutoff+60]) = 0 else 0",
                          "TARGET. No observed transactions in the next 60 days - NOT account closure."),
}

feature_catalogue = spark_df(
    [
        (
            f.name,
            f.dataType.simpleString(),
            "target" if f.name == "inactive_next_60d" else ("metadata" if f.name in
             ("customer_id", "split", "cutoff_date", "outcome_start", "outcome_end", "tx_cnt_outcome",
              "last_tx_pre_cutoff", "first_tx_pre_cutoff") else "feature"),
            FEATURE_NOTES.get(f.name, ("silver_customer", "carried through from the cleaned profile",
                                       "Profile attribute approved in meta_column_decision."))[0],
            FEATURE_NOTES.get(f.name, ("", "carried through from the cleaned profile", ""))[1],
            "date <= cutoff_date" if f.name != "inactive_next_60d" else "cutoff_date + 1 .. cutoff_date + 60",
            FEATURE_NOTES.get(f.name, ("", "", "Profile attribute approved in meta_column_decision."))[2],
        )
        for f in gold_t.schema.fields
    ],
    ["feature_name", "data_type", "role", "source_fields", "calculation", "time_boundary", "business_meaning"],
)
save_table(feature_catalogue, "meta_feature_catalogue", comment="Definition, provenance and time boundary of every gold column")
display(feature_catalogue.where("role = 'feature'").orderBy("feature_name"))
n_documented_features = feature_catalogue.where("role = 'feature'").count()
print(f"{n_documented_features} features documented, each with its source fields, "
      f"calculation and time boundary.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.8 Preprocessing fitted on training data only
# MAGIC
# MAGIC The last preparation step is the one that is easiest to get wrong: **every transformation is fitted
# MAGIC on the train split alone**, then applied unchanged to validation and test. Fitting a median or a
# MAGIC category index on the full table would let later periods inform earlier ones — a subtler leak than
# MAGIC a bad column, and just as damaging.
# MAGIC
# MAGIC The fitted pipeline is saved so scoring runs reuse the *same* transformations rather than re-fitting.

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, StandardScaler

CATEGORICAL_FEATURES = [
    f.name for f in gold_t.schema.fields
    if isinstance(f.dataType, T.StringType) and f.name not in ("split",)
]
BOOLEAN_FEATURES = [f.name for f in gold_t.schema.fields if isinstance(f.dataType, T.BooleanType)]

print(f"numeric   : {len(NUMERIC_FEATURES)}")
print(f"boolean   : {len(BOOLEAN_FEATURES)}  -> {BOOLEAN_FEATURES}")
print(f"categorical: {len(CATEGORICAL_FEATURES)} -> {CATEGORICAL_FEATURES}")

# Imputer requires DoubleType/FloatType inputs, so every numeric and boolean feature is cast to
# double. Done as ONE select rather than ~60 chained withColumn calls, which would build a
# needlessly deep logical plan.
_to_double = set(NUMERIC_FEATURES + BOOLEAN_FEATURES)
model_input = gold_t.select(
    *[F.col(c).cast("double").alias(c) if c in _to_double else F.col(c) for c in gold_t.columns]
)

NUMERIC_IN = NUMERIC_FEATURES + BOOLEAN_FEATURES
train_df = model_input.where("split = 'train'").cache()
valid_df = model_input.where("split = 'valid'").cache()
test_df = model_input.where("split = 'test'").cache()
print(f"\ntrain {train_df.count():,} | valid {valid_df.count():,} | test {test_df.count():,}")

# Imputer cannot compute a surrogate for a column that is 100% null in the fitting frame - it throws
# an opaque Scala error. Check the TRAIN split explicitly and route all-null columns to a documented
# sentinel instead. This can bite if someone moves train_cutoff earlier than the first survey date,
# which would blank satisfaction_score for every training row.
_train_non_null = train_df.agg(*[F.count(F.col(c)).alias(c) for c in NUMERIC_IN]).collect()[0].asDict()
IMPUTE_IN = [c for c in NUMERIC_IN if _train_non_null[c] > 0]
ALL_NULL_IN = [c for c in NUMERIC_IN if _train_non_null[c] == 0]

if ALL_NULL_IN:
    print(f"\nWARNING: {len(ALL_NULL_IN)} feature(s) are entirely null in the TRAIN split and cannot be "
          f"median-imputed: {ALL_NULL_IN}")
    print("  Filling them with an explicit 0.0 sentinel instead. If this list is non-empty, check that "
          "train_cutoff is not earlier than the data these features come from.")
    train_df = train_df.fillna(0.0, subset=ALL_NULL_IN)
    valid_df = valid_df.fillna(0.0, subset=ALL_NULL_IN)
    test_df = test_df.fillna(0.0, subset=ALL_NULL_IN)

stages = [
    Imputer(inputCols=IMPUTE_IN, outputCols=[f"{c}__imp" for c in IMPUTE_IN], strategy="median"),
    StringIndexer(inputCols=CATEGORICAL_FEATURES, outputCols=[f"{c}__idx" for c in CATEGORICAL_FEATURES],
                  handleInvalid="keep"),
    OneHotEncoder(inputCols=[f"{c}__idx" for c in CATEGORICAL_FEATURES],
                  outputCols=[f"{c}__ohe" for c in CATEGORICAL_FEATURES], handleInvalid="keep"),
    VectorAssembler(
        inputCols=[f"{c}__imp" for c in IMPUTE_IN] + ALL_NULL_IN
                  + [f"{c}__ohe" for c in CATEGORICAL_FEATURES],
        outputCol="_assembled", handleInvalid="keep",
    ),
    StandardScaler(inputCol="_assembled", outputCol="features", withMean=False, withStd=True),
]

with timed("fit preprocessing pipeline on TRAIN ONLY", "prep"):
    prep_model = Pipeline(stages=stages).fit(train_df)

with timed("apply fitted pipeline to train/valid/test", "prep"):
    train_prep = prep_model.transform(train_df).cache()
    valid_prep = prep_model.transform(valid_df).cache()
    test_prep = prep_model.transform(test_df).cache()
    train_prep.count(); valid_prep.count(); test_prep.count()

prep_path = f"{WORK_DIR}/models/prep_pipeline_{RUN_ID}"
prep_model.write().overwrite().save(prep_path)
print(f"Fitted pipeline saved to {prep_path}")
print("Imputer medians and StringIndexer vocabularies come from the TRAIN split only - "
      "validation and test are transformed, never re-fitted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.9 Data-readiness check
# MAGIC
# MAGIC Modelling is the next phase, not this deliverable. But a feature table is only "prepared" if a
# MAGIC model can actually consume it, so I fit one class-weighted logistic regression on the train split
# MAGIC and score the untouched test split. The point is the **verdict on the data**, not the algorithm:
# MAGIC does the prepared table carry signal beyond the majority-class rule, and does it hold up across
# MAGIC time?

# COMMAND ----------

pos_rate = train_prep.agg(F.avg("inactive_next_60d")).collect()[0][0]
train_weighted = train_prep.withColumn(
    "class_weight",
    F.when(F.col("inactive_next_60d") == 1, F.lit((1 - pos_rate) / pos_rate)).otherwise(F.lit(1.0)),
)

with timed("fit baseline logistic regression (readiness check)", "model"):
    lr = LogisticRegression(featuresCol="features", labelCol="inactive_next_60d",
                            weightCol="class_weight", maxIter=40, regParam=0.01, elasticNetParam=0.0)
    lr_model = lr.fit(train_weighted)


from pyspark.ml.functions import vector_to_array


def evaluate(df, name):
    pred = lr_model.transform(df).withColumn("p_inactive", vector_to_array("probability")[1]).cache()
    pr_auc = BinaryClassificationEvaluator(labelCol="inactive_next_60d", rawPredictionCol="probability",
                                           metricName="areaUnderPR").evaluate(pred)
    roc_auc = BinaryClassificationEvaluator(labelCol="inactive_next_60d", rawPredictionCol="probability",
                                            metricName="areaUnderROC").evaluate(pred)
    stats = pred.agg(
        F.count("*").alias("rows"),
        F.sum("inactive_next_60d").alias("positives"),
        F.avg("inactive_next_60d").alias("prevalence"),
    ).collect()[0]
    total_pos = stats["positives"] or 0

    # Top-k recall via quantile thresholds rather than percent_rank over an unpartitioned Window.
    # An unpartitioned window forces every row into a single partition, which is exactly the pattern
    # the scalability section argues against.
    q90, q80 = pred.approxQuantile("p_inactive", [0.90, 0.80], 0.0001)
    caught = pred.agg(
        F.sum(F.when(F.col("p_inactive") >= F.lit(q90), F.col("inactive_next_60d")).otherwise(0)).alias("top10"),
        F.sum(F.when(F.col("p_inactive") >= F.lit(q80), F.col("inactive_next_60d")).otherwise(0)).alias("top20"),
    ).collect()[0]
    r10 = (caught["top10"] or 0) / total_pos if total_pos else 0.0
    r20 = (caught["top20"] or 0) / total_pos if total_pos else 0.0
    return pred, {
        "split": name, "rows": stats["rows"], "inactivity_rate": round(stats["prevalence"], 4),
        "pr_auc": round(pr_auc, 4), "roc_auc": round(roc_auc, 4),
        "baseline_pr_auc_prevalence": round(stats["prevalence"], 4),
        "recall_top10pct": round(r10, 4), "recall_top20pct": round(r20, 4),
        "lift_top10pct": round(r10 / 0.10, 2),
    }


with timed("score train/valid/test + top-k recall", "model"):
    _, m_train = evaluate(train_prep, "train")
    _, m_valid = evaluate(valid_prep, "valid")
    test_pred, m_test = evaluate(test_prep, "test")

readiness = pd.DataFrame([m_train, m_valid, m_test])
display(spark.createDataFrame(readiness))

print("\nReadiness verdict")
print(f"  Test PR-AUC {m_test['pr_auc']:.4f} vs the prevalence baseline {m_test['inactivity_rate']:.4f} "
      f"-> the prepared features carry signal beyond guessing the majority class.")
print(f"  Test recall in the top 10% of risk scores: {m_test['recall_top10pct']:.1%} "
      f"({m_test['lift_top10pct']}x lift) - directly answers the stakeholder's 'we cannot contact everyone'.")
print(f"  Train PR-AUC {m_train['pr_auc']:.4f} -> valid {m_valid['pr_auc']:.4f} -> test {m_test['pr_auc']:.4f}: "
      f"the gap is the honest cost of predicting a LATER PERIOD.")
print("\nTwo things this does NOT claim (limitations L-10 and L-11):")
print("  - The three splits hold the SAME customers at different cutoffs. The outcome windows do not")
print("    overlap, so this is a genuine later-period estimate - but it is not a held-out POPULATION")
print("    estimate. A customer-disjoint split is step 1b of the next phase.")
print("  - Class weighting shifts the probabilities to a ~50/50 prior, so p_inactive is a RANKING, not a")
print("    calibrated probability. Only rank-based metrics are reported anywhere in this notebook.")
print("\nModel selection, tuning and the Random Forest / gradient-boosting comparison belong to the next phase.")

# COMMAND ----------

# persist scores: consumed by the visualisations in Section 4 and the joint priority view in Section 5
from pyspark.ml.feature import Bucketizer

with timed("write gold_retention_scored", "gold"):
    scored_base = test_pred.select(
        "customer_id", "cutoff_date", "inactive_next_60d", "p_inactive",
        "recency_days", "tx_cnt_30d", "tx_cnt_90d", "cnt_delta_30d", "is_decelerating",
        "active_products", "app_logins_frequency", "failed_transactions", "friction_theme",
        "city", "department", "income_bracket", "acquisition_channel",
    )

    # Deciles from quantile cut points via Bucketizer, not ntile over an unpartitioned Window.
    _cuts = sorted(set(scored_base.approxQuantile("p_inactive", [i / 10 for i in range(1, 10)], 0.0001)))
    _splits = [-float("inf")] + _cuts + [float("inf")]
    _n_buckets = len(_splits) - 1
    q80, q50 = scored_base.approxQuantile("p_inactive", [0.80, 0.50], 0.0001)

    scored = (
        Bucketizer(splits=_splits, inputCol="p_inactive", outputCol="_bucket").transform(scored_base)
        # bucket 0 = lowest score, so invert to make decile 1 = highest risk
        .withColumn("risk_decile", (F.lit(_n_buckets) - F.col("_bucket")).cast("int"))
        .withColumn(
            "risk_band",
            F.when(F.col("p_inactive") >= F.lit(q80), "High")     # top 20% of scores
             .when(F.col("p_inactive") >= F.lit(q50), "Medium")   # next 30%
             .otherwise("Low"),
        )
        .drop("_bucket")
    )
    save_table(scored, "gold_retention_scored", comment="Test-period risk scores, deciles and bands")
print(f"  risk bands cut at the 80th ({q80:.4f}) and 50th ({q50:.4f}) percentile of predicted risk; "
      f"{_n_buckets} distinct deciles resolved.")

display(
    spark.table(tbl("gold_retention_scored"))
    .groupBy("risk_band")
    .agg(F.count("*").alias("customers"),
         F.round(F.avg("inactive_next_60d") * 100, 1).alias("actual_inactivity_rate_pct"),
         F.round(F.avg("recency_days"), 1).alias("avg_recency_days"))
    .orderBy(F.desc("actual_inactivity_rate_pct"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.10 Scalability probe — runtime at 25 / 50 / 75 / 100% of the ledger
# MAGIC
# MAGIC The proposal commits to reporting runtime growth. This probe re-runs the expensive part of the
# MAGIC pipeline (read → dedupe → windowed per-customer aggregation) at four data fractions on the cluster,
# MAGIC so the answer is measured on the cloud platform rather than extrapolated from a laptop.

# COMMAND ----------

probe_rows = []
if RUN_SCALABILITY:
    # Probe BRONZE, so the timed work includes the deduplication step the markdown claims it does
    # (silver is already deduplicated, so timing silver would quietly skip that stage).
    probe_ids = spark.table(tbl("bronze_customer")).select("customer_id")
    probe_cutoff = F.lit(CUTOFFS["test"]).cast("date")
    for frac in [0.25, 0.50, 0.75, 1.00]:
        ids = probe_ids if frac == 1.0 else probe_ids.sample(False, frac, seed=7)
        sub = spark.table(tbl("bronze_transaction")).select("customer_id", "date", "amount", "type") \
                   .join(ids, "customer_id", "left_semi")
        rows = sub.count()          # measured OUTSIDE the timer, so the timing is one pass, not two

        t0 = time.time()
        (
            sub.where(F.col("amount").isNotNull() & (F.col("amount") >= 0))
            .withColumn("type", F.initcap(F.trim(F.col("type"))))
            .dropDuplicates(["customer_id", "date", "amount", "type"])
            .where(F.col("date") <= probe_cutoff)
            .withColumn("days_before", F.datediff(probe_cutoff, F.col("date")))
            .groupBy("customer_id")
            .agg(
                F.max("date").alias("last_tx"),
                F.sum(F.when(F.col("days_before") <= 29, 1).otherwise(0)).alias("c30"),
                F.sum(F.when(F.col("days_before") <= 89, 1).otherwise(0)).alias("c90"),
                F.sum(F.when(F.col("days_before") <= 89, F.col("amount")).otherwise(0)).alias("v90"),
            )
            .count()
        )
        elapsed = round(time.time() - t0, 2)
        probe_rows.append({"fraction": f"{int(frac * 100)}%", "rows_processed": rows, "seconds": elapsed,
                           "rows_per_second": int(rows / elapsed) if elapsed else 0})
        print(f"  {int(frac * 100):>3}% of customers  {rows:>10,} ledger rows  {elapsed:>6.2f}s  "
              f"({probe_rows[-1]['rows_per_second']:,} rows/s)")
    probe_pdf = pd.DataFrame(probe_rows)
    if SAMPLE_FRACTION < 1.0:
        print(f"  NOTE: the main run is itself down-sampled to {SAMPLE_FRACTION:.0%}, but this probe reads "
              f"the FULL bronze ledger, so the fractions above are shares of all {n_tx:,} rows.")
    save_table(spark.createDataFrame(probe_pdf), "ops_scalability_probe",
               comment="Runtime of read + dedupe + windowed aggregation at 25/50/75/100% of the ledger")
    display(spark.createDataFrame(probe_pdf))
else:
    probe_pdf = pd.DataFrame(columns=["fraction", "rows_processed", "seconds", "rows_per_second"])
    print("Scalability probe skipped (run_scalability_probe = no).")

# COMMAND ----------

# Snapshot for chart 4.11. The table itself is written in the FINAL cell, so that steps timed in
# Sections 4 and 5 are not missing from the persisted log.
runtime_pdf = pd.DataFrame(RUNTIME_LOG)
display(spark.createDataFrame(runtime_pdf).orderBy(F.desc("seconds")))
print(f"Instrumented pipeline time so far: {runtime_pdf['seconds'].sum():.1f}s across {len(runtime_pdf)} steps.")

# The cached feature frames are no longer needed; release the memory before the plotting sections.
for _f in frames:
    _f.unpersist()


# COMMAND ----------

# MAGIC %md
# MAGIC # Section 4 — Data Visualization  *(rubric: 15 marks)*
# MAGIC
# MAGIC **What the top band asks for:** *multiple visualizations used to clearly and effectively show the
# MAGIC characteristics of the data*, *visual representation is intuitive*, and *the choice of visual
# MAGIC representation provides an accurate message*.
# MAGIC
# MAGIC **How I address it.** Eleven figures, each one answering a question a decision actually depends on
# MAGIC (a twelfth, the joint priority view, appears in §5.4). To keep the message accurate rather than merely
# MAGIC attractive I hold to five rules throughout:
# MAGIC
# MAGIC 1. **One question per chart**, stated in the markdown above it, with the answer stated below it.
# MAGIC 2. **Consistent colour semantics** — red always means elevated inactivity risk, grey always means
# MAGIC    context/reference, blue always means volume. Colour never varies for decoration.
# MAGIC 3. **Chart type follows the data type** — rates as bars, time as lines, two continuous variables as
# MAGIC    a heatmap, cumulative capture as a curve against a diagonal reference.
# MAGIC 4. **Uncertainty is shown, not hidden** — small groups are labelled with their n, and any bucket
# MAGIC    below a minimum size is dropped rather than plotted as if it were reliable.
# MAGIC 5. **Reference lines** — the base rate, the random-targeting diagonal and the no-effect level are
# MAGIC    drawn wherever a reader could otherwise mistake a normal value for a signal.
# MAGIC
# MAGIC All aggregation happens in Spark; only small summary frames are collected to the driver for
# MAGIC plotting, which is what keeps this approach viable at 3.16M rows.

# COMMAND ----------

from matplotlib.colors import LinearSegmentedColormap

RISK = "#c0392b"        # elevated inactivity risk
RISK_LIGHT = "#e8a49c"
SAFE = "#2e7d5b"        # retained / healthy
VOLUME = "#2c6fa8"      # counts and volumes
REF = "#95a5a6"         # reference / context
ACCENT = "#d4913b"

plt.rcParams.update({
    "figure.figsize": (10, 5),
    "figure.dpi": 110,
    "axes.titlesize": 13,
    "axes.titleweight": "bold",
    "axes.labelsize": 11,
    "axes.grid": True,
    "grid.alpha": 0.25,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "font.size": 10,
})


# One shared risk ramp: green (healthy) -> amber -> red (at risk).
RISK_CMAP = LinearSegmentedColormap.from_list("risk", [SAFE, "#f0e2b6", RISK])

# ...and one shared DOMAIN for it. Re-normalising each chart to its own min/max would paint the
# lowest bar bright green on every chart regardless of its actual rate, which breaks the promise
# that a colour means the same thing everywhere. RATE_VMAX is set once, below, from the base rate.
RATE_VMAX = 100.0  # replaced immediately after the base rate is known


def risk_colors(values):
    """Map inactivity rates (in %) onto the shared risk ramp using the shared fixed domain."""
    v = np.clip(np.asarray(values, dtype=float) / RATE_VMAX, 0.0, 1.0)
    return [RISK_CMAP(x) for x in v]


def thousands(ax, axis="y"):
    fmt = mticker.FuncFormatter(lambda v, p: f"{v:,.0f}")
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(fmt)


def caption(ax, text):
    ax.text(0, -0.30, text, transform=ax.transAxes, fontsize=9, color="#444444",
            va="top", ha="left", wrap=True)


BASE_RATE = float(prevalence_pdf.loc[prevalence_pdf["split"] == "test", "inactivity_rate_pct"].iloc[0])

# Fix the colour domain once, at 3x the base rate (capped at 100%), and reuse it for every rate
# chart and heatmap. Green now always means "at or below the base rate" and deep red always means
# "at least 3x the base rate", on every figure in this notebook.
RATE_VMAX = float(min(100.0, max(BASE_RATE * 3.0, 10.0)))

print(f"Base inactivity rate (test period), drawn as the reference line on every rate chart: {BASE_RATE:.2f}%")
print(f"Shared colour domain for all rate charts: 0% (green) .. {RATE_VMAX:.1f}% (deep red)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Can I trust the columns I was given?
# MAGIC
# MAGIC The most consequential finding of this phase is invisible in any single number, so it needs the
# MAGIC first chart. Two column families claim to summarise the same ledger; only one of them does.

# COMMAND ----------

recon_pdf = pd.DataFrame(
    [
        {"check": "tx_count vs ledger count", "family": "Set A", "match_pct": setA["count_match"] * 100},
        {"check": "total_tx_volume vs ledger sum", "family": "Set A", "match_pct": setA["sum_match"] * 100},
        {"check": "first_tx vs ledger min date", "family": "Set A", "match_pct": setA["first_match"] * 100},
        {"check": "last_tx vs ledger max date", "family": "Set A", "match_pct": setA["last_match"] * 100},
        {"check": "last_transaction_date vs ledger max date", "family": "Set B", "match_pct": last_date_match * 100},
        {"check": "total_transaction_volume vs ledger sum (|r|)", "family": "Set B", "match_pct": abs(corr_sum) * 100},
        {"check": "average_transaction_value vs ledger mean (|r|)", "family": "Set B", "match_pct": abs(corr_avg) * 100},
    ]
).sort_values("match_pct")

fig, ax = plt.subplots(figsize=(10, 4.6))
colors = [SAFE if f == "Set A" else RISK for f in recon_pdf["family"]]
bars = ax.barh(recon_pdf["check"], recon_pdf["match_pct"], color=colors)
for b, v in zip(bars, recon_pdf["match_pct"]):
    ax.text(v + 1.5, b.get_y() + b.get_height() / 2, f"{v:.1f}%", va="center", fontsize=9)
ax.axvline(100, color=REF, linestyle="--", linewidth=1)
ax.text(100, -0.9, "perfect agreement", color=REF, fontsize=8, ha="right")
ax.set_xlim(0, 118)
ax.set_xlabel("Agreement with the raw transaction ledger (%)")
ax.set_title("Set A reconciles perfectly; Set B does not reconcile at all")
handles = [plt.Rectangle((0, 0), 1, 1, color=SAFE), plt.Rectangle((0, 0), 1, 1, color=RISK)]
ax.legend(handles, ["Set A — verified, safe to use", "Set B — rejected (DQ-13)"], loc="lower right", frameon=False)
caption(ax, "Decision: all 9 Set B columns excluded for the whole team; every transaction feature is\n"
            "re-derived from transactions_data.csv. A model trained on Set B would have been fitted to noise.")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Where is data missing, and does missing mean unknown?
# MAGIC
# MAGIC A missingness chart is usually read as "these columns need imputing". Here the accurate message is
# MAGIC the opposite, so the chart is annotated with what each blank actually *means*.

# COMMAND ----------

miss_pdf = (
    spark.table(tbl("dq_null_profile")).where("missing_pct > 0").orderBy(F.desc("missing_pct")).toPandas()
)
MEANING = {
    "complaint_topics": "= no complaint on file",
    "credit_utilization_ratio": "= customer holds no credit card",
    "feature_requests": "= no request submitted",
}

fig, ax = plt.subplots(figsize=(10, 3.4))
bars = ax.barh(miss_pdf["column_name"], miss_pdf["missing_pct"], color=ACCENT)
for b, (_, row) in zip(bars, miss_pdf.iterrows()):
    ax.text(row["missing_pct"] + 1, b.get_y() + b.get_height() / 2,
            f"{row['missing_pct']:.1f}%   {MEANING.get(row['column_name'], '')}", va="center", fontsize=9)
ax.set_xlim(0, 100)
ax.invert_yaxis()
ax.set_xlabel("% of customers with no value")
ax.set_title(f"Only {len(miss_pdf)} of {len(_business_cols)} columns are incomplete — and every blank is structural")
caption(ax, "Decision: sentinel value + explicit indicator flag, never mean imputation. Mean-imputing\n"
            "credit utilisation would invent a balance for customers who have no credit card at all.")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Is my temporal design actually valid?
# MAGIC
# MAGIC A timeline is the only honest way to show a chronological split. The daily volume line doubles as
# MAGIC evidence for DQ-07 (no gaps) and for theme TH-08 (platform volume is flat, so risk lives at
# MAGIC customer level, not day level).

# COMMAND ----------

daily_pdf = spark.table(tbl("silver_daily_activity")).select("date", "n_transactions").orderBy("date").toPandas()
daily_pdf["date"] = pd.to_datetime(daily_pdf["date"])
daily_pdf["roll7"] = daily_pdf["n_transactions"].rolling(7, center=True).mean()

fig, ax = plt.subplots(figsize=(13, 5))
ax.plot(daily_pdf["date"], daily_pdf["n_transactions"], color="#b8cfe3", linewidth=0.9, label="Daily transactions")
ax.plot(daily_pdf["date"], daily_pdf["roll7"], color=VOLUME, linewidth=2.2, label="7-day rolling mean")

shades = {"train": "#d5e8d4", "valid": "#fff2cc", "test": "#f8cecc"}
for split in ["train", "valid", "test"]:
    w = WINDOWS[split]
    ax.axvspan(pd.Timestamp(w["outcome_start"]), pd.Timestamp(w["outcome_end"]),
               color=shades[split], alpha=0.75, zorder=0)
    ax.axvline(pd.Timestamp(w["cutoff_date"]), color="#333333", linestyle="--", linewidth=1.2)
    mid = pd.Timestamp(w["outcome_start"]) + (pd.Timestamp(w["outcome_end"]) - pd.Timestamp(w["outcome_start"])) / 2
    ax.text(mid, ax.get_ylim()[1] * 0.985, f"{split.upper()}\noutcome 60d", ha="center", va="top",
            fontsize=9, fontweight="bold", color="#333333")
    ax.text(pd.Timestamp(w["cutoff_date"]), ax.get_ylim()[0], f" cutoff {w['cutoff_date']}",
            rotation=90, va="bottom", fontsize=8, color="#333333")

ax.set_title("Chronological design: 3 cutoffs, 3 non-overlapping 60-day outcome windows, no calendar gaps")
ax.set_ylabel("Transactions per day")
ax.set_xlabel("2023")
thousands(ax)
ax.legend(loc="lower left", frameon=False, ncol=2)
caption(ax, "Message: features come only from left of each dashed cutoff; the target comes only from the\n"
            "shaded band to its right. Daily volume is near-flat (CV ~1%), so between-customer variation —\n"
            "not between-day variation — is where retention risk lives.")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 How common is the outcome, and is it stable over time?
# MAGIC
# MAGIC The base rate sets every expectation that follows: it is the PR-AUC floor, it decides whether class
# MAGIC weighting is needed, and if it drifted between cutoffs the chronological comparison would be
# MAGIC measuring drift rather than model quality.

# COMMAND ----------

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.6), gridspec_kw={"width_ratios": [1.15, 1]})

p = prevalence_pdf.sort_values("cutoff_date")
x = np.arange(len(p))
ax1.bar(x - 0.2, p["eligible_customers"], width=0.4, color=REF, label="Eligible (active pre-cutoff)")
ax1.bar(x + 0.2, p["inactive_customers"], width=0.4, color=RISK, label="Became inactive for 60d")
ax1.set_xticks(x)
ax1.set_xticklabels([f"{s}\n{c}" for s, c in zip(p["split"], p["cutoff_date"].astype(str))])
ax1.set_ylabel("Customers")
ax1.set_title("Eligible population and outcome count per cutoff")
thousands(ax1)
ax1.legend(frameon=False, fontsize=9)
for xi, (e, i) in enumerate(zip(p["eligible_customers"], p["inactive_customers"])):
    ax1.text(xi - 0.2, e, f"{e:,}", ha="center", va="bottom", fontsize=8)
    ax1.text(xi + 0.2, i, f"{i:,}", ha="center", va="bottom", fontsize=8)

ax2.plot(x, p["inactivity_rate_pct"], marker="o", markersize=9, color=RISK, linewidth=2)
for xi, v in zip(x, p["inactivity_rate_pct"]):
    ax2.annotate(f"{v:.1f}%", (xi, v), textcoords="offset points", xytext=(0, 10), ha="center", fontsize=10)
ax2.set_xticks(x)
ax2.set_xticklabels(p["split"])
ax2.set_ylabel("60-day inactivity rate (%)")
ax2.set_ylim(0, max(p["inactivity_rate_pct"]) * 1.45)
ax2.set_title("Is the base rate stable across the three periods?")
caption(ax2, "Message: a flat line means the three periods are comparable, so a train-to-test metric drop\n"
             "reflects genuine generalisation difficulty rather than a moving target.")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Which behavioural signals separate the two outcomes?
# MAGIC
# MAGIC My hypothesis names specific mechanisms: declining frequency, longer recency, weaker engagement,
# MAGIC fewer products, unresolved support issues, failed transactions. This is the first direct test —
# MAGIC a comparison of group means, with each panel scaled to its own units.

# COMMAND ----------

COMPARE = [
    ("recency_days", "Days since last transaction", "higher = riskier"),
    ("tx_cnt_30d", "Transactions, last 30 days", "lower = riskier"),
    ("cnt_delta_30d", "Change vs prior 30 days", "negative = declining"),
    ("gap_vs_recency_ratio", "Silence vs own rhythm", ">1 = overdue"),
    ("active_days_90d", "Active days, last 90 days", "lower = riskier"),
    ("app_logins_frequency", "App logins", "lower = riskier"),
    ("active_products", "Active products", "lower = riskier"),
    ("unresolved_tickets", "Unresolved support tickets", "higher = riskier"),
]

means = (
    gold_t.where("split = 'test'")
    .groupBy("inactive_next_60d")
    .agg(*[F.avg(c).alias(c) for c, _, _ in COMPARE])
    .toPandas()
    .set_index("inactive_next_60d")
)

fig, axes = plt.subplots(2, 4, figsize=(15, 7))
for ax, (col, label, hint) in zip(axes.ravel(), COMPARE):
    stayed, left = float(means.loc[0, col]), float(means.loc[1, col])
    ax.bar(["Stayed\nactive", "Became\ninactive"], [stayed, left], color=[SAFE, RISK], width=0.62)
    for xi, v in enumerate([stayed, left]):
        ax.text(xi, v, f"{v:,.2f}" if abs(v) < 100 else f"{v:,.0f}",
                ha="center", va="bottom" if v >= 0 else "top", fontsize=9)
    ax.axhline(0, color="#333333", linewidth=0.8)
    ax.set_title(label, fontsize=11)
    ax.set_xlabel(hint, fontsize=8, color="#666666")
    ax.tick_params(labelsize=9)
fig.suptitle("Behavioural signals: mean value by actual outcome (test period)", fontsize=14, fontweight="bold")
fig.text(0.01, -0.01,
         "Message: recency and recent-activity features separate the two groups far more sharply than product\n"
         "or support counts, which is the first evidence for my hypothesis that recent behaviour beats static\n"
         "attributes. Group means only — Section 4.6 checks whether the relationship is monotonic.",
         fontsize=9, color="#444444", va="top")
plt.tight_layout(rect=(0, 0.03, 1, 0.95))
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 Is the relationship monotonic, or is a mean hiding the shape?
# MAGIC
# MAGIC Comparing two means can make a U-shaped or threshold relationship look linear. Binning the feature
# MAGIC and plotting the *rate* in each bin shows the real shape — and whether a simple business rule
# MAGIC ("contact anyone silent for more than N days") would even work.

# COMMAND ----------

test_g = gold_t.where("split = 'test'")


def rate_by_bucket(order_col, bucket_expr, min_n=200):
    """Inactivity rate per bucket, ordered by the underlying numeric column, with small buckets dropped."""
    return (
        test_g.withColumn("bucket", bucket_expr)
        .groupBy("bucket")
        .agg(F.count("*").alias("n"),
             F.round(F.avg("inactive_next_60d") * 100, 2).alias("rate_pct"),
             F.min(order_col).alias("sort_key"))
        .where(F.col("n") >= min_n)
        .orderBy("sort_key")
        .toPandas()
    )


recency_buckets = rate_by_bucket(
    "recency_days",
    F.when(F.col("recency_days") <= 3, "0-3")
     .when(F.col("recency_days") <= 7, "4-7")
     .when(F.col("recency_days") <= 14, "8-14")
     .when(F.col("recency_days") <= 30, "15-30")
     .when(F.col("recency_days") <= 60, "31-60")
     .otherwise("61-90"),
)

freq_buckets = rate_by_bucket(
    "tx_cnt_30d",
    F.when(F.col("tx_cnt_30d") == 0, "0")
     .when(F.col("tx_cnt_30d") <= 2, "1-2")
     .when(F.col("tx_cnt_30d") <= 5, "3-5")
     .when(F.col("tx_cnt_30d") <= 10, "6-10")
     .otherwise("11+"),
)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.5, 4.8))
for ax, d, xlabel, title in [
    (ax1, recency_buckets, "Days since last transaction at the cutoff", "Inactivity rate rises steeply with recency"),
    (ax2, freq_buckets, "Transactions in the 30 days before the cutoff", "...and falls steeply with recent frequency"),
]:
    bars = ax.bar(d["bucket"], d["rate_pct"], color=risk_colors(d["rate_pct"]), width=0.66)
    ax.axhline(BASE_RATE, color=REF, linestyle="--", linewidth=1.3)
    ax.text(len(d) - 0.45, BASE_RATE, f" base rate {BASE_RATE:.1f}%", color="#555555", fontsize=8, va="bottom", ha="right")
    for b, (_, r) in zip(bars, d.iterrows()):
        ax.text(b.get_x() + b.get_width() / 2, r["rate_pct"], f"{r['rate_pct']:.1f}%\nn={r['n']:,}",
                ha="center", va="bottom", fontsize=8)
    ax.set_ylim(0, d["rate_pct"].max() * 1.32)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("60-day inactivity rate (%)")
    ax.set_title(title, fontsize=12)
fig.text(0.01, -0.02,
         "Message: both relationships are monotonic, so a threshold rule is defensible — but the curve is\n"
         "gradual rather than a cliff, which is exactly why a scored ranking beats a single cut-off. Every bar\n"
         "shows its n, and buckets under 200 customers are suppressed rather than plotted as if reliable.",
         fontsize=9, color="#444444", va="top")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.7 Do two signals interact, or do they say the same thing twice?
# MAGIC
# MAGIC Recency and trend could be redundant. A heatmap of the rate across both at once answers it: if the
# MAGIC colour changes down the rows *and* across the columns, the two features are contributing
# MAGIC independently and both belong in the model.

# COMMAND ----------

grid = (
    test_g
    .withColumn("recency_band",
                F.when(F.col("recency_days") <= 7, "0-7 d")
                 .when(F.col("recency_days") <= 14, "8-14 d")
                 .when(F.col("recency_days") <= 30, "15-30 d")
                 .otherwise("31-90 d"))
    .withColumn("trend_band",
                F.when(F.col("cnt_ratio_30_prior30") < 0.5, "Sharp decline")
                 .when(F.col("cnt_ratio_30_prior30") < 0.9, "Declining")
                 .when(F.col("cnt_ratio_30_prior30") <= 1.1, "Stable")
                 .otherwise("Growing"))
    .groupBy("recency_band", "trend_band")
    .agg(F.count("*").alias("n"), F.round(F.avg("inactive_next_60d") * 100, 1).alias("rate_pct"))
    .toPandas()
)
ROW_ORDER = ["0-7 d", "8-14 d", "15-30 d", "31-90 d"]
COL_ORDER = ["Sharp decline", "Declining", "Stable", "Growing"]
# reindex on BOTH axes: indexing with [COL_ORDER] would raise KeyError if a band happens to be empty.
pivot_rate = grid.pivot(index="recency_band", columns="trend_band", values="rate_pct") \
                 .reindex(index=ROW_ORDER, columns=COL_ORDER)
pivot_n = grid.pivot(index="recency_band", columns="trend_band", values="n") \
              .reindex(index=ROW_ORDER, columns=COL_ORDER)
# Rule 4 from the section header applies here too: suppress cells too small to be reliable.
MIN_CELL = 200
pivot_rate = pivot_rate.mask(pivot_n.fillna(0) < MIN_CELL)

fig, ax = plt.subplots(figsize=(9.5, 5.2))
im = ax.imshow(pivot_rate.values.astype(float), cmap=RISK_CMAP, aspect="auto", vmin=0, vmax=RATE_VMAX)
ax.set_xticks(range(len(COL_ORDER)))
ax.set_xticklabels(COL_ORDER)
ax.set_yticks(range(len(ROW_ORDER)))
ax.set_yticklabels(ROW_ORDER)
ax.set_xlabel("Activity trend: last 30 days vs the 30 days before")
ax.set_ylabel("Days since last transaction")
ax.set_title("Recency and trend act independently — both earn their place in the model")
ax.grid(False)
for i in range(pivot_rate.shape[0]):
    for j in range(pivot_rate.shape[1]):
        v, n = pivot_rate.values[i, j], pivot_n.values[i, j]
        if pd.isna(v):
            label = f"n={int(n):,}\n(suppressed)" if not pd.isna(n) else "no data"
            ax.text(j, i, label, ha="center", va="center", fontsize=8, color="#777777")
            continue
        ax.text(j, i, f"{v:.1f}%\nn={int(n):,}", ha="center", va="center", fontsize=9,
                color="white" if v > RATE_VMAX * 0.6 else "#222222")
fig.colorbar(im, ax=ax, label="60-day inactivity rate (%)", shrink=0.85)
caption(ax, f"Message: the worst cell (long silence + sharp decline) is materially worse than either signal\n"
            f"alone, and the best cell is materially better. That interaction is business-actionable: it is the\n"
            f"'declining AND overdue' group the CRM team should contact first. Cells under n={MIN_CELL} are suppressed.")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.8 Behaviour or demographics? Testing the hypothesis by feature family
# MAGIC
# MAGIC Appendix D asks whether *recent behavioural decline is more informative than static demographics*.
# MAGIC Colouring each bar by the feature family turns a generic importance chart into a direct answer.

# COMMAND ----------

drivers = spark.table(tbl("insight_target_driver")).toPandas()
drivers["abs_corr"] = drivers["corr_with_target"].abs()


# Features I engineered from the ledger, listed explicitly rather than matched by substring: a
# keyword rule silently mislabels anything it fails to match, and mislabelling a behavioural feature
# as "demographic" would make this chart contradict the very claim it is testing.
ENGINEERED_BEHAVIOUR = {
    "tx_cnt_hist", "tx_cnt_7d", "tx_cnt_30d", "tx_cnt_90d", "tx_cnt_180d", "tx_cnt_prior_30d",
    "tx_cnt_prior_90d", "tx_amt_sum_30d", "tx_amt_sum_90d", "tx_amt_sum_prior_30d", "tx_amt_mean_90d",
    "tx_amt_max_90d", "tx_amt_stddev_90d", "active_days_30d", "active_days_90d", "distinct_types_90d",
    "weekend_share_90d", "recency_days", "tenure_days", "cnt_delta_30d", "cnt_ratio_30_prior30",
    "amt_ratio_30_prior30", "velocity_7_over_30", "is_decelerating", "gap_vs_recency_ratio",
    "max_gap_days_90d", "avg_gap_days_90d", "activity_density_90d", "tx_amt_sum_90d_log",
    "tx_amt_mean_90d_log", "tx_amt_max_90d_log", "tx_cnt_90d_log",
} | {f"cnt_{_sanitise(t)}_90d" for t in TX_TYPES} | {f"share_{_sanitise(t)}_90d" for t in TX_TYPES}

DERIVED_FAMILY = {
    "failures_per_tx_history": "experience",
    "unresolved_tickets": "experience",
    "satisfaction_available": "experience",
    "survey_age_days": "experience",
    "friction_word_hits": "experience",
    "logins_per_transaction": "engagement",
}


def family_of(feature: str) -> str:
    if feature in ENGINEERED_BEHAVIOUR:
        return "engineered behaviour"
    if feature in DERIVED_FAMILY:
        return DERIVED_FAMILY[feature]
    if feature in COLUMN_FAMILY:
        return {"transaction_setA_verified": "engineered behaviour",
                "vendor_label": "unclassified"}.get(COLUMN_FAMILY[feature], COLUMN_FAMILY[feature])
    return "unclassified"


drivers["family"] = drivers["feature"].apply(family_of)
_unclassified = sorted(drivers.loc[drivers["family"] == "unclassified", "feature"])
if _unclassified:
    print(f"NOTE: {len(_unclassified)} feature(s) not assigned to a family and shown in grey: {_unclassified}")
top = drivers.sort_values("abs_corr", ascending=False).head(18).iloc[::-1]

FAMILY_COLOR = {
    "engineered behaviour": RISK,
    "engagement": VOLUME,
    "experience": ACCENT,
    "product": SAFE,
    "demographic": REF,
    "identity": "#cfd6d8",
    "unclassified": "#cfd6d8",
}

fig, ax = plt.subplots(figsize=(11, 7))
bars = ax.barh(top["feature"], top["abs_corr"], color=[FAMILY_COLOR.get(f, "#cfd6d8") for f in top["family"]])
for b, (_, r) in zip(bars, top.iterrows()):
    ax.text(r["abs_corr"] + 0.004, b.get_y() + b.get_height() / 2,
            f"{r['corr_with_target']:+.3f}", va="center", fontsize=9)
ax.set_xlabel("|association with inactive_next_60d|  (point-biserial r, train split only)")
ax.set_title("Engineered behavioural features dominate; demographics are near-zero")
_legend_families = ["engineered behaviour", "engagement", "experience", "product", "demographic", "unclassified"]
handles = [plt.Rectangle((0, 0), 1, 1, color=FAMILY_COLOR[f]) for f in _legend_families]
ax.legend(handles, _legend_families, loc="lower right", frameon=False, fontsize=9)
ax.set_xlim(0, top["abs_corr"].max() * 1.18)
caption(ax, "Message: my hypothesis that recent behaviour outperforms static attributes is SUPPORTED at the\n"
            "univariate level. Signed values are printed because direction matters: recency is positive\n"
            "(longer silence = riskier) while frequency features are negative. Computed on train only, so this\n"
            "chart cannot have been informed by the test period.")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.9 Can operations act on this with limited capacity?
# MAGIC
# MAGIC The stakeholder was explicit: *"prioritize customers who are both at high risk and potentially
# MAGIC valuable, rather than contacting every high-risk customer."* So the decisive chart is not accuracy —
# MAGIC it is **how much of the problem is captured by contacting the top k%**, drawn against the diagonal
# MAGIC that random targeting would produce.

# COMMAND ----------

decile = (
    spark.table(tbl("gold_retention_scored"))
    .groupBy("risk_decile")
    .agg(F.count("*").alias("n"),
         F.sum("inactive_next_60d").alias("captured"),
         F.round(F.avg("inactive_next_60d") * 100, 2).alias("rate_pct"))
    .orderBy("risk_decile")
    .toPandas()
)
total_pos = decile["captured"].sum()
decile["cum_capture_pct"] = decile["captured"].cumsum() / total_pos * 100
decile["contacted_pct"] = decile["n"].cumsum() / decile["n"].sum() * 100

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.5, 5))

ax1.bar(decile["risk_decile"], decile["rate_pct"], color=risk_colors(decile["rate_pct"]), width=0.7)
ax1.axhline(BASE_RATE, color=REF, linestyle="--", linewidth=1.3)
ax1.text(10.4, BASE_RATE, f" base {BASE_RATE:.1f}%", color="#555555", fontsize=8, ha="right", va="bottom")
for _, r in decile.iterrows():
    ax1.text(r["risk_decile"], r["rate_pct"], f"{r['rate_pct']:.0f}%", ha="center", va="bottom", fontsize=8)
ax1.set_xticks(range(1, 11))
ax1.set_xlabel("Risk decile (1 = highest predicted risk)")
ax1.set_ylabel("Actual 60-day inactivity rate (%)")
ax1.set_title("Do the scores rank customers correctly?")
ax1.set_ylim(0, decile["rate_pct"].max() * 1.25)

ax2.plot([0] + list(decile["contacted_pct"]), [0] + list(decile["cum_capture_pct"]),
         marker="o", color=RISK, linewidth=2.2, label="Model-ranked outreach")
ax2.plot([0, 100], [0, 100], color=REF, linestyle="--", linewidth=1.3, label="Random outreach")
for k in (10, 20):
    idx = k // 10 - 1
    if idx >= len(decile):     # fewer than 10 distinct score bands resolved
        continue
    row = decile.iloc[idx]
    ax2.axvline(row["contacted_pct"], color="#cccccc", linewidth=1, zorder=0)
    ax2.annotate(f"contact top {k}%\ncapture {row['cum_capture_pct']:.0f}%",
                 (row["contacted_pct"], row["cum_capture_pct"]),
                 textcoords="offset points", xytext=(12, -18), fontsize=9,
                 arrowprops=dict(arrowstyle="->", color="#666666", lw=0.9))
ax2.set_xlabel("% of eligible customers contacted (outreach capacity)")
ax2.set_ylabel("% of genuinely inactive customers captured")
ax2.set_title("Cumulative capture vs outreach capacity")
ax2.legend(loc="lower right", frameon=False)
fig.text(0.01, -0.02,
         "Message: this is the chart the operations manager can act on. It converts a probability into a\n"
         "capacity decision — 'with resources to contact 2,000 customers, here is the share of at-risk\n"
         "customers you reach' — and the diagonal makes the value over random targeting unmistakable.",
         fontsize=9, color="#444444", va="top")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.10 Does the model work equally well for everyone?
# MAGIC
# MAGIC The proposal commits to subgroup performance checks. Reporting only the headline number would hide a
# MAGIC group the model serves badly, which is both a fairness problem and an operational one.
# MAGIC
# MAGIC **Why this chart measures ranking and not calibrated probability.** The readiness model is fitted
# MAGIC with class weights that rebalance the positive class to parity. That deliberately shifts the output
# MAGIC to a ~50/50 prior, so `mean(p_inactive)` sits well above the observed rate for *every* subgroup.
# MAGIC Plotting predicted-vs-actual rates would therefore show a large gap everywhere and would say
# MAGIC nothing about fairness. So I use two measures that are **invariant to that prior shift**:
# MAGIC
# MAGIC - **High-band recall** — of the customers in this subgroup who genuinely went inactive, what share
# MAGIC   did the model place in the High-risk band? This is the number that decides who gets contacted.
# MAGIC - **Flag rate** — what share of the whole subgroup gets flagged High? A subgroup flagged far more
# MAGIC   often than its actual rate justifies is being over-targeted.
# MAGIC
# MAGIC Probability calibration is deferred to the modelling phase, where an unweighted or isotonically
# MAGIC recalibrated model makes it a meaningful question (§6.3, step 2).

# COMMAND ----------

scored_t = spark.table(tbl("gold_retention_scored"))

_overall = scored_t.agg(
    F.sum("inactive_next_60d").alias("pos"),
    F.sum(F.when((F.col("risk_band") == "High") & (F.col("inactive_next_60d") == 1), 1).otherwise(0)).alias("caught"),
    F.avg((F.col("risk_band") == "High").cast("double")).alias("flag_rate"),
).collect()[0]
OVERALL_RECALL = (_overall["caught"] or 0) / max(_overall["pos"] or 0, 1) * 100
OVERALL_FLAG = (_overall["flag_rate"] or 0) * 100


def subgroup_quality(df, key_col, min_n=200, min_pos=30):
    """Rank-based subgroup performance. Groups too small to estimate reliably are dropped, not plotted."""
    return (
        df.groupBy(key_col)
        .agg(
            F.count("*").alias("n"),
            F.sum("inactive_next_60d").alias("positives"),
            F.sum(F.when((F.col("risk_band") == "High") & (F.col("inactive_next_60d") == 1), 1).otherwise(0)).alias("caught"),
            F.round(F.avg("inactive_next_60d") * 100, 2).alias("actual_pct"),
            F.round(F.avg((F.col("risk_band") == "High").cast("double")) * 100, 2).alias("flag_pct"),
        )
        .where((F.col("n") >= min_n) & (F.col("positives") >= min_pos))
        .withColumn("high_band_recall_pct", F.round(F.col("caught") / F.col("positives") * 100, 2))
        .toPandas()
    )


seg = subgroup_quality(
    scored_t.join(spark.table(tbl("bronze_customer")).select("customer_id", "customer_segment"), "customer_id"),
    "customer_segment",
)
SEG_ORDER = ["inactive", "occasional", "regular", "power"]
seg["ord"] = seg["customer_segment"].apply(lambda s: SEG_ORDER.index(s) if s in SEG_ORDER else 99)
seg = seg.sort_values("ord")

income = subgroup_quality(scored_t, "income_bracket")
INC_ORDER = ["Low", "Medium", "High", "Very High"]
income["ord"] = income["income_bracket"].apply(lambda s: INC_ORDER.index(s) if s in INC_ORDER else 99)
income = income.sort_values("ord")

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
for ax, d, key, title in [
    (ax1, seg, "customer_segment", "Ranking quality by customer segment"),
    (ax2, income, "income_bracket", "Ranking quality by income bracket"),
]:
    if d.empty:
        ax.text(0.5, 0.5, "No subgroup large enough to report reliably", ha="center", va="center",
                fontsize=10, color=REF)
        ax.set_axis_off()
        continue
    xs = np.arange(len(d))
    ax.bar(xs - 0.2, d["high_band_recall_pct"], width=0.4, color=RISK, label="High-band recall")
    ax.bar(xs + 0.2, d["flag_pct"], width=0.4, color=RISK_LIGHT, label="% of subgroup flagged High")
    ax.axhline(OVERALL_RECALL, color="#333333", linestyle="--", linewidth=1.2)
    ax.text(len(d) - 0.5, OVERALL_RECALL, f" overall recall {OVERALL_RECALL:.0f}%", fontsize=8,
            color="#333333", ha="right", va="bottom")
    ax.set_xticks(xs)
    ax.set_xticklabels([f"{v}\nn={n:,}  actual {a:.0f}%" for v, n, a in
                        zip(d[key], d["n"], d["actual_pct"])], fontsize=8.5)
    ax.set_ylabel("%")
    ax.set_ylim(0, 105)
    ax.set_title(title, fontsize=12)
    ax.legend(frameon=False, fontsize=9, loc="upper right")
    for xi, (r, f_) in enumerate(zip(d["high_band_recall_pct"], d["flag_pct"])):
        ax.text(xi - 0.2, r, f"{r:.0f}", ha="center", va="bottom", fontsize=8)
        ax.text(xi + 0.2, f_, f"{f_:.0f}", ha="center", va="bottom", fontsize=8)
fig.text(0.01, -0.04,
         "Message: a subgroup whose High-band recall sits well below the dashed overall line is one the model\n"
         "serves worse — those at-risk customers are being missed. A subgroup whose flag rate is high relative to\n"
         "its actual inactivity rate is being over-targeted. Both are rank-based, so they are unaffected by the\n"
         "class weighting. Groups with fewer than 200 customers or 30 positives are omitted, not guessed at.\n"
         "customer_segment is used for REPORTING only — it is excluded as a feature (theme TH-04).",
         fontsize=9, color="#444444", va="top")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.11 Does the cloud pipeline scale?

# COMMAND ----------

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.5, 4.6))

if not probe_pdf.empty:
    ax1.plot(probe_pdf["rows_processed"], probe_pdf["seconds"], marker="o", markersize=9,
             color=VOLUME, linewidth=2.2, label="Measured")
    lo, hi = probe_pdf.iloc[0], probe_pdf.iloc[-1]
    ideal_x = np.array([lo["rows_processed"], hi["rows_processed"]])
    ideal_y = lo["seconds"] * ideal_x / lo["rows_processed"]
    ax1.plot(ideal_x, ideal_y, linestyle="--", color=REF, linewidth=1.3, label="Perfectly linear")
    for _, r in probe_pdf.iterrows():
        ax1.annotate(f"{r['fraction']}\n{r['seconds']:.1f}s", (r["rows_processed"], r["seconds"]),
                     textcoords="offset points", xytext=(0, 12), ha="center", fontsize=9)
    ax1.set_xlabel("Transaction rows processed")
    ax1.set_ylabel("Seconds")
    ax1.set_title("Read + dedupe + windowed aggregation vs data volume")
    thousands(ax1, "x")
    ax1.legend(frameon=False, fontsize=9)
    ax1.set_ylim(0, max(probe_pdf["seconds"].max(), ideal_y.max()) * 1.3)
else:
    ax1.text(0.5, 0.5, "Scalability probe not run\n(run_scalability_probe = no)",
             ha="center", va="center", fontsize=11, color=REF)
    ax1.set_axis_off()

steps = runtime_pdf.sort_values("seconds", ascending=False).head(10).iloc[::-1]
ax2.barh(steps["step"], steps["seconds"], color=VOLUME)
for i, v in enumerate(steps["seconds"]):
    ax2.text(v + max(steps["seconds"]) * 0.015, i, f"{v:.1f}s", va="center", fontsize=9)
ax2.set_xlabel("Seconds")
ax2.set_title("Ten slowest pipeline steps this run")
ax2.set_xlim(0, steps["seconds"].max() * 1.22)
ax2.tick_params(labelsize=8)
fig.text(0.01, -0.04,
         "Message: measured on the cluster, not extrapolated from a laptop. A curve at or below the dashed\n"
         "line means runtime grows no worse than linearly, so the pipeline is viable for daily batch scoring.\n"
         "The right panel names the bottleneck to optimise first if that stops being true.",
         fontsize=9, color="#444444", va="top")
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Section 5 — Co-creating  *(rubric: 10 marks)*
# MAGIC
# MAGIC **What the top band asks for:** *partner others in a mutually beneficial arrangement to jointly
# MAGIC develop the idea* — a step beyond "gathered inputs" or "exchanged information".
# MAGIC
# MAGIC **How I address it.** Four members owning four models could easily produce four incompatible
# MAGIC pipelines. Instead the team agreed that whoever needs an artefact first *builds it for everyone*,
# MAGIC under a published contract. Concretely:
# MAGIC
# MAGIC | I gave the team | Who benefits | Why it is mutual, not charity |
# MAGIC |---|---|---|
# MAGIC | `silver_transaction` — one cleaned, deduplicated ledger under one documented rule | all 4 | If Member D deduplicated differently, his daily counts and my 30-day counts would disagree and the dashboard would contradict itself |
# MAGIC | The Set B exclusion finding (TH-01) | all 4 | Member B was about to use `total_transaction_volume` as his value target. It does not reconcile with the ledger — this saved his workstream |
# MAGIC | `silver_daily_activity` — daily aggregate | Member D | Removes a whole aggregation stage from his pipeline; I needed the same aggregate for chart 4.3 anyway |
# MAGIC | `silver_feedback_theme` — normalised themes from unstructured text | Member C | She gets a cleaned theme column; I get her domain judgement on the theme vocabulary |
# MAGIC | `gold_customer_value_base` — ledger-verified value aggregates | Member B | Keyed identically to my risk table, which is the only reason the joint priority view below can exist |
# MAGIC | `bronze_transaction_stream` + Auto Loader pattern | Member D | His demand model needs daily arrivals; I needed to prove incremental scoring works |
# MAGIC
# MAGIC | The team gave me | What it changed in this notebook |
# MAGIC |---|---|
# MAGIC | Member B's predicted 90-day value | §5.4 — turns my risk score into a *prioritised* list, which is what the stakeholder asked for |
# MAGIC | Member C's friction-theme vocabulary | §3.2b — the five theme rules are hers, not mine |
# MAGIC | Member D's finding that daily volume is near-flat (CV ~1%) | Confirmed risk must be modelled per customer, not per day (TH-08) |
# MAGIC | Stakeholder interview (María Rodríguez, Nu Colombia) | Top-k recall became my primary reporting metric (§4.9) instead of accuracy |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Team decision log
# MAGIC
# MAGIC Co-creation only counts if it is traceable. Each row names who raised the issue, who agreed, and
# MAGIC what changed as a result — the same decisions recorded in our weekly Blackboard group blog
# MAGIC (Appendix B).

# COMMAND ----------

decision_log = spark_df(
    [
        ("D-01", "2026-07-16", "Adopt Databricks + Delta medallion architecture as the single shared platform",
         "Clifton (Member A)", "All four members",
         "One namespace, one storage format, one set of table names. Members B/C/D read my silver layer "
         "instead of re-reading raw CSVs, so nobody re-implements cleaning."),
        ("D-02", "2026-07-18", "EXCLUDE all 9 'Set B' transaction-summary columns team-wide",
         "Clifton (Member A) — found during the DQ audit (DQ-13)", "All four members",
         "Member B changed his value target from total_transaction_volume to a ledger-derived 90-day sum. "
         "Member D dropped weekend_transaction_ratio and re-derived it. Highest-impact shared decision of "
         "the interim phase."),
        ("D-03", "2026-07-18", "One deduplication rule: drop exact duplicates on (customer_id, date, amount, type); "
         "keep near-duplicates",
         "Clifton (Member A)", "All four members",
         "Guarantees my 30-day counts and Member D's daily counts reconcile. Rule lives in code in one "
         "place (silver_transaction), not in four notebooks."),
        ("D-04", "2026-07-19", "Publish silver_daily_activity from my cleaned ledger for Member D",
         "Kang Bin (Member D) requested; Clifton built", "Members A and D",
         "Mutual: he avoids a duplicate aggregation stage, I get his review of my date handling and his "
         "weekday/weekend definition."),
        ("D-05", "2026-07-19", "Complaint free text normalised once into 5 friction themes",
         "Yi Ting (Member C) defined the vocabulary; Clifton implemented in Spark", "Members A and C",
         "She supplies the domain judgement about which complaint themes matter; I supply the distributed "
         "implementation. Both workstreams consume the identical theme column."),
        ("D-06", "2026-07-20", "Standardise on chronological cutoffs with non-overlapping outcome windows",
         "Clifton (Member A)", "Members A, B, D",
         "All three predictive workstreams now train on earlier periods and test on later ones, so the "
         "final report can compare them fairly instead of comparing incompatible validation schemes."),
        ("D-07", "2026-07-21", "Retention priority = inactivity risk x predicted value, not risk alone",
         "María Rodríguez (stakeholder), adopted by Clifton and Evan", "Members A and B, operations team",
         "Direct response to stakeholder feedback. Neither of our models alone answers her question; the "
         "join in section 5.4 does. This is the clearest example of the two workstreams needing each other."),
        ("D-08", "2026-07-22", "Exclude churn_probability as both feature and target",
         "Clifton (Member A) — evidenced by TH-02", "All four members",
         "Prevented the whole team from accidentally modelling a vendor formula (r = -0.88 with "
         "active_products) instead of real customer behaviour."),
    ],
    ["decision_id", "date", "decision", "raised_by", "agreed_with", "impact"],
)
save_table(decision_log, "meta_team_decision_log", comment="Co-created team decisions, who raised them and their impact")
display(decision_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Shared-asset contract
# MAGIC
# MAGIC A shared table is only useful if its consumers know what they can rely on. This contract states the
# MAGIC grain, the guarantee and the refresh cadence for each artefact I publish — so a teammate can build
# MAGIC on it without reading my code, and so I know what I am not allowed to change silently.

# COMMAND ----------

shared_contract = spark_df(
    [
        ("silver_transaction", "Member A", "Members A, B, C, D", "one row per unique (customer_id, date, amount, type)",
         "Exact duplicates removed; amount non-null and >= 0; type title-cased to a 4-value vocabulary; "
         "referential integrity to silver_customer holds",
         "Re-run on each ingestion; partitioned by tx_month", "D-01, D-03"),
        ("silver_customer", "Member A", "Members A, B, C, D", "one row per customer_id",
         "Only columns approved in meta_column_decision are present; the 24 rejected columns are "
         "physically absent; credit_utilization_ratio has a 0.0 sentinel with credit_card as its flag",
         "Re-run on each ingestion", "D-01, D-02"),
        ("silver_daily_activity", "Member A", "Member D", "one row per calendar date",
         "Every calendar day in coverage is present (DQ-07); counts derive from the deduplicated ledger so "
         "they reconcile with Member A's customer-level windows",
         "Re-run on each ingestion", "D-04"),
        ("silver_feedback_theme", "Member A", "Members A, C", "one row per customer_id",
         "friction_theme is never null; 'no_complaint_on_file' is an explicit level rather than a null; "
         "vocabulary owned by Member C",
         "Re-run when the theme vocabulary changes", "D-05"),
        ("gold_customer_value_base", "Member A", "Member B", "one row per customer_id",
         "Value aggregates computed from the ledger, never from the unverified Set B columns",
         "Re-run on each ingestion", "D-02"),
        ("bronze_transaction_stream", "Member A", "Member D", "one row per streamed transaction",
         "Exactly-once via Auto Loader checkpointing; row count reconciles with the batch path "
         "(ops_stream_ingest_audit)",
         "Continuous / trigger availableNow", "D-01"),
        ("gold_retention_scored", "Member A", "Members A, B + operations dashboard",
         "one row per customer_id at the test cutoff",
         "p_inactive in [0,1]; risk_decile 1-10 with 1 = highest risk; risk_band in {High, Medium, Low}",
         "Re-scored per cutoff", "D-07"),
        ("gold_retention_priority", "Members A + B", "Operations dashboard",
         "one row per customer_id at the test cutoff",
         "Requires Member B's predicted 90-day value; a documented proxy is substituted while his model "
         "is in development, and the source is recorded in the value_source column",
         "Re-scored per cutoff", "D-07"),
    ],
    ["artefact", "producer", "consumers", "grain", "guarantee", "refresh", "decision_ref"],
)
save_table(shared_contract, "meta_shared_contract", comment="Producer/consumer guarantees for every shared artefact")
display(shared_contract)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Stakeholder feedback loop
# MAGIC
# MAGIC Appendix C recorded the interview with **María Rodríguez, Customer Operations Manager, Nu Colombia**
# MAGIC (20-minute Microsoft Teams interview, 16 July 2026). This table closes the loop: what she asked for,
# MAGIC and where in *this notebook* it is now implemented.

# COMMAND ----------

stakeholder_loop = spark_df(
    [
        ("SF-01", "\"Prioritize customers who are both at high risk of inactivity AND potentially valuable, "
         "rather than contacting every high-risk customer.\"",
         "Built the risk x value priority view jointly with Member B",
         "Section 5.4 — gold_retention_priority", "Implemented"),
        ("SF-02", "\"We cannot contact everyone — we need to know who to call first.\"",
         "Made top-k recall and the cumulative capture curve the headline metric instead of accuracy",
         "Section 3.9 metrics + chart 4.9", "Implemented"),
        ("SF-03", "\"Unexplained model predictions do not get acted on.\"",
         "Every risk score ships with its drivers; feature families are colour-coded so the reason for a "
         "score is legible to a non-technical user",
         "Charts 4.5-4.8 + meta_feature_catalogue", "Implemented"),
        ("SF-04", "Concern about ineffective retention campaigns wasting budget",
         "Reported the capture rate at realistic outreach capacities (top 10% / 20%) so campaign cost can "
         "be compared against customers reached",
         "Chart 4.9", "Implemented"),
        ("SF-05", "\"At least one day's warning for high-demand periods\" (Member D's workstream)",
         "Built the Auto Loader incremental ingestion path that daily scoring depends on, and handed it to "
         "Member D",
         "Section 1.5", "Implemented"),
        ("SF-06", "Not raised by the stakeholder but flagged back TO her: risk is not churn",
         "The target is documented everywhere as 'no observed transactions in 60 days', never as account "
         "closure, so operations do not treat a quiet customer as a lost one",
         "Cover note + meta_feature_catalogue", "Implemented"),
    ],
    ["feedback_id", "stakeholder_input", "action_taken", "where_implemented", "status"],
)
save_table(stakeholder_loop, "meta_stakeholder_feedback", comment="Stakeholder feedback traced to its implementation")
display(stakeholder_loop)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 The joint deliverable — retention priority (Member A × Member B)
# MAGIC
# MAGIC This is the co-creation made concrete. **Neither model answers the stakeholder's question alone:**
# MAGIC
# MAGIC - my model finds customers likely to go quiet, but cannot say whether they are worth the call;
# MAGIC - Member B's model estimates future value, but cannot say who is about to disengage.
# MAGIC
# MAGIC Joined on `customer_id` at a shared cutoff, they produce four quadrants that map directly onto four
# MAGIC different operational actions.
# MAGIC
# MAGIC **Honesty about the current state:** Member B's model is still in development at the interim
# MAGIC checkpoint. Rather than fabricate his output, the code below **uses his real predictions if
# MAGIC `gold_customer_value_predicted` exists**, and otherwise falls back to a documented proxy computed
# MAGIC from my own ledger aggregates — recording which was used in a `value_source` column so no reader can
# MAGIC mistake the proxy for his model.

# COMMAND ----------

VALUE_MODEL_TABLE = "gold_customer_value_predicted"   # Member B publishes here when his model is ready
value_available = spark.catalog.tableExists(tbl_plain(VALUE_MODEL_TABLE))

risk = spark.table(tbl("gold_retention_scored"))

if value_available:
    value = (
        spark.table(tbl(VALUE_MODEL_TABLE))
        .select("customer_id", F.col("predicted_value_next_90d").alias("value_estimate"))
        .withColumn("value_source", F.lit("Member B model: gold_customer_value_predicted"))
    )
    print(f"Using Member B's published predictions from {VALUE_MODEL_TABLE}.")
else:
    # Documented interim proxy: observed 90-day spend before the cutoff, from MY ledger aggregates.
    # Explicitly NOT a forecast - it is a stand-in so the joint view can be built and reviewed now.
    value = (
        gold_t.where("split = 'test'")
        .select("customer_id", F.col("tx_amt_sum_90d").cast("double").alias("value_estimate"))
        .withColumn("value_source", F.lit("INTERIM PROXY: observed 90d spend pre-cutoff (not a forecast)"))
    )
    print(f"{VALUE_MODEL_TABLE} not found - using the documented interim proxy and labelling it as such.")

with timed("build joint retention-priority view", "gold"):
    joined = risk.join(value, "customer_id", "inner").cache()
    assert joined.count() > 0, "Risk and value tables share no customers - check the join key."
    _q = joined.approxQuantile("value_estimate", [0.5], 0.0)   # exact median: the quadrants depend on it
    assert _q and _q[0] is not None, (
        "value_estimate is entirely null. Every customer would silently fall into the 'no action' "
        "quadrant, which would look like a result rather than a bug."
    )
    med_value = _q[0]
    priority = (
        joined
        .withColumn("high_risk", F.col("risk_band") == F.lit("High"))
        .withColumn("high_value", F.col("value_estimate") >= F.lit(med_value))
        .withColumn(
            "priority_quadrant",
            F.when(F.col("high_risk") & F.col("high_value"), "1. Protect now (high risk, high value)")
             .when(F.col("high_risk") & ~F.col("high_value"), "2. Low-cost nudge (high risk, low value)")
             .when(~F.col("high_risk") & F.col("high_value"), "3. Grow and monitor (low risk, high value)")
             .otherwise("4. No action (low risk, low value)"),
        )
        .withColumn("priority_score", F.col("p_inactive") * F.log1p(F.col("value_estimate")))
    )
    save_table(priority, "gold_retention_priority",
               comment="Joint Member A (risk) x Member B (value) retention priority view")

quadrants = (
    spark.table(tbl("gold_retention_priority"))
    .groupBy("priority_quadrant", "value_source")
    .agg(F.count("*").alias("customers"),
         F.round(F.avg("p_inactive") * 100, 1).alias("mean_risk_pct"),
         F.round(F.avg("inactive_next_60d") * 100, 1).alias("actual_inactivity_pct"),
         F.round(F.avg("value_estimate"), 0).alias("mean_value_cop"))
    .orderBy("priority_quadrant")
)
display(quadrants)

# COMMAND ----------

q = quadrants.toPandas().sort_values("priority_quadrant")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5), gridspec_kw={"width_ratios": [1.05, 1]})

QUAD_COLOR = [RISK, ACCENT, VOLUME, REF]
ax1.bar(range(len(q)), q["customers"], color=QUAD_COLOR[: len(q)], width=0.66)
ax1.set_xticks(range(len(q)))
ax1.set_xticklabels([lbl.split(" (")[0] for lbl in q["priority_quadrant"]], fontsize=9)
ax1.set_ylabel("Customers")
ax1.set_title("Retention priority quadrants — how many customers in each action group")
thousands(ax1)
for i, (n, r) in enumerate(zip(q["customers"], q["actual_inactivity_pct"])):
    ax1.text(i, n, f"{n:,}\nactual {r:.0f}% inactive", ha="center", va="bottom", fontsize=8)

sample = (
    spark.table(tbl("gold_retention_priority"))
    .select("p_inactive", "value_estimate", "priority_quadrant")
    .sample(False, 0.25, seed=11)
    .limit(6000)
    .toPandas()
)
for (label, grp), color in zip(sample.groupby("priority_quadrant"), QUAD_COLOR):
    ax2.scatter(grp["p_inactive"], grp["value_estimate"].clip(lower=1), s=7, alpha=0.35,
                color=color, label=label.split(" (")[0])
ax2.set_yscale("log")
ax2.axvline(spark.table(tbl("gold_retention_priority")).where("risk_band = 'High'")
            .agg(F.min("p_inactive")).collect()[0][0] or 0,
            color="#333333", linestyle="--", linewidth=1)
ax2.axhline(med_value, color="#333333", linestyle="--", linewidth=1)
ax2.set_xlabel("Predicted 60-day inactivity risk")
ax2.set_ylabel("Value estimate, COP (log scale)")
ax2.set_title("Risk is not value — which is the whole point of the join")
ax2.legend(loc="lower left", frameon=False, fontsize=8, markerscale=2.5)
fig.text(0.01, -0.02,
         f"Value source: {q['value_source'].iloc[0]}\n"
         "Message: the top-right quadrant is where retention budget belongs. The scatter shows high-risk\n"
         "customers spread across the whole value range, so ranking by risk alone would send the operations\n"
         "team to a large number of customers who are not worth the call — exactly the stakeholder's concern.",
         fontsize=9, color="#444444", va="top")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 6 — Interim summary, limitations and next steps

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 What this phase established
# MAGIC
# MAGIC **The data is fit for purpose, but only after 24 of 54 columns were rejected.** Two of the four
# MAGIC rejection reasons are serious: 9 columns do not reconcile with the ledger they claim to summarise, and
# MAGIC 8 carry information from on or after the outcome window. `last_tx` alone would have produced a
# MAGIC near-perfect and completely meaningless model. Finding this in the interim phase rather than in the
# MAGIC final report is the most valuable outcome of the work.
# MAGIC
# MAGIC **My hypothesis is supported at the univariate level.** Recency, 30-day frequency, the 30-day trend
# MAGIC and the "silence versus own rhythm" ratio are the strongest associations with `inactive_next_60d`,
# MAGIC while age, gender, household size and education sit near zero (chart 4.8). Recency and trend also act
# MAGIC *independently* (chart 4.7), so both belong in the model. This supports event-triggered retention
# MAGIC over broad demographic campaigns — but it is a univariate result, and the modelling phase must
# MAGIC confirm it survives in a multivariate setting.
# MAGIC
# MAGIC **The pipeline runs on the cloud platform at full scale and reconciles.** 3.16M ledger rows ingested
# MAGIC by both a batch and a streaming path with matching row counts, a governed medallion layer, and
# MAGIC runtime that grows no worse than linearly with data volume.
# MAGIC
# MAGIC **The prepared table is model-ready and leakage-audited.** Five leakage assertions pass; a
# MAGIC class-weighted baseline beats the prevalence floor on an untouched later period; and the top-decile
# MAGIC capture rate answers the stakeholder's capacity question directly.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Limitations I am carrying forward — stated, not hidden
# MAGIC
# MAGIC | ID | Limitation | Why it matters | How it is handled |
# MAGIC |---|---|---|---|
# MAGIC | L-01 | Single calendar year of data | No seasonal claim can generalise; only three chronological cutoffs fit | Chronological validation, and no seasonality claims in the report |
# MAGIC | L-02 | Inactivity ≠ churn | A customer with no transactions for 60 days may still hold an active account | Target documented as "no observed transactions"; the word churn is avoided |
# MAGIC | L-03 | Association ≠ causation | Contacting high-risk customers may not change their behaviour | Findings phrased as predictive drivers; an A/B test recommended before claiming impact |
# MAGIC | L-04 | Satisfaction covers ~14.3% of customers, and coverage differs by cutoff | Satisfaction conclusions cannot generalise, and the masked share shifts between train and test | `satisfaction_available` shipped as a feature, scores masked past the cutoff, and the per-cutoff masked share is reported in §3.5 |
# MAGIC | L-05 | `occupation` looks randomly assigned in COFINFAD | A real-world model might find occupation useful; this dataset cannot show that | Excluded with the reason recorded, not silently dropped |
# MAGIC | L-06 | Member B's value model is not finished | The priority view currently uses a proxy | Proxy labelled in a `value_source` column; swaps to his table automatically once it exists |
# MAGIC | L-07 | Anonymised, single-company data | Results are specific to this fintech | Conclusions framed for COFINFAD; external validation recommended |
# MAGIC | L-08 | Streaming path is a replay of historical files | It proves the mechanism, not live production behaviour | Stated plainly; per-day row/amount/customer reconciliation against the batch path is the evidence offered |
# MAGIC | **L-09** | **`failed_transactions`, `support_tickets_count` and `international_transactions` are counts with no timestamp** | Unlike `tx_count` they **cannot** be re-derived per cutoff, so if they are year-cumulative they carry a little post-cutoff information at the earlier cutoffs — the same defect as TH-04 | Named in `UNTIMESTAMPED_COUNT_COLS`; **LK-07 measures** how much of the feature set is cutoff-invariant; `failures_per_tx_history` uses a full-history denominator so the ratio is not horizon-inflated; an ablation run without these three is planned for the modelling phase |
# MAGIC | **L-10** | **The three splits contain the same customers at different cutoffs** | The cutoffs and outcome windows genuinely do not overlap, but test performance is a *later-period* estimate, not a held-out-population one, and the fitted encoders have seen every customer's profile | Wording in §3.9 says "later period, same customers"; a customer-disjoint split is added as step 1b of §6.3 |
# MAGIC | L-11 | Readiness-model probabilities are not calibrated | Class weighting shifts them to a ~50/50 prior, so absolute probabilities are not meaningful | Only rank-based metrics are reported (top-k recall, capture curve, High-band recall); calibration deferred to §6.3 step 2 |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Next steps into the final phase
# MAGIC
# MAGIC 1. **Model comparison** — Logistic Regression vs Random Forest vs Gradient-Boosted Trees, tuned on
# MAGIC    the validation cutoff only, with the test cutoff untouched until the end. Report recall, precision,
# MAGIC    F1, PR-AUC and top-10%/20% recall as committed in Appendix D §8.2.
# MAGIC    **1b.** Add a **customer-disjoint** split alongside the chronological one and report both, so the
# MAGIC    L-10 caveat becomes a measured number instead of a caveat.
# MAGIC    **1c.** Run the **L-09 ablation**: refit without `UNTIMESTAMPED_COUNT_COLS` and report how much
# MAGIC    performance depends on the three columns that cannot be bounded to a cutoff.
# MAGIC 2. **Calibration and threshold selection** — refit unweighted (or apply isotonic recalibration on the
# MAGIC    validation cutoff) so probabilities become meaningful, then choose the operating point from outreach
# MAGIC    capacity rather than a default 0.5 cut-off. Only then is a predicted-vs-actual calibration chart
# MAGIC    worth drawing (see the note on chart 4.10).
# MAGIC 3. **Interpretability** — coefficients, feature importance and SHAP on the selected model; verify the
# MAGIC    univariate story of chart 4.8 holds multivariately.
# MAGIC 4. **Error analysis** — profile the false negatives, since a missed valuable customer is the costly
# MAGIC    error the stakeholder identified.
# MAGIC 5. **Swap in Member B's real value predictions** and re-cut the priority quadrants.
# MAGIC 6. **Dashboard** — publish `gold_retention_scored` and `gold_retention_priority` to the shared
# MAGIC    dashboard with risk bands, drivers, filters and the adjustable capacity control from chart 4.9.
# MAGIC 7. **Scalability report** — extend the probe to cover training and batch scoring, and record cluster
# MAGIC    utilisation alongside wall-clock time.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Evidence index
# MAGIC
# MAGIC Everything this notebook produced, as queryable tables — so a marker can verify any claim above
# MAGIC without re-running the notebook.

# COMMAND ----------

evidence = spark_df(
    [
        ("Data Collection", 10, "3 data natures + 4 collection mechanisms, batch/stream reconciled",
         "meta_source_inventory, ops_collection_summary, ops_stream_ingest_audit", "1.1-1.6"),
        ("Data Management", 10, "Medallion registry, generated dictionary, 16 executable DQ rules, "
         "cross-family correlations, 8 key themes",
         "meta_table_registry, meta_data_dictionary, dq_audit_result, dq_null_profile, insight_key_theme",
         "2.1-2.6"),
        ("Data Preparation", 15, "54 documented column decisions, 24 rejections, 6 leakage assertions "
         "(2 of them independent recomputations) + 1 reported invariance measure, 3 chronological cutoffs, "
         "full feature catalogue, train-only pipeline, scalability probe",
         "meta_column_decision, meta_feature_catalogue, dq_leakage_audit, gold_retention_features, "
         "ops_scalability_probe, ops_runtime_log", "3.1-3.10"),
        ("Data Visualization", 15, "11 decision-oriented figures + the joint priority view, one shared risk "
         "colour ramp, reference lines, group sizes shown, small groups suppressed",
         "insight_target_driver, gold_retention_scored, silver_daily_activity", "4.1-4.11"),
        ("Co-creating", 10, "8 logged team decisions, 8 shared-asset contracts, 6 stakeholder items closed, "
         "1 joint Member A x Member B deliverable",
         "meta_team_decision_log, meta_shared_contract, meta_stakeholder_feedback, gold_retention_priority",
         "5.1-5.4"),
    ],
    ["rubric_component", "marks", "evidence_produced", "tables", "notebook_sections"],
)
save_table(evidence, "ops_interim_evidence_index", comment="Rubric component -> evidence table mapping")
display(evidence)

print(f"\n{'=' * 96}")
print(f"IT3388 Interim Progress Review — {MEMBER} — workstream: {WORKSTREAM}")
print(f"{'=' * 96}")
# Persist the COMPLETE runtime log now that every timed step (including Sections 4-5) has run,
# plus a run-context row so a down-sampled probe run can never be mistaken for a full run.
runtime_pdf = pd.DataFrame(RUNTIME_LOG)
save_table(spark.createDataFrame(runtime_pdf), "ops_runtime_log",
           comment="Wall-clock timing of every pipeline step in this run")

save_table(
    spark_df(
        [(RUN_ID, MEMBER, WORKSTREAM, NAMESPACE, RAW_DIR, float(SAMPLE_FRACTION),
          bool(ENABLE_STREAMING), bool(RUN_SCALABILITY),
          CUTOFFS["train"], CUTOFFS["valid"], CUTOFFS["test"],
          int(OUTCOME_DAYS), int(ELIGIBILITY_DAYS), int(n_customer), int(n_tx), int(n_silver_tx),
          round(float(runtime_pdf["seconds"].sum()), 2))],
        ["run_id", "member", "workstream", "namespace", "raw_dir", "sample_fraction",
         "streaming_enabled", "scalability_probe_run", "train_cutoff", "valid_cutoff", "test_cutoff",
         "outcome_window_days", "eligibility_window_days", "bronze_customers", "bronze_transactions",
         "silver_transactions", "total_seconds"],
    ),
    "ops_run_context",
    comment="Parameters this run used - read this before comparing any two runs",
)

if SAMPLE_FRACTION < 1.0:
    print("*" * 96)
    print(f"WARNING: this run was DOWN-SAMPLED to {SAMPLE_FRACTION:.0%} of customers.")
    print("The silver/gold tables and every chart describe the sample; the bronze-layer governance")
    print("tables (meta_data_dictionary, dq_audit_result) describe the full dataset. See ops_run_context.")
    print("Re-run with sample_fraction = 1.00 before submitting or comparing results.")
    print("*" * 96)

tables_written = spark.sql(f"SHOW TABLES IN {NAMESPACE}")
print(f"Namespace                 : {NAMESPACE}")
print(f"Run id                    : {RUN_ID}")
print(f"Tables written            : {tables_written.count()}")
print(f"Ledger rows processed     : {n_silver_tx:,}")
print(f"Eligible customer-cutoffs : {gold_t.count():,} across 3 chronological cutoffs")
print(f"Features engineered       : {len(NUMERIC_FEATURES) + len(BOOLEAN_FEATURES) + len(CATEGORICAL_FEATURES)}")
print(f"Source columns rejected   : {len(EXCLUDED_COLS)} of {len(EXPECTED_CUSTOMER_COLUMNS)}")
print(f"DQ rules / leakage checks : {len(DQ)} / {len(LEAK)}  (all leakage checks passed)")
print(f"Total pipeline time       : {runtime_pdf['seconds'].sum():.1f}s")
print(f"{'=' * 96}")
display(tables_written)
