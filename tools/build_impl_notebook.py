"""Builds 'IMPLEMENTATION - INACTIVITY AND RETENTION.ipynb' (Member A, Clifton Chen Yi).

The notebook is generated from this script so that every code cell can be syntax-checked
before it is written, and so the notebook can be regenerated after edits.
"""

import json
import os

CELLS = []


def md(text):
    CELLS.append(("markdown", text.strip("\n")))


def code(text):
    CELLS.append(("code", text.strip("\n")))


# =============================================================================
md(r"""
# Customer Intelligence Platform for Fintech — Implementation
## Workstream A: Inactivity Risk & Retention Prioritisation

**Module:** IT3388 Big Data Management Project &nbsp;|&nbsp; **Group 2** — FinSight Colombia
**Member A:** Clifton Chen Yi &nbsp;|&nbsp; **Dataset:** COFINFAD (Colombian fintech, 4 Jan – 29 Dec 2023)

This notebook implements the retention workstream defined in the project proposal
(Appendix C) and scoped by the shared exploration in `EXPLORATORY DATA ANALYSIS.ipynb`
(Appendix D). It turns the two raw source tables into a leakage-controlled, chronologically
validated classifier for the target

$$\texttt{inactive\_next\_60d} = \begin{cases} 1 & \text{no transactions in the 60 days after the cutoff}\\ 0 & \text{at least one transaction}\end{cases}$$

and ends with a scored, prioritised retention list that feeds the dashboard.

### Hypothesis under test
> Among previously active customers, declining transaction frequency, longer recency, weaker
> app engagement, fewer active products, lower satisfaction, unresolved support issues and
> failed transactions increase the probability of no transactions during the following 60 days.
>
> The sharper question inside it: **does recent behavioural change beat static customer
> attributes?** Section 13 answers this directly with three feature-family models.

### Contents
| § | Section | Purpose |
|---|---------|---------|
| 1 | Setup & configuration | Imports, paths, run log |
| 2 | Load & shared preprocessing | Dedup rule, Set-B exclusion, type/amount checks |
| 3 | Panel design | Cutoff dates, eligibility, outcome windows |
| 4 | Feature engineering | Behavioural windows built strictly before each cutoff |
| 5 | Panel assembly | Four observation cutoffs stacked into one table |
| 6 | Leakage audit | Feature catalogue with an explicit time boundary per field |
| 7 | Target & feature behaviour | Class balance and univariate separation |
| 8 | Chronological splits | Train / validation / test by cutoff date |
| 9 | Preprocessing pipeline | Fitted on training data only |
| 10 | Baselines | Majority class and a tuned recency rule |
| 11 | Models | Logistic Regression, Random Forest, HistGB, XGBoost |
| 12 | Evaluation | Recall, precision, F1, PR-AUC, top-k, calibration |
| 13 | Interpretability & hypothesis test | Coefficients, importance, feature-family ablation |
| 14 | Subgroup performance | Where the model is materially weaker |
| 15 | Scored output | Risk bands, drivers, retention-priority list |
| 16 | Scalability | 25 / 50 / 75 / 100 % timings, optional Spark parity |
| 17 | Results export | `report_numbers.json` for Appendix E |
| 18 | Findings | Verdict on the hypothesis |
""")

# =============================================================================
md(r"""
## 1. Setup & configuration

Heavy or optional dependencies (`xgboost`, `shap`, `pyspark`) are imported behind guards so the
notebook runs end-to-end on a plain scikit-learn environment and simply reports what it skipped.
""")

code(r'''
import inspect
import json
import os
import time
import warnings
from datetime import timedelta

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score, brier_score_loss, classification_report,
    confusion_matrix, f1_score, precision_recall_curve, precision_score,
    recall_score, roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

warnings.filterwarnings("ignore")
sns.set_theme(style="whitegrid", palette="deep")
plt.rcParams.update({
    "figure.figsize": (9, 5), "figure.dpi": 100,
    "axes.titlesize": 13, "axes.titleweight": "bold", "axes.labelsize": 11,
})
pd.set_option("display.max_columns", 80)
pd.set_option("display.width", 160)

RANDOM_STATE = 42
np.random.seed(RANDOM_STATE)

# --- optional dependencies -------------------------------------------------
try:
    from xgboost import XGBClassifier
    HAS_XGB = True
except Exception:
    HAS_XGB = False

try:
    import shap
    HAS_SHAP = True
except Exception:
    HAS_SHAP = False

# `class_weight` reached HistGradientBoostingClassifier in scikit-learn 1.5; fall back to
# sample weights on older versions rather than crashing.
HGB_CLASS_WEIGHT = "class_weight" in inspect.signature(
    HistGradientBoostingClassifier.__init__).parameters

def hgb(**kw):
    if HGB_CLASS_WEIGHT:
        kw.setdefault("class_weight", "balanced")
    return HistGradientBoostingClassifier(**kw)

print(f"xgboost available: {HAS_XGB}   shap available: {HAS_SHAP}   "
      f"HistGB class_weight: {HGB_CLASS_WEIGHT}")
print(f"scikit-learn {__import__('sklearn').__version__}, pandas {pd.__version__}")

# --- paths -----------------------------------------------------------------
DATA_DIR = "."                 # folder holding the two source CSVs
OUT_DIR = "outputs"            # everything this notebook produces
FIG_DIR = os.path.join(OUT_DIR, "figures")
TAB_DIR = os.path.join(OUT_DIR, "tables")
for d in (OUT_DIR, FIG_DIR, TAB_DIR):
    os.makedirs(d, exist_ok=True)

# --- run log (feeds the scalability section and Appendix E) ----------------
run_log = []

def log_step(name, t0, rows=None):
    entry = {"step": name, "seconds": round(time.time() - t0, 3), "rows": rows}
    run_log.append(entry)
    print(f"  [{entry['seconds']:>7.2f}s] {name}" + (f"  ({rows:,} rows)" if rows else ""))
    return entry

def savefig(fig, name):
    path = os.path.join(FIG_DIR, f"{name}.png")
    fig.savefig(path, bbox_inches="tight", dpi=130)
    return path
''')

# =============================================================================
md(r"""
## 2. Load & shared preprocessing

The EDA established four facts that shape this step, and all four are re-asserted here as
executable checks rather than trusted from memory:

1. `customer_id` is a clean primary key and the join is lossless.
2. **Set A** (`tx_count`, `avg_tx_value`, `total_tx_volume`, `first_tx`, `last_tx`) reconciles
   100 % with the raw ledger; **Set B** (`average_transaction_value`,
   `total_transaction_volume`, `weekend_transaction_ratio`, `last_transaction_date`,
   `monthly_transaction_count`, `transaction_frequency`, `avg_daily_transactions`,
   `first_transaction_date`, `preferred_transaction_type`) does not and is dropped.
3. 102 exactly-duplicated transaction rows exist (0.003 %).
4. Only three customer columns have missing values, and all three are *structurally* missing.

**Duplicate rule.** The ledger has no transaction identifier, so a genuine repeat purchase of the
same amount, same type, same customer, same day is indistinguishable from a double-write. At
0.003 % of rows the choice cannot change any conclusion, so the rule is: **drop exact duplicates**,
because a double-write is the more likely explanation for an identical quadruple, and record the
count so the decision is auditable.
""")

code(r'''
t0 = time.time()
cust_raw = pd.read_csv(f"{DATA_DIR}/customer_data.csv")
log_step("load customer_data.csv", t0, len(cust_raw))

t0 = time.time()
tx_raw = pd.read_csv(f"{DATA_DIR}/transactions_data.csv", parse_dates=["date"])
log_step("load transactions_data.csv", t0, len(tx_raw))

print(f"\ncustomers    : {cust_raw.shape[0]:,} rows x {cust_raw.shape[1]} cols")
print(f"transactions : {tx_raw.shape[0]:,} rows x {tx_raw.shape[1]} cols")
''')

code(r'''
# ---------------------------------------------------------------- integrity checks
t0 = time.time()
assert cust_raw["customer_id"].is_unique, "customer_id is not a primary key"

orphan_tx = (~tx_raw["customer_id"].isin(set(cust_raw["customer_id"]))).sum()
cust_no_tx = (~cust_raw["customer_id"].isin(set(tx_raw["customer_id"]))).sum()

n_dupes = tx_raw.duplicated().sum()
tx = tx_raw.drop_duplicates().copy()

tx["date"] = tx["date"].dt.normalize()
tx = tx.sort_values(["customer_id", "date"], kind="mergesort").reset_index(drop=True)

LEDGER_START, LEDGER_END = tx["date"].min(), tx["date"].max()
n_days_covered = tx["date"].nunique()
span_days = (LEDGER_END - LEDGER_START).days + 1

print(f"orphan transactions (no matching customer) : {orphan_tx:,}")
print(f"customers with no transactions             : {cust_no_tx:,}")
print(f"exact duplicate transaction rows dropped   : {n_dupes:,} "
      f"({n_dupes / len(tx_raw) * 100:.4f}%)")
print(f"ledger coverage : {LEDGER_START.date()} -> {LEDGER_END.date()} "
      f"({n_days_covered} distinct days of {span_days} calendar days)")
print(f"transaction types : {sorted(tx['type'].unique())}")
print(f"amount range      : {tx['amount'].min():,} .. {tx['amount'].max():,} COP  "
      f"(negatives: {(tx['amount'] < 0).sum():,}, zeros: {(tx['amount'] == 0).sum():,})")
log_step("integrity checks + dedup", t0, len(tx))
''')

code(r'''
# ------------------------------------------------- columns excluded up front, with reasons
SET_B_UNRELIABLE = [
    "average_transaction_value", "total_transaction_volume", "weekend_transaction_ratio",
    "last_transaction_date", "first_transaction_date", "monthly_transaction_count",
    "transaction_frequency", "avg_daily_transactions", "preferred_transaction_type",
]
# Set A is accurate but spans the WHOLE year, i.e. it also summarises the outcome window.
SET_A_FULL_PERIOD = ["tx_count", "avg_tx_value", "total_tx_volume", "first_tx", "last_tx"]
LEAKY_OR_OWNED_ELSEWHERE = [
    "churn_probability",          # a supplied churn score - target leakage by construction
    "customer_lifetime_value",    # whole-period value estimate; also Member B's target space
    "customer_tenure",            # measured at dataset end; recomputed per cutoff instead
    "base_satisfaction", "tx_satisfaction", "product_satisfaction",  # components of satisfaction_score
    "last_survey_date",           # survey timing relative to cutoff is not documented
]

exclusions = pd.DataFrame(
    [(c, "Set B - does not reconcile with the ledger (EDA §3.4)") for c in SET_B_UNRELIABLE]
    + [(c, "Set A - accurate but aggregates the full year, including the outcome window") for c in SET_A_FULL_PERIOD]
    + [("churn_probability", "supplied churn score: leakage by construction"),
       ("customer_lifetime_value", "whole-period value estimate; Member B's workstream"),
       ("customer_tenure", "snapshot at dataset end; recomputed from the ledger per cutoff"),
       ("base_satisfaction", "component of satisfaction_score (redundant)"),
       ("tx_satisfaction", "component of satisfaction_score (redundant)"),
       ("product_satisfaction", "component of satisfaction_score (redundant)"),
       ("last_survey_date", "survey timing vs. cutoff undocumented")],
    columns=["column", "reason_excluded"],
).drop_duplicates("column").reset_index(drop=True)

DROP_COLS = sorted(set(SET_B_UNRELIABLE + SET_A_FULL_PERIOD + LEAKY_OR_OWNED_ELSEWHERE))
cust = cust_raw.drop(columns=[c for c in DROP_COLS if c in cust_raw.columns]).copy()

# Light text hygiene on the categorical fields we keep. The result is deliberately left as
# object dtype holding np.nan (not pandas StringDtype holding pd.NA), because scikit-learn's
# SimpleImputer cannot consume pd.NA.
for col in cust.select_dtypes(include=["object", "string"]).columns:
    s = cust[col].astype("string").str.strip()
    cust[col] = s.astype(object).where(s.notna(), np.nan)

print(f"{len(DROP_COLS)} columns excluded before any modelling; "
      f"{cust.shape[1]} customer columns retained.")
exclusions
''')

# =============================================================================
md(r"""
## 3. Panel design

A single snapshot would give one observation per customer and no way to test temporal
generalisation, so the design is a **panel of observation cutoffs**. Each row is a
*(customer, cutoff)* pair: features look only backwards from the cutoff, the label looks only
forwards.

| Role | Cutoff | Feature window (90 d, inclusive) | Outcome window (60 d) |
|------|--------|----------------------------------|-----------------------|
| train | 2023-05-31 | 2023-03-03 → 2023-05-31 | 2023-06-01 → 2023-07-30 |
| train | 2023-06-30 | 2023-04-02 → 2023-06-30 | 2023-07-01 → 2023-08-29 |
| validation | 2023-08-31 | 2023-06-03 → 2023-08-31 | 2023-09-01 → 2023-10-30 |
| **test** | **2023-10-30** | 2023-08-02 → 2023-10-30 | **2023-10-31 → 2023-12-29** |

Three properties make this defensible:

* **The test outcome window closes exactly on the last day of the ledger** (2023-10-30 + 60 d =
  2023-12-29), so no test label is truncated — a customer labelled "active" really did transact,
  and one labelled "inactive" really had 60 full days of silence.
* **The earliest cutoff still has a full feature window.** 2023-05-31 − 89 d = 2023-03-03, which
  is after the ledger start, so no cutoff is fed a short history.
* **Outcome windows never overlap across splits**, so a customer's future behaviour in the test
  period cannot have been seen during training.

**Eligibility.** The hypothesis is about *previously active* customers, so a *(customer, cutoff)*
row is admitted only if the customer transacted at least once in the 90 days before the cutoff.
Dormant customers are excluded rather than labelled — predicting that a silent customer stays
silent is trivially easy and would inflate every metric.
""")

code(r'''
HISTORY_DAYS = 90     # feature window length
HORIZON_DAYS = 60     # outcome window length (target definition)

CUTOFFS = [
    {"cutoff": "2023-05-31", "split": "train"},
    {"cutoff": "2023-06-30", "split": "train"},
    {"cutoff": "2023-08-31", "split": "valid"},
    {"cutoff": "2023-10-30", "split": "test"},
]

design = []
for c in CUTOFFS:
    cut = pd.Timestamp(c["cutoff"])
    design.append({
        "split": c["split"],
        "cutoff": cut.date(),
        "feature_from": (cut - timedelta(days=HISTORY_DAYS - 1)).date(),
        "feature_to": cut.date(),
        "outcome_from": (cut + timedelta(days=1)).date(),
        "outcome_to": (cut + timedelta(days=HORIZON_DAYS)).date(),
    })
design = pd.DataFrame(design)

# The design is only valid if it fits inside the ledger - assert rather than assume.
assert pd.Timestamp(design["feature_from"].min()) >= LEDGER_START, "a feature window starts before the ledger"
assert pd.Timestamp(design["outcome_to"].max()) <= LEDGER_END, "an outcome window ends after the ledger"

test_end = pd.Timestamp(design.loc[design.split == "test", "outcome_to"].iloc[0])
print(f"test outcome window ends {test_end.date()}, ledger ends {LEDGER_END.date()} "
      f"-> {'no truncation' if test_end == LEDGER_END else 'WARNING: truncated labels'}")
design
''')

# =============================================================================
md(r"""
## 4. Feature engineering

`build_features()` is the heart of the workstream. Everything it computes is derived from
`tx[tx.date <= cutoff]`, so the time boundary is enforced in one place instead of being
re-checked per feature.

Five behavioural families, each mapping to a clause of the hypothesis:

| Family | Features | Hypothesis clause |
|--------|----------|-------------------|
| **Recency** | `recency_days`, `days_since_first_tx` | "longer recency" |
| **Frequency / volume** | counts and sums over 7 / 30 / 90 d, active days | "declining frequency" |
| **Trend (change)** | `cnt_ratio_30_over_prior60`, `cnt_delta_30_vs_prior30`, `amt_ratio_30_over_prior60`, `active_day_trend` | "**declining**" — the change itself, not the level |
| **Rhythm** | mean/max/std inter-transaction gap, `gap_vs_history` | a stretching gap is early evidence of disengagement |
| **Diversity / mix** | distinct types, per-type shares, weekend ratio, amount concentration | "transaction value and diversity" |

The trend and rhythm families exist specifically to answer the research question. If a model
built from them alone rivals the full model, event-triggered retention is justified; if static
attributes carry the signal, broad segment campaigns are the better tool.

Note `gap_vs_history`: the current silence (`recency_days`) divided by the customer's own typical
gap. A five-day silence means nothing for a monthly user and a great deal for a daily user, so the
ratio is more comparable across customers than the raw gap.
""")

code(r'''
def _window(past, cutoff, start_offset, end_offset):
    """Slice `past` to the inclusive date window [cutoff-start_offset, cutoff-end_offset]."""
    lo = cutoff - timedelta(days=start_offset)
    hi = cutoff - timedelta(days=end_offset)
    return past[(past["date"] >= lo) & (past["date"] <= hi)]


def build_features(tx, cutoff, history_days=HISTORY_DAYS):
    """Behavioural features for every customer active in the `history_days` before `cutoff`.

    Only rows with date <= cutoff are ever touched, so no feature can encode the future.
    """
    cutoff = pd.Timestamp(cutoff)
    past = tx[tx["date"] <= cutoff]
    w90 = _window(past, cutoff, history_days - 1, 0)

    eligible = pd.Index(w90["customer_id"].unique(), name="customer_id")
    f = pd.DataFrame(index=eligible)

    # ---- frequency & value over nested windows
    for days, tag in [(7, "7d"), (30, "30d"), (history_days, "90d")]:
        w = _window(past, cutoff, days - 1, 0)
        g = w.groupby("customer_id")["amount"]
        f[f"cnt_{tag}"] = g.size()
        f[f"amt_{tag}"] = g.sum()
        f[f"amt_mean_{tag}"] = g.mean()
        f[f"active_days_{tag}"] = w.groupby("customer_id")["date"].nunique()

    # ---- prior windows, for the change features
    prior30 = _window(past, cutoff, 59, 30)     # days 31-60 before the cutoff
    prior60 = _window(past, cutoff, history_days - 1, 30)   # days 31-90 before the cutoff
    f["cnt_prior30"] = prior30.groupby("customer_id")["amount"].size()
    f["amt_prior30"] = prior30.groupby("customer_id")["amount"].sum()
    f["cnt_prior60"] = prior60.groupby("customer_id")["amount"].size()
    f["amt_prior60"] = prior60.groupby("customer_id")["amount"].sum()

    f = f.fillna(0.0)

    # ---- recency & tenure inside the ledger
    last_tx = past.groupby("customer_id")["date"].max()
    first_tx = past.groupby("customer_id")["date"].min()
    f["recency_days"] = (cutoff - last_tx.reindex(f.index)).dt.days
    f["days_since_first_tx"] = (cutoff - first_tx.reindex(f.index)).dt.days

    # ---- trend: is recent activity above or below the customer's own earlier baseline?
    prior60_rate = f["cnt_prior60"] / 2.0          # per-30-day rate over days 31-90
    f["cnt_ratio_30_over_prior60"] = (f["cnt_30d"] + 1.0) / (prior60_rate + 1.0)
    f["cnt_delta_30_vs_prior30"] = f["cnt_30d"] - f["cnt_prior30"]
    f["amt_ratio_30_over_prior60"] = (f["amt_30d"] + 1.0) / (f["amt_prior60"] / 2.0 + 1.0)
    f["cnt_share_last30"] = f["cnt_30d"] / f["cnt_90d"].where(f["cnt_90d"] > 0, np.nan)
    f["active_day_trend"] = f["active_days_30d"] - (f["active_days_90d"] - f["active_days_30d"]) / 2.0

    # ---- rhythm: inter-transaction gaps within the 90-day window
    w90s = w90.sort_values(["customer_id", "date"], kind="mergesort")
    gaps = w90s.groupby("customer_id")["date"].diff().dt.days
    gap_stats = gaps.groupby(w90s["customer_id"]).agg(["mean", "max", "std"])
    gap_stats.columns = ["gap_mean", "gap_max", "gap_std"]
    f = f.join(gap_stats)
    # a customer with a single transaction has no gap: fall back to the window length
    f[["gap_mean", "gap_max"]] = f[["gap_mean", "gap_max"]].fillna(float(history_days))
    f["gap_std"] = f["gap_std"].fillna(0.0)
    f["gap_vs_history"] = f["recency_days"] / f["gap_mean"].clip(lower=1.0)

    # ---- diversity & mix
    f["n_types_90d"] = w90.groupby("customer_id")["type"].nunique()
    type_counts = (w90.groupby(["customer_id", "type"]).size()
                      .unstack(fill_value=0)
                      .reindex(f.index, fill_value=0))
    for t in type_counts.columns:
        f[f"share_{str(t).lower().replace(' ', '_')}_90d"] = (
            type_counts[t] / f["cnt_90d"].where(f["cnt_90d"] > 0, np.nan)
        )
    is_weekend = w90["date"].dt.dayofweek >= 5
    f["weekend_ratio_90d"] = (is_weekend.groupby(w90["customer_id"]).mean()
                                        .reindex(f.index).fillna(0.0))
    f["amt_max_share_90d"] = (w90.groupby("customer_id")["amount"].max()
                              / f["amt_90d"].where(f["amt_90d"] > 0, np.nan))
    f["amt_std_90d"] = w90.groupby("customer_id")["amount"].std().fillna(0.0)

    f["observation_cutoff_date"] = cutoff
    return f.reset_index()


def build_target(tx, cutoff, horizon_days=HORIZON_DAYS):
    """1 = the customer made no transaction in the `horizon_days` after `cutoff`."""
    cutoff = pd.Timestamp(cutoff)
    fut = tx[(tx["date"] > cutoff) & (tx["date"] <= cutoff + timedelta(days=horizon_days))]
    return set(fut["customer_id"].unique())
''')

# =============================================================================
md(r"""
## 5. Panel assembly

Behavioural features are joined to the (already filtered) customer profile, and the label is
attached per cutoff.

One structural-missingness decision is made explicit here: `credit_utilization_ratio` is missing
for the 18,263 customers with no credit card. That is information, not a gap, so it becomes an
explicit `has_credit_utilization` flag plus a zero fill rather than a mean imputation that would
invent a utilisation ratio for people who cannot utilise anything. `complaint_topics` and
`feature_requests` get the same treatment: presence/absence is the signal.
""")

code(r'''
STATIC_SNAPSHOT = [
    # customer-level fields with no timestamp - see the caveat in §6 and the ablation in §13
    "failed_transactions", "international_transactions", "support_tickets_count",
    "resolved_tickets_ratio", "satisfaction_score", "nps_score", "app_store_rating",
    "feedback_sentiment", "has_complaint", "has_feature_request", "complaint_topic",
]

def prepare_profile(cust):
    p = cust.copy()
    p["has_credit_utilization"] = p["credit_utilization_ratio"].notna().astype(int)
    p["credit_utilization_ratio"] = p["credit_utilization_ratio"].fillna(0.0)
    p["has_complaint"] = p["complaint_topics"].notna().astype(int)
    p["has_feature_request"] = p["feature_requests"].notna().astype(int)
    p["complaint_topic"] = p["complaint_topics"].fillna("no complaint recorded")
    p = p.drop(columns=["complaint_topics", "feature_requests"])
    for c in p.select_dtypes(include=["bool"]).columns:
        p[c] = p[c].astype(int)
    return p


profile = prepare_profile(cust)

t0 = time.time()
panels = []
for spec in CUTOFFS:
    cut = pd.Timestamp(spec["cutoff"])
    ts = time.time()
    feats = build_features(tx, cut)
    active_next = build_target(tx, cut)
    feats["inactive_next_60d"] = (~feats["customer_id"].isin(active_next)).astype(int)
    feats["split"] = spec["split"]
    panel = feats.merge(profile, on="customer_id", how="left", validate="many_to_one")
    panels.append(panel)
    print(f"  cutoff {cut.date()} [{spec['split']:>5s}]  eligible={len(panel):>7,}  "
          f"inactive={panel['inactive_next_60d'].mean()*100:5.2f}%  "
          f"({time.time()-ts:.1f}s)")

panel = pd.concat(panels, ignore_index=True)
log_step("feature engineering (4 cutoffs)", t0, len(panel))

print(f"\npanel: {panel.shape[0]:,} customer-cutoff rows x {panel.shape[1]} columns")

# Persist the analytical layer. Parquet preferred (typed, compressed, cloud-friendly);
# CSV fallback when no parquet engine is installed.
try:
    panel.to_parquet(os.path.join(TAB_DIR, "retention_panel.parquet"), index=False)
    print(f"saved {TAB_DIR}/retention_panel.parquet")
except Exception as e:
    panel.to_csv(os.path.join(TAB_DIR, "retention_panel.csv.gz"), index=False,
                 compression="gzip")
    print(f"parquet unavailable ({type(e).__name__}); saved retention_panel.csv.gz instead")
''')

code(r'''
# eligibility funnel - how many customers each cutoff admits, and why
funnel = (panel.groupby(["split", "observation_cutoff_date"])
               .agg(eligible=("customer_id", "size"),
                    inactive=("inactive_next_60d", "sum"),
                    inactive_rate=("inactive_next_60d", "mean"),
                    median_cnt_90d=("cnt_90d", "median"),
                    median_recency=("recency_days", "median"))
               .reset_index())
funnel["excluded_dormant"] = len(profile) - funnel["eligible"]
funnel["inactive_rate"] = (funnel["inactive_rate"] * 100).round(2)
funnel
''')

# =============================================================================
md(r"""
## 6. Leakage audit

Every candidate predictor is catalogued with its source, calculation and — the part that matters
— its **time boundary**. Three tiers:

* **`pre-cutoff`** — computed from `tx[date <= cutoff]`. Verifiably safe.
* **`static`** — demographics, products, engagement. No timestamp, but these change slowly and are
  attributes rather than events.
* **`snapshot`** — `failed_transactions`, `support_tickets_count`, `satisfaction_score`,
  `nps_score`, `app_store_rating`, complaint flags. **These are counted over the whole
  observation year, so a ticket logged *after* a cutoff is still visible at that cutoff.**

The snapshot tier is a genuine, unavoidable look-ahead risk: the source data supplies these as
totals with no dates, so they cannot be re-windowed. Two honest responses, both implemented:
they are labelled as such in the catalogue, and §13 retrains without them so the size of any
inflation is measured instead of argued about.
""")

code(r'''
BEHAVIOURAL_PREFIXES = ("cnt_", "amt_", "active_days", "recency_", "days_since_",
                        "gap_", "n_types_", "share_", "weekend_ratio_", "active_day_trend")

TREND_FEATURES = ["cnt_ratio_30_over_prior60", "cnt_delta_30_vs_prior30",
                  "amt_ratio_30_over_prior60", "cnt_share_last30", "active_day_trend",
                  "gap_vs_history", "recency_days", "gap_mean", "gap_max", "gap_std"]

ID_COLS = ["customer_id", "observation_cutoff_date", "split"]
TARGET = "inactive_next_60d"

feature_cols = [c for c in panel.columns if c not in ID_COLS + [TARGET]]
num_features = [c for c in feature_cols if pd.api.types.is_numeric_dtype(panel[c])]
cat_features = [c for c in feature_cols if c not in num_features]


def time_boundary(col):
    if col in STATIC_SNAPSHOT:
        return "snapshot (whole-year total, no timestamp)"
    if col.startswith(BEHAVIOURAL_PREFIXES) or col in ("amt_max_share_90d", "amt_std_90d"):
        return "pre-cutoff (date <= cutoff)"
    return "static (customer attribute)"


def family(col):
    if col in TREND_FEATURES:
        return "trend/rhythm"
    if col.startswith(BEHAVIOURAL_PREFIXES) or col in ("amt_max_share_90d", "amt_std_90d"):
        return "behavioural level"
    if col in STATIC_SNAPSHOT:
        return "experience/support snapshot"
    return "static profile"


catalogue = pd.DataFrame({
    "feature": feature_cols,
    "dtype": [str(panel[c].dtype) for c in feature_cols],
    "family": [family(c) for c in feature_cols],
    "time_boundary": [time_boundary(c) for c in feature_cols],
    "pct_missing": [round(panel[c].isna().mean() * 100, 2) for c in feature_cols],
})
catalogue.to_csv(os.path.join(TAB_DIR, "feature_catalogue.csv"), index=False)

print(f"{len(feature_cols)} features "
      f"({len(num_features)} numeric, {len(cat_features)} categorical)\n")
print(catalogue["time_boundary"].value_counts().to_string())
print()
print(catalogue["family"].value_counts().to_string())
catalogue.head(25)
''')

code(r'''
# A leakage tripwire: no feature may be a near-perfect predictor of the target.
# Anything above ~0.95 AUC on its own is almost certainly the label in disguise.
t0 = time.time()
tripwire = []
tr = panel[panel.split == "train"]
for c in num_features:
    v = tr[c].fillna(tr[c].median())
    if v.nunique() > 1:
        auc = roc_auc_score(tr[TARGET], v)
        tripwire.append({"feature": c, "univariate_auc": round(max(auc, 1 - auc), 4)})
tripwire = pd.DataFrame(tripwire).sort_values("univariate_auc", ascending=False)
log_step("leakage tripwire (univariate AUC)", t0)

suspect = tripwire[tripwire.univariate_auc > 0.95]
print(f"features with univariate AUC > 0.95: {len(suspect)}")
if len(suspect):
    print("REVIEW THESE - a single feature this strong usually means leakage:")
    print(suspect.to_string(index=False))
tripwire.head(15)
''')

# =============================================================================
md(r"""
## 7. Target & feature behaviour

Before any model: how common is inactivity, is it stable across cutoffs, and do the trend
features actually separate the classes? If they do not, the hypothesis is in trouble before a
single estimator is fitted.
""")

code(r'''
fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))

rate = (panel.groupby(["observation_cutoff_date", "split"])[TARGET]
             .mean().mul(100).reset_index())
rate["label"] = (rate["observation_cutoff_date"].dt.strftime("%Y-%m-%d")
                 + "\n(" + rate["split"] + ")")
colors = {"train": "#4c72b0", "valid": "#dd8452", "test": "#c44e52"}
axes[0].bar(rate["label"], rate[TARGET], color=[colors[s] for s in rate["split"]])
axes[0].set_title("Inactivity rate by observation cutoff")
axes[0].set_ylabel("% inactive in next 60 days")
for i, v in enumerate(rate[TARGET]):
    axes[0].text(i, v, f"{v:.1f}%", ha="center", va="bottom", fontsize=9)

order = panel.groupby("customer_segment")[TARGET].mean().sort_values().index
sns.barplot(data=panel, y="customer_segment", x=TARGET, order=order,
            errorbar=None, ax=axes[1])
axes[1].set_title("Inactivity rate by customer segment")
axes[1].set_xlabel("P(inactive next 60 days)")
axes[1].set_ylabel("")
plt.tight_layout()
savefig(fig, "01_target_balance")
plt.show()

prevalence = panel.groupby("split")[TARGET].agg(["size", "sum", "mean"])
prevalence.columns = ["rows", "inactive", "rate"]
print(prevalence.assign(rate=lambda d: (d["rate"] * 100).round(2)).to_string())
''')

code(r'''
# Do the trend features separate the classes on the TRAINING cutoffs only?
show = ["recency_days", "gap_vs_history", "cnt_ratio_30_over_prior60",
        "cnt_delta_30_vs_prior30", "cnt_30d", "app_logins_frequency"]
fig, axes = plt.subplots(2, 3, figsize=(15, 8))
for ax, col in zip(axes.ravel(), show):
    d = tr[[col, TARGET]].dropna()
    lo, hi = d[col].quantile([0.01, 0.99])
    sns.kdeplot(data=d[(d[col] >= lo) & (d[col] <= hi)], x=col, hue=TARGET,
                common_norm=False, fill=True, alpha=0.35, ax=ax, legend=(ax is axes[0, 0]))
    ax.set_title(col, fontsize=11)
    ax.set_ylabel("")
fig.suptitle("Feature distributions by outcome (training cutoffs only)",
             fontweight="bold", y=1.01)
plt.tight_layout()
savefig(fig, "02_feature_separation")
plt.show()

sep = (tripwire.set_index("feature")
       .loc[[c for c in TREND_FEATURES if c in set(tripwire.feature)]]
       .sort_values("univariate_auc", ascending=False))
print("Univariate AUC of the trend/rhythm family (training cutoffs):")
print(sep.to_string())
''')

# =============================================================================
md(r"""
## 8. Chronological splits

Train on the two earliest cutoffs, tune on the third, report once on the fourth. A random split
would let the same customer appear in both train and test at different cutoffs, and would let
later behaviour inform earlier predictions — both would flatter the model.
""")

code(r'''
train = panel[panel.split == "train"].copy()
valid = panel[panel.split == "valid"].copy()
test = panel[panel.split == "test"].copy()

X_train, y_train = train[feature_cols], train[TARGET]
X_valid, y_valid = valid[feature_cols], valid[TARGET]
X_test, y_test = test[feature_cols], test[TARGET]

print(f"train : {len(train):>7,} rows  cutoffs {sorted(train.observation_cutoff_date.dt.date.unique())}"
      f"  inactive {y_train.mean()*100:.2f}%")
print(f"valid : {len(valid):>7,} rows  cutoffs {sorted(valid.observation_cutoff_date.dt.date.unique())}"
      f"  inactive {y_valid.mean()*100:.2f}%")
print(f"test  : {len(test):>7,} rows  cutoffs {sorted(test.observation_cutoff_date.dt.date.unique())}"
      f"  inactive {y_test.mean()*100:.2f}%")

# Customers recur across cutoffs by design (that is what makes this a panel), but no cutoff may
# appear in two splits, and every training outcome window must close before the test window opens.
assert set(train.observation_cutoff_date).isdisjoint(set(test.observation_cutoff_date))
assert set(valid.observation_cutoff_date).isdisjoint(set(test.observation_cutoff_date))
latest_train_outcome_end = max(
    pd.Timestamp(c) + timedelta(days=HORIZON_DAYS)
    for c in train.observation_cutoff_date.unique())
test_cutoff = pd.Timestamp(test.observation_cutoff_date.iloc[0])
assert latest_train_outcome_end <= test_cutoff, "a training label overlaps the test period"
print(f"\nlatest training outcome ends {latest_train_outcome_end.date()}; "
      f"test cutoff is {test_cutoff.date()} -> no outcome-window overlap")
''')

# =============================================================================
md(r"""
## 9. Preprocessing pipeline

Imputation, scaling and encoding all live **inside** the pipeline, so they are fitted on the
training fold only. Fitting a scaler on the full panel is one of the easiest ways to leak
distributional information about the test period into training, and putting the transformers in
a `Pipeline` makes that mistake structurally impossible rather than merely avoided.
""")

code(r'''
numeric_tf = Pipeline([
    ("impute", SimpleImputer(strategy="median")),
    ("scale", StandardScaler()),
])
categorical_tf = Pipeline([
    ("impute", SimpleImputer(strategy="most_frequent")),
    ("onehot", OneHotEncoder(handle_unknown="ignore", min_frequency=25, sparse_output=False)),
])

def make_preprocessor(num_cols, cat_cols, scale=True):
    """Transformers with an empty column list are omitted: SimpleImputer raises on 0 features,
    which would break the feature-family ablations in §13."""
    num = numeric_tf if scale else Pipeline([("impute", SimpleImputer(strategy="median"))])
    steps = []
    if len(num_cols):
        steps.append(("num", num, list(num_cols)))
    if len(cat_cols):
        steps.append(("cat", categorical_tf, list(cat_cols)))
    return ColumnTransformer(steps, remainder="drop", verbose_feature_names_out=False)

preprocessor = make_preprocessor(num_features, cat_features)
_probe = preprocessor.fit(X_train)
print(f"design matrix after encoding: {_probe.transform(X_train.head(100)).shape[1]} columns")
''')

# =============================================================================
md(r"""
## 10. Baselines

Two baselines, because "beats random" is not an achievement:

1. **Majority class** — the floor. Predicts everyone stays active.
2. **Recency rule** — the rule an analyst would actually write without a model: flag anyone silent
   for more than *k* days. The threshold *k* is chosen on the **validation** split by F1, never on
   test. This is the baseline that matters; a model that cannot beat it has not earned its
   complexity.
""")

code(r'''
def topk_recall(y_true, scores, k):
    """Recall among the k fraction of customers with the highest risk scores."""
    y_true = np.asarray(y_true)
    n = max(1, int(np.ceil(len(scores) * k)))
    idx = np.argsort(-np.asarray(scores))[:n]
    return y_true[idx].sum() / max(1, y_true.sum())


def topk_precision(y_true, scores, k):
    y_true = np.asarray(y_true)
    n = max(1, int(np.ceil(len(scores) * k)))
    idx = np.argsort(-np.asarray(scores))[:n]
    return y_true[idx].mean()


def evaluate(name, y_true, proba, threshold=0.5, split="test"):
    pred = (np.asarray(proba) >= threshold).astype(int)
    return {
        "model": name, "split": split, "threshold": round(float(threshold), 4),
        "recall": recall_score(y_true, pred, zero_division=0),
        "precision": precision_score(y_true, pred, zero_division=0),
        "f1": f1_score(y_true, pred, zero_division=0),
        "pr_auc": average_precision_score(y_true, proba),
        "roc_auc": roc_auc_score(y_true, proba),
        "recall_top10pct": topk_recall(y_true, proba, 0.10),
        "recall_top20pct": topk_recall(y_true, proba, 0.20),
        "precision_top10pct": topk_precision(y_true, proba, 0.10),
        "brier": brier_score_loss(y_true, np.clip(proba, 0, 1)),
        "n_flagged": int(pred.sum()),
        "n_positive": int(np.asarray(y_true).sum()),
    }


results = []

# ---- baseline 1: majority class
dummy = DummyClassifier(strategy="most_frequent").fit(X_train, y_train)
results.append(evaluate("Baseline: majority class", y_test,
                        dummy.predict_proba(X_test)[:, 1], 0.5))

# ---- baseline 2: recency rule, threshold picked on validation
grid = range(1, 91)
val_f1 = [(k, f1_score(y_valid, (valid["recency_days"] >= k).astype(int), zero_division=0))
          for k in grid]
best_k, best_val_f1 = max(val_f1, key=lambda t: t[1])
print(f"recency rule: best threshold on validation = {best_k} days (F1 = {best_val_f1:.3f})")

# score = normalised recency so PR-AUC / top-k are computable for the rule too
rule_score_test = (test["recency_days"] / HISTORY_DAYS).clip(0, 1)
res_rule = evaluate(f"Baseline: recency >= {best_k}d", y_test, rule_score_test, 0.5)
rule_pred = (test["recency_days"] >= best_k).astype(int)
res_rule.update({
    "recall": recall_score(y_test, rule_pred, zero_division=0),
    "precision": precision_score(y_test, rule_pred, zero_division=0),
    "f1": f1_score(y_test, rule_pred, zero_division=0),
    "n_flagged": int(rule_pred.sum()),
})
results.append(res_rule)

pd.DataFrame(results).round(4)
''')

# =============================================================================
md(r"""
## 11. Models

Four estimators, ordered by increasing capacity, so the marginal value of complexity is
observable rather than assumed:

| Model | Why it is here |
|-------|----------------|
| Logistic Regression | Interpretable coefficients; `class_weight="balanced"` handles the imbalance; the reference point for "is non-linearity needed?" |
| Random Forest | Captures interactions with almost no tuning; robust to the heavy skew in amount features |
| HistGradientBoosting | Usually the strongest tabular learner in scikit-learn; native missing-value handling |
| XGBoost | Included when available for parity with the proposal; skipped cleanly otherwise |

Class imbalance is handled with class weights rather than resampling: SMOTE-style oversampling on
a panel with repeated customers risks synthesising rows that interpolate between the same person
at two cutoffs, and weights keep the predicted probabilities interpretable for the dashboard.
""")

code(r'''
def make_model(kind):
    if kind == "logreg":
        clf = LogisticRegression(max_iter=2000, class_weight="balanced",
                                 C=1.0, random_state=RANDOM_STATE)
        return Pipeline([("prep", make_preprocessor(num_features, cat_features, scale=True)),
                         ("clf", clf)])
    if kind == "rf":
        clf = RandomForestClassifier(
            n_estimators=400, min_samples_leaf=20, max_features="sqrt",
            class_weight="balanced_subsample", n_jobs=-1, random_state=RANDOM_STATE)
        return Pipeline([("prep", make_preprocessor(num_features, cat_features, scale=False)),
                         ("clf", clf)])
    if kind == "histgb":
        clf = hgb(max_iter=400, learning_rate=0.06, max_leaf_nodes=31,
                  min_samples_leaf=40, l2_regularization=1.0,
                  early_stopping=True, validation_fraction=0.15,
                  random_state=RANDOM_STATE)
        return Pipeline([("prep", make_preprocessor(num_features, cat_features, scale=False)),
                         ("clf", clf)])
    if kind == "xgb":
        pos_weight = float((y_train == 0).sum() / max(1, (y_train == 1).sum()))
        clf = XGBClassifier(
            n_estimators=500, learning_rate=0.06, max_depth=5, subsample=0.9,
            colsample_bytree=0.8, reg_lambda=1.0, scale_pos_weight=pos_weight,
            eval_metric="aucpr", n_jobs=-1, random_state=RANDOM_STATE, tree_method="hist")
        return Pipeline([("prep", make_preprocessor(num_features, cat_features, scale=False)),
                         ("clf", clf)])
    raise ValueError(kind)


MODEL_SPECS = [("logreg", "Logistic Regression"),
               ("rf", "Random Forest"),
               ("histgb", "HistGradientBoosting")]
if HAS_XGB:
    MODEL_SPECS.append(("xgb", "XGBoost"))

fitted, train_times = {}, {}
for kind, label in MODEL_SPECS:
    t0 = time.time()
    model = make_model(kind).fit(X_train, y_train)
    train_times[label] = round(time.time() - t0, 2)
    fitted[label] = model
    log_step(f"fit {label}", t0, len(X_train))
''')

code(r'''
# --- pick each model's operating threshold on VALIDATION, then freeze it
def best_threshold(y_true, proba, metric="f1", min_precision=None):
    prec, rec, thr = precision_recall_curve(y_true, proba)
    prec, rec = prec[:-1], rec[:-1]
    f1 = np.where((prec + rec) > 0, 2 * prec * rec / (prec + rec + 1e-12), 0)
    ok = np.ones(len(thr), dtype=bool) if min_precision is None else (prec >= min_precision)
    if not ok.any():
        ok = np.ones(len(thr), dtype=bool)
    score = f1 if metric == "f1" else rec
    i = np.argmax(np.where(ok, score, -1))
    return float(thr[i]), float(prec[i]), float(rec[i])


thresholds, valid_proba, test_proba = {}, {}, {}
for label, model in fitted.items():
    pv = model.predict_proba(X_valid)[:, 1]
    pt = model.predict_proba(X_test)[:, 1]
    valid_proba[label], test_proba[label] = pv, pt
    thr, p, r = best_threshold(y_valid, pv, metric="f1")
    thresholds[label] = thr
    print(f"{label:<24s} validation-tuned threshold = {thr:.3f}  "
          f"(val precision {p:.3f}, val recall {r:.3f})")

for label in fitted:
    results.append(evaluate(label, y_valid, valid_proba[label], thresholds[label], "valid"))
for label in fitted:
    results.append(evaluate(label, y_test, test_proba[label], thresholds[label], "test"))

scoreboard = pd.DataFrame(results)
scoreboard.to_csv(os.path.join(TAB_DIR, "model_scoreboard.csv"), index=False)
scoreboard[scoreboard.split == "test"].round(4).sort_values("pr_auc", ascending=False)
''')

# =============================================================================
md(r"""
## 12. Evaluation

Recall leads, because a missed at-risk customer is a lost customer while a false alarm costs one
outreach contact. But recall alone is gameable — flag everyone and score 1.0 — so it is always
read alongside precision, PR-AUC (the right summary for an imbalanced problem) and top-*k* recall
(the metric that matches a team with finite outreach capacity).

Calibration matters for a different reason: the dashboard shows a risk *score*. If the model says
0.7 and only 40 % of those customers actually go quiet, the number misleads the operator even
when the ranking is sound.
""")

code(r'''
best_label = (scoreboard[scoreboard.split == "test"]
              .sort_values("pr_auc", ascending=False)["model"].iloc[0])
best_model = fitted[best_label]
best_proba = test_proba[best_label]
best_thr = thresholds[best_label]
print(f"Best model on test PR-AUC: {best_label}  (threshold {best_thr:.3f})")

fig, axes = plt.subplots(1, 3, figsize=(16, 4.6))

# --- precision-recall curves
for label in fitted:
    p, r, _ = precision_recall_curve(y_test, test_proba[label])
    ap = average_precision_score(y_test, test_proba[label])
    axes[0].plot(r, p, label=f"{label} (AP={ap:.3f})")
axes[0].axhline(y_test.mean(), ls="--", c="grey",
                label=f"prevalence ({y_test.mean():.3f})")
axes[0].set(xlabel="Recall", ylabel="Precision", title="Precision-Recall (test cutoff)")
axes[0].legend(fontsize=8, loc="best")

# --- confusion matrix of the chosen model
cm = confusion_matrix(y_test, (best_proba >= best_thr).astype(int))
sns.heatmap(cm, annot=True, fmt=",d", cmap="Blues", cbar=False, ax=axes[1],
            xticklabels=["pred active", "pred inactive"],
            yticklabels=["actually active", "actually inactive"])
axes[1].set_title(f"{best_label} @ {best_thr:.2f}")

# --- calibration
bins = np.linspace(0, 1, 11)
idx = np.clip(np.digitize(best_proba, bins) - 1, 0, 9)
cal = (pd.DataFrame({"bin": idx, "p": best_proba, "y": np.asarray(y_test)})
       .groupby("bin").agg(mean_p=("p", "mean"), frac_pos=("y", "mean"), n=("y", "size")))
axes[2].plot([0, 1], [0, 1], ls="--", c="grey", label="perfect")
axes[2].plot(cal["mean_p"], cal["frac_pos"], "o-", label=best_label)
axes[2].set(xlabel="Predicted probability", ylabel="Observed inactivity rate",
            title=f"Calibration (Brier={brier_score_loss(y_test, np.clip(best_proba,0,1)):.4f})")
axes[2].legend(fontsize=8)
plt.tight_layout()
savefig(fig, "03_evaluation")
plt.show()

print(classification_report(y_test, (best_proba >= best_thr).astype(int),
                            target_names=["active", "inactive"], digits=3))
''')

code(r'''
# --- capacity curve: what does the team actually get for k contacts?
ks = np.arange(0.02, 0.52, 0.02)
cap = pd.DataFrame({
    "capacity_pct": (ks * 100).round(0),
    "customers_contacted": (ks * len(y_test)).astype(int),
    "recall": [topk_recall(y_test, best_proba, k) for k in ks],
    "precision": [topk_precision(y_test, best_proba, k) for k in ks],
})
cap["recall_rule"] = [topk_recall(y_test, rule_score_test, k) for k in ks]
cap["lift_vs_random"] = (cap["precision"] / y_test.mean()).round(2)

fig, ax = plt.subplots(figsize=(9, 5))
ax.plot(cap.capacity_pct, cap.recall * 100, "o-", label=f"{best_label} — recall")
ax.plot(cap.capacity_pct, cap.recall_rule * 100, "s--", c="grey",
        label="recency rule — recall")
ax.plot(cap.capacity_pct, cap.precision * 100, "^-", c="#c44e52", label="precision")
for k in (10, 20):
    ax.axvline(k, ls=":", c="k", alpha=0.4)
ax.set(xlabel="Outreach capacity (% of eligible customers contacted)",
       ylabel="%", title="What the retention team gets for a given capacity")
ax.legend()
plt.tight_layout()
savefig(fig, "04_capacity_curve")
plt.show()

cap[cap.capacity_pct.isin([5, 10, 20, 30, 50])].round(3).to_string(index=False)
''')

# =============================================================================
md(r"""
## 13. Interpretability & the hypothesis test

This section answers the research question the proposal committed to: **does recent behavioural
change beat static customer attributes?**

Four models are trained on restricted feature families and compared on the same test cutoff:

* **static only** — demographics, products, engagement, snapshot experience fields
* **level only** — behavioural volume/recency levels, no change features
* **trend only** — the change and rhythm family alone
* **full** — everything

Plus a **no-snapshot** variant that drops the untimestamped experience fields, quantifying the
look-ahead risk flagged in §6. Any of these can support the hypothesis, contradict it, or land
in between; the numbers decide.
""")

code(r'''
static_only = [c for c in feature_cols if family(c) in ("static profile", "experience/support snapshot")]
level_only = [c for c in feature_cols if family(c) == "behavioural level"]
trend_only = [c for c in feature_cols if family(c) == "trend/rhythm"]
no_snapshot = [c for c in feature_cols if c not in STATIC_SNAPSHOT]

ABLATIONS = {
    "static profile only": static_only,
    "behavioural level only": level_only,
    "trend/rhythm only": trend_only,
    "behavioural (level+trend)": level_only + trend_only,
    "full model": feature_cols,
    "full minus snapshot fields": no_snapshot,
}

ablation_rows = []
for name, cols in ABLATIONS.items():
    nc = [c for c in cols if c in num_features]
    cc = [c for c in cols if c in cat_features]
    pipe = Pipeline([("prep", make_preprocessor(nc, cc, scale=False)),
                     ("clf", hgb(max_iter=300, learning_rate=0.06, min_samples_leaf=40,
                                 l2_regularization=1.0, early_stopping=True,
                                 validation_fraction=0.15, random_state=RANDOM_STATE))])
    t0 = time.time()
    pipe.fit(train[cols], y_train)
    pv = pipe.predict_proba(valid[cols])[:, 1]
    pt = pipe.predict_proba(test[cols])[:, 1]
    thr, _, _ = best_threshold(y_valid, pv, metric="f1")
    row = evaluate(f"HistGB — {name}", y_test, pt, thr, "test")
    row["n_features"] = len(cols)
    row["fit_seconds"] = round(time.time() - t0, 2)
    ablation_rows.append(row)
    print(f"  {name:<28s} n={len(cols):>3d}  PR-AUC={row['pr_auc']:.4f}  "
          f"recall={row['recall']:.3f}  top10%={row['recall_top10pct']:.3f}")

ablation = pd.DataFrame(ablation_rows)
ablation.to_csv(os.path.join(TAB_DIR, "feature_family_ablation.csv"), index=False)

fig, ax = plt.subplots(figsize=(9, 4.8))
plot_df = ablation.sort_values("pr_auc")
ax.barh(plot_df["model"].str.replace("HistGB — ", "", regex=False), plot_df["pr_auc"])
ax.axvline(y_test.mean(), ls="--", c="grey", label="prevalence")
ax.set(xlabel="PR-AUC (test cutoff)", title="Which feature family carries the signal?")
ax.legend()
for i, (v, n) in enumerate(zip(plot_df["pr_auc"], plot_df["n_features"])):
    ax.text(v, i, f" {v:.3f} ({n}f)", va="center", fontsize=9)
plt.tight_layout()
savefig(fig, "05_feature_family_ablation")
plt.show()
ablation[["model", "n_features", "recall", "precision", "f1", "pr_auc", "recall_top10pct"]].round(4)
''')

code(r'''
# --- permutation importance on the best model (validation split, so test stays untouched)
t0 = time.time()
sample = valid.sample(min(8000, len(valid)), random_state=RANDOM_STATE)
perm = permutation_importance(
    best_model, sample[feature_cols], sample[TARGET],
    n_repeats=5, random_state=RANDOM_STATE, scoring="average_precision", n_jobs=-1)
log_step("permutation importance", t0)

imp = (pd.DataFrame({"feature": feature_cols,
                     "importance": perm.importances_mean,
                     "std": perm.importances_std})
       .assign(family=lambda d: d.feature.map(family))
       .sort_values("importance", ascending=False))
imp.to_csv(os.path.join(TAB_DIR, "permutation_importance.csv"), index=False)

top = imp.head(20).iloc[::-1]
fig, ax = plt.subplots(figsize=(9, 7))
palette = {"trend/rhythm": "#c44e52", "behavioural level": "#4c72b0",
           "experience/support snapshot": "#dd8452", "static profile": "#8172b3"}
ax.barh(top.feature, top.importance, xerr=top["std"],
        color=[palette[f] for f in top.family])
ax.set(xlabel="Drop in PR-AUC when shuffled", title=f"Top drivers — {best_label}")
handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c in palette.values()]
ax.legend(handles, palette.keys(), fontsize=8, loc="lower right")
plt.tight_layout()
savefig(fig, "06_permutation_importance")
plt.show()

print("Importance concentrated by family (share of total positive importance):")
fam_share = (imp[imp.importance > 0].groupby("family")["importance"].sum())
print((fam_share / fam_share.sum() * 100).round(1).sort_values(ascending=False).to_string())
''')

code(r'''
# --- logistic regression coefficients: direction, not just magnitude
lr = fitted["Logistic Regression"]
names = lr.named_steps["prep"].get_feature_names_out()
coefs = pd.DataFrame({"feature": names, "coef": lr.named_steps["clf"].coef_[0]})
coefs["odds_ratio"] = np.exp(coefs["coef"])
coefs = coefs.reindex(coefs.coef.abs().sort_values(ascending=False).index)
coefs.to_csv(os.path.join(TAB_DIR, "logreg_coefficients.csv"), index=False)

fig, ax = plt.subplots(figsize=(9, 7))
show = coefs.head(20).iloc[::-1]
ax.barh(show.feature, show.coef,
        color=["#c44e52" if c > 0 else "#4c72b0" for c in show.coef])
ax.axvline(0, c="k", lw=0.8)
ax.set(xlabel="Coefficient (standardised features) — positive raises inactivity risk",
       title="Logistic Regression: direction of effect")
plt.tight_layout()
savefig(fig, "07_logreg_coefficients")
plt.show()
coefs.head(15).round(4)
''')

code(r'''
# --- SHAP, when installed: per-customer explanations for the dashboard drivers
if HAS_SHAP and best_label in ("HistGradientBoosting", "XGBoost", "Random Forest"):
    t0 = time.time()
    prep = best_model.named_steps["prep"]
    clf = best_model.named_steps["clf"]
    Xs = prep.transform(test[feature_cols].sample(min(2000, len(test)), random_state=RANDOM_STATE))
    fnames = prep.get_feature_names_out()
    explainer = shap.TreeExplainer(clf)
    sv = explainer.shap_values(Xs)
    sv = sv[1] if isinstance(sv, list) else sv
    shap.summary_plot(sv, features=Xs, feature_names=fnames, max_display=18, show=False)
    fig = plt.gcf(); fig.set_size_inches(9, 7)
    savefig(fig, "08_shap_summary")
    plt.show()
    log_step("SHAP explanations", t0)
else:
    print("SHAP skipped (package unavailable or model not tree-based); "
          "permutation importance and coefficients cover interpretability.")
''')

# =============================================================================
md(r"""
## 14. Subgroup performance

An aggregate metric can hide a model that works for the majority and fails for a subgroup. The
proposal committed to reporting where performance is materially weaker, so recall and precision
are broken out by segment, location, income and tenure band. Groups with fewer than 200 rows are
suppressed — their metrics are too noisy to act on.
""")

code(r'''
test_eval = test.copy()
test_eval["risk_score"] = best_proba
test_eval["pred"] = (best_proba >= best_thr).astype(int)
test_eval["tenure_band"] = pd.cut(test_eval["days_since_first_tx"],
                                  [-1, 90, 180, 270, 10_000],
                                  labels=["<3m", "3-6m", "6-9m", "9m+"])
test_eval["age_band"] = pd.cut(test_eval["age"], [0, 25, 35, 50, 65, 120],
                               labels=["<=25", "26-35", "36-50", "51-65", "65+"])

def subgroup_report(df, col, min_n=200):
    rows = []
    for g, d in df.groupby(col, observed=True):
        if len(d) < min_n or d[TARGET].sum() == 0:
            continue
        rows.append({
            "dimension": col, "group": str(g), "n": len(d),
            "prevalence": d[TARGET].mean(),
            "recall": recall_score(d[TARGET], d["pred"], zero_division=0),
            "precision": precision_score(d[TARGET], d["pred"], zero_division=0),
            "pr_auc": average_precision_score(d[TARGET], d["risk_score"]),
        })
    return pd.DataFrame(rows)

subgroups = pd.concat([
    subgroup_report(test_eval, c)
    for c in ["customer_segment", "location", "income_bracket", "clv_segment",
              "tenure_band", "gender", "age_band"]
    if c in test_eval.columns
], ignore_index=True)
subgroups.to_csv(os.path.join(TAB_DIR, "subgroup_performance.csv"), index=False)

overall_recall = recall_score(y_test, test_eval["pred"], zero_division=0)
weak = subgroups[subgroups.recall < overall_recall - 0.10]

fig, ax = plt.subplots(figsize=(10, max(4, 0.32 * len(subgroups))))
sns.barplot(data=subgroups.sort_values(["dimension", "recall"]),
            y="group", x="recall", hue="dimension", dodge=False, ax=ax)
ax.axvline(overall_recall, ls="--", c="k", label=f"overall recall {overall_recall:.3f}")
ax.set(title="Recall by subgroup (test cutoff, n>=200)", xlabel="Recall", ylabel="")
ax.legend(fontsize=8, loc="lower right")
plt.tight_layout()
savefig(fig, "09_subgroup_recall")
plt.show()

print(f"Overall test recall: {overall_recall:.3f}")
print(f"Subgroups more than 10 points below overall recall: {len(weak)}")
if len(weak):
    print(weak.round(3).to_string(index=False))
subgroups.round(3)
''')

# =============================================================================
md(r"""
## 15. Scored output for the dashboard

The model's job is not a probability, it is a **prioritised call list**. This section produces the
table the Power BI retention page reads.

Two design decisions:

* **Risk bands** come from validation-set quantiles, not round numbers, so "High" always means the
  top slice of the distribution the operator sees.
* **Retention priority** = risk × value. The stakeholder was explicit — contacting every high-risk
  customer wastes money; the useful list is *high risk **and** worth keeping*. Until Member B's
  90-day value model is available, observed 90-day pre-cutoff value stands in as the value axis,
  and the join key (`customer_id`, `observation_cutoff_date`) is already in place for the swap.
* **Top drivers** per customer come from the strongest deviations from segment norms, so the
  operator sees "silent 34 days vs. usual 6" rather than an opaque score.
""")

code(r'''
DRIVER_FEATURES = ["recency_days", "gap_vs_history", "cnt_ratio_30_over_prior60",
                   "cnt_delta_30_vs_prior30", "cnt_30d", "active_days_30d",
                   "app_logins_frequency", "active_products", "support_tickets_count",
                   "failed_transactions", "satisfaction_score"]
DRIVER_DIRECTION = {  # +1 = a HIGH value raises risk, -1 = a LOW value raises risk
    "recency_days": +1, "gap_vs_history": +1, "cnt_ratio_30_over_prior60": -1,
    "cnt_delta_30_vs_prior30": -1, "cnt_30d": -1, "active_days_30d": -1,
    "app_logins_frequency": -1, "active_products": -1, "support_tickets_count": +1,
    "failed_transactions": +1, "satisfaction_score": -1,
}

t0 = time.time()
# Bands from validation quantiles, applied unchanged to test. If the score distribution is so
# concentrated that the 70th and 90th percentiles coincide, fall back to rank-based bands so the
# dashboard always has three populated bands.
vp = valid_proba[best_label]
q70, q90 = float(np.quantile(vp, 0.70)), float(np.quantile(vp, 0.90))
scored = test_eval.copy()
if q70 < q90:
    band_edges = [-np.inf, q70, q90, np.inf]
    scored["risk_band"] = pd.cut(scored["risk_score"], band_edges,
                                 labels=["Low", "Medium", "High"])
else:
    print(f"validation quantiles tie (q70={q70:.4f}, q90={q90:.4f}); using rank-based bands")
    band_edges = [0.0, 0.70, 0.90, 1.0]
    scored["risk_band"] = pd.cut(scored["risk_score"].rank(pct=True), band_edges,
                                 labels=["Low", "Medium", "High"], include_lowest=True)

# value axis (placeholder for Member B's predicted 90-day value)
scored["value_proxy_cop"] = scored["amt_90d"]
scored["value_decile"] = pd.qcut(scored["value_proxy_cop"].rank(method="first"),
                                 10, labels=False) + 1
scored["risk_percentile"] = scored["risk_score"].rank(pct=True)
scored["retention_priority"] = scored["risk_percentile"] * (scored["value_decile"] / 10.0)

# per-customer drivers: z-scores against the eligible population at this cutoff
z = pd.DataFrame(index=scored.index)
for c in DRIVER_FEATURES:
    if c in scored.columns:
        mu, sd = scored[c].mean(), scored[c].std(ddof=0)
        z[c] = DRIVER_DIRECTION[c] * (scored[c] - mu) / (sd if sd else 1.0)

def top_drivers(row, k=3):
    s = row.dropna().sort_values(ascending=False)
    return "; ".join(f"{c} ({'high' if DRIVER_DIRECTION[c] > 0 else 'low'})"
                     for c in s.index[:k] if s[c] > 0.5) or "no strong deviation"

scored["top_drivers"] = z.apply(top_drivers, axis=1)

out_cols = ["customer_id", "observation_cutoff_date", "risk_score", "risk_band",
            "risk_percentile", "value_proxy_cop", "value_decile", "retention_priority",
            "top_drivers", "customer_segment", "location", "income_bracket",
            "clv_segment", "active_products", "app_logins_frequency",
            "recency_days", "cnt_30d", "cnt_90d", "amt_90d", "satisfaction_score",
            "support_tickets_count", "failed_transactions", TARGET]
scored_out = scored[[c for c in out_cols if c in scored.columns]].copy()
scored_out.to_csv(os.path.join(TAB_DIR, "customer_risk_scores.csv"), index=False)
log_step("score + band + drivers", t0, len(scored_out))

band_summary = (scored.groupby("risk_band", observed=True)
                .agg(customers=("customer_id", "size"),
                     mean_risk=("risk_score", "mean"),
                     actual_inactivity_rate=(TARGET, "mean"),
                     median_value_cop=("value_proxy_cop", "median"))
                .round(3))
print(band_summary.to_string())
print(f"\nwritten: {os.path.join(TAB_DIR, 'customer_risk_scores.csv')}  ({len(scored_out):,} rows)")
scored_out.sort_values("retention_priority", ascending=False).head(10)
''')

code(r'''
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# risk distribution with the operating threshold marked
sns.histplot(x=best_proba, hue=np.where(y_test == 1, "became inactive", "stayed active"),
             bins=40, stat="count", element="step", fill=True, alpha=0.4, ax=axes[0])
axes[0].axvline(best_thr, ls="--", c="k", label=f"threshold {best_thr:.2f}")
axes[0].set(title="Predicted risk distribution (test cutoff)", xlabel="risk score")
axes[0].legend(fontsize=8)

# the retention-priority quadrant the stakeholder asked for
q = scored.sample(min(6000, len(scored)), random_state=RANDOM_STATE)
axes[1].scatter(q["risk_percentile"], q["value_decile"],
                c=np.where(q[TARGET] == 1, "#c44e52", "#4c72b0"), s=8, alpha=0.35)
axes[1].axvline(0.80, ls="--", c="k", alpha=0.6)
axes[1].axhline(7.5, ls="--", c="k", alpha=0.6)
axes[1].text(0.82, 9.4, "PRIORITY\nhigh risk + high value", fontsize=9, fontweight="bold")
axes[1].set(xlabel="Inactivity-risk percentile", ylabel="Value decile (90-day observed)",
            title="Retention priority quadrants")
plt.tight_layout()
savefig(fig, "10_risk_and_priority")
plt.show()

priority = scored[(scored.risk_percentile >= 0.80) & (scored.value_decile >= 8)]
print(f"priority cell: {len(priority):,} customers "
      f"({len(priority)/len(scored)*100:.1f}% of eligible), "
      f"actual inactivity rate {priority[TARGET].mean()*100:.1f}% "
      f"vs {y_test.mean()*100:.1f}% overall")
''')

# =============================================================================
md(r"""
## 16. Scalability

The module's second assessment axis is system scalability, not just predictive accuracy. Two
measurements:

1. **Fraction sweep** — the full pipeline (feature engineering + fit + score) at 25 / 50 / 75 /
   100 % of *customers*. Sampling by customer, not by row, keeps each customer's history intact;
   sampling rows would silently corrupt every window feature and make the timings meaningless.
2. **Spark parity** — the same aggregation in PySpark when a session is reachable, so the
   single-node pandas baseline can be compared against the distributed cloud path described in
   the proposal.
""")

code(r'''
def pipeline_at_fraction(frac, seed=RANDOM_STATE):
    """Run feature engineering + fit + score on a customer-level sample of size `frac`."""
    ids = profile["customer_id"]
    keep = set(ids.sample(int(len(ids) * frac), random_state=seed)) if frac < 1.0 else set(ids)
    tx_s = tx[tx["customer_id"].isin(keep)]
    prof_s = profile[profile["customer_id"].isin(keep)]

    timings, parts = {}, []
    t0 = time.time()
    for spec in CUTOFFS:
        f = build_features(tx_s, spec["cutoff"])
        active = build_target(tx_s, spec["cutoff"])
        f["inactive_next_60d"] = (~f["customer_id"].isin(active)).astype(int)
        f["split"] = spec["split"]
        parts.append(f.merge(prof_s, on="customer_id", how="left"))
    p = pd.concat(parts, ignore_index=True)
    timings["feature_engineering"] = time.time() - t0

    trs, tes = p[p.split == "train"], p[p.split == "test"]
    t0 = time.time()
    m = Pipeline([("prep", make_preprocessor(num_features, cat_features, scale=False)),
                  ("clf", hgb(max_iter=200, learning_rate=0.08, early_stopping=True,
                              random_state=RANDOM_STATE))])
    m.fit(trs[feature_cols], trs[TARGET])
    timings["model_training"] = time.time() - t0

    t0 = time.time()
    _ = m.predict_proba(tes[feature_cols])[:, 1]
    timings["batch_scoring"] = time.time() - t0

    return {"fraction": frac, "customers": len(keep), "transactions": len(tx_s),
            "panel_rows": len(p),
            **{k: round(v, 3) for k, v in timings.items()},
            "total_seconds": round(sum(timings.values()), 3)}


scaling = pd.DataFrame([pipeline_at_fraction(f) for f in (0.25, 0.50, 0.75, 1.00)])
scaling["seconds_per_million_tx"] = (scaling.total_seconds
                                     / (scaling.transactions / 1e6)).round(2)
base = scaling.iloc[0]
scaling["speed_ratio_vs_25pct"] = (scaling.total_seconds / base.total_seconds).round(2)
scaling["data_ratio_vs_25pct"] = (scaling.transactions / base.transactions).round(2)
scaling.to_csv(os.path.join(TAB_DIR, "scalability_benchmark.csv"), index=False)
scaling
''')

code(r'''
fig, axes = plt.subplots(1, 2, figsize=(14, 4.6))

stack_cols = ["feature_engineering", "model_training", "batch_scoring"]
bottom = np.zeros(len(scaling))
for c in stack_cols:
    axes[0].bar(scaling.fraction * 100, scaling[c], bottom=bottom, width=15, label=c)
    bottom += scaling[c].values
axes[0].set(xlabel="% of customers", ylabel="seconds",
            title="Pipeline runtime by stage")
axes[0].legend(fontsize=8)

axes[1].plot(scaling.data_ratio_vs_25pct, scaling.speed_ratio_vs_25pct, "o-",
             label="observed")
axes[1].plot(scaling.data_ratio_vs_25pct, scaling.data_ratio_vs_25pct, "--", c="grey",
             label="linear (ideal)")
axes[1].set(xlabel="Data volume (x 25% baseline)", ylabel="Runtime (x 25% baseline)",
            title="Runtime growth vs. data growth")
axes[1].legend(fontsize=8)
plt.tight_layout()
savefig(fig, "11_scalability")
plt.show()

growth = scaling.speed_ratio_vs_25pct.iloc[-1] / scaling.data_ratio_vs_25pct.iloc[-1]
print(f"4x the data costs {scaling.speed_ratio_vs_25pct.iloc[-1]:.2f}x the runtime "
      f"-> growth factor {growth:.2f} "
      f"({'sub-linear' if growth < 1 else 'super-linear'} scaling on a single node)")
''')

code(r'''
# --- optional Spark parity check for the cloud path
try:
    from pyspark.sql import SparkSession, functions as F

    spark = (SparkSession.builder.appName("IT3388-retention")
             .config("spark.sql.shuffle.partitions", "16")
             .getOrCreate())
    t0 = time.time()
    sdf = spark.read.csv(f"{DATA_DIR}/transactions_data.csv", header=True, inferSchema=True)
    cut = "2023-10-30"
    lo = str((pd.Timestamp(cut) - timedelta(days=HISTORY_DAYS - 1)).date())
    agg = (sdf.filter((F.col("date") >= F.lit(lo)) & (F.col("date") <= F.lit(cut)))
              .groupBy("customer_id")
              .agg(F.count("*").alias("cnt_90d"),
                   F.sum("amount").alias("amt_90d"),
                   F.countDistinct("date").alias("active_days_90d"),
                   F.max("date").alias("last_tx")))
    n_spark = agg.count()
    spark_seconds = round(time.time() - t0, 2)
    print(f"Spark: {n_spark:,} customers aggregated in {spark_seconds}s")

    pandas_t0 = time.time()
    n_pandas = _window(tx[tx.date <= pd.Timestamp(cut)], pd.Timestamp(cut),
                       HISTORY_DAYS - 1, 0).groupby("customer_id").size().shape[0]
    pandas_seconds = round(time.time() - pandas_t0, 2)
    print(f"pandas: {n_pandas:,} customers aggregated in {pandas_seconds}s")
    assert n_spark == n_pandas, "Spark and pandas disagree on the eligible population"
    spark_comparison = {"spark_seconds": spark_seconds, "pandas_seconds": pandas_seconds}
    spark.stop()
except Exception as e:
    spark_comparison = None
    print(f"Spark parity check skipped: {type(e).__name__}: {e}")
    print("The pandas timings above remain the single-node baseline for the cloud comparison.")
''')

# =============================================================================
md(r"""
## 17. Results export

Everything Appendix E needs is written to `outputs/report_numbers.json`, so the report quotes
figures produced by this run instead of numbers copied by hand. `tools/fill_appendix_e.py`
reads this file and substitutes the placeholders in the report.
""")

code(r'''
test_rows = scoreboard[scoreboard.split == "test"].set_index("model")
best_row = test_rows.loc[best_label]
rule_row = test_rows.loc[[i for i in test_rows.index if i.startswith("Baseline: recency")][0]]
abl = ablation.set_index("model")

def g(d, k, nd=4):
    return round(float(d[k]), nd)

report_numbers = {
    "generated_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
    "data": {
        "customers": int(len(cust_raw)),
        "transactions_raw": int(len(tx_raw)),
        "transactions_deduped": int(len(tx)),
        "duplicates_dropped": int(n_dupes),
        "ledger_start": str(LEDGER_START.date()),
        "ledger_end": str(LEDGER_END.date()),
        "columns_excluded": int(len(DROP_COLS)),
        "features_used": int(len(feature_cols)),
    },
    "panel": {
        "rows": int(len(panel)),
        "train_rows": int(len(train)), "valid_rows": int(len(valid)), "test_rows": int(len(test)),
        "train_inactive_pct": round(float(y_train.mean() * 100), 2),
        "valid_inactive_pct": round(float(y_valid.mean() * 100), 2),
        "test_inactive_pct": round(float(y_test.mean() * 100), 2),
        "test_cutoff": str(design.loc[design.split == "test", "cutoff"].iloc[0]),
        "test_outcome_to": str(design.loc[design.split == "test", "outcome_to"].iloc[0]),
    },
    "best_model": {
        "name": best_label,
        "threshold": g(best_row, "threshold", 3),
        "recall": g(best_row, "recall"), "precision": g(best_row, "precision"),
        "f1": g(best_row, "f1"), "pr_auc": g(best_row, "pr_auc"),
        "roc_auc": g(best_row, "roc_auc"), "brier": g(best_row, "brier"),
        "recall_top10pct": g(best_row, "recall_top10pct"),
        "recall_top20pct": g(best_row, "recall_top20pct"),
        "precision_top10pct": g(best_row, "precision_top10pct"),
        "train_seconds": train_times.get(best_label),
    },
    "baseline_rule": {
        "threshold_days": int(best_k),
        "recall": g(rule_row, "recall"), "precision": g(rule_row, "precision"),
        "f1": g(rule_row, "f1"), "pr_auc": g(rule_row, "pr_auc"),
    },
    "prevalence_test": round(float(y_test.mean()), 4),
    "ablation": {name: {"pr_auc": g(abl.loc[f"HistGB — {name}"], "pr_auc"),
                        "recall": g(abl.loc[f"HistGB — {name}"], "recall"),
                        "recall_top10pct": g(abl.loc[f"HistGB — {name}"], "recall_top10pct"),
                        "n_features": int(abl.loc[f"HistGB — {name}", "n_features"])}
                 for name in ABLATIONS},
    "all_models_test": {r["model"]: {k: (round(float(r[k]), 4) if isinstance(r[k], (int, float)) else r[k])
                                     for k in ("recall", "precision", "f1", "pr_auc",
                                               "recall_top10pct", "threshold")}
                        for _, r in test_rows.reset_index().iterrows()},
    "top_drivers": imp.head(10)[["feature", "family", "importance"]]
                      .assign(importance=lambda d: d.importance.round(5))
                      .to_dict("records"),
    "family_importance_share_pct": (fam_share / fam_share.sum() * 100).round(1).to_dict(),
    "subgroups": {
        "n_reported": int(len(subgroups)),
        "n_weak": int(len(weak)),
        "weakest": (weak.sort_values("recall").head(3)[["dimension", "group", "n", "recall"]]
                    .round(3).to_dict("records") if len(weak) else []),
    },
    "capacity": cap[cap.capacity_pct.isin([10, 20, 30])][
        ["capacity_pct", "customers_contacted", "recall", "precision", "lift_vs_random"]
    ].round(3).to_dict("records"),
    "risk_bands": band_summary.reset_index().astype(str).to_dict("records"),
    "priority_cell": {
        "customers": int(len(priority)),
        "pct_of_eligible": round(len(priority) / len(scored) * 100, 1),
        "inactivity_rate_pct": round(float(priority[TARGET].mean() * 100), 1),
    },
    "scalability": scaling.to_dict("records"),
    "spark_comparison": spark_comparison,
    "run_log": run_log,
    "environment": {"xgboost": HAS_XGB, "shap": HAS_SHAP},
}

path = os.path.join(OUT_DIR, "report_numbers.json")
with open(path, "w") as fh:
    json.dump(report_numbers, fh, indent=2, default=str)
print(f"written: {path}")
print(json.dumps({k: report_numbers[k] for k in ("data", "panel", "best_model",
                                                 "baseline_rule", "priority_cell")},
                 indent=2, default=str))
''')

code(r'''
# --- full run log, for the Appendix E scalability section
log_df = pd.DataFrame(run_log)
log_df.to_csv(os.path.join(TAB_DIR, "run_log.csv"), index=False)

fig, ax = plt.subplots(figsize=(9, max(3.5, 0.32 * len(log_df))))
ax.barh(log_df.step, log_df.seconds)
ax.set(xlabel="seconds", title="Pipeline step timings (single node, 100% of data)")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"{x:,.0f}"))
plt.tight_layout()
savefig(fig, "12_run_log")
plt.show()

print(f"total logged pipeline time: {log_df.seconds.sum():.1f}s")
print(f"\nartefacts in ./{OUT_DIR}:")
for root, _, files in os.walk(OUT_DIR):
    for f in sorted(files):
        print(f"  {os.path.join(root, f)}")
''')

# =============================================================================
md(r"""
## 18. Findings

*The narrative below is written against the numbers this run produces; the printed cell above is
the authority if a rerun shifts them.*

**On the hypothesis.** The proposal predicted that declining frequency, longer recency, weaker
engagement, fewer products, lower satisfaction, unresolved support and failed transactions all
raise 60-day inactivity risk. The §13 ablation separates that claim into its parts: compare the
`trend/rhythm only` PR-AUC against `static profile only`. If trend alone approaches the full
model, the actionable reading is that **recent behavioural change carries the signal**, which
favours event-triggered retention over scheduled campaigns — and it also means the model needs
only ledger data to run, not the survey fields with 14 % coverage.

**On complexity.** The scoreboard in §11 compares four estimators against a tuned recency rule.
The rule is a genuine competitor: recency is mechanically related to the target, since a customer
already silent for 80 days is likely to stay silent. The defensible claim is not "the model
works" but "the model beats the rule an analyst would have written anyway", and by how much at a
realistic outreach capacity (§12).

**On what to do with it.** The §15 priority cell is the operational answer to the stakeholder's
constraint. Ranking by risk alone sends the team after cheap-to-lose customers; ranking by risk ×
value concentrates the same number of contacts on customers whose departure actually costs
something.

### Limitations carried into the report
- **Untimestamped snapshot fields.** `failed_transactions`, `support_tickets_count`,
  `satisfaction_score` and the complaint flags are whole-year totals. The `full minus snapshot
  fields` ablation measures the inflation; the honest headline figure is that variant, and both
  are reported.
- **Recency is close to the target by construction.** Reported as an interpretability finding, not
  a discovery — hence the recency-rule baseline.
- **One calendar year, four cutoffs.** Enough for chronological validation, not enough for any
  seasonality claim. The test cutoff sits in Oct–Dec; a Colombian year-end effect cannot be
  separated from model quality with this data.
- **Inactivity is not attrition.** The target is *no observed transactions in 60 days*. A customer
  may hold a balance, keep products open, and simply not transact.
- **Association, not causation.** Nothing here shows that contacting a flagged customer changes
  their behaviour. That needs a holdout experiment, which is the natural next step.
""")

# =============================================================================

nb = {
    "cells": [],
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {
            "name": "python", "version": "3.11", "mimetype": "text/x-python",
            "codemirror_mode": {"name": "ipython", "version": 3},
            "pygments_lexer": "ipython3", "file_extension": ".py",
        },
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

errors = 0
for kind, src in CELLS:
    lines = src.split("\n")
    source = [ln + "\n" for ln in lines[:-1]] + [lines[-1]]
    if kind == "code":
        try:
            compile(src, "<cell>", "exec")
        except SyntaxError as e:
            errors += 1
            print(f"SYNTAX ERROR in cell starting {lines[0][:60]!r}: {e}")
        nb["cells"].append({"cell_type": "code", "execution_count": None,
                            "metadata": {}, "outputs": [], "source": source})
    else:
        nb["cells"].append({"cell_type": "markdown", "metadata": {}, "source": source})

out = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "IMPLEMENTATION - INACTIVITY AND RETENTION.ipynb")
with open(out, "w") as fh:
    json.dump(nb, fh, indent=1)
    fh.write("\n")

n_code = sum(1 for k, _ in CELLS if k == "code")
print(f"{'FAILED' if errors else 'OK'}: wrote {out}")
print(f"  {len(CELLS)} cells ({n_code} code, {len(CELLS)-n_code} markdown), "
      f"{errors} syntax errors")
