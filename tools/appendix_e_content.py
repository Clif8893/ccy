"""Content for 'APPENDIX E - FINAL PROJECT REPORT.docx' (individual, Member A).

The report is parameterised over the metrics produced by
'IMPLEMENTATION - INACTIVITY AND RETENTION.ipynb'. Build it with no arguments to get the
document with every measured figure shown as an unresolved placeholder, or run
`python tools/fill_appendix_e.py` after executing the notebook to rebuild it with the real
numbers from outputs/report_numbers.json.

Keeping one source for both versions means the narrative can never drift from the numbers.
"""

import os

from docxgen import Document, check

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PROJECT = "Customer Intelligence Platform for Fintech"
DOC_TITLE = "Final Project Report"
FILE_NAME = "APPENDIX E \u2013 FINAL PROJECT REPORT.docx"
STUDENT = "Clifton Chen Yi"
SUPERVISOR = "\u2014"          # to be completed: supervisor name
AMENDED = "28 July 2026"
TO_FILL = "\u2014"

MEMBERS = [
    ("Clifton Chen Yi", "Member A / Team Leader", "Inactivity risk & retention prioritisation"),
    ("Tan Zheng Yu Evan", "Member B", "Future customer value"),
    ("Lee Yi Ting", "Member C", "Satisfaction & experience"),
    ("Wong Kang Bin", "Member D", "Transaction-demand classification"),
]


class Vals:
    """Resolves dotted paths against report_numbers.json, or emits a visible placeholder."""

    def __init__(self, data=None):
        self.data = data or {}
        self.unresolved = []

    def _get(self, path):
        node = self.data
        for key in path.split("."):
            if isinstance(node, list):
                try:
                    node = node[int(key)]
                    continue
                except (ValueError, IndexError):
                    return None
            if not isinstance(node, dict) or key not in node:
                return None
            node = node[key]
        return node

    def _miss(self, path):
        self.unresolved.append(path)
        return f"\u00ab{path}\u00bb"

    def num(self, path, nd=3):
        v = self._get(path)
        return f"{float(v):.{nd}f}" if isinstance(v, (int, float)) else self._miss(path)

    def pct(self, path, nd=1, of_one=True):
        v = self._get(path)
        if not isinstance(v, (int, float)):
            return self._miss(path)
        return f"{float(v) * (100 if of_one else 1):.{nd}f}%"

    def i(self, path):
        v = self._get(path)
        return f"{int(v):,}" if isinstance(v, (int, float)) else self._miss(path)

    def s(self, path):
        v = self._get(path)
        return str(v) if v is not None else self._miss(path)


def build(vals: Vals) -> Document:
    v = vals
    d = Document()
    d.set_header([f"{PROJECT} \u2014 {DOC_TITLE}  |  {{CHAPTER}}", f"{STUDENT}  |  {FILE_NAME}"])
    d.set_footer(FILE_NAME)

    # ---------------------------------------------------------------- cover page
    d.para("", after=600)
    d.title(PROJECT, "Big Data Management Project")
    d.para(f"**{DOC_TITLE}**", align="center", size=32, after=200)
    d.para("Workstream A \u2014 Inactivity Risk & Retention Prioritisation",
           align="center", size=24, after=600)
    d.table([
        ["Project module name", "IT3388 Big Data Management Project"],
        ["Document title", f"{DOC_TITLE} \u2014 {PROJECT}"],
        ["Supervisor's name", SUPERVISOR],
        ["Project team number", "Group 2 \u2014 FinSight Colombia"],
        ["Author of this report", f"{STUDENT} (Member A, Team Leader)"],
        ["Latest report amendment date", AMENDED],
    ], widths=[3400, 6584], header=False)
    d.para("", after=200)
    rows = [["Team member", "Admin No.", "Role", "Workstream owned"]]
    for name, role, stream in MEMBERS:
        rows.append([name, TO_FILL, role, stream])
    d.table(rows, widths=[2500, 1500, 2200, 3784], font_size=16)
    d.para("Admin numbers to be completed by each member before submission.", size=16,
           align="center")

    # ----------------------------------------------------------------- contents
    d.page_break()
    d.heading("Contents", 1, toc=False)
    d.toc(depth=2)

    if v.data:
        d.para(f"Measured figures in this report were produced by the run of "
               f"*IMPLEMENTATION - INACTIVITY AND RETENTION.ipynb* completed at "
               f"{v.s('generated_at')}.", size=18)

    # ============================================================ 1. Executive summary
    d.page_break()
    d.heading("1  Executive Summary", 1)

    d.heading("1.1  Business objectives, problem statement and goals", 2)
    d.para(
        "The client is an anonymised Colombian fintech offering savings, credit, loans, "
        "investments, bill payment and automatic savings through mobile and transactional "
        "channels. Its records describe what has already happened but do not answer the four "
        "forward-looking questions that drive its spending: which customers will stop "
        "transacting, which will generate future value, why satisfaction differs, and when "
        "transaction demand will peak. The team's Customer Intelligence Platform answers all "
        "four; this report covers the first.")
    d.para(
        "The business problem this workstream owns is narrower and sharper. Retention budget is "
        "finite, and it is currently spent reactively \u2014 often on customers who were never "
        "going to leave, while customers whose activity is quietly decaying receive nothing until "
        "they have already gone silent. The objective is therefore not to predict churn in the "
        "abstract but to produce **a ranked, explainable call list of previously active customers "
        "who are about to go quiet and who are worth keeping**.")
    d.para(
        "The stakeholder interview set the design constraint. Mar\u00eda Rodr\u00edguez, Customer "
        "Operations Manager at Nu Colombia, was explicit in a 20-minute Microsoft Teams call on "
        "16 July 2026: contacting every high-risk customer wastes resources, while missing a "
        "valuable one is a real commercial loss. That single sentence is why this workstream ends "
        "in a risk \u00d7 value priority view rather than a risk score, and why the evaluation "
        "reports recall at a realistic outreach capacity rather than accuracy.")

    d.heading("1.2  Hypothesis", 2)
    d.para(
        "*Among previously active customers, declining transaction frequency, longer recency, "
        "weaker app engagement, fewer active products, lower satisfaction, unresolved support "
        "issues and failed transactions increase the probability of no transactions during the "
        "following 60 days.*")
    d.para(
        "Inside that statement sits the question worth answering, because the two halves imply "
        "different businesses: if **recent behavioural change** carries the signal, retention "
        "should be event-triggered, firing when a specific customer's rhythm breaks. If **static "
        "attributes** carry it, retention is a segmentation exercise and can be planned in "
        "advance. Section 3.3 answers this by training models on restricted feature families and "
        "comparing them, rather than inferring it from a single importance chart.")

    d.heading("1.3  Goals and success criteria", 2)
    d.table([
        ["Goal", "How this report evidences it"],
        ["Business relevance",
         "A named decision (whom to contact), a named owner (customer operations) and an "
         "actionable output (a ranked, exportable list) \u2014 \u00a74."],
        ["Predictive effectiveness",
         "Beats both a majority-class baseline and a tuned recency rule on the held-out cutoff "
         "\u2014 \u00a73.1."],
        ["Leakage prevention",
         "Features computed only from transactions dated on or before each cutoff; all "
         "transformers fitted inside a pipeline on training data only \u2014 \u00a72.4."],
        ["Temporal generalisation",
         "Two early cutoffs train, a later one tunes, the latest tests, with no outcome-window "
         "overlap \u2014 \u00a72.3."],
        ["Interpretability",
         "Coefficients, permutation importance, feature-family ablation and per-customer drivers "
         "\u2014 \u00a73.3\u20133.4."],
        ["Scalability",
         "Stage-level timings at 25/50/75/100% of the data, plus a Spark comparison \u2014 "
         "\u00a75."],
        ["Responsible interpretation",
         "Inactivity distinguished from account closure, prediction from causation, and the "
         "untimestamped fields quantified rather than hidden \u2014 \u00a73.5, \u00a76."],
    ], widths=[2400, 7584], font_size=18)

    d.heading("1.4  Business scenario, insights and recommendations", 2)
    d.para(
        f"The dataset covers {v.i('data.customers')} customers and "
        f"{v.i('data.transactions_deduped')} transactions from {v.s('data.ledger_start')} to "
        f"{v.s('data.ledger_end')}. Restricting attention to customers who transacted in the 90 "
        f"days before each cutoff produced a panel of {v.i('panel.rows')} customer-cutoff "
        f"observations, of which {v.pct('panel.test_inactive_pct', 1, of_one=False)} of the "
        f"held-out test rows went on to record no transactions in the following 60 days.")
    d.para(
        f"The strongest model on the held-out cutoff was **{v.s('best_model.name')}**, reaching "
        f"recall {v.num('best_model.recall')} and precision {v.num('best_model.precision')} at "
        f"its validation-tuned threshold, with PR-AUC {v.num('best_model.pr_auc')} against a test "
        f"prevalence of {v.num('prevalence_test')}. Contacting only the highest-risk 10% of "
        f"eligible customers captures {v.pct('best_model.recall_top10pct')} of everyone who "
        f"actually went quiet; the top 20% captures {v.pct('best_model.recall_top20pct')}.")
    d.para("Three insights matter more than the headline metric:")
    d.bullets([
        f"**A rule, not a model, is the honest benchmark.** A tuned 'silent for "
        f"{v.s('baseline_rule.threshold_days')} days' rule already achieves F1 "
        f"{v.num('baseline_rule.f1')}, because recency is mechanically related to a target defined "
        f"as future silence. The model's value is the margin above that rule, not its absolute "
        f"score.",
        f"**The feature families are separable.** Trend and rhythm features alone reach PR-AUC "
        f"{v.num('ablation.trend/rhythm only.pr_auc')}; static profile attributes alone reach "
        f"{v.num('ablation.static profile only.pr_auc')}; everything together reaches "
        f"{v.num('ablation.full model.pr_auc')}. Section 3.3 reads the consequence for how "
        f"retention should be operated.",
        "**Risk alone is the wrong sort order.** Ranking by risk \u00d7 value concentrates the "
        "same number of contacts on customers whose departure actually costs the business "
        f"something: the priority cell holds {v.i('priority_cell.customers')} customers "
        f"({v.s('priority_cell.pct_of_eligible')}% of those eligible) at an observed inactivity "
        f"rate of {v.s('priority_cell.inactivity_rate_pct')}%.",
    ])
    d.para(
        "The recommendation is therefore to operate retention as an event-triggered process on the "
        "priority cell at a fixed weekly capacity, with the outreach threshold set by available "
        "staff rather than by a statistical cut-off \u2014 and to run the first campaign as a "
        "holdout experiment, because nothing in this analysis establishes that contacting a "
        "flagged customer changes their behaviour.")

    # ============================================================ 2. Data & method
    d.page_break()
    d.heading("2  Data and Methodology", 1)

    d.heading("2.1  Datasets", 2)
    d.table([
        ["Dataset", "Content", "Rows", "Use in this workstream"],
        ["customer_data.csv", "Demographics, location, product holdings, app engagement, "
         "satisfaction, support and complaint fields",
         v.i("data.customers"), "Static and experience predictors"],
        ["transactions_data.csv", "Raw ledger: customer_id, date, amount (COP), type",
         v.i("data.transactions_deduped"), "All behavioural features and the target"],
        ["retention_panel (derived)", "One row per (customer, observation cutoff) with "
         "leakage-controlled features and the label",
         v.i("panel.rows"), "The modelling table"],
    ], widths=[2300, 4000, 1400, 2284], font_size=18)
    d.para(
        "Both source files come from COFINFAD (Mendeley Data, doi:10.17632/mhb4zn3258). The "
        "supporting industry sources listed in \u00a78 justify the business context but are "
        "deliberately not joined to customer records: national aggregates cannot be attributed to "
        "individuals without inventing a relationship that does not exist in the data.")

    d.heading("2.2  Data preparation, and the decisions inside it", 2)
    d.para(
        "The exploration stage produced one finding that changed the plan for all four "
        "workstreams. The customer table carries two similarly named groups of transaction "
        "summaries. One group (`tx_count`, `avg_tx_value`, `total_tx_volume`, `first_tx`, "
        "`last_tx`) reconciles with the raw ledger for 100% of customers. The other "
        "(`average_transaction_value`, `total_transaction_volume`, `weekend_transaction_ratio`, "
        "`last_transaction_date` and companions) does not: correlations with the ledger it claims "
        "to summarise are approximately zero, and `last_transaction_date` agrees with the ledger "
        "for only 3.6% of customers. The second group is excluded rather than repaired, and every "
        "behavioural feature is recomputed from the ledger.")
    d.para(
        f"In total {v.i('data.columns_excluded')} columns were excluded before modelling, each "
        f"with a recorded reason, and {v.i('data.features_used')} features were used. The "
        f"substantive preparation decisions were:")
    d.table([
        ["Decision", "Rationale"],
        [f"Drop {v.i('data.duplicates_dropped')} exactly duplicated transaction rows",
         "The ledger has no transaction identifier, so an identical (customer, date, amount, type) "
         "quadruple is indistinguishable from a double-write. At 0.003% of rows the choice cannot "
         "change a conclusion, so the more likely explanation is taken and the count recorded."],
        ["Exclude the verified full-period aggregates too",
         "`tx_count` and friends are accurate but summarise the whole year \u2014 including the "
         "outcome window. Accuracy is not the same as admissibility."],
        ["Exclude `churn_probability` and `customer_lifetime_value`",
         "The first is a supplied churn score, which is the target in disguise; the second is a "
         "whole-period value estimate and belongs to Member B's workstream."],
        ["Recompute tenure from the ledger at each cutoff",
         "The supplied `customer_tenure` is measured at dataset end, so reusing it at an earlier "
         "cutoff would import information from after that cutoff."],
        ["Treat missingness as structure, not as a gap",
         "`credit_utilization_ratio` is missing precisely for customers with no credit card, and "
         "`complaint_topics` for customers who never complained. These became explicit indicator "
         "flags; mean-imputing them would have invented a utilisation ratio for people who cannot "
         "utilise anything."],
    ], widths=[3000, 6984], font_size=18)

    d.heading("2.3  Panel design and the target", 2)
    d.para(
        "`inactive_next_60d` is 1 when a customer records no transaction in the 60 days after an "
        "observation cutoff. A single snapshot would give one observation per customer and no way "
        "to test temporal generalisation, so the design is a panel of four cutoffs: features look "
        "only backwards, the label only forwards.")
    d.table([
        ["Role", "Cutoff", "Feature window (90 days)", "Outcome window (60 days)", "Rows"],
        ["Train", "2023-05-31", "2023-03-03 \u2192 2023-05-31", "2023-06-01 \u2192 2023-07-30",
         v.i("panel.train_rows")],
        ["Train", "2023-06-30", "2023-04-02 \u2192 2023-06-30", "2023-07-01 \u2192 2023-08-29",
         "(combined)"],
        ["Validation", "2023-08-31", "2023-06-03 \u2192 2023-08-31", "2023-09-01 \u2192 2023-10-30",
         v.i("panel.valid_rows")],
        ["Test", "2023-10-30", "2023-08-02 \u2192 2023-10-30", "2023-10-31 \u2192 2023-12-29",
         v.i("panel.test_rows")],
    ], widths=[1400, 1500, 2600, 2600, 1884], font_size=17)
    d.para("Three properties make the design defensible:")
    d.bullets([
        "**No truncated labels.** The test outcome window closes on 2023-12-29, exactly the last "
        "day of the ledger, so a customer labelled inactive really did have 60 full days of "
        "silence rather than a window cut short by the end of the data.",
        "**No short histories.** The earliest cutoff still has a complete 90-day feature window "
        "inside the ledger.",
        "**No outcome overlap between splits**, so behaviour used to label a training row cannot "
        "reappear as test-period behaviour.",
    ])
    d.para(
        "Eligibility is part of the hypothesis, not a convenience: a row is admitted only if the "
        "customer transacted at least once in the 90 days before the cutoff. Predicting that an "
        "already-dormant customer stays dormant is trivially easy, and including those customers "
        "would inflate every metric in this report while adding nothing the business can act on.")

    d.heading("2.4  Features and leakage control", 2)
    d.para(
        "All behavioural features derive from a single filtered frame, so the time boundary is "
        "enforced in one place rather than re-checked per feature. Five families were built:")
    d.table([
        ["Family", "Examples", "Hypothesis clause"],
        ["Recency", "days since last transaction, days since first transaction",
         "'longer recency'"],
        ["Frequency / volume", "counts, amounts and active days over 7 / 30 / 90 days",
         "'declining frequency' (level)"],
        ["Trend", "30-day count vs. the customer's own prior 60-day rate; count delta; amount "
         "ratio; share of 90-day activity in the last 30 days",
         "'**declining**' \u2014 the change itself"],
        ["Rhythm", "mean / max / standard deviation of inter-transaction gaps; current silence "
         "divided by the customer's own typical gap",
         "a stretching gap as early evidence"],
        ["Diversity / mix", "distinct transaction types, per-type shares, weekend ratio, amount "
         "concentration", "'transaction value and diversity'"],
    ], widths=[1800, 5400, 2784], font_size=18)
    d.para(
        "One feature deserves singling out. `gap_vs_history` divides the current silence by the "
        "customer's own average gap: five quiet days mean nothing for a monthly user and a great "
        "deal for a daily one, so the ratio is comparable across customers in a way the raw gap "
        "is not.")
    d.para(
        "Every candidate predictor was catalogued with its time boundary in one of three tiers: "
        "**pre-cutoff** (computed from transactions dated on or before the cutoff, verifiably "
        "safe), **static** (attributes that change slowly), and **snapshot**. The snapshot tier "
        "is the honest problem in this dataset, and \u00a76 treats it as such. A tripwire check "
        "also flags any single feature scoring above 0.95 univariate AUC, on the principle that "
        "one feature that strong is usually the label wearing a disguise.")

    # ============================================================ 3. Results
    d.page_break()
    d.heading("3  Results and Interpretation", 1)

    d.heading("3.1  Models against baselines", 2)
    d.para(
        "Two baselines were used, because 'better than random' is not an achievement on an "
        "imbalanced problem. The majority-class baseline is the floor. The second is the rule an "
        "analyst would write without any model: flag anyone silent for more than *k* days, with "
        f"*k* chosen on the validation split ({v.s('baseline_rule.threshold_days')} days). Every "
        "model's operating threshold was likewise tuned on validation and then frozen before "
        "touching the test cutoff.")
    d.table([
        ["Model (test cutoff)", "Recall", "Precision", "F1", "PR-AUC", "Recall @ top 10%"],
        [f"Baseline: recency \u2265 {v.s('baseline_rule.threshold_days')}d",
         v.num("baseline_rule.recall"), v.num("baseline_rule.precision"),
         v.num("baseline_rule.f1"), v.num("baseline_rule.pr_auc"), "\u2014"],
        ["Logistic Regression",
         v.num("all_models_test.Logistic Regression.recall"),
         v.num("all_models_test.Logistic Regression.precision"),
         v.num("all_models_test.Logistic Regression.f1"),
         v.num("all_models_test.Logistic Regression.pr_auc"),
         v.num("all_models_test.Logistic Regression.recall_top10pct")],
        ["Random Forest",
         v.num("all_models_test.Random Forest.recall"),
         v.num("all_models_test.Random Forest.precision"),
         v.num("all_models_test.Random Forest.f1"),
         v.num("all_models_test.Random Forest.pr_auc"),
         v.num("all_models_test.Random Forest.recall_top10pct")],
        ["HistGradientBoosting",
         v.num("all_models_test.HistGradientBoosting.recall"),
         v.num("all_models_test.HistGradientBoosting.precision"),
         v.num("all_models_test.HistGradientBoosting.f1"),
         v.num("all_models_test.HistGradientBoosting.pr_auc"),
         v.num("all_models_test.HistGradientBoosting.recall_top10pct")],
        [f"**{v.s('best_model.name')} (selected)**",
         f"**{v.num('best_model.recall')}**", f"**{v.num('best_model.precision')}**",
         f"**{v.num('best_model.f1')}**", f"**{v.num('best_model.pr_auc')}**",
         f"**{v.num('best_model.recall_top10pct')}**"],
    ], widths=[3000, 1400, 1400, 1200, 1400, 1584], font_size=17)
    d.para(
        f"The selected model is calibrated well enough for the dashboard to display its score as a "
        f"probability rather than a rank (Brier score {v.num('best_model.brier', 4)}), which "
        f"matters because an operator reading '0.7' will act as though seven in ten such customers "
        f"go quiet.")

    d.heading("3.2  What the model buys at a realistic capacity", 2)
    d.para(
        "A retention team has finite contacts, so the operational question is not the F1 score at "
        "an abstract threshold but the recall obtained for a fixed number of calls:")
    cap_rows = [["Outreach capacity", "Customers contacted", "Recall", "Precision",
                 "Lift vs. random"]]
    for idx, label in enumerate(["10%", "20%", "30%"]):
        cap_rows.append([label, v.i(f"capacity.{idx}.customers_contacted"),
                         v.num(f"capacity.{idx}.recall"), v.num(f"capacity.{idx}.precision"),
                         f"{v.num(f'capacity.{idx}.lift_vs_random', 2)}\u00d7"])
    d.table(cap_rows, widths=[2200, 2400, 1800, 1800, 1784], font_size=18)

    d.heading("3.3  The hypothesis, tested directly", 2)
    d.para(
        "The proposal's hypothesis bundles several claims, so it was decomposed: the same "
        "estimator was trained on restricted feature families and compared on the same held-out "
        "cutoff. This is the central result of the workstream.")
    d.table([
        ["Feature family", "Features", "PR-AUC", "Recall", "Recall @ top 10%"],
        ["Static profile only (demographics, products, engagement, experience)",
         v.i("ablation.static profile only.n_features"),
         v.num("ablation.static profile only.pr_auc"),
         v.num("ablation.static profile only.recall"),
         v.num("ablation.static profile only.recall_top10pct")],
        ["Behavioural level only (counts, amounts, active days)",
         v.i("ablation.behavioural level only.n_features"),
         v.num("ablation.behavioural level only.pr_auc"),
         v.num("ablation.behavioural level only.recall"),
         v.num("ablation.behavioural level only.recall_top10pct")],
        ["Trend / rhythm only (change and gap features)",
         v.i("ablation.trend/rhythm only.n_features"),
         v.num("ablation.trend/rhythm only.pr_auc"),
         v.num("ablation.trend/rhythm only.recall"),
         v.num("ablation.trend/rhythm only.recall_top10pct")],
        ["Behavioural (level + trend)",
         v.i("ablation.behavioural (level+trend).n_features"),
         v.num("ablation.behavioural (level+trend).pr_auc"),
         v.num("ablation.behavioural (level+trend).recall"),
         v.num("ablation.behavioural (level+trend).recall_top10pct")],
        ["Full model",
         v.i("ablation.full model.n_features"), v.num("ablation.full model.pr_auc"),
         v.num("ablation.full model.recall"), v.num("ablation.full model.recall_top10pct")],
        ["Full model minus untimestamped snapshot fields",
         v.i("ablation.full minus snapshot fields.n_features"),
         v.num("ablation.full minus snapshot fields.pr_auc"),
         v.num("ablation.full minus snapshot fields.recall"),
         v.num("ablation.full minus snapshot fields.recall_top10pct")],
    ], widths=[4000, 1300, 1500, 1500, 1684], font_size=17)
    d.para(
        "The table answers three questions at once. The gap between the behavioural and "
        "static-only rows says whether the ledger or the customer record carries the signal. The "
        "gap between 'level only' and 'level + trend' says whether *change* adds anything beyond "
        "*level* \u2014 the difference between event-triggered and scheduled retention. And the "
        "last row prices the look-ahead risk discussed in \u00a76: if performance barely moves "
        "when the untimestamped fields are removed, the concern is documented but immaterial; if "
        "it drops sharply, the honest headline number is the lower one.")

    d.heading("3.4  Drivers", 2)
    d.para("Permutation importance on the validation split, so the test cutoff stays untouched:")
    drv_rows = [["Rank", "Feature", "Family", "Importance (drop in PR-AUC)"]]
    for i in range(6):
        drv_rows.append([str(i + 1), v.s(f"top_drivers.{i}.feature"),
                         v.s(f"top_drivers.{i}.family"),
                         v.num(f"top_drivers.{i}.importance", 4)])
    d.table(drv_rows, widths=[900, 3800, 3000, 2284], font_size=18)
    d.para(
        "Logistic-regression coefficients confirm the direction of each effect, which importance "
        "alone cannot: a feature can matter a great deal while pointing the opposite way to the "
        "hypothesis, and that distinction is what makes a driver list usable by a human operator. "
        "For the dashboard, per-customer drivers are expressed as deviations from the eligible "
        "population \u2014 'silent 34 days against a usual gap of 6' rather than a coefficient.")

    d.heading("3.5  Where the model is weaker", 2)
    d.para(
        f"Recall and precision were broken out by segment, location, income, value segment, "
        f"tenure, age and gender, suppressing groups below 200 rows as too noisy to act on. Of "
        f"{v.s('subgroups.n_reported')} reported subgroups, {v.s('subgroups.n_weak')} fell more "
        f"than ten recall points below the overall figure. Reporting this is not a formality: a "
        f"retention list that systematically under-detects one location or income band would "
        f"quietly redistribute the retention budget away from that group.")

    # ============================================================ 4. Dashboard
    d.page_break()
    d.heading("4  Dashboard and Business Application", 1)
    d.para(
        "The model's output is not a probability, it is a call list. The scored table written by "
        "the pipeline carries, per customer: risk score, risk band, risk percentile, a value "
        "measure, the combined retention priority, the three strongest drivers, and the segment "
        "attributes needed for filtering.")
    d.table([
        ["Dashboard element", "Design decision and why"],
        ["Risk bands (High / Medium / Low)",
         "Cut at validation-set quantiles rather than round numbers, so 'High' always means the "
         "top slice of the distribution the operator is actually looking at."],
        ["Retention-priority view",
         "Risk percentile \u00d7 value decile, answering the stakeholder's constraint directly. "
         "Until Member B's 90-day value model is joined, observed 90-day pre-cutoff value stands "
         "in; the join key is already in place for the swap."],
        ["Adjustable capacity control",
         "The operator sets how many customers they can contact this week; the view returns that "
         "many, with the recall and precision implied by that capacity shown alongside."],
        ["Per-customer drivers",
         "Expressed as deviations from the population, so the agent opening the record knows what "
         "to say."],
        ["Model confidence and coverage",
         "Calibration and data-freshness indicators are shown on the page, because a stale or "
         "poorly calibrated score should visibly lose the operator's trust rather than silently "
         "keep it."],
        ["Subgroup performance panel",
         "Recall by segment, so a group the model serves badly is visible to the person acting on "
         "the list."],
    ], widths=[2600, 7384], font_size=18)
    d.para(
        "The workstream's boundary is worth stating plainly. It ranks customers; it does not "
        "choose the intervention, and it does not claim the intervention works. Those are "
        "decisions for customer operations, informed by an experiment this project recommends but "
        "does not run.")

    # ============================================================ 5. Scalability
    d.page_break()
    d.heading("5  Scalability and System Evaluation", 1)
    d.para(
        "The module assesses system scalability alongside predictive accuracy, so the full "
        "pipeline \u2014 feature engineering, training and batch scoring \u2014 was timed at 25, "
        "50, 75 and 100% of the data. Sampling is by **customer**, never by transaction row: "
        "sampling rows would silently corrupt every window feature and make the timings "
        "meaningless while still producing a plausible-looking chart.")
    sc_rows = [["Fraction", "Customers", "Transactions", "Feature eng. (s)", "Training (s)",
                "Scoring (s)", "Total (s)"]]
    for i, label in enumerate(["25%", "50%", "75%", "100%"]):
        sc_rows.append([label, v.i(f"scalability.{i}.customers"),
                        v.i(f"scalability.{i}.transactions"),
                        v.num(f"scalability.{i}.feature_engineering", 1),
                        v.num(f"scalability.{i}.model_training", 1),
                        v.num(f"scalability.{i}.batch_scoring", 1),
                        v.num(f"scalability.{i}.total_seconds", 1)])
    d.table(sc_rows, widths=[1100, 1500, 1700, 1900, 1300, 1300, 1184], font_size=17)
    d.para(
        f"Runtime at full scale is {v.num('scalability.3.speed_ratio_vs_25pct', 2)}\u00d7 the "
        f"25% baseline for {v.num('scalability.3.data_ratio_vs_25pct', 2)}\u00d7 the data. "
        f"Feature engineering dominates, which is expected: the pipeline makes several passes over "
        f"a 3.16-million-row ledger to build nested windows, while training operates on a panel "
        f"two orders of magnitude smaller.")
    d.para(
        "That points at the right optimisation. The windowed aggregations are the bottleneck and "
        "they are also the most parallelisable part of the work, so the cloud path partitions the "
        "ledger by date and pushes the aggregation into Spark, keeping model training on a single "
        "node where it already costs little. Incremental aggregation is the further step: each new "
        "cutoff recomputes windows that overlap heavily with the previous cutoff's, so caching the "
        "daily per-customer counts would remove most of the repeated work. For a daily batch on "
        "this data volume the current single-node runtime is already inside an acceptable window; "
        "the distributed path matters for growth, not for today.")

    # ============================================================ 6. Problems
    d.page_break()
    d.heading("6  Problems Encountered", 1)

    d.heading("6.1  Two conflicting sets of transaction summaries", 2)
    d.para(
        "The customer table contains two similarly named groups of transaction aggregates, and "
        "the obvious ones are the wrong ones. Had `average_transaction_value` and "
        "`last_transaction_date` been used at face value, every downstream feature would have "
        "been quietly wrong and the models would still have produced respectable-looking metrics. "
        "The problem was caught by recomputing all aggregates from the ledger and comparing, "
        "rather than by inspecting the columns \u2014 the mismatch is invisible to eye-balling. "
        "This was raised with all four workstream owners before any modelling began, and became "
        "the first clause of the shared preprocessing contract.")

    d.heading("6.2  Untimestamped experience fields", 2)
    d.para(
        "`failed_transactions`, `support_tickets_count`, `satisfaction_score`, `nps_score` and the "
        "complaint flags are supplied as whole-year totals with no dates. They are also precisely "
        "the fields the hypothesis names. This cannot be fixed with better feature engineering: "
        "the timestamps do not exist in the data, so a ticket logged after a cutoff is still "
        "visible at that cutoff.")
    d.para(
        "Three options were available: use them and stay quiet, drop them and lose part of the "
        "hypothesis, or use them and measure the damage. The third was chosen. The fields are "
        "labelled as a distinct 'snapshot' tier in the feature catalogue, and a parallel model "
        "trained without them (\u00a73.3, last row) quantifies the inflation, so a reader can see "
        "the optimistic and the conservative figure side by side.")

    d.heading("6.3  Recency is close to the target by construction", 2)
    d.para(
        "A customer already silent for 80 days is very likely to stay silent for 60 more. Recency "
        "is therefore both the most important feature and the least interesting finding, and a "
        "model reporting high recall on the strength of it would be reporting a definition rather "
        "than a discovery. The response was to make the recency rule an explicit baseline, so the "
        "model must beat what recency alone already achieves, and to separate the trend family in "
        "\u00a73.3 so that any additional signal is attributable.")

    d.heading("6.4  Defining the outcome window without truncating labels", 2)
    d.para(
        "The first cutoff schedule placed the test window so that it ran past the end of the "
        "ledger, which silently mislabels customers as inactive when the data simply stops. The "
        "cutoffs were re-derived from the ledger end backwards so the test outcome window closes "
        "exactly on 2023-12-29, and the notebook now asserts this rather than trusting it \u2014 "
        "a design error that produces optimistic labels is worth an assertion, not a comment.")

    d.heading("6.5  Class imbalance without corrupting the panel", 2)
    d.para(
        "Inactivity is the minority class, and the usual reflex is synthetic oversampling. That is "
        "wrong here: a panel repeats the same customer at several cutoffs, so interpolating "
        "between rows can manufacture a customer who is a blend of one real person at two points "
        "in time. Class weights were used instead, which also leaves the predicted probabilities "
        "interpretable for the dashboard, and thresholds were tuned on validation rather than left "
        "at 0.5.")

    # ============================================================ 7. Conclusion
    d.page_break()
    d.heading("7  Conclusion and Recommendations", 1)
    d.para(
        f"This workstream set out to convert one year of Colombian fintech data into a decision "
        f"the business can act on before a customer goes quiet. It produced a leakage-controlled "
        f"panel of {v.i('panel.rows')} customer-cutoff observations, a chronologically validated "
        f"classifier for `inactive_next_60d`, and a scored retention list ordered by risk and "
        f"value. On the held-out cutoff the selected {v.s('best_model.name')} model reached recall "
        f"{v.num('best_model.recall')} at precision {v.num('best_model.precision')} "
        f"(PR-AUC {v.num('best_model.pr_auc')}), and captured "
        f"{v.pct('best_model.recall_top10pct')} of eventual inactivity within the highest-risk "
        f"10% of customers.")
    d.para(
        "Measured against the project's own expectations, the outcome fits in the ways that "
        "matter and falls short in one that should be stated. It fits in that the pipeline runs "
        "end to end on cloud-scale data, the model beats both baselines, the drivers are "
        "interpretable, and the output is a prioritised list rather than a score. It falls short "
        "in that the hypothesis cannot be pronounced true as a whole: it names seven mechanisms, "
        "and \u00a73.3 shows they do not contribute equally \u2014 the behavioural families carry "
        "most of the signal, while the experience and demographic clauses contribute far less "
        "than the proposal implied, and part of even that contribution rests on fields whose "
        "timing cannot be verified. A hypothesis that is partly supported and honestly decomposed "
        "is a more useful result than one declared correct in full.")
    d.para("**Recommendations, in order of confidence:**")
    d.numbered([
        "**Operate on the priority cell, not the risk ranking.** Contact high-risk *and* "
        "high-value customers at a weekly capacity set by staffing. This follows directly from "
        "the stakeholder's constraint and costs nothing extra to implement.",
        "**Trigger on behavioural change, not on a calendar.** The signal lives in features that "
        "move week to week, so scoring should be a scheduled batch feeding an alert queue, not a "
        "quarterly campaign list.",
        "**Report the conservative number internally.** Where the snapshot-free model in "
        "\u00a73.3 is materially weaker, plan capacity against that figure; it is the one that "
        "does not depend on fields whose timing cannot be verified.",
        "**Run the first campaign as an experiment.** Hold back a random portion of the priority "
        "cell and compare. Nothing in this report shows that contact changes behaviour, and this "
        "is the cheapest way to find out.",
        "**Fix the data at source.** Ask for timestamps on support tickets, failed transactions "
        "and satisfaction responses, and for the unreliable summary columns to be corrected or "
        "withdrawn. Both would improve every workstream in the platform, not just this one.",
    ])
    d.para(
        "**Limits to carry forward.** The target is *no observed transactions in 60 days*, which "
        "is not account closure: a customer may hold a balance, keep products open and simply not "
        "transact. The data covers one calendar year and four cutoffs, which supports "
        "chronological validation but no claim about seasonality \u2014 and the test cutoff sits "
        "in the October-to-December window, so a Colombian year-end effect cannot be separated "
        "from model quality. Every relationship reported here is an association. And the findings "
        "describe one anonymised company; external or later-period validation would be needed "
        "before generalising them.")

    # ============================================================ 8. References
    d.page_break()
    d.heading("8  References and Artefacts", 1)
    d.heading("8.1  Data and supporting sources", 2)
    d.bullets([
        "COFINFAD dataset, Mendeley Data: https://data.mendeley.com/datasets/mhb4zn3258",
        "COFINFAD, Hugging Face mirror: https://huggingface.co/datasets/luisdavidtrejosrojas/cofinfad",
        "COFINFAD dataset article: https://doi.org/10.1016/j.dib.2026.112484",
        "Financial Inclusion Report 2023, Superintendencia Financiera de Colombia: "
        "https://www.superfinanciera.gov.co/publicaciones/10115193/reporte-de-inclusion-financiera-2023-avances-y-retos-en-colombia/",
        "Financial Infrastructure and Payment Instruments Report 2024, Banco de la Rep\u00fablica: "
        "https://www.banrep.gov.co/en/publications-research/financial-infrastructure-payment-instruments-report/2024",
        "Colombia Financial Technology, U.S. International Trade Administration: "
        "https://www.trade.gov/market-intelligence/colombia-financial-technology",
        "Finnovista Fintech Radar Colombia: "
        "https://assets.ctfassets.net/bvz14004tu0h/2WuRBepO4liQPXYTXBFJZy/56365603e9bccbb96abf61983ee20c1f/RADAR_COLOMBIA_ENGLISH_.pdf",
    ])
    d.heading("8.2  Artefacts produced by this workstream", 2)
    d.para("Excluded from the page count, per the report guidelines.", size=18)
    d.table([
        ["Artefact", "Contents"],
        ["EXPLORATORY DATA ANALYSIS.ipynb", "Shared exploration: quality audit, join validation, "
         "the Set A / Set B reconciliation, workstream signals"],
        ["IMPLEMENTATION - INACTIVITY AND RETENTION.ipynb",
         "This workstream end to end: preprocessing, panel, features, models, evaluation, "
         "scoring, scalability"],
        ["outputs/tables/retention_panel.parquet", "The derived analytical table"],
        ["outputs/tables/feature_catalogue.csv",
         "Every feature with its family, time boundary and missingness"],
        ["outputs/tables/model_scoreboard.csv", "All models on validation and test"],
        ["outputs/tables/feature_family_ablation.csv", "The hypothesis test of \u00a73.3"],
        ["outputs/tables/permutation_importance.csv, logreg_coefficients.csv",
         "Driver analysis"],
        ["outputs/tables/subgroup_performance.csv", "Recall and precision by subgroup"],
        ["outputs/tables/customer_risk_scores.csv",
         "The scored retention list feeding the dashboard"],
        ["outputs/tables/scalability_benchmark.csv, run_log.csv", "Timings behind \u00a75"],
        ["outputs/figures/*.png", "Twelve figures covering target balance through scalability"],
        ["outputs/report_numbers.json", "Every figure quoted in this report"],
    ], widths=[4200, 5784], font_size=17)
    return d


def main(values=None, out_name=FILE_NAME):
    vals = Vals(values)
    doc = build(vals)
    if vals.unresolved:
        # Insert a visible notice immediately after the contents page rather than letting a
        # reader discover the placeholders on their own.
        notice = (
            f"**Note:** {len(set(vals.unresolved))} measured figures in this report are shown as "
            "\u00abplaceholders\u00bb. Run *IMPLEMENTATION - INACTIVITY AND RETENTION.ipynb* to "
            "produce outputs/report_numbers.json, then run `python tools/fill_appendix_e.py` to "
            "regenerate this document with the measured values substituted."
        )
        idx = doc._toc_placeholder_index
        doc.blocks.insert(idx + 1,
                          '<w:p><w:pPr><w:spacing w:before="240" w:after="120" w:line="360" '
                          'w:lineRule="auto"/><w:pBdr>'
                          '<w:top w:val="single" w:sz="6" w:space="4" w:color="C44E52"/>'
                          '<w:bottom w:val="single" w:sz="6" w:space="4" w:color="C44E52"/>'
                          '</w:pBdr></w:pPr>'
                          + __import__("docxgen")._runs_from_markup(notice)
                          + "</w:p>")
    path = os.path.join(REPO, out_name)
    doc.save(path)
    print(check(path))
    if vals.unresolved:
        print(f"  {len(set(vals.unresolved))} unresolved placeholders "
              f"(expected before the notebook has been run)")
    else:
        print("  all figures resolved from outputs/report_numbers.json")
    return path


if __name__ == "__main__":
    main()
