"""Generates Appendix A, B, F and G for the IT3388 project submission.

Appendix E is generated separately (see appendix_e_content.py / fill_appendix_e.py) because it
embeds figures from the modelling run.
"""

import os

from docxgen import Document, check

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

MODULE_GROUP = "IT3388"
GROUP_NUMBER = "2"
TEAM_NAME = "FinSight Colombia"
PROJECT_TITLE = "Customer Intelligence Platform for Fintech"

MEMBERS = [
    ("A", "Clifton Chen Yi", "Team Leader", "Inactivity risk & retention prioritisation"),
    ("B", "Tan Zheng Yu Evan", "Member", "Future customer value (90-day transaction value)"),
    ("C", "Lee Yi Ting", "Member", "Satisfaction, experience & complaint NLP"),
    ("D", "Wong Kang Bin", "Member", "Next-day transaction-demand classification"),
]

TO_FILL = "\u2014"  # em dash: a field the team must complete by hand


def cover_block(doc, doc_title, subtitle=None):
    doc.para("", after=400)
    doc.title(PROJECT_TITLE, subtitle or "Big Data Management Project")
    doc.para(doc_title, align="center", size=28, after=400)
    doc.table(
        [["Module Group", MODULE_GROUP],
         ["Project Group Number", GROUP_NUMBER],
         ["Team Name", TEAM_NAME],
         ["Dataset", "COFINFAD \u2014 Colombian fintech customer & transaction data (2023)"]],
        widths=[3200, 6784], header=False,
    )


# =============================================================================
# APPENDIX A
# =============================================================================
def appendix_a():
    d = Document()
    d.set_header([f"{PROJECT_TITLE} \u2014 Appendix A: Project Team Organisation",
                  "APPENDIX A \u2013 PROJECT TEAM ORGANISATION.docx"])
    d.set_footer(f"{MODULE_GROUP} Group {GROUP_NUMBER} \u2014 {TEAM_NAME}")

    d.heading("Appendix A \u2013 Form for Submitting Project Team Organisation", 1, toc=False)
    d.para("(To be completed as a team)", size=20)

    d.table([["Module Group", MODULE_GROUP],
             ["Project Group Number", GROUP_NUMBER],
             ["Team Name (optional)", TEAM_NAME]],
            widths=[3000, 6984], header=False)

    d.heading("Team members", 2, toc=False)
    rows = [["S/n", "Name", "Admin No.", "Handphone", "Personal email"]]
    for i, (_letter, name, role, _stream) in enumerate(MEMBERS, start=1):
        star = "1*" if role == "Team Leader" else str(i)
        rows.append([star, name, TO_FILL, TO_FILL, TO_FILL])
    d.table(rows, widths=[700, 3100, 1600, 1800, 2784])
    d.para("* denotes the Team Leader", size=18)
    d.para(
        "Admin numbers, handphone numbers and personal email addresses are intentionally left "
        "blank: they are personal data, and each member should enter their own before the form "
        "is submitted.", size=20)

    d.heading("Workstream ownership", 2, toc=False)
    d.para(
        "The team shares one dataset, one cloud pipeline and one dashboard, but each member owns a "
        "distinct predictive problem, target variable and set of conclusions. This division was "
        "agreed at team formation and is reflected in the proposal (Appendix C).")
    rows = [["Member", "Name", "Role", "Workstream owned", "Target variable"]]
    targets = {
        "A": "inactive_next_60d (binary)",
        "B": "transaction_value_next_90d (continuous)",
        "C": "satisfaction_score (ordinal, 1\u20136)",
        "D": "next_day_demand_class (low / normal / high)",
    }
    for letter, name, role, stream in MEMBERS:
        rows.append([f"Member {letter}", name, role, stream, targets[letter]])
    d.table(rows, widths=[1000, 2100, 1300, 3300, 2284])

    d.heading("Agreed ways of working", 2, toc=False)
    d.bullets([
        "**Weekly team meeting** during the practical slot, with progress captured in the "
        "Blackboard Group Blog by the team leader (Appendix B).",
        "**Shared preprocessing is written once.** The deduplication rule, the excluded-column "
        "list and the cutoff-date convention are agreed by all four members before any member "
        "engineers features, so no two workstreams clean the same field differently.",
        "**Separate conclusions.** Members may reuse each other's cleaned tables but must not "
        "reuse each other's targets, metrics or findings.",
        "**Notebook integration.** Each member owns one implementation notebook with a consistent "
        "section structure, so the notebooks can be integrated for the final submission.",
        "**Escalation.** Blockers are raised in the weekly meeting; anything that threatens a "
        "milestone is escalated to the supervisor by the team leader within the same week.",
    ])
    return d


# =============================================================================
# APPENDIX B
# =============================================================================
WEEKS = [
    {
        "week": 11, "dates": "29 Jun \u2013 5 Jul 2026", "status": "Completed",
        "theme": "Big Data Analytics Project Overview \u2014 briefing & team formation",
        "progress": [
            ("Clifton Chen Yi", "Elected team leader. Set up the shared repository and the "
             "Blackboard blog. Read the project guide and drafted the assessment/milestone map "
             "(Appendix A ways-of-working)."),
            ("Tan Zheng Yu Evan", "Surveyed candidate datasets for a fintech scenario; shortlisted "
             "COFINFAD on Mendeley Data and verified its licence and documentation article."),
            ("Lee Yi Ting", "Reviewed the four rubrics and extracted the concrete evidence each "
             "one asks for, so the team knows what has to exist by week 16 and week 18."),
            ("Wong Kang Bin", "Downloaded COFINFAD and confirmed both files load and join on "
             "customer_id; reported row counts (48,723 customers / 3,159,157 transactions)."),
        ],
        "misc": "Agreed the scenario: an anonymised Colombian fintech. Confirmed that four "
                "genuinely distinct ML problems can be carved out of one dataset, which the "
                "rubric requires (four owners, four targets).",
    },
    {
        "week": 12, "dates": "6 \u2013 12 Jul 2026", "status": "Completed",
        "theme": "Exploratory Data Analysis",
        "progress": [
            ("Clifton Chen Yi", "Built the shared EDA notebook: data-quality audit, missingness "
             "profiling, date-coverage check and the customer/transaction join validation. Found "
             "that the 'Set B' transaction-summary columns do not reconcile with the raw ledger "
             "(correlation \u2248 0.00) and raised it with the whole team."),
            ("Tan Zheng Yu Evan", "Explored customer value and CLV segments; showed that activity "
             "level and lifetime value are related but not interchangeable."),
            ("Lee Yi Ting", "Explored satisfaction, support tickets and complaint topics; "
             "quantified survey coverage (\u224814% of customers) and its effect on claims."),
            ("Wong Kang Bin", "Aggregated the ledger to daily volume/value; confirmed all 360 "
             "covered days have activity and identified the weekday/weekend split."),
        ],
        "misc": "Key shared decision recorded: build features from the verified 'Set A' columns "
                "and the raw ledger, and exclude the unreliable 'Set B' columns. This single "
                "finding changed the feature plan for all four workstreams.",
    },
    {
        "week": 13, "dates": "13 \u2013 19 Jul 2026", "status": "Completed \u2014 submitted",
        "theme": "Project Proposal + Exploratory Data Analysis (deliverable week)",
        "progress": [
            ("Clifton Chen Yi", "Wrote the retention workstream sections of the proposal and the "
             "individual EDA report (Appendix D). Co-ran the stakeholder interview and rewrote the "
             "retention design around her feedback."),
            ("Tan Zheng Yu Evan", "Wrote the future-value hypothesis, feature plan and evaluation "
             "metrics; confirmed the 90-day outcome window fits inside the ledger."),
            ("Lee Yi Ting", "Wrote the satisfaction hypothesis and the ordinal-evaluation plan; "
             "specified the complaint-NLP approach and the respondent-only caveat."),
            ("Wong Kang Bin", "Wrote the demand-classification hypothesis, defined "
             "next_day_demand_class and the walk-forward validation plan."),
        ],
        "misc": "Stakeholder interview: Mar\u00eda Rodr\u00edguez, Customer Operations Manager, Nu "
                "Colombia \u2014 20-minute Microsoft Teams call on 16 July 2026. Her constraint "
                "(\"prioritise customers who are both high-risk and valuable\") produced three "
                "concrete proposal changes: a retention-priority view, high-demand recall as "
                "Member D's primary metric, and adjustable capacity thresholds. Submitted "
                "Appendix C and Appendix D by 19 July 2026, 2359 hrs.",
    },
    {
        "week": 14, "dates": "20 \u2013 26 Jul 2026", "status": "Completed",
        "theme": "Project Implementation \u2014 shared preprocessing & feature layer",
        "progress": [
            ("Clifton Chen Yi", "Agreed and implemented the shared preprocessing contract: drop "
             "exact duplicate transactions (102 rows, 0.003%), exclude the Set B columns and the "
             "full-period Set A aggregates, and use a common (customer, cutoff) panel key. Built "
             "the retention feature engineering and the inactive_next_60d target with a "
             "four-cutoff chronological design."),
            ("Tan Zheng Yu Evan", "Built the 90-day value target on the same cutoff convention; "
             "began comparing regularised linear models against tree-based regressors."),
            ("Lee Yi Ting", "Standardised complaint topics and began the ordinal satisfaction "
             "baseline; excluded the satisfaction components and NPS to avoid circularity."),
            ("Wong Kang Bin", "Built the daily aggregation, calendar features and shifted lags; "
             "fitted the demand-class thresholds on training-period quantiles only."),
        ],
        "misc": "Leakage review held as a team. Agreed that churn_probability and "
                "customer_lifetime_value are excluded everywhere, and that the untimestamped "
                "customer-level counters (failed_transactions, support_tickets_count, "
                "satisfaction_score) must be reported as a known look-ahead risk and tested by "
                "ablation rather than quietly used.",
    },
    {
        "week": 15, "dates": "27 Jul \u2013 2 Aug 2026", "status": "In progress",
        "theme": "Project Implementation \u2014 modelling & evaluation",
        "progress": [
            ("Clifton Chen Yi", "Model comparison for the retention workstream: majority-class "
             "and tuned recency-rule baselines against Logistic Regression, Random Forest, "
             "HistGradientBoosting and XGBoost, with thresholds tuned on validation only. Running "
             "the feature-family ablation that tests the core hypothesis (trend vs. static)."),
            ("Tan Zheng Yu Evan", "Evaluating MAE / RMSE / R\u00b2 and top-value-quantile "
             "identification; testing the effect of log transformation on the skewed target."),
            ("Lee Yi Ting", "Comparing ordinal classification approaches on Macro F1, weighted "
             "kappa and within-one-level accuracy; building the complaint-topic comparison."),
            ("Wong Kang Bin", "Walk-forward validation across the demand models; tracking "
             "high-demand recall as the primary operational metric."),
        ],
        "misc": "Next: cloud deployment of the pipeline and the first end-to-end dashboard "
                "refresh, so the interim review can show a working path from raw upload to a "
                "scored table.",
    },
    {
        "week": 16, "dates": "3 \u2013 9 Aug 2026", "status": "Planned",
        "theme": "Project Implementation \u2014 Interim Progress Review (30%)",
        "progress": [
            ("Clifton Chen Yi", "Present the retention pipeline end-to-end: collection, "
             "management, preparation, visualisation. Show the scored risk table and the "
             "retention-priority view in the dashboard."),
            ("Tan Zheng Yu Evan", "Present value predictions, error analysis in COP and the "
             "value-segment definitions."),
            ("Lee Yi Ting", "Present satisfaction results, confusion patterns and complaint "
             "themes with respondent-coverage caveats."),
            ("Wong Kang Bin", "Present demand classification with the peak-day alert view and "
             "false-negative review."),
        ],
        "misc": "Deliverable: interim progress review, assessed individually (30%). Evidence "
                "needed per rubric: multiple data sources, systematic data management, complete "
                "cleaning with rationale, multiple effective visualisations, and evidence of "
                "co-creation.",
    },
    {
        "week": 17, "dates": "10 \u2013 16 Aug 2026", "status": "Planned",
        "theme": "Project Implementation \u2014 integration, scalability & rehearsal",
        "progress": [
            ("Clifton Chen Yi", "Integrate the four notebooks into one runnable sequence; run the "
             "25/50/75/100% scalability benchmark and the Spark comparison; freeze the retention "
             "results for the report."),
            ("Tan Zheng Yu Evan", "Finalise value model, export the targeting list and join "
             "predicted value into the retention-priority view."),
            ("Lee Yi Ting", "Finalise the experience page and the improvement-priority ranking."),
            ("Wong Kang Bin", "Finalise the operations page, capacity thresholds and next-day "
             "alerting."),
        ],
        "misc": "Planned: replace the observed-value placeholder in Clifton's priority view with "
                "Member B's predicted 90-day value, which is the last cross-workstream "
                "dependency. Full presentation rehearsal against the Appendix G checklist.",
    },
    {
        "week": 18, "dates": "17 \u2013 23 Aug 2026", "status": "Planned",
        "theme": "Final Project Presentation & Report Submission",
        "progress": [
            ("Clifton Chen Yi", "Deliver the retention segment of the presentation; submit the "
             "individual final report (Appendix E) and the peer review (Appendix F)."),
            ("Tan Zheng Yu Evan", "Deliver the value segment; submit report and peer review."),
            ("Lee Yi Ting", "Deliver the experience segment; submit report and peer review."),
            ("Wong Kang Bin", "Deliver the operations segment; submit report and peer review."),
        ],
        "misc": "All submissions via Brightspace by 23 August 2026, 2359 hrs. Late submissions "
                "are penalised.",
    },
]

GANTT_TASKS = [
    ("Team formation & scoping", 11, 11),
    ("Dataset selection & licence check", 11, 12),
    ("Exploratory data analysis (shared)", 12, 13),
    ("Stakeholder interview & empathy map", 13, 13),
    ("Proposal + individual EDA writeup", 13, 13),
    ("Shared preprocessing contract", 14, 14),
    ("Feature engineering & target design", 14, 15),
    ("Model training & evaluation", 15, 16),
    ("Cloud deployment", 15, 17),
    ("Dashboard build", 15, 17),
    ("Interim progress review", 16, 16),
    ("Notebook integration", 17, 17),
    ("Scalability benchmarking", 17, 17),
    ("Final report", 17, 18),
    ("Presentation rehearsal & delivery", 17, 18),
]


def appendix_b():
    d = Document()
    d.set_header([f"{PROJECT_TITLE} \u2014 Appendix B: Weekly Progress Updates",
                  "APPENDIX B \u2013 WEEKLY PROGRESS UPDATES.docx"])
    d.set_footer(f"{MODULE_GROUP} Group {GROUP_NUMBER} \u2014 {TEAM_NAME}")

    d.heading("Appendix B \u2013 Weekly Progress Updates", 1, toc=False)
    d.para("(To be completed as a team in the Blackboard Group Blog)", size=20)
    d.para(
        "This document is the consolidated record of the weekly team and progress update "
        "meetings. Each entry below is posted to the Blackboard Group Blog in the format "
        "prescribed by the project guide; this file keeps them together so the supervisor and the "
        "team can see the whole trajectory in one place. Weeks marked **Planned** are the agreed "
        "forward plan, not work already done.")

    d.table([["Module Group", MODULE_GROUP], ["Project Group Number", GROUP_NUMBER],
             ["Team Name", TEAM_NAME], ["Team Leader", "Clifton Chen Yi"]],
            widths=[3000, 6984], header=False)

    # ---- Gantt chart
    d.heading("Gantt chart", 2, toc=False)
    d.para("Weeks 11\u201318. \u25a0 = scheduled work. The chart is revised at each weekly "
           "meeting; the version below is current as at week 15.")
    header = ["Task"] + [f"W{w}" for w in range(11, 19)]
    rows = [header]
    for task, start, end in GANTT_TASKS:
        rows.append([task] + ["\u25a0" if start <= w <= end else "" for w in range(11, 19)])
    d.table(rows, widths=[3584] + [800] * 8, font_size=16)

    d.para("**Milestones:** W13 \u2014 proposal and EDA submitted (19 Jul 2026). "
           "W16 \u2014 interim progress review. W18 \u2014 final presentation and report "
           "(23 Aug 2026).")

    # ---- weekly entries
    for wk in WEEKS:
        d.page_break()
        d.heading(f"Progress Updates for Week {wk['week']}", 2, toc=False)
        d.table([["Week", f"Week {wk['week']} ({wk['dates']})"],
                 ["Focus", wk["theme"]],
                 ["Status", wk["status"]]],
                widths=[1800, 8184], header=False)
        d.para("**Team Member's Progress**")
        for i, (name, work) in enumerate(wk["progress"], start=1):
            d.para(f"{i}. **{name}** \u2014 {work}", after=80)
        d.para("**Gantt Chart**")
        d.para("Revised chart as shown on page 1; no change to milestone dates this week."
               if wk["status"] != "Planned" else
               "Forward plan as shown on page 1.", after=80)
        d.para("**Miscellaneous**")
        d.para(wk["misc"], after=80)
    return d


# =============================================================================
# APPENDIX F
# =============================================================================
CRITERIA = [
    "Attends team meetings regularly and arrives on time.",
    "Contributes meaningfully to team discussions.",
    "Completes assigned tasks in a quality and timely manner.",
    "Demonstrates a cooperative and supportive attitude.",
    "Contributes significantly to the success of the project.",
]


def appendix_f():
    d = Document()
    d.set_header([f"{PROJECT_TITLE} \u2014 Appendix F: Self and Peer Review",
                  "APPENDIX F \u2013 SELF AND PEER REVIEW.docx"])
    d.set_footer(f"{MODULE_GROUP} Group {GROUP_NUMBER} \u2014 {TEAM_NAME}")

    d.heading("Appendix F \u2013 Self and Peer Review", 1, toc=False)
    d.para("Your Name: **Clifton Chen Yi**  (Member A, Team Leader)")
    d.para(
        "For each team member, indicate the extent to which you agree with the statement on the "
        "left using a scale of 1\u20134 (1 = strongly disagree, 2 = disagree, 3 = agree, "
        "4 = strongly agree). Total the numbers in each column.", size=20)

    names = [n for _l, n, _r, _s in MEMBERS]
    rows = [["Evaluation Criteria"] + names]
    for c in CRITERIA:
        rows.append([c] + [""] * len(names))
    rows.append(["**TOTALS:**"] + [""] * len(names))
    d.table(rows, widths=[3584] + [1600] * 4, font_size=16)

    d.para("**Comments on free riders (if any)**")
    d.table([[""]], widths=[9984], header=False)

    d.heading("Notes on completing this form", 2, toc=False)
    d.bullets([
        "The ratings above are deliberately left blank. A peer review is a personal judgement "
        "made at the end of the project and must be entered by the individual submitting it \u2014 "
        "pre-filling it would defeat its purpose.",
        "Scores should reflect collaboration quality: self-awareness and management, leadership, "
        "interpersonal and collaboration skills, and communication and inclusivity, together with "
        "attendance and punctuality at group meetings.",
        "A separate confidential 20-point digital peer evaluation form is administered by the "
        "tutor. A member whose **average** score from the others is 10 or below out of 20 is "
        "identified as a free rider, and that average then determines the penalty applied to "
        "their group-component score.",
        "If a member is rated as a free rider, substantiating reasons, justifications or examples "
        "must be provided; a low score without evidence is not actionable.",
    ])

    d.heading("Evidence available to support the review", 2, toc=False)
    d.para(
        "The following artefacts record who did what, and can be referred to when completing the "
        "confidential form:")
    d.bullets([
        "Weekly Group Blog entries for weeks 11\u201318 (Appendix B), which name each member's "
        "work for the week.",
        "The revised Gantt chart, showing task ownership and completion against plan.",
        "The four implementation notebooks, each authored and owned by a named member.",
        "The individual dashboard pages, one per workstream.",
        "Meeting attendance recorded by the team leader.",
    ])
    return d


# =============================================================================
# APPENDIX G
# =============================================================================
def appendix_g():
    d = Document()
    d.set_header([f"{PROJECT_TITLE} \u2014 Appendix G: Final Presentation Plan & Checklist",
                  "APPENDIX G \u2013 FINAL PRESENTATION CHECKLIST.docx"])
    d.set_footer(f"{MODULE_GROUP} Group {GROUP_NUMBER} \u2014 {TEAM_NAME}")

    d.heading("Appendix G \u2013 Final Presentation Plan & Quality Checklist", 1, toc=False)
    d.para(
        "The final presentation carries 30% (group integration 10%, individual 20%). This "
        "document turns the project guide's quality checklist into a rehearsal checklist, and "
        "sets out the slide plan for Member A's segment.")

    d.heading("1. Slide plan \u2014 Member A (retention)", 2, toc=False)
    d.para(
        "The individual 20% is assessed on iteration (5), implementation (10) and communication "
        "(5). Each slide below is mapped to the criterion it provides evidence for, so nothing "
        "assessed is left to chance.")
    d.table([
        ["#", "Slide", "Content", "Evidence for"],
        ["1", "The decision, not the model",
         "One sentence: which previously-active customers will go quiet in the next 60 days, and "
         "which of those are worth contacting.", "Communication"],
        ["2", "Stakeholder feedback \u2192 design change",
         "Mar\u00eda Rodr\u00edguez's constraint, and the three concrete changes it caused "
         "(priority view, capacity control, recall emphasis).", "Iteration (5)"],
        ["3", "Data & the 4 Vs",
         "48,723 customers joined to 3.16M transactions; volume, variety (profile + ledger + "
         "free-text complaints), veracity (the Set B reconciliation failure), velocity (daily "
         "batch).", "Implementation (10)"],
        ["4", "Leakage-safe design",
         "The four-cutoff panel; the test outcome window closing exactly on the last ledger day; "
         "the excluded-column list and why.", "Implementation"],
        ["5", "Baselines first",
         "Majority class and the tuned recency rule, then the four models against them.",
         "Implementation"],
        ["6", "Does the model earn its complexity?",
         "Scoreboard: recall, precision, PR-AUC, top-10%/20% recall on the held-out cutoff.",
         "Implementation"],
        ["7", "The hypothesis, answered",
         "Feature-family ablation: trend/rhythm vs. static profile vs. full model.",
         "Implementation"],
        ["8", "What it means operationally",
         "Capacity curve: recall and precision at 10/20/30% outreach; the risk \u00d7 value "
         "priority cell.", "Communication"],
        ["9", "Honest limits",
         "Untimestamped snapshot fields, recency's mechanical link to the target, one year of "
         "data, inactivity \u2260 attrition, association \u2260 causation.", "Communication"],
        ["10", "Scalability",
         "25/50/75/100% timings, stage breakdown, Spark comparison, and the bottleneck.",
         "Implementation"],
        ["11", "Recommendation & next step",
         "Event-triggered outreach on the priority cell, plus a holdout experiment to test whether "
         "contact actually changes behaviour.", "Communication"],
    ], widths=[500, 2200, 5284, 2000], font_size=16)

    d.para(
        "**Q&A preparation.** The three questions most likely to be asked, and the honest answer "
        "to each: *Is recency just the label in disguise?* \u2014 partly, which is why the "
        "recency rule is a baseline and not a feature to be proud of. *Why not SMOTE?* \u2014 "
        "the panel repeats customers across cutoffs, so synthetic rows would interpolate between "
        "the same person at two points in time; class weights keep probabilities interpretable. "
        "*Would this work next year?* \u2014 unknown; one calendar year cannot separate a "
        "year-end effect from model quality.")

    d.page_break()
    d.heading("2. Quality checklist for the final presentation", 2, toc=False)
    d.para("Worked through in the week 17 rehearsal, then again before delivery.", size=20)

    d.para("**Delivery \u2014 presentation materials**")
    d.table([
        ["Attribute", "What good looks like here", "Checked"],
        ["Content \u2014 quality of information, well organised, logical",
         "Each slide answers one question and leads to the next; results are always paired with "
         "the baseline they beat.", ""],
        ["Vocal \u2014 clear and audible",
         "Rehearsed at presentation volume; numbers spoken slowly ('point seven two', not "
         "'zero point seven two').", ""],
        ["Visual \u2014 effective use of visual aid",
         "One chart per slide, axis labels readable from the back, consistent colour meaning "
         "(red = risk) across all four workstreams.", ""],
        ["Verbal \u2014 standard of English",
         "No unexplained jargon; PR-AUC and top-k recall defined the first time they appear.", ""],
        ["Impact \u2014 convincing and lasting impression",
         "Opens and closes on the decision, not the algorithm.", ""],
    ], widths=[2600, 6184, 1200], font_size=16)

    d.para("**Delivery \u2014 quality of presentation**")
    d.table([
        ["Attribute", "Check", "Checked"],
        ["Attire", "Smart casual, no torn tops, per the dress code in the project guide.", ""],
        ["Eye contact", "Address the panel, not the slides or the laptop.", ""],
        ["Body language", "Stable posture; no pacing; hands away from face.", ""],
        ["Confidence & enthusiasm",
         "Know the three headline numbers without reading them off the slide.", ""],
        ["Time management",
         "Rehearsed to the allotted time with a 10% margin; slide 9 is the one to compress if "
         "running late, never slide 6.", ""],
    ], widths=[2600, 6184, 1200], font_size=16)

    d.para("**Q & A**")
    d.table([
        ["Attribute", "Check", "Checked"],
        ["Ability to understand & respond",
         "Restate the question before answering; ask for clarification rather than guessing.", ""],
        ["Quality of response",
         "Answer from the measured evidence; say 'we did not test that' when true.", ""],
        ["Level of confidence",
         "Limitations stated as deliberate scope decisions, not apologies.", ""],
    ], widths=[2600, 6184, 1200], font_size=16)

    d.heading("3. Group integration check (10%)", 2, toc=False)
    d.para("The group mark depends on all four notebooks being integrated and functional. "
           "Verified in week 17:")
    d.bullets([
        "All four notebooks run top-to-bottom from a clean kernel against the same source files.",
        "All four use the shared preprocessing contract: the same deduplication rule, the same "
        "excluded columns, the same (customer, cutoff) key convention.",
        "Member B's predicted 90-day value is joined into Member A's retention-priority view \u2014 "
        "the one genuine cross-workstream dependency.",
        "All four write scored outputs to the same analytical layer, and the dashboard reads only "
        "from that layer.",
        "One landing page summarises all four workstreams; each member has a dedicated page.",
    ])
    return d


# =============================================================================
if __name__ == "__main__":
    outputs = [
        ("APPENDIX A \u2013 PROJECT TEAM ORGANISATION.docx", appendix_a()),
        ("APPENDIX B \u2013 WEEKLY PROGRESS UPDATES.docx", appendix_b()),
        ("APPENDIX F \u2013 SELF AND PEER REVIEW.docx", appendix_f()),
        ("APPENDIX G \u2013 FINAL PRESENTATION CHECKLIST.docx", appendix_g()),
    ]
    for name, doc in outputs:
        path = os.path.join(REPO, name)
        doc.save(path)
        print(check(path))
