# IT3301 Assignment — Summarised Walkthrough Transcript

A summarised walkthrough covering the problem, methodology and results, with the code explained at
the level of *why each decision was made*. Plain text is spoken word-for-word; `[Square brackets]`
are screen cues — don't read them aloud.

* **Length:** 846 spoken words — **5:50** at 145 words per minute, **6:16** at a slow 135. That
  leaves roughly three minutes of the 10-minute allowance spare, so you can pause on a table, answer
  a question, or slow down without risk.
* **Cell references:** Colab prints an execution label to the left of each code cell. This notebook
  was run top to bottom, so the labels read `[1]`–`[41]` in order. Markdown cells aren't numbered, so
  they're named by section.
* Keep your face visible throughout, and show *outputs* rather than walls of code.

| Time | Show | Notebook section |
|---|---|---|
| 0:00 | title markdown cell | header / executive summary |
| 0:28 | `[5]`, `[6]` | 2.2 Label distribution, 2.3 Quality audit |
| 0:52 | `[9]` | 3.1 Cleaning (`clean_flows`) |
| 1:24 | `[11]`, `[12]`, `[13]` | 3.3 Feature engineering, 3.4 Splits, 3.5 Pipeline + `clone()` |
| 2:09 | `[16]`, `[18]` | 4.2 CV bake-off, 4.4 Validation results |
| 2:44 | `[14]`, `[19]`, `[21]` | 3.6 `score_row`, 5.1 Confusion matrices, 5.3 Threshold sweep |
| 3:24 | `[22]`, `[23]`, `[25]` | 6.1 Search spaces, 6.2 cv_results, 6.4 Final test table |
| 4:05 | `[27]`, markdown §7.1c, `[31]` | 7.1 Permutation importance, shortcut analysis, 7.4a Holdout |
| 4:51 | `[38]`, `[39]` | 8.4 Fairness audit, 8.5 Mitigation |
| 5:23 | `[36]`, `[40]` | 7.8 Deployment, 8.8 Model Card |

---

## 0:00 – 0:28 · The problem

[Screen: the title markdown cell. Face to camera.]

Hi, I'm `<YOUR NAME>`, admin number `<YOUR ADMIN NO>`. XYZ Cybersecurity gave me three CSVs — just
over a million network flows, each summarised by seventy-eight statistics and a label — and asked me
to separate malicious traffic from legitimate traffic.

I framed it as binary classification and fixed my metrics up front: recall and PR-AUC, never
accuracy, because labelling everything benign already scores seventy-nine percent here.

## 0:28 – 0:52 · Data understanding

[Screen: cell **[5]** — the label table; then cell **[6]** — the quality audit.]

Two things shaped everything that followed. First, the attacks are hugely uneven: DoS Hulk alone is
ninety percent of them, while Heartbleed has eleven flows. Second, the data is genuinely dirty — the
rate columns contain infinities from zero-duration flows, ten columns are constant, sixteen pairs are
exact duplicates, and ten percent of the rows are duplicated outright.

## 0:52 – 1:24 · Cleaning, in a deliberate order

[Screen: cell **[9]** — `clean_flows`.]

Infinities become NaN first, because scikit-learn's imputer tolerates NaN but rejects infinity. Then
I drop duplicate rows *before* splitting — otherwise the same flow sits in train and test and
inflates recall for free — and note the key excludes the source-file column, or a flow captured in
two sessions survives twice. Negative TCP window values are sentinels, so I keep them and add an
is-sentinel flag; but forty-one negative *durations* are real corruption, which I documented.

## 1:24 – 2:09 · Features and a leakage-free split

[Screen: cell **[11]** — `engineer_features` and `safe_ratio`.]

Raw features are absolute, so I engineered twenty relational ones: bytes per packet,
forward-to-backward asymmetry, header overhead, a service group from the port. Every ratio goes
through `safe_ratio`, which turns a zero denominator into zero rather than a fresh infinity.

[Screen: cell **[12]** — the split; then cell **[13]** — `build_pipeline`.]

Imputation and scaling live *inside* the pipeline, so they refit per fold and never see test data,
and I excluded the source-file column because it correlates almost perfectly with attack type. One
subtle bug worth showing: a Pipeline stores a *reference* to your estimator, not a copy — without
this `clone`, tuning one pipeline silently mutates the others and my tuned-versus-untuned comparison
would compare the tuned model with itself.

## 2:09 – 2:44 · Model selection

[Screen: cell **[16]** — the cross-validation table; then cell **[18]** — validation results.]

Seven learners plus a no-skill baseline, all sharing one `StratifiedKFold` so they see identical
folds. The baseline's PR-AUC matches attack prevalence, which proves the metric isn't flattered by
imbalance — and every serious model then lands at the ceiling, inside the fold-to-fold noise. So
PR-AUC couldn't choose for me. I chose on error cost instead: XGBoost misses four attacks with
thirteen false alarms, Logistic Regression a hundred and one with three hundred and thirty-seven —
twenty-five times the cost. k-NN I rejected on latency alone.

## 2:44 – 3:24 · Performance measurement

[Screen: cell **[14]** — `score_row`; cell **[19]** — confusion matrices; cell **[21]** — the sweep.]

Every metric comes from one function that also returns a business cost: ten for a missed attack, one
for a false alarm. The curves are saturated, so I report counts — ten thousand and forty-five caught,
four missed, thirteen false alarms.

Then the part that matters most: zero point five is a modelling artefact, not a business decision, so
I sweep ninety-nine thresholds and cost each one. The cost-optimal point cuts cost by a third, and
zero misses is purchasable for ninety-five false alarms. That trade is the client's call; my job was
to price it.

## 3:24 – 4:05 · Hyperparameter tuning

[Screen: cell **[22]** — search spaces; cell **[23]** — results; cell **[25]** — the test table.]

Randomised search then a focused grid, scored on average precision, with the `model__` prefix so each
candidate refits through the whole pipeline rather than around it.

And here's my most interesting negative result: all twelve configurations landed within two
hundred-thousandths of each other — the same size as the noise between folds — and on test the tuned
model was two flows *worse* than the default. Hyperparameters only matter where a model is
capacity-limited, and Logistic Regression was the only candidate that actually was. Meanwhile the
threshold cut cost by a third: one line beat my whole eighteen-minute search.

## 4:05 – 4:51 · The finding that explains everything

[Screen: cell **[27]** — permutation importance; then markdown **7.1c**; then cell **[31]**.]

Why was this so easy? Permutation importance — which I prefer to built-in gain because it measures
the metric drop I care about — shows one feature, `port_web`, is a hundred and fifty-four times more
important than the runner-up. The cause is my own EDA: eighty-seven percent of attacks are DoS Hulk
and they all hit the web port, so one binary feature nearly solves the task.

Here's the proof it's a shortcut, not intelligence. I retrained with every Bot flow removed, then
tested on Bot: zero detected out of a hundred and twenty-four, while the families still in training
were caught perfectly. The model learned these attack tools' fingerprints, not maliciousness.

## 4:51 – 5:23 · AI ethics

[Screen: cell **[38]** — the fairness audit; then cell **[39]** — the mitigation table.]

I use flow statistics only, never payloads. With no protected attribute, I audited fairness by proxy
and found web users carry the entire false-alarm burden while DNS and TLS traffic sit at zero. I then
tried per-service thresholds and they *backfired* — equalising recall but tripling the false-positive
gap — because I'd calibrated to a one percent budget when the model runs fourteen times tighter. I
published that failure and the fix rather than deleting the cell.

## 5:23 – 5:50 · Close

[Screen: cell **[36]** — throughput and artefact; cell **[40]** — the Model Card. Then camera.]

The final model misses one attack in sixty thousand flows at eighty-eight thousand flows a second,
saved with its threshold and feature contract. My recommendations: deploy at the cost-optimal
threshold, don't sell it as zero-day detection, and rebalance training data by attack family. My next
step is the ablation — retrain without the port features and find out what this detector is really
worth. Thank you.
