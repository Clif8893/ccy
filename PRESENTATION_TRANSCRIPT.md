# IT3301 Assignment — Video Walkthrough Transcript (max 10 minutes)

**How to use this file:** plain text is spoken word-for-word. `[Square brackets]` are screen cues —
do not read them aloud.

* **Length:** 1,440 spoken words — **9:56** at 145 words per minute. Screen cues are not spoken,
  so they cost no time. Three passages are marked `[OPTIONAL]` (93 words); dropping them leaves
  1,347 words, i.e. **9:59** even at a slow 135 wpm, so the take fits however you deliver
  it. Section timestamps assume 145 wpm and include the optional passages.
* Figures are spoken as plain language ("rounds to one", "a third lower") instead of long strings of
  digits. Digits are slow to say and earn no marks, so that airtime goes into explaining the code —
  the exact values stay on screen behind you.
* Keep your face visible for the whole take, and scroll to *outputs*, not walls of code.

### How the cell references work

Colab prints an execution number to the left of every code cell. Those are stable in this notebook —
it was run top to bottom, so the labels read `[1]` to `[41]` in order. A cue like
**`Screen: cell [21]`** means the code cell labelled `[21]`, which is §5.3. Markdown cells have no
number, so they are referenced by section heading.

### Show order at a glance

| Time | Show | Notebook section |
|---|---|---|
| 0:00 | title markdown cell | header / executive summary |
| 0:40 | `[2]` | 1. Data loading |
| 1:00 | `[5]` | 2.2 Target distribution |
| 1:15 | `[6]` | 2.3 Data-quality audit |
| 1:35 | `[9]` | 3.1 Cleaning |
| 2:14 | `[10]`, `[11]` | 3.2 Targets, 3.3 Feature engineering |
| 3:00 | `[12]`, `[13]` | 3.4 Splits, 3.5 Pipeline + `clone()` |
| 3:36 | `[15]`, `[16]` | 4.1 Candidates, 4.2 CV bake-off |
| 4:15 | `[18]`, `[17]` | 4.4 Validation table, 4.3 flows/second |
| 4:41 | `[14]`, `[19]` | 3.6 `score_row`, 5.1 Confusion matrices |
| 5:10 | `[21]` | 5.3 Threshold + cost sweep |
| 5:41 | `[22]` | 6.1 Search spaces |
| 6:05 | `[23]`, `[24]`, `[25]` | 6.2 cv_results, 6.3 Grid tie, 6.4 Test result |
| 7:06 | `[27]` + markdown §7.1c | 7.1 Permutation importance + shortcut analysis |
| 7:50 | `[31]` | 7.4a Unseen-family holdout |
| 8:12 | `[38]`, `[39]` + markdown §8.5b | 8.4 Fairness audit, 8.5 Mitigation |
| 9:10 | `[36]`, `[40]` | 7.8 Deployment, 8.8 Model Card |

---

## 0:00 – 0:40 · The problem and how I framed it

[Screen: the title markdown cell at the top of the notebook. Face to camera.]

Hi, I'm `<YOUR NAME>`, admin number `<YOUR ADMIN NO>`. This is my IT3301 assignment: a machine
learning approach to predict fraud for XYZ Cybersecurity.

The client gave me three CSVs — just over a million network flows, each summarised by seventy-eight
statistics plus a label. A flow is one conversation between two machines.

I framed it as binary classification and fixed my metrics before modelling: recall and PR-AUC, never
accuracy — because a model that labels everything benign already scores seventy-nine percent accuracy
here. I'll cover the code, the results, and three findings that went against me.

## 0:40 – 2:14 · Data understanding and cleaning

[Screen: cell **[2]** — §1 Data loading. Scroll so the three shapes and the `head()` output show.]

I hardened the loader I was given: it strips the leading space off every padded header, retries with
latin-1 because the web-attack file has a non-UTF-8 byte in its label text, and asserts that the three
captures share a schema.

[Screen: cell **[5]** — §2.2. Point at the label table and the crosstab of families per capture file.]

This is the most consequential thing I found. Three quarters of flows are benign, but the attacks are
hugely uneven: DoS Hulk alone is ninety percent of all attacks, while Heartbleed has eleven flows.
Hold on to that.

[Screen: cell **[6]** — §2.3. The missing/infinite/negative tables and the constant + duplicated column lists.]

The audit found four problems. The two rate columns hold missing values *and* infinities, because a
zero-duration flow divides by zero. Ten columns are constant and sixteen pairs are exact duplicates,
so seventeen columns get dropped.

[Screen: cell **[9]** — §3.1 `clean_flows`. Show the function body, then its printed de-duplication line.]

Cleaning happens in a deliberate order. Infinities become NaN first, because scikit-learn's imputer
tolerates NaN but rejects infinity outright. Then ten percent of rows turn out to be exact duplicates,
which I drop *before* splitting — otherwise the identical flow lands in train and test and inflates
recall for free. Note the key excludes the source-file column, or a flow captured twice survives twice.

[Still cell **[9]** for the sentinel flags; the negative counts themselves were in cell **[6]**.]

[OPTIONAL — cut this paragraph if you speak slower than 145 wpm.] And negatives come in two kinds: minus one in the
TCP window field is a sentinel for "no window advertised", so I keep it and add an is-sentinel
indicator — but forty-one rows have a negative *duration*, which is real corruption, and I documented
it rather than quietly clipping it.

## 2:14 – 3:36 · Targets, features, and keeping the split honest

[Screen: cell **[10]** — §3.2 target engineering, then cell **[11]** — §3.3 `engineer_features`.]

From the label column I derive two targets: a binary attack flag, and an attack family for triage,
with families under a hundred flows grouped — you can't learn, or even stratify, on eleven flows.

Raw features are absolute; attacks are better described by relationships. So I engineered twenty
features from analyst heuristics: bytes per packet, forward-to-backward asymmetry, header overhead as a
share of the flow, and a service group derived from the destination port.

[Screen: still cell **[11]** — scroll up to the `safe_ratio` helper at the top of §3.3.]

Every ratio goes through this helper. It swaps a zero denominator for NaN, divides, then fills with
zero — so feature engineering can't re-introduce the infinities I just removed.

[Screen: cell **[12]** — §3.4 splits (show the printed train/validation/test line), then cell **[13]** — §3.5.]

Two choices protect the result. Imputation and scaling sit *inside* a scikit-learn pipeline, so they're
refitted on every training fold and never see test data. And I excluded the source-file column from
the predictors — it correlates almost perfectly with attack type, so keeping it would be leakage.

[Screen: cell **[13]** — §3.5 `build_pipeline`. Point at the `clone(estimator)` line and its comment.]

One subtle bug worth showing. A scikit-learn Pipeline stores a *reference* to the estimator you give
it, not a copy — so without this `clone`, tuning one pipeline silently mutates the others, and my
tuned-versus-untuned comparison would have compared the tuned model against itself.

## 3:36 – 4:41 · Model selection

[Screen: cell **[15]** — §4.1 candidate dictionary, then cell **[16]** — §4.2 cross-validation table.]

I compared seven learners plus a no-skill baseline, all sharing one `StratifiedKFold` object so every
model sees identical folds — otherwise I'd be measuring the split, not the model.

[Screen: cell **[16]** output — the bottom row, `Dummy (baseline)`.]

The baseline's PR-AUC matches attack prevalence almost exactly — so the metric isn't being flattered
by imbalance.

[Screen: cell **[16]** output — the full sorted table, PR-AUC column.]

Now the interesting part. Every serious model is at the ceiling — the boosting models round to one,
and even a plain Decision Tree reaches nine nine three. Those gaps are smaller than the fold-to-fold
standard deviations, so **PR-AUC cannot choose my model for me.**

[Screen: cell **[18]** — §4.4 validation table; the `fn`, `fp` and `cost` columns.]

So I chose on what differs. On validation XGBoost misses four attacks and raises thirteen false
alarms; Logistic Regression misses a hundred and one and raises three hundred and thirty-seven —
twenty-five times the business cost behind a PR-AUC gap of two thousandths.

[Screen: cell **[17]** — §4.3, the `flows_per_second` table.]

[OPTIONAL] k-nearest-neighbours I rejected on engineering grounds, not accuracy: forty times slower to
score, and worse as the training set grows. Unusable in a live sensor.

## 4:41 – 5:41 · Performance measurement

[Screen: cell **[14]** — §3.6 `score_row`, then cell **[19]** — §5.1 confusion matrices and reports.]

Every metric comes from this one function, which also returns a business cost — ten for a missed
attack, one for a false alarm — because those errors are not equally expensive.

The curves are saturated, so I report counts: ten thousand and forty-five attacks caught, four missed,
and thirteen false alarms across nearly thirty-eight thousand benign flows.

[Screen: cell **[21]** — §5.3. The three-panel plot, then the four-row threshold comparison table.]

Here's what I think matters most. The default zero point five threshold is a modelling artefact, not a
business decision, so I sweep ninety-nine thresholds and cost each one. The cost-optimal point cuts
cost by about a third. And zero misses is available too — for ninety-five false alarms instead of
thirteen.

So the honest statement is: your last three attacks cost about five false alarms each, and eliminating
the final one costs eighty-two more. That's the client's decision; my job is to price it.

## 5:41 – 7:06 · Hyperparameter tuning

[Screen: cell **[22]** — §6.1 `SEARCH_SPACES` and the printed best parameters per family.]

Note the `model__` prefix on every parameter — that addresses the estimator *inside* the pipeline, so
each candidate is refitted through imputation and scaling rather than around it. Scoring is average
precision, never accuracy. Randomised search first, because these spaces run to tens of thousands of
combinations, then a focused grid around the winner, all on a training slice only.

[Screen: cell **[23]** — §6.2 top-5 configurations per family, with the `cv_pr_auc` and `std_test_score` columns side by side.]

And here's my most interesting negative result: all twelve XGBoost configurations landed within two
hundred-thousandths of each other — the same size as the noise between folds of one configuration.
**The spread between configurations is smaller than the noise within one.** The grid search then
"improved" the score to the identical number, and on test the tuned model was two flows *worse* than
the untuned default.

[Screen: cell **[24]** for the grid-search tie, then cell **[25]** — §6.4 final test table, comparing the `TUNED` and `default` rows.]

Rather than hide that, I explain it: hyperparameters only matter where a model is capacity-limited, and
I'll show you next why this one barely is.

[Screen: still cell **[23]** — the two rows tied at the top with different `mean_fit_time`.]

[OPTIONAL] What tuning did buy was efficiency — two configurations tied exactly and one fits in half
the time. [End optional.] And it genuinely mattered for one model: Logistic Regression improved once I relaxed regularisation,
because it was the only candidate actually underfitting. Meanwhile the threshold cut cost by a third:
one line beat my whole eighteen-minute search.

## 7:06 – 8:12 · The finding that explains everything

[Screen: cell **[27]** — §7.1 permutation-importance table and chart, then the markdown section **7.1c** just below cell [28].]

So why is this so easy? Permutation importance answers it — and I use permutation rather than the
model's built-in gain because it measures the drop in the metric I actually care about. One feature,
`port_web`, is a hundred and fifty-four times more important than the runner-up, and forty-two of my
ninety-six features score exactly zero.

[Screen: flick back to cell **[5]**, then cell **[11]**'s service-mix table showing the `web` group is 70% attack.]

The cause is in my own EDA: eighty-seven percent of attacks are DoS Hulk and they all hit the web port,
so one binary feature nearly solves the labelled task. That explains why every model scored so high,
why tuning changed nothing, and why ten features tie with ninety-six.

[Screen: cell **[29]** — §7.2, the feature-reduction table, if you want to show the last claim.]

[Screen: cell **[31]** — §7.4a. Show the three-row generalisation table and the two recall lines under it.]

Here's the proof it's a shortcut and not intelligence. I retrained with every Bot flow removed — same
split, so only the family differs — then tested on Bot. Detection: zero out of a hundred and
twenty-four, while the families still in training were caught perfectly. My model learned *these
attack tools' fingerprints*, not maliciousness.

## 8:12 – 9:10 · AI ethics

[Screen: cell **[38]** — §8.4 fairness audit: the service-group table and the two bar charts. The
23-of-31 false-alarm breakdown is in cell **[35]** if you want it. Then cell **[39]** — §8.5
mitigation table — and the markdown **8.5b** below it.]

Ethically, I use flow statistics only, never packet contents, so message content is never inspected.

There's no protected attribute here, so I audited fairness by proxy: error rates per service, flow
size, duration and capture session. Web traffic carries the entire false-alarm burden — twenty-three
of my thirty-one false alarms are web flows, while DNS, TLS and auth sit at exactly zero. So web users
are the only group ever wrongly investigated: an allocative harm with no personal data in the model.

I then tried the textbook fix — per-service thresholds — and it *backfired*: it equalised the recall
gap but tripled the false-positive gap and cost twelve times more, because I'd calibrated to a one
percent alert budget when the model already runs fourteen times tighter. I published that failure and
the corrected recipe instead of deleting the cell.

## 9:10 – 9:56 · Deployment and conclusion

[Screen: cell **[36]** — §7.8 throughput, artefact and `score_flows` output; then cell **[40]** — the Model Card. Return to camera for the last two paragraphs.]

For deployment I save the pipeline, threshold and feature contract as one artefact, and my scoring
function re-applies every preparation step to raw rows, then reindexes onto that contract — so
production can't hand the model a different feature vector than I trained on.

My recommendations: deploy at the cost-optimal threshold, not zero point five; don't sell this as
zero-day detection, because I measured zero percent on an unseen family; and rebalance the training
data by attack family.

My biggest limitation is that port-eighty shortcut, and my next step is the ablation — retrain without
the port features and find out what this detector is really worth. Thank you for watching.
