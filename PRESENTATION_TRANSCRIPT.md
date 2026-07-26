# IT3301 Assignment — Video Walkthrough Transcript (max 10 minutes)

**How to use this file:** plain text is spoken word-for-word. `[Square brackets]` are screen cues —
do not read them aloud.

* **Length:** 1,440 spoken words — **9:56** at 145 words per minute. Three sentences are marked
  `[OPTIONAL]`; cutting them saves ~45 seconds, which keeps you under 10:00 even at a slow 135 wpm.
  Section timestamps assume 145 wpm and include the optional lines.
* Figures are spoken as plain language ("rounds to one", "a third lower") instead of long strings of
  digits. Digits are slow to say and earn no marks, so that airtime goes into explaining the code —
  the exact values stay on screen behind you.
* Keep your face visible for the whole take, and scroll to *outputs*, not walls of code.

---

## 0:00 – 0:40 · The problem and how I framed it

[Face to camera, title cell on screen.]

Hi, I'm `<YOUR NAME>`, admin number `<YOUR ADMIN NO>`. This is my IT3301 assignment: a machine
learning approach to predict fraud for XYZ Cybersecurity.

The client gave me three CSVs — just over a million network flows, each summarised by seventy-eight
statistics plus a label. A flow is one conversation between two machines.

I framed it as binary classification and fixed my metrics before modelling: recall and PR-AUC, never
accuracy — because a model that labels everything benign already scores seventy-nine percent accuracy
here. I'll cover the code, the results, and three findings that went against me.

## 0:40 – 2:14 · Data understanding and cleaning

[Scroll to the loader.]

I hardened the loader I was given: it strips the leading space off every padded header, retries with
latin-1 because the web-attack file has a non-UTF-8 byte in its label text, and asserts that the three
captures share a schema.

[Point at the label distribution.]

This is the most consequential thing I found. Three quarters of flows are benign, but the attacks are
hugely uneven: DoS Hulk alone is ninety percent of all attacks, while Heartbleed has eleven flows.
Hold on to that.

[Point at the audit output.]

The audit found four problems. The two rate columns hold missing values *and* infinities, because a
zero-duration flow divides by zero. Ten columns are constant and sixteen pairs are exact duplicates,
so seventeen columns get dropped.

[Show clean_flows.]

Cleaning happens in a deliberate order. Infinities become NaN first, because scikit-learn's imputer
tolerates NaN but rejects infinity outright. Then ten percent of rows turn out to be exact duplicates,
which I drop *before* splitting — otherwise the identical flow lands in train and test and inflates
recall for free. Note the key excludes the source-file column, or a flow captured twice survives twice.

[OPTIONAL — cut this paragraph if you speak slowly.] And negatives come in two kinds: minus one in the
TCP window field is a sentinel for "no window advertised", so I keep it and add an is-sentinel
indicator — but forty-one rows have a negative *duration*, which is real corruption, and I documented
it rather than quietly clipping it.

## 2:14 – 3:36 · Targets, features, and keeping the split honest

[Show the label mapping and engineer_features.]

From the label column I derive two targets: a binary attack flag, and an attack family for triage,
with families under a hundred flows grouped — you can't learn, or even stratify, on eleven flows.

Raw features are absolute; attacks are better described by relationships. So I engineered twenty
features from analyst heuristics: bytes per packet, forward-to-backward asymmetry, header overhead as a
share of the flow, and a service group derived from the destination port.

[Point at safe_ratio.]

Every ratio goes through this helper. It swaps a zero denominator for NaN, divides, then fills with
zero — so feature engineering can't re-introduce the infinities I just removed.

[Point at the split and pipeline cells.]

Two choices protect the result. Imputation and scaling sit *inside* a scikit-learn pipeline, so they're
refitted on every training fold and never see test data. And I excluded the source-file column from
the predictors — it correlates almost perfectly with attack type, so keeping it would be leakage.

[Point at the clone() call.]

One subtle bug worth showing. A scikit-learn Pipeline stores a *reference* to the estimator you give
it, not a copy — so without this `clone`, tuning one pipeline silently mutates the others, and my
tuned-versus-untuned comparison would have compared the tuned model against itself.

## 3:36 – 4:41 · Model selection

[Show the candidates, then the CV table.]

I compared seven learners plus a no-skill baseline, all sharing one `StratifiedKFold` object so every
model sees identical folds — otherwise I'd be measuring the split, not the model.

[Point at the Dummy row.]

The baseline's PR-AUC matches attack prevalence almost exactly — so the metric isn't being flattered
by imbalance.

[Point at the table.]

Now the interesting part. Every serious model is at the ceiling — the boosting models round to one,
and even a plain Decision Tree reaches nine nine three. Those gaps are smaller than the fold-to-fold
standard deviations, so **PR-AUC cannot choose my model for me.**

So I chose on what differs. On validation XGBoost misses four attacks and raises thirteen false
alarms; Logistic Regression misses a hundred and one and raises three hundred and thirty-seven —
twenty-five times the business cost behind a PR-AUC gap of two thousandths.

[OPTIONAL] k-nearest-neighbours I rejected on engineering grounds, not accuracy: forty times slower to
score, and worse as the training set grows. Unusable in a live sensor.

## 4:41 – 5:41 · Performance measurement

[Show score_row, then the confusion matrix.]

Every metric comes from this one function, which also returns a business cost — ten for a missed
attack, one for a false alarm — because those errors are not equally expensive.

The curves are saturated, so I report counts: ten thousand and forty-five attacks caught, four missed,
and thirteen false alarms across nearly thirty-eight thousand benign flows.

[Point at the threshold sweep.]

Here's what I think matters most. The default zero point five threshold is a modelling artefact, not a
business decision, so I sweep ninety-nine thresholds and cost each one. The cost-optimal point cuts
cost by about a third. And zero misses is available too — for ninety-five false alarms instead of
thirteen.

So the honest statement is: your last three attacks cost about five false alarms each, and eliminating
the final one costs eighty-two more. That's the client's decision; my job is to price it.

## 5:41 – 7:06 · Hyperparameter tuning

[Show the search spaces.]

Note the `model__` prefix on every parameter — that addresses the estimator *inside* the pipeline, so
each candidate is refitted through imputation and scaling rather than around it. Scoring is average
precision, never accuracy. Randomised search first, because these spaces run to tens of thousands of
combinations, then a focused grid around the winner, all on a training slice only.

[Point at cv_results.]

And here's my most interesting negative result: all twelve XGBoost configurations landed within two
hundred-thousandths of each other — the same size as the noise between folds of one configuration.
**The spread between configurations is smaller than the noise within one.** The grid search then
"improved" the score to the identical number, and on test the tuned model was two flows *worse* than
the untuned default.

Rather than hide that, I explain it: hyperparameters only matter where a model is capacity-limited, and
I'll show you next why this one barely is.

[OPTIONAL] What tuning did buy was efficiency — two configurations tied exactly and one fits in half the
time. [End optional.] And it genuinely mattered for one model: Logistic Regression improved once I relaxed regularisation,
because it was the only candidate actually underfitting. Meanwhile the threshold cut cost by a third:
one line beat my whole eighteen-minute search.

## 7:06 – 8:12 · The finding that explains everything

[Show Section 7.1c and the importance chart.]

So why is this so easy? Permutation importance answers it — and I use permutation rather than the
model's built-in gain because it measures the drop in the metric I actually care about. One feature,
`port_web`, is a hundred and fifty-four times more important than the runner-up, and forty-two of my
ninety-six features score exactly zero.

The cause is in my own EDA: eighty-seven percent of attacks are DoS Hulk and they all hit the web port,
so one binary feature nearly solves the labelled task. That explains why every model scored so high,
why tuning changed nothing, and why ten features tie with ninety-six.

[Show 7.4a.]

Here's the proof it's a shortcut and not intelligence. I retrained with every Bot flow removed — same
split, so only the family differs — then tested on Bot. Detection: zero out of a hundred and
twenty-four, while the families still in training were caught perfectly. My model learned *these
attack tools' fingerprints*, not maliciousness.

## 8:12 – 9:10 · AI ethics

[Show 8.4 and 8.5.]

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

[Show 7.8 and the Model Card, then face to camera.]

For deployment I save the pipeline, threshold and feature contract as one artefact, and my scoring
function re-applies every preparation step to raw rows, then reindexes onto that contract — so
production can't hand the model a different feature vector than I trained on.

My recommendations: deploy at the cost-optimal threshold, not zero point five; don't sell this as
zero-day detection, because I measured zero percent on an unseen family; and rebalance the training
data by attack family.

My biggest limitation is that port-eighty shortcut, and my next step is the ablation — retrain without
the port features and find out what this detector is really worth. Thank you for watching.
