# IT3301 Assignment — 10-Minute Video Walkthrough Transcript

**How to use this file:** plain text is spoken word-for-word. `[Square brackets]` are screen cues —
do not read them aloud.

* **Length:** 1,458 spoken words. At 148 words per minute that is **9:51**, so you are inside the
  10-minute cap with ~9 seconds of slack. The section timestamps below are derived from the word
  counts, so if you hit each one you will finish on time.
* **Pace warning:** the number-heavy sentences (§5 thresholds, §6 tuning) take longer to say than
  ordinary prose. Practise those two sections once against a timer.
* **If you run long,** the two safest cuts are the duplicate-column efficiency aside in §2 (the
  "three billion comparisons" sentence) and the two-tied-configurations sentence in §6 — about 20
  seconds combined, with no loss of a rubric point.
* Keep your face visible for the whole take, and scroll to *outputs*, not walls of code.

---

## 0:00 – 0:39 · Introduction and the problem

[Face to camera, title cell on screen.]

Hi, I'm `<YOUR NAME>`, admin number `<YOUR ADMIN NO>`. This is my IT3301 assignment: a machine
learning approach to predict fraud for XYZ Cybersecurity.

The client gave me three CSVs — just over a million network flows, each described by seventy-eight
statistics and a label. A flow is one conversation between two machines, summarised statistically.

My job is to separate malicious flows from legitimate traffic. I framed it as binary classification
and chose recall and PR-AUC as headline metrics, not accuracy — because a model that says "everything
is benign" already scores seventy-nine percent accuracy here.

## 0:39 – 2:22 · Data understanding and cleaning

[Scroll to the loader, then the quality audit output.]

I hardened the loader I was given: it strips the padded spaces off every column name, retries with
latin-1 because the web-attack file has a non-UTF-8 byte in its label text, and asserts that all
three files share one schema.

[Point at the label distribution.]

The most consequential thing I found before modelling: three quarters of flows are benign, but the
attacks are wildly uneven — DoS Hulk alone is ninety percent of all attacks, while Heartbleed has
eleven flows and SQL injection twenty-one. Remember that, because it explains nearly every later
result.

[Point at the audit output.]

The audit found four real problems. First, the two rate columns contain missing values *and*
infinities — zero-duration flows, so the rate divides by zero. Second, ten columns
are constant and sixteen pairs are exact duplicates; Fwd Header Length literally ships twice. I
dropped seventeen columns.

[Point at the duplicate-detection code.]

Comparing all columns pairwise across a million rows is three billion comparisons, so I group
candidates by a cheap signature and verify only the survivors.

Third, ten percent of rows are exact duplicates. I removed them *before* splitting, because the same
flow in training and test inflates your score for free — and my de-duplication key excludes the
source-file column, or an identical flow captured twice would survive as two rows.

Fourth, negatives come in two kinds. Most are minus one in the TCP window field — a legitimate
sentinel meaning "no window advertised" — so I keep it and add an is-sentinel flag. But forty-one
rows have a negative flow *duration*, which is genuine corruption. I documented it, not hid it,
because a production loader ought to reject those flows outright.

## 2:22 – 3:23 · Feature engineering and leakage-free splits

[Show engineer_features.]

Raw features are absolute; attacks are better described by relationships. So I engineered twenty
features encoding analyst heuristics: bytes per packet, forward-to-backward asymmetry, header
overhead as a share of the flow, a flag for an unanswered probe, and a service group from the port.

[Point at safe_ratio.]

Every ratio goes through this helper, which turns a divide-by-zero into zero rather than a fresh
infinity — otherwise I'd re-create the problem I just cleaned up.

[Point at the split cell.]

Three things here matter for correctness. My imputation and scaling live *inside* a scikit-learn
pipeline, so they're refitted on each training fold and never see test data. My splits are
stratified — sixty-four percent train, sixteen validation, twenty test — and this helper folds
ultra-rare families together so stratifying on eleven Heartbleed flows can't crash the split. And I
excluded the source-file column from the predictors: it correlates almost perfectly with the attack
type, so that would be leakage.

## 3:23 – 4:31 · Model selection

[Show the candidate dictionary, then the CV table.]

I compared seven learners plus a no-skill baseline on identical stratified five-fold splits.

[Point at the Dummy row.]

The baseline is my sanity check: PR-AUC zero point two zero nine, and actual prevalence is zero point
two zero nine. The metrics are honest.

[Point at the whole table.]

Now the interesting part. Every serious model is already at the ceiling — boosting at nought point
nine nine nine nine, down to a plain Decision Tree at nine nine three three. Those gaps sit inside
the fold-to-fold standard deviations, so **PR-AUC cannot choose my model for me.**

So I chose on the things that actually differ. On validation, XGBoost misses four attacks and raises
thirteen false alarms; Logistic Regression misses a hundred and one and raises three hundred and
thirty-seven. Under my ten-to-one cost model that's a twenty-five-times difference in business cost,
hidden behind a PR-AUC gap of two thousandths.

And I rejected k-nearest-neighbours on engineering grounds, not accuracy: thirteen hundred flows a
second against XGBoost's fifty-eight thousand, and slower as you add data. Unusable in a live sensor.

## 4:31 – 5:32 · Performance measurement

[Show the confusion matrix, then the PR curve.]

Because the curves are saturated, I report counts. XGBoost caught ten thousand and forty-five
attacks, missed four, and raised thirteen false alarms out of nearly thirty-eight thousand benign
flows — three hundredths of a percent of clean traffic.

[Point at the threshold sweep.]

Now the part I think matters most. The default zero point five threshold is a modelling artefact, not
a business decision, so I swept it and scored every point with my cost function: ten for a missed
attack, one for a false alarm. At zero point five, cost fifty-three. At zero point one nine, cost
thirty-seven — thirty percent better. And if the client demands zero misses, they can have it, for
ninety-five false alarms instead of thirteen.

So the honest statement is: your last three attacks cost about five false alarms each, and
eliminating the final one costs eighty-two more. That's the client's call — my job is to quantify it.

## 5:32 – 6:52 · Hyperparameter tuning

[Show the search spaces, then cv_results.]

I tuned three model families with randomised search scored on PR-AUC — never accuracy — under
stratified three-fold cross-validation, then a focused grid search, all on a training slice only.

[Point at the results.]

And here's my most interesting negative result: all twelve XGBoost configurations landed within two
hundred-thousandths of each other — the same size as the noise between folds of a single
configuration. **The spread between configurations is smaller than the noise within one.** My grid
search then "improved" the score from nine nine nine nine four to nine nine nine nine four — a tie. And on test,
the tuned model was two flows *worse* than the untuned default.

I could have hidden that. Instead I explain it: hyperparameters only matter where a model is
capacity-limited, and this problem is nearly separable — I'll show you why next.

[Point at the two tied configurations.]

What tuning did buy was efficiency: two configurations tied exactly and one fits in half the time.
When quality ties, the cheapest model wins. And tuning genuinely mattered for one model — Logistic
Regression improved once I relaxed regularisation, because it was genuinely underfitting.

The threshold, meanwhile, took cost from sixty-six to forty-one. That one line beat my whole
eighteen-minute search.

## 6:52 – 8:16 · The finding that explains everything

[Show Section 7.1c and the importance chart.]

So why is this problem so easy? Permutation importance answers it. The feature `port_web` — is the
destination port eighty — scores zero point one eight eight. The runner-up scores zero point zero
zero one two. That's a hundred and fifty-four times gap, and forty-two of my ninety-six features are
exactly zero.

The cause is in my own EDA: eighty-seven percent of attacks are DoS Hulk, and they all hit the web
port. One binary feature nearly solves the labelled task. That explains why every model scored above
nine nine three, why tuning changed nothing, and why ten features tie with ninety-six.

[Show 7.4a output.]

And here's the proof it's a shortcut, not intelligence. I retrained with every Bot flow removed, then
tested on Bot. Detection: zero out of a hundred and twenty-four. Zero percent — while the families
still in training were caught perfectly. The model learned *these attack tools' fingerprints*, not
maliciousness.

[Show 7.4b and 7.6 briefly.]

I tested an unsupervised layer as a safety net: it flags seventy-four percent of DoS but gives no lift
at all on Bot, so I report honestly that it doesn't cover the gap. My evasion probe told the same
story — the model shrugs off timing manipulation, so unseen families, not evasion, are the weakness.

## 8:16 – 9:11 · AI ethics

[Show Sections 8.4 and 8.5.]

Ethically I use flow statistics only, never packet contents, so message content is never inspected.

There's no protected attribute here, so I audited fairness by proxy: error rates per service, flow
size, duration and capture session. Web traffic carries a false-alarm rate of nought point nought
nought four four while DNS, TLS and auth sit at exactly zero — twenty-three of my thirty-one false
alarms are web flows. Web users bear essentially the whole burden of being wrongly investigated.

I then tried the textbook fix, per-service thresholds, and it *backfired*: it equalised the recall
gap but tripled the false-positive gap and multiplied cost twelve times, because I'd calibrated to a
one percent budget when the model already runs at nought point nought seven. I published the failure
and the corrected recipe instead of deleting the cell.

## 9:11 – 9:51 · Conclusion

[Show the Model Card, then face to camera.]

To summarise: on unseen data my final model catches all but one of twelve and a half thousand
attacks, with thirty-one false alarms, at eighty-eight thousand flows a second.

But my recommendations are: deploy at threshold zero point one three, not zero point five; don't
market this as zero-day detection, because I measured zero percent on an unseen family; and rebalance
the training data by attack family — more benign traffic is worthless.

My biggest limitation is that port-eighty shortcut, and my next step is the ablation — retrain
without the port features and re-measure. Thank you for watching.
