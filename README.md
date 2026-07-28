# IT3388 Big Data Management Project — Group 2, FinSight Colombia

Customer Intelligence Platform for an anonymised Colombian fintech (COFINFAD dataset).
This repository holds **Member A's** deliverables (Clifton Chen Yi — inactivity risk and
retention prioritisation) plus the team-level appendices.

## Deliverables against the project guide

| Guide requirement | Artefact | State |
|---|---|---|
| Appendix A — project team organisation | `APPENDIX A – PROJECT TEAM ORGANISATION.docx` | Complete except personal contact fields |
| Appendix B — weekly progress updates | `APPENDIX B – WEEKLY PROGRESS UPDATES.docx` | Weeks 11–14 done, 15 current, 16–18 planned |
| Appendix C — project proposal (group) | `APPENDIX C – PROJECT PROPOSAL.docx` | Submitted week 13 |
| Appendix D — exploratory data analysis (individual) | `APPENDIX D – EXPLORATORY DATA ANALYSIS.docx`, `EXPLORATORY DATA ANALYSIS.ipynb` | Submitted week 13 |
| Interim progress review — implementation | `IMPLEMENTATION - INACTIVITY AND RETENTION.ipynb` | Written; needs to be executed |
| Appendix E — final project report (individual) | `APPENDIX E – FINAL PROJECT REPORT.docx` | Written; measured figures pending the notebook run |
| Appendix F — self and peer review | `APPENDIX F – SELF AND PEER REVIEW.docx` | Form ready; ratings are yours to enter |
| Appendix G — final presentation | `APPENDIX G – FINAL PRESENTATION CHECKLIST.docx` | Slide plan + rehearsal checklist |

## Running the implementation notebook

The two source CSVs are not in the repository (size and licence). Download `customer_data.csv`
and `transactions_data.csv` from [COFINFAD on Mendeley Data](https://data.mendeley.com/datasets/mhb4zn3258),
place them next to the notebooks, then:

```bash
pip install pandas numpy matplotlib seaborn scikit-learn pyarrow
pip install xgboost shap        # optional: the notebook skips them cleanly if absent
jupyter lab "IMPLEMENTATION - INACTIVITY AND RETENTION.ipynb"
```

Run all cells. The notebook writes to `outputs/`:

- `report_numbers.json` — every figure quoted in Appendix E
- `tables/` — the panel, feature catalogue, model scoreboard, ablation, importances,
  subgroup performance, scored risk list, scalability benchmark
- `figures/` — 12 charts

Then fill the report with the measured values:

```bash
python tools/fill_appendix_e.py
```

This rewrites `APPENDIX E – FINAL PROJECT REPORT.docx`, replacing every `«placeholder»` with the
real number. Until it is run, the report shows placeholders and says so on its contents page —
deliberately, so no figure in a submitted document is ever invented.

## Still to be completed by hand

- Admin numbers in Appendices A and E, handphone and email in Appendix A (personal data).
- Supervisor's name on the Appendix E cover page.
- Peer-review ratings in Appendix F.
- Weeks 16–18 of Appendix B, once that work actually happens.
- Cloud deployment evidence (the notebook benchmarks single-node pandas and, if a session is
  reachable, compares against Spark; the module still requires the pipeline to run on the
  approved cloud platform).

## `tools/`

Generation scripts, dependency-free (standard library only):

| File | Purpose |
|---|---|
| `docxgen.py` | Minimal `.docx` writer; clones the Appendix C package so all documents share styling |
| `build_impl_notebook.py` | Builds the implementation notebook, syntax-checking every code cell |
| `build_appendices.py` | Builds Appendices A, B, F, G |
| `appendix_e_content.py` | Appendix E content, parameterised over the metrics |
| `fill_appendix_e.py` | Rebuilds Appendix E from `outputs/report_numbers.json` |

Regenerate everything with:

```bash
cd tools && python build_impl_notebook.py && python build_appendices.py && python appendix_e_content.py
```
