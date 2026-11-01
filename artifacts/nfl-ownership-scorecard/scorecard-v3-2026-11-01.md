# NFL DFS ownership — scorecard regrade (nfl-ownership-scorecard-v3)

Generated 2026-11-01 23:28 UTC by `python -m research.nfl_ownership_scorecard` (grader `nfl-ownership-scorecard-grader-v1`). Read-only except the append-only `nfl_ownership_scorecard_rows` (1314 new rows written this run; 5284 rows under this scorecard and grader version in total, one grade per forecast (ON CONFLICT DO NOTHING)).

Status: BUILT, TESTED (unit tests on the metric functions and label gating), BACKTESTED on frozen 2026 inputs. Descriptive only: no verdict, no constant changed. Showdown has no registered gate; the Classic Phase 2 gate needs >= 4 held-out slates of a FITTED model and is not evaluated here. The drafted v5 is graded beside v3 and v4; weeks 2-6 are v5 `design` slates (their v5 design grade is `python -m research.nfl_ownership_prior_v5_design`) and week 7 on is v5's prospective grade (docs/nfl-ownership-prior-v5.md section 5).

## How to read this

- **Frozen input.** Per slate and horizon (canonical = last capture at or before first kickoff minus 20 min; early = first capture inside 24 h), each source's capture is loaded: pool capture (one capture instant covering every game; ownership and DK average read from each player's `evidence`), the owner optimizer run's `input_snapshot`, the ghost freeze. Scorecard v3 (owner, 2026-10-20): `primary` is the LATEST of them, from any source; `preferred` is the v1/v2 preference order (pool, run, ghost), pooled beside it in section 3 for comparison.
- **Versions.** v3, v4 and the drafted v5 are recomputed from the same frozen input (Classic: Python port of ownership-prior.ts; Showdown: the pinned mirrors in model/nfl_showdown_ownership_eval.py). Where the input stored a v3 (or the identical Classic v2) forecast, the recomputation reproduced it to 0.00 on every player (section 7). A stored v1/v2 Showdown forecast is graded as `stored`.
- **Four slates' inputs hold projections and DK averages but no stored ownership** (week 2 Classic and NYG@LAR, week 3 Classic and ATL@GB; `stored none` in section 2), so their v3/v4 are recomputations from a frozen pregame input, not stored forecasts. The task-1 inventory counted only captures with a stored `ownPct` and listed them as `no_frozen_forecast`; graded here, the history is 16 slates with a frozen input rather than the inventory's 12 with a stored forecast. They are `design` slates for v4 (and the two Showdowns for v3).
- **Labels.** Classes from model/nfl_ownership_provenance.py (v1 rules, mass floor 0.995), classified in memory; contests already in `nfl_ownership_label_provenance` matched the persisted classes exactly, the three week-6 imports are in memory only. Only `observed` and `reconciled_zero` are scored.
- **Population.** Players OUT in the frozen input are not graded (their prediction is 0 by construction); this is how the WAS@SF pre-grade counted (45 active). Full pool = every active player in the input with a gate-class label. Reference contest = the largest field whose labels are complete; when no contest on a slate is complete (weeks 4 and 6 Classic: stat-api top-100 users, mass < 0.995 or partial standings) the largest contest is graded on its observed labels only and is NOT gate-eligible (excluded from pooled tables).
- **Relevant (population 2).** Classic: per position, top N_RELEVANT by the frozen STORED forecast (v3 recomputation where nothing was stored) plus top N by field points per $1,000, union; the same set is used for every version so versions are compared on one population. Showdown: every active player.
- **Design tags.** v4: weeks 2-5 `design` (both formats). v3: Showdown weeks 2-3 `design`. v5: weeks 2-6 `design` (both formats; its constants were chosen on them). Week 6 is v4's first non-design data and week 7 v5's. `not_design` pooled rows exclude design slates.
- **Game script (v4 Showdown).** Favorite/underdog from the game's moneyline as the eval harness reads it (nfl_matchups, then nfl_season_games). These are CURRENT stored values, not as of the capture.
- Spearman uses averaged tied ranks. Pooled MAE/bias/RMSE are player-weighted over concatenated slates; pooled Spearman is the mean of per-slate values. 95% intervals resample SLATES (2,000 reps, seed 20261020).

## 1. Slates and frozen inputs

| wk | slate | first kickoff UTC | reference contest | entries | labels complete | canonical sources (capture, UTC) | early sources | favorite |
|---:|---|---|---|---:|---|---|---|---|
| 2 | classic 13 games | 09-20 17:00 | 195648006 | 317,082 | yes | optimizer 09-20 16:03 | optimizer 09-20 16:03 | - |
| 2 | showdown NYG@LAR | 09-22 00:15 | 195786068 | 88,235 | yes | pool 09-21 20:00; optimizer 09-21 20:02 | pool 09-21 12:04; optimizer 09-21 12:07 | LAR |
| 3 | showdown ATL@GB | 09-25 00:15 | 195943234 | 88,235 | yes | pool 09-24 18:05; optimizer 09-24 23:34 | pool 09-24 17:51; optimizer 09-24 23:12 | GB |
| 3 | classic 13 games | 09-27 17:00 | 195905122 | 161,764 | yes | pool 09-27 00:11; optimizer 09-27 16:32 | pool 09-26 23:00; optimizer 09-26 23:00 | - |
| 3 | showdown PHI@CHI | 09-29 00:15 | 196036238 | 88,235 | yes | pool 09-28 21:01; optimizer 09-28 22:24 | pool 09-28 21:01; optimizer 09-28 22:11 | PHI |
| 4 | showdown PIT@CLE | 10-02 00:15 | 196187074 | 88,235 | yes | pool 10-01 22:13; optimizer 10-01 22:13 | pool 10-01 22:13; optimizer 10-01 22:13 | PIT |
| 4 | showdown IND@WAS | 10-04 13:30 | 196204783 | 22,136 | yes | pool 10-04 08:25; optimizer 10-04 08:25 | pool 10-03 21:11; optimizer 10-03 21:12 | IND |
| 4 | classic 12 games | 10-04 17:00 | 196151357 | 161,764 | NO | pool 10-04 16:04; optimizer 10-04 16:07 | optimizer 10-03 21:09 | - |
| 4 | showdown DET@CAR | 10-05 00:20 | 196285055 | 35,671 | yes | pool 10-04 22:13; optimizer 10-04 22:18 | pool 10-04 21:56; optimizer 10-04 21:58 | DET |
| 4 | showdown ATL@NO | 10-06 00:15 | 196285155 | 88,235 | yes | pool 10-05 21:52; optimizer 10-05 21:55 | pool 10-05 11:14; optimizer 10-05 11:14 | NO |
| 5 | showdown TB@DAL | 10-09 00:15 | 196438550 | 88,235 | yes | pool 10-08 00:15 | pool 10-08 00:15 | DAL |
| 5 | classic 11 games | 10-11 17:00 | 196403752 | 161,557 | yes | pool 10-11 15:07; optimizer 10-11 15:37 | optimizer 10-10 17:08 | - |
| 5 | showdown BUF@LAR | 10-13 00:15 | 196480476 | 500 | yes | pool 10-12 22:35; optimizer 10-12 23:13 | pool 10-12 22:35; optimizer 10-12 22:36 | LAR |
| 6 | showdown SEA@DEN | 10-16 00:15 | 196689718 | 84,384 | yes | pool 10-15 13:54; optimizer 10-15 22:12 | pool 10-15 13:29; optimizer 10-15 13:54 | SEA |
| 6 | classic 10 games | 10-18 17:00 | 196653900 | 158,904 | NO | pool 10-18 13:12; optimizer 10-18 14:02; ghost 10-18 15:15 | optimizer 10-18 00:47; ghost 10-18 15:15 | - |
| 6 | showdown WAS@SF | 10-20 00:15 | 196786695 | 47,562 | yes | pool 10-19 23:03; optimizer 10-19 23:13; ghost 10-19 22:30 | pool 10-19 21:57; optimizer 10-19 21:57; ghost 10-19 22:30 | SF |
| 7 | showdown NE@CHI | 10-23 00:15 | 196947078 | 47,562 | yes | pool 10-22 23:50; optimizer 10-22 23:20; ghost 10-22 22:30 | pool 10-22 00:15; optimizer 10-22 21:01; ghost 10-22 22:30 | CHI |
| 7 | classic 10 games | 10-25 17:00 | 196905290 | 147,058 | NO | optimizer 10-25 13:23; ghost 10-25 15:55 | optimizer 10-25 00:41; ghost 10-25 15:55 | - |
| 7 | showdown DAL@PHI | 10-27 00:15 | 197055073 | 73,529 | NO | pool 10-26 23:50; optimizer 10-26 23:33; ghost 10-26 23:13 | pool 10-26 17:14; optimizer 10-26 20:54; ghost 10-26 23:13 | DAL |
| 8 | showdown CAR@GB | 10-30 00:15 | 197208280 | 5,945 | yes | pool 10-29 23:50; optimizer 10-29 22:50; ghost 10-29 23:10 | pool 10-29 11:48; optimizer 10-29 11:49; ghost 10-29 23:10 | GB |
| 8 | classic 11 games | 11-01 18:00 | 197220774 | 198,176 | yes | optimizer 11-01 16:12; ghost 11-01 16:55 | optimizer 11-01 01:46; ghost 11-01 16:55 | - |

- Not graded: contest 195677825 (week 2, NFL Showdown $1.5M Thursday Night Showdown [$500K to 1st] (D) has no salary upload (no_salary_upload (DET@BUF week 2)), so there is no slate, no frozen input and no `no_frozen_forecast` row.

`no_frozen_forecast` rows: 0.

## 2. Per-slate grades — reference contest, canonical horizon, primary source, full pool (gate population)

| wk | slate | contest | source / capture (min before) | stored | anchored/players | version | tag | slot | n | MAE | bias | RMSE | Spearman | Pearson | top-10 |
|---:|---|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 2 | classic 13 games | 195648006 | optimizer 09-20 16:03 (56) | none | 0/670 | v3 | not_design | overall | 593 | 1.12 | +0.00 | 3.13 | 0.754 | 0.725 | 4 |
| 2 | classic 13 games | 195648006 | optimizer 09-20 16:03 (56) | none | 0/670 | v4 | design | overall | 593 | 1.14 | +0.00 | 3.14 | 0.752 | 0.714 | 4 |
| 2 | classic 13 games | 195648006 | optimizer 09-20 16:03 (56) | none | 0/670 | v5 | design | overall | 593 | 1.12 | +0.00 | 2.70 | 0.756 | 0.745 | 4 |
| 2 | showdown NYG@LAR | 195786068 | optimizer 09-21 20:02 (253) | none | 0/53 | v3 | design | CPT | 39 | 1.55 | -0.02 | 3.16 | 0.604 | 0.820 | 8 |
| 2 | showdown NYG@LAR | 195786068 | optimizer 09-21 20:02 (253) | none | 0/53 | v4 | design | CPT | 39 | 1.51 | -0.02 | 3.25 | 0.559 | 0.818 | 8 |
| 2 | showdown NYG@LAR | 195786068 | optimizer 09-21 20:02 (253) | none | 0/53 | v5 | design | CPT | 39 | 1.68 | -0.10 | 2.88 | 0.580 | 0.825 | 8 |
| 2 | showdown NYG@LAR | 195786068 | optimizer 09-21 20:02 (253) | none | 0/53 | v3 | design | FLEX | 46 | 6.30 | +0.01 | 10.64 | 0.629 | 0.810 | 7 |
| 2 | showdown NYG@LAR | 195786068 | optimizer 09-21 20:02 (253) | none | 0/53 | v4 | design | FLEX | 46 | 6.35 | +0.01 | 10.49 | 0.603 | 0.808 | 7 |
| 2 | showdown NYG@LAR | 195786068 | optimizer 09-21 20:02 (253) | none | 0/53 | v5 | design | FLEX | 46 | 5.64 | +0.01 | 9.03 | 0.692 | 0.841 | 7 |
| 3 | classic 13 games | 195905122 | optimizer 09-27 16:32 (27) | none | 0/671 | v3 | not_design | overall | 586 | 1.11 | +0.01 | 3.61 | 0.821 | 0.701 | 4 |
| 3 | classic 13 games | 195905122 | optimizer 09-27 16:32 (27) | none | 0/671 | v4 | design | overall | 586 | 1.09 | +0.01 | 3.54 | 0.822 | 0.703 | 4 |
| 3 | classic 13 games | 195905122 | optimizer 09-27 16:32 (27) | none | 0/671 | v5 | design | overall | 586 | 1.03 | +0.01 | 2.87 | 0.805 | 0.742 | 4 |
| 3 | showdown ATL@GB | 195943234 | optimizer 09-24 23:34 (40) | none | 0/53 | v3 | design | CPT | 38 | 2.08 | +0.00 | 4.48 | 0.841 | 0.838 | 7 |
| 3 | showdown ATL@GB | 195943234 | optimizer 09-24 23:34 (40) | none | 0/53 | v4 | design | CPT | 38 | 2.16 | +0.00 | 4.84 | 0.843 | 0.836 | 7 |
| 3 | showdown ATL@GB | 195943234 | optimizer 09-24 23:34 (40) | none | 0/53 | v5 | design | CPT | 38 | 1.25 | -0.04 | 2.25 | 0.886 | 0.903 | 8 |
| 3 | showdown ATL@GB | 195943234 | optimizer 09-24 23:34 (40) | none | 0/53 | v3 | design | FLEX | 41 | 5.34 | +0.02 | 8.76 | 0.891 | 0.918 | 8 |
| 3 | showdown ATL@GB | 195943234 | optimizer 09-24 23:34 (40) | none | 0/53 | v4 | design | FLEX | 41 | 4.93 | +0.02 | 7.37 | 0.868 | 0.926 | 8 |
| 3 | showdown ATL@GB | 195943234 | optimizer 09-24 23:34 (40) | none | 0/53 | v5 | design | FLEX | 41 | 4.38 | +0.02 | 6.53 | 0.897 | 0.916 | 8 |
| 3 | showdown PHI@CHI | 196036238 | optimizer 09-28 22:24 (110) | none | 0/56 | v3 | design | CPT | 34 | 1.75 | +0.07 | 3.82 | 0.793 | 0.829 | 8 |
| 3 | showdown PHI@CHI | 196036238 | optimizer 09-28 22:24 (110) | none | 0/56 | v4 | design | CPT | 34 | 1.81 | +0.07 | 3.97 | 0.776 | 0.831 | 8 |
| 3 | showdown PHI@CHI | 196036238 | optimizer 09-28 22:24 (110) | none | 0/56 | v5 | design | CPT | 34 | 1.74 | -0.02 | 2.95 | 0.762 | 0.854 | 7 |
| 3 | showdown PHI@CHI | 196036238 | optimizer 09-28 22:24 (110) | none | 0/56 | v3 | design | FLEX | 42 | 5.26 | +0.61 | 8.79 | 0.785 | 0.898 | 8 |
| 3 | showdown PHI@CHI | 196036238 | optimizer 09-28 22:24 (110) | none | 0/56 | v4 | design | FLEX | 42 | 6.00 | +0.61 | 8.68 | 0.704 | 0.881 | 7 |
| 3 | showdown PHI@CHI | 196036238 | optimizer 09-28 22:24 (110) | none | 0/56 | v5 | design | FLEX | 42 | 5.32 | +0.61 | 7.11 | 0.796 | 0.898 | 8 |
| 4 | classic 12 games | 196151357 | optimizer 10-04 16:07 (52) | v2 | 0/629 | v3 | not_design | overall | 181 | 3.57 | -0.10 | 6.50 | 0.655 | 0.562 | 3 | **labels incomplete: observed only, not gate-eligible**
| 4 | classic 12 games | 196151357 | optimizer 10-04 16:07 (52) | v2 | 0/629 | v4 | design | overall | 181 | 3.57 | -0.09 | 6.42 | 0.659 | 0.561 | 3 | **labels incomplete: observed only, not gate-eligible**
| 4 | classic 12 games | 196151357 | optimizer 10-04 16:07 (52) | v2 | 0/629 | v5 | design | overall | 181 | 3.18 | -0.23 | 5.23 | 0.660 | 0.591 | 3 | **labels incomplete: observed only, not gate-eligible**
| 4 | classic 12 games | 196151357 | optimizer 10-04 16:07 (52) | v2 | 0/629 | stored v2 | stored_forecast | overall | 181 | 3.57 | -0.10 | 6.50 | 0.655 | 0.562 | 3 | **labels incomplete: observed only, not gate-eligible**
| 4 | showdown ATL@NO | 196285155 | optimizer 10-05 21:55 (140) | v3 | 0/56 | v3 | not_design | CPT | 36 | 1.30 | +0.01 | 2.63 | 0.818 | 0.951 | 7 |
| 4 | showdown ATL@NO | 196285155 | optimizer 10-05 21:55 (140) | v3 | 0/56 | v4 | design | CPT | 36 | 1.43 | +0.01 | 2.87 | 0.841 | 0.947 | 8 |
| 4 | showdown ATL@NO | 196285155 | optimizer 10-05 21:55 (140) | v3 | 0/56 | v5 | design | CPT | 36 | 0.87 | -0.03 | 1.42 | 0.872 | 0.974 | 9 |
| 4 | showdown ATL@NO | 196285155 | optimizer 10-05 21:55 (140) | v3 | 0/56 | v3 | not_design | FLEX | 42 | 6.81 | -0.15 | 10.23 | 0.799 | 0.896 | 7 |
| 4 | showdown ATL@NO | 196285155 | optimizer 10-05 21:55 (140) | v3 | 0/56 | v4 | design | FLEX | 42 | 6.54 | -0.07 | 9.17 | 0.746 | 0.896 | 7 |
| 4 | showdown ATL@NO | 196285155 | optimizer 10-05 21:55 (140) | v3 | 0/56 | v5 | design | FLEX | 42 | 3.34 | -0.30 | 5.33 | 0.798 | 0.938 | 7 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | v3 | not_design | CPT | 40 | 1.52 | +0.01 | 3.09 | 0.871 | 0.927 | 9 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | v4 | design | CPT | 40 | 1.65 | +0.01 | 3.29 | 0.877 | 0.925 | 9 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | v5 | design | CPT | 40 | 1.18 | +0.01 | 2.48 | 0.872 | 0.904 | 9 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | stored v2 | stored_forecast | CPT | 40 | 1.71 | +0.01 | 3.75 | 0.826 | 0.929 | 7 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | v3 | not_design | FLEX | 40 | 7.54 | +0.07 | 13.41 | 0.869 | 0.806 | 8 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | v4 | design | FLEX | 40 | 7.38 | +0.07 | 12.69 | 0.815 | 0.793 | 8 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | v5 | design | FLEX | 40 | 4.58 | +0.07 | 8.13 | 0.870 | 0.862 | 8 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | stored v2 | stored_forecast | FLEX | 40 | 7.81 | +0.07 | 13.87 | 0.855 | 0.793 | 7 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | v3 | not_design | CPT | 44 | 1.07 | +0.02 | 2.54 | 0.808 | 0.926 | 9 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | v4 | design | CPT | 44 | 1.12 | +0.02 | 2.98 | 0.798 | 0.929 | 9 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | v5 | design | CPT | 44 | 1.00 | +0.02 | 1.90 | 0.812 | 0.943 | 8 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | stored v2 | stored_forecast | CPT | 44 | 1.37 | +0.02 | 3.40 | 0.740 | 0.880 | 9 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | v3 | not_design | FLEX | 44 | 5.04 | +0.11 | 8.13 | 0.827 | 0.921 | 9 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | v4 | design | FLEX | 44 | 4.50 | +0.11 | 6.99 | 0.765 | 0.922 | 9 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | v5 | design | FLEX | 44 | 4.20 | +0.11 | 6.00 | 0.836 | 0.929 | 9 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | stored v2 | stored_forecast | FLEX | 44 | 5.70 | +0.11 | 11.04 | 0.811 | 0.859 | 9 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | v3 | not_design | CPT | 30 | 1.87 | +0.00 | 3.50 | 0.893 | 0.792 | 7 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | v4 | design | CPT | 30 | 1.98 | +0.00 | 3.71 | 0.911 | 0.792 | 8 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | v5 | design | CPT | 30 | 1.61 | -0.07 | 2.74 | 0.859 | 0.893 | 7 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | stored v2 | stored_forecast | CPT | 30 | 2.23 | +0.00 | 3.91 | 0.851 | 0.748 | 6 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | v3 | not_design | FLEX | 40 | 3.19 | +0.01 | 6.28 | 0.879 | 0.943 | 8 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | v4 | design | FLEX | 40 | 4.72 | +0.01 | 7.03 | 0.814 | 0.911 | 8 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | v5 | design | FLEX | 40 | 4.50 | +0.01 | 6.50 | 0.887 | 0.946 | 8 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | stored v2 | stored_forecast | FLEX | 40 | 4.21 | +0.01 | 7.92 | 0.867 | 0.918 | 7 |
| 5 | classic 11 games | 196403752 | optimizer 10-11 15:37 (82) | v3 | 0/587 | v3 | not_design | overall | 442 | 1.64 | +0.00 | 4.80 | 0.848 | 0.612 | 3 |
| 5 | classic 11 games | 196403752 | optimizer 10-11 15:37 (82) | v3 | 0/587 | v4 | design | overall | 442 | 1.63 | +0.00 | 4.71 | 0.850 | 0.616 | 3 |
| 5 | classic 11 games | 196403752 | optimizer 10-11 15:37 (82) | v3 | 0/587 | v5 | design | overall | 442 | 1.55 | +0.00 | 4.02 | 0.840 | 0.664 | 3 |
| 5 | showdown BUF@LAR | 196480476 | optimizer 10-12 23:13 (62) | v3 | 18/49 | v3 | not_design | CPT | 25 | 1.23 | -0.05 | 2.66 | 0.948 | 0.946 | 9 |
| 5 | showdown BUF@LAR | 196480476 | optimizer 10-12 23:13 (62) | v3 | 18/49 | v4 | design | CPT | 25 | 1.29 | -0.02 | 2.82 | 0.950 | 0.946 | 9 |
| 5 | showdown BUF@LAR | 196480476 | optimizer 10-12 23:13 (62) | v3 | 18/49 | v5 | design | CPT | 25 | 1.43 | -0.36 | 2.52 | 0.898 | 0.929 | 9 |
| 5 | showdown BUF@LAR | 196480476 | optimizer 10-12 23:13 (62) | v3 | 18/49 | v3 | not_design | FLEX | 37 | 7.43 | -0.00 | 12.88 | 0.849 | 0.839 | 8 |
| 5 | showdown BUF@LAR | 196480476 | optimizer 10-12 23:13 (62) | v3 | 18/49 | v4 | design | FLEX | 37 | 7.41 | -0.00 | 11.70 | 0.806 | 0.829 | 9 |
| 5 | showdown BUF@LAR | 196480476 | optimizer 10-12 23:13 (62) | v3 | 18/49 | v5 | design | FLEX | 37 | 4.87 | -0.00 | 9.04 | 0.850 | 0.850 | 8 |
| 5 | showdown TB@DAL | 196438550 | pool 10-08 00:15 (1439) | v3 | 0/52 | v3 | not_design | CPT | 33 | 2.42 | +0.00 | 6.16 | 0.849 | 0.765 | 7 |
| 5 | showdown TB@DAL | 196438550 | pool 10-08 00:15 (1439) | v3 | 0/52 | v4 | design | CPT | 33 | 2.56 | +0.00 | 6.49 | 0.858 | 0.766 | 7 |
| 5 | showdown TB@DAL | 196438550 | pool 10-08 00:15 (1439) | v3 | 0/52 | v5 | design | CPT | 33 | 1.43 | -0.03 | 2.60 | 0.886 | 0.858 | 7 |
| 5 | showdown TB@DAL | 196438550 | pool 10-08 00:15 (1439) | v3 | 0/52 | v3 | not_design | FLEX | 39 | 5.25 | +0.01 | 10.03 | 0.914 | 0.862 | 8 |
| 5 | showdown TB@DAL | 196438550 | pool 10-08 00:15 (1439) | v3 | 0/52 | v4 | design | FLEX | 39 | 4.89 | +0.01 | 9.84 | 0.897 | 0.847 | 8 |
| 5 | showdown TB@DAL | 196438550 | pool 10-08 00:15 (1439) | v3 | 0/52 | v5 | design | FLEX | 39 | 4.36 | +0.01 | 7.41 | 0.927 | 0.891 | 8 |
| 6 | classic 10 games | 196653900 | ghost 10-18 15:15 (104) | v3 | 128/526 | v3 | not_design | overall | 162 | 2.94 | -0.12 | 5.05 | 0.801 | 0.764 | 5 | **labels incomplete: observed only, not gate-eligible**
| 6 | classic 10 games | 196653900 | ghost 10-18 15:15 (104) | v3 | 128/526 | v4 | not_design | overall | 162 | 2.95 | -0.13 | 4.95 | 0.807 | 0.768 | 5 | **labels incomplete: observed only, not gate-eligible**
| 6 | classic 10 games | 196653900 | ghost 10-18 15:15 (104) | v3 | 128/526 | v5 | design | overall | 162 | 2.82 | -0.26 | 4.66 | 0.811 | 0.772 | 5 | **labels incomplete: observed only, not gate-eligible**
| 6 | showdown SEA@DEN | 196689718 | optimizer 10-15 22:12 (122) | v3 | 15/56 | v3 | not_design | CPT | 38 | 1.27 | +0.00 | 3.03 | 0.947 | 0.942 | 7 |
| 6 | showdown SEA@DEN | 196689718 | optimizer 10-15 22:12 (122) | v3 | 15/56 | v4 | not_design | CPT | 38 | 1.28 | +0.00 | 3.04 | 0.956 | 0.947 | 8 |
| 6 | showdown SEA@DEN | 196689718 | optimizer 10-15 22:12 (122) | v3 | 15/56 | v5 | design | CPT | 38 | 1.50 | -0.04 | 3.68 | 0.929 | 0.855 | 8 |
| 6 | showdown SEA@DEN | 196689718 | optimizer 10-15 22:12 (122) | v3 | 15/56 | v3 | not_design | FLEX | 45 | 3.28 | +0.01 | 5.81 | 0.954 | 0.950 | 8 |
| 6 | showdown SEA@DEN | 196689718 | optimizer 10-15 22:12 (122) | v3 | 15/56 | v4 | not_design | FLEX | 45 | 3.87 | +0.01 | 5.99 | 0.938 | 0.926 | 7 |
| 6 | showdown SEA@DEN | 196689718 | optimizer 10-15 22:12 (122) | v3 | 15/56 | v5 | design | FLEX | 45 | 3.40 | +0.01 | 5.45 | 0.954 | 0.932 | 8 |
| 6 | showdown WAS@SF | 196786695 | optimizer 10-19 23:13 (62) | v3 | 13/60 | v3 | not_design | CPT | 45 | 1.11 | +0.01 | 2.65 | 0.916 | 0.920 | 9 |
| 6 | showdown WAS@SF | 196786695 | optimizer 10-19 23:13 (62) | v3 | 13/60 | v4 | not_design | CPT | 45 | 1.27 | +0.01 | 3.11 | 0.898 | 0.897 | 8 |
| 6 | showdown WAS@SF | 196786695 | optimizer 10-19 23:13 (62) | v3 | 13/60 | v5 | design | CPT | 45 | 1.01 | +0.01 | 2.12 | 0.893 | 0.933 | 8 |
| 6 | showdown WAS@SF | 196786695 | optimizer 10-19 23:13 (62) | v3 | 13/60 | v3 | not_design | FLEX | 45 | 5.11 | +0.08 | 8.53 | 0.895 | 0.927 | 7 |
| 6 | showdown WAS@SF | 196786695 | optimizer 10-19 23:13 (62) | v3 | 13/60 | v4 | not_design | FLEX | 45 | 5.05 | +0.08 | 8.15 | 0.860 | 0.915 | 7 |
| 6 | showdown WAS@SF | 196786695 | optimizer 10-19 23:13 (62) | v3 | 13/60 | v5 | design | FLEX | 45 | 2.85 | +0.08 | 4.03 | 0.903 | 0.965 | 7 |
| 7 | classic 10 games | 196905290 | ghost 10-25 15:55 (64) | v5,v5+nfl-ownership-bands-v1 | 133/518 | v3 | not_design | overall | 171 | 3.36 | -0.04 | 5.50 | 0.780 | 0.710 | 5 | **labels incomplete: observed only, not gate-eligible**
| 7 | classic 10 games | 196905290 | ghost 10-25 15:55 (64) | v5,v5+nfl-ownership-bands-v1 | 133/518 | v4 | not_design | overall | 171 | 3.30 | -0.04 | 5.42 | 0.786 | 0.709 | 5 | **labels incomplete: observed only, not gate-eligible**
| 7 | classic 10 games | 196905290 | ghost 10-25 15:55 (64) | v5,v5+nfl-ownership-bands-v1 | 133/518 | v5 | not_design | overall | 171 | 3.19 | -0.12 | 5.08 | 0.790 | 0.717 | 5 | **labels incomplete: observed only, not gate-eligible**
| 7 | classic 10 games | 196905290 | ghost 10-25 15:55 (64) | v5,v5+nfl-ownership-bands-v1 | 133/518 | stored v5,v5+nfl-ownership-bands-v1 | stored_forecast | overall | 171 | 3.48 | +0.47 | 5.12 | 0.777 | 0.714 | 5 | **labels incomplete: observed only, not gate-eligible**
| 7 | showdown DAL@PHI | 197055073 | pool 10-26 23:50 (24) | v5 | 20/55 | v3 | not_design | CPT | 21 | 1.94 | -0.00 | 3.69 | 0.940 | 0.902 | 10 | **labels incomplete: observed only, not gate-eligible**
| 7 | showdown DAL@PHI | 197055073 | pool 10-26 23:50 (24) | v5 | 20/55 | v4 | not_design | CPT | 21 | 2.18 | +0.00 | 3.98 | 0.917 | 0.899 | 10 | **labels incomplete: observed only, not gate-eligible**
| 7 | showdown DAL@PHI | 197055073 | pool 10-26 23:50 (24) | v5 | 20/55 | v5 | not_design | CPT | 21 | 1.90 | -0.21 | 2.89 | 0.936 | 0.871 | 10 | **labels incomplete: observed only, not gate-eligible**
| 7 | showdown DAL@PHI | 197055073 | pool 10-26 23:50 (24) | v5 | 20/55 | v3 | not_design | FLEX | 29 | 6.18 | +0.09 | 9.45 | 0.898 | 0.924 | 10 | **labels incomplete: observed only, not gate-eligible**
| 7 | showdown DAL@PHI | 197055073 | pool 10-26 23:50 (24) | v5 | 20/55 | v4 | not_design | FLEX | 29 | 6.48 | -0.19 | 8.97 | 0.800 | 0.903 | 10 | **labels incomplete: observed only, not gate-eligible**
| 7 | showdown DAL@PHI | 197055073 | pool 10-26 23:50 (24) | v5 | 20/55 | v5 | not_design | FLEX | 29 | 4.34 | -0.19 | 5.78 | 0.900 | 0.951 | 10 | **labels incomplete: observed only, not gate-eligible**
| 7 | showdown NE@CHI | 196947078 | pool 10-22 23:50 (24) | v5 | 17/54 | v3 | not_design | CPT | 39 | 1.03 | +0.02 | 2.54 | 0.952 | 0.935 | 10 |
| 7 | showdown NE@CHI | 196947078 | pool 10-22 23:50 (24) | v5 | 17/54 | v4 | not_design | CPT | 39 | 1.14 | +0.02 | 2.80 | 0.946 | 0.932 | 9 |
| 7 | showdown NE@CHI | 196947078 | pool 10-22 23:50 (24) | v5 | 17/54 | v5 | not_design | CPT | 39 | 1.12 | +0.02 | 1.91 | 0.906 | 0.944 | 10 |
| 7 | showdown NE@CHI | 196947078 | pool 10-22 23:50 (24) | v5 | 17/54 | v3 | not_design | FLEX | 39 | 3.70 | +0.09 | 6.11 | 0.955 | 0.957 | 9 |
| 7 | showdown NE@CHI | 196947078 | pool 10-22 23:50 (24) | v5 | 17/54 | v4 | not_design | FLEX | 39 | 3.87 | +0.09 | 5.78 | 0.948 | 0.950 | 9 |
| 7 | showdown NE@CHI | 196947078 | pool 10-22 23:50 (24) | v5 | 17/54 | v5 | not_design | FLEX | 39 | 3.05 | +0.09 | 4.26 | 0.955 | 0.970 | 9 |
| 8 | classic 11 games | 197220774 | ghost 11-01 16:55 (64) | nfl-ownership-fit-v1 | 245/562 | v3 | not_design | overall | 334 | 1.69 | +0.00 | 4.03 | 0.940 | 0.705 | 3 |
| 8 | classic 11 games | 197220774 | ghost 11-01 16:55 (64) | nfl-ownership-fit-v1 | 245/562 | v4 | not_design | overall | 334 | 1.69 | +0.00 | 3.99 | 0.941 | 0.705 | 2 |
| 8 | classic 11 games | 197220774 | ghost 11-01 16:55 (64) | nfl-ownership-fit-v1 | 245/562 | v5 | not_design | overall | 334 | 1.60 | +0.00 | 3.44 | 0.940 | 0.749 | 3 |
| 8 | classic 11 games | 197220774 | ghost 11-01 16:55 (64) | nfl-ownership-fit-v1 | 245/562 | stored nfl-ownership-fit-v1 | stored_forecast | overall | 334 | 1.81 | -0.01 | 4.33 | 0.936 | 0.638 | 3 |
| 8 | showdown CAR@GB | 197208280 | pool 10-29 23:50 (24) | v5 | 24/53 | v3 | not_design | CPT | 36 | 1.12 | +0.01 | 2.62 | 0.927 | 0.860 | 9 |
| 8 | showdown CAR@GB | 197208280 | pool 10-29 23:50 (24) | v5 | 24/53 | v4 | not_design | CPT | 36 | 1.17 | +0.01 | 2.69 | 0.922 | 0.858 | 10 |
| 8 | showdown CAR@GB | 197208280 | pool 10-29 23:50 (24) | v5 | 24/53 | v5 | not_design | CPT | 36 | 1.05 | +0.01 | 2.06 | 0.927 | 0.921 | 9 |
| 8 | showdown CAR@GB | 197208280 | pool 10-29 23:50 (24) | v5 | 24/53 | v3 | not_design | FLEX | 36 | 6.19 | +0.08 | 9.32 | 0.952 | 0.912 | 8 |
| 8 | showdown CAR@GB | 197208280 | pool 10-29 23:50 (24) | v5 | 24/53 | v4 | not_design | FLEX | 36 | 5.86 | +0.08 | 8.32 | 0.888 | 0.900 | 9 |
| 8 | showdown CAR@GB | 197208280 | pool 10-29 23:50 (24) | v5 | 24/53 | v5 | not_design | FLEX | 36 | 4.17 | +0.08 | 5.75 | 0.945 | 0.939 | 8 |

### Relevant population (pregame-defined) and early horizon, v3 vs v4

| wk | slate | slot | relevant n | v3 MAE / rho | v4 MAE / rho | early: source | early v3 MAE / rho | early v4 MAE / rho |
|---:|---|---|---:|---|---|---|---|---|
| 2 | classic 13 games | overall | 97 | 4.48 / 0.615 | 4.60 / 0.607 | optimizer 09-20 16:03 | 1.12 / 0.754 | 1.14 / 0.752 |
| 2 | showdown NYG@LAR | CPT | 39 | 1.55 / 0.604 | 1.51 / 0.559 | optimizer 09-21 12:07 | 1.54 / 0.518 | 1.50 / 0.475 |
| 2 | showdown NYG@LAR | FLEX | 46 | 6.30 / 0.629 | 6.35 / 0.603 | optimizer 09-21 12:07 | 6.45 / 0.537 | 6.42 / 0.530 |
| 3 | classic 13 games | overall | 98 | 4.92 / 0.581 | 4.84 / 0.577 | pool 09-26 23:00 | 1.11 / 0.822 | 1.09 / 0.823 |
| 3 | showdown ATL@GB | CPT | 38 | 2.08 / 0.841 | 2.16 / 0.843 | optimizer 09-24 23:12 | 2.08 / 0.841 | 2.16 / 0.843 |
| 3 | showdown ATL@GB | FLEX | 41 | 5.34 / 0.891 | 4.93 / 0.868 | optimizer 09-24 23:12 | 5.34 / 0.891 | 4.93 / 0.868 |
| 3 | showdown PHI@CHI | CPT | 34 | 1.75 / 0.793 | 1.81 / 0.776 | optimizer 09-28 22:11 | 1.75 / 0.793 | 1.81 / 0.776 |
| 3 | showdown PHI@CHI | FLEX | 42 | 5.26 / 0.785 | 6.00 / 0.704 | optimizer 09-28 22:11 | 5.26 / 0.785 | 6.00 / 0.704 |
| 4 | classic 12 games | overall | 92 | 5.29 / 0.540 | 5.31 / 0.540 | optimizer 10-03 21:09 | 3.53 / 0.655 | 3.52 / 0.660 |
| 4 | showdown ATL@NO | CPT | 36 | 1.30 / 0.818 | 1.43 / 0.841 | optimizer 10-05 11:14 | 1.30 / 0.818 | 1.43 / 0.841 |
| 4 | showdown ATL@NO | FLEX | 42 | 6.81 / 0.799 | 6.54 / 0.746 | optimizer 10-05 11:14 | 6.81 / 0.799 | 6.54 / 0.746 |
| 4 | showdown DET@CAR | CPT | 40 | 1.52 / 0.871 | 1.65 / 0.877 | optimizer 10-04 21:58 | 1.52 / 0.871 | 1.65 / 0.877 |
| 4 | showdown DET@CAR | FLEX | 40 | 7.54 / 0.869 | 7.38 / 0.815 | optimizer 10-04 21:58 | 7.54 / 0.869 | 7.38 / 0.815 |
| 4 | showdown IND@WAS | CPT | 44 | 1.07 / 0.808 | 1.12 / 0.798 | optimizer 10-03 21:12 | 1.11 / 0.808 | 1.10 / 0.798 |
| 4 | showdown IND@WAS | FLEX | 44 | 5.04 / 0.827 | 4.50 / 0.765 | optimizer 10-03 21:12 | 4.88 / 0.825 | 4.33 / 0.762 |
| 4 | showdown PIT@CLE | CPT | 30 | 1.87 / 0.893 | 1.98 / 0.911 | optimizer 10-01 22:13 | 1.87 / 0.893 | 1.98 / 0.911 |
| 4 | showdown PIT@CLE | FLEX | 40 | 3.19 / 0.879 | 4.72 / 0.814 | optimizer 10-01 22:13 | 3.19 / 0.879 | 4.72 / 0.814 |
| 5 | classic 11 games | overall | 95 | 6.13 / 0.469 | 6.09 / 0.471 | optimizer 10-10 17:08 | 1.66 / 0.843 | 1.65 / 0.845 |
| 5 | showdown BUF@LAR | CPT | 25 | 1.23 / 0.948 | 1.29 / 0.950 | optimizer 10-12 22:36 | 1.01 / 0.920 | 1.07 / 0.917 |
| 5 | showdown BUF@LAR | FLEX | 37 | 7.43 / 0.849 | 7.41 / 0.806 | optimizer 10-12 22:36 | 7.01 / 0.825 | 6.96 / 0.791 |
| 5 | showdown TB@DAL | CPT | 33 | 2.42 / 0.849 | 2.56 / 0.858 | pool 10-08 00:15 | 2.42 / 0.849 | 2.56 / 0.858 |
| 5 | showdown TB@DAL | FLEX | 39 | 5.25 / 0.914 | 4.89 / 0.897 | pool 10-08 00:15 | 5.25 / 0.914 | 4.89 / 0.897 |
| 6 | classic 10 games | overall | 87 | 4.62 / 0.545 | 4.62 / 0.550 | ghost 10-18 15:15 | 2.94 / 0.801 | 2.95 / 0.807 |
| 6 | showdown SEA@DEN | CPT | 38 | 1.27 / 0.947 | 1.28 / 0.956 | optimizer 10-15 13:54 | 1.27 / 0.947 | 1.28 / 0.956 |
| 6 | showdown SEA@DEN | FLEX | 45 | 3.28 / 0.954 | 3.87 / 0.938 | optimizer 10-15 13:54 | 3.28 / 0.954 | 3.87 / 0.938 |
| 6 | showdown WAS@SF | CPT | 45 | 1.11 / 0.916 | 1.27 / 0.898 | ghost 10-19 22:30 | 1.07 / 0.892 | 1.23 / 0.876 |
| 6 | showdown WAS@SF | FLEX | 45 | 5.11 / 0.895 | 5.05 / 0.860 | ghost 10-19 22:30 | 5.19 / 0.870 | 5.20 / 0.829 |
| 7 | classic 10 games | overall | 102 | 5.00 / 0.566 | 4.89 / 0.589 | ghost 10-25 15:55 | 3.36 / 0.780 | 3.30 / 0.786 |
| 7 | showdown DAL@PHI | CPT | 21 | 1.94 / 0.940 | 2.18 / 0.917 | ghost 10-26 23:13 | 1.86 / 0.940 | 2.09 / 0.918 |
| 7 | showdown DAL@PHI | FLEX | 29 | 6.18 / 0.898 | 6.48 / 0.800 | ghost 10-26 23:13 | 5.87 / 0.877 | 6.21 / 0.782 |
| 7 | showdown NE@CHI | CPT | 39 | 1.03 / 0.952 | 1.14 / 0.946 | ghost 10-22 22:30 | 0.93 / 0.941 | 1.03 / 0.932 |
| 7 | showdown NE@CHI | FLEX | 39 | 3.70 / 0.955 | 3.87 / 0.948 | ghost 10-22 22:30 | 3.43 / 0.954 | 3.65 / 0.949 |
| 8 | classic 11 games | overall | 100 | 4.89 / 0.597 | 4.92 / 0.596 | ghost 11-01 16:55 | 1.69 / 0.940 | 1.69 / 0.941 |
| 8 | showdown CAR@GB | CPT | 36 | 1.12 / 0.927 | 1.17 / 0.922 | ghost 10-29 23:10 | 1.12 / 0.927 | 1.17 / 0.922 |
| 8 | showdown CAR@GB | FLEX | 36 | 6.19 / 0.952 | 5.86 / 0.888 | ghost 10-29 23:10 | 6.19 / 0.952 | 5.86 / 0.888 |

## 3. Pooled (reference contests with complete labels, full pool, slate-clustered 95% interval)

| format | horizon | source selector | slates | version | slot | subset | n | MAE [95%] | bias [95%] | RMSE | mean Spearman [95%] |
|---|---|---|---:|---|---|---|---:|---|---|---:|---|
| classic | canonical | latest | 4 | v3 | overall | all | 1955 | 1.33 [1.11, 1.66] | +0.00 [0.00, 0.01] | 3.86 | 0.841 [0.777, 0.910] |
| classic | canonical | latest | 4 | v4 | overall | all | 1955 | 1.33 [1.12, 1.66] | +0.00 [0.00, 0.01] | 3.81 | 0.841 [0.777, 0.911] |
| classic | canonical | latest | 4 | v5 | overall | all | 1955 | 1.27 [1.08, 1.57] | +0.00 [0.00, 0.01] | 3.22 | 0.835 [0.777, 0.906] |
| classic | canonical | latest | 1 | stored nfl-ownership-fit-v1 | overall | all | 334 | 1.81 - | -0.01 - | 4.33 | 0.936 - |
| classic | canonical | latest | 4 | v3 | overall | not_design | 1955 | 1.33 [1.11, 1.66] | +0.00 [0.00, 0.01] | 3.86 | 0.841 [0.777, 0.910] |
| classic | canonical | latest | 1 | v4 | overall | not_design | 334 | 1.69 - | +0.00 - | 3.99 | 0.941 - |
| classic | canonical | latest | 1 | v5 | overall | not_design | 334 | 1.60 - | +0.00 - | 3.44 | 0.940 - |
| classic | canonical | latest | 1 | stored nfl-ownership-fit-v1 | overall | not_design | 334 | 1.81 - | -0.01 - | 4.33 | 0.936 - |
| classic | canonical | preferred | 4 | v3 | overall | all | 1953 | 1.34 [1.12, 1.65] | +0.00 [0.00, 0.01] | 3.87 | 0.840 [0.777, 0.907] |
| classic | canonical | preferred | 4 | v4 | overall | all | 1953 | 1.34 [1.13, 1.65] | +0.00 [0.00, 0.01] | 3.82 | 0.841 [0.777, 0.908] |
| classic | canonical | preferred | 4 | v5 | overall | all | 1953 | 1.28 [1.09, 1.57] | +0.00 [0.00, 0.01] | 3.23 | 0.835 [0.777, 0.903] |
| classic | canonical | preferred | 1 | stored nfl-ownership-fit-v1 | overall | all | 347 | 1.76 - | -0.01 - | 4.27 | 0.934 - |
| classic | canonical | preferred | 4 | v3 | overall | not_design | 1953 | 1.34 [1.12, 1.65] | +0.00 [0.00, 0.01] | 3.87 | 0.840 [0.777, 0.907] |
| classic | canonical | preferred | 1 | v4 | overall | not_design | 347 | 1.67 - | +0.00 - | 4.00 | 0.935 - |
| classic | canonical | preferred | 1 | v5 | overall | not_design | 347 | 1.59 - | +0.00 - | 3.44 | 0.933 - |
| classic | canonical | preferred | 1 | stored nfl-ownership-fit-v1 | overall | not_design | 347 | 1.76 - | -0.01 - | 4.27 | 0.934 - |
| classic | early | latest | 4 | v3 | overall | all | 1955 | 1.33 [1.11, 1.67] | +0.00 [0.00, 0.01] | 3.87 | 0.840 [0.776, 0.911] |
| classic | early | latest | 4 | v4 | overall | all | 1955 | 1.34 [1.12, 1.67] | +0.00 [0.00, 0.01] | 3.82 | 0.840 [0.775, 0.912] |
| classic | early | latest | 4 | v5 | overall | all | 1955 | 1.28 [1.07, 1.58] | +0.00 [0.00, 0.01] | 3.22 | 0.834 [0.776, 0.906] |
| classic | early | latest | 1 | stored nfl-ownership-fit-v1 | overall | all | 334 | 1.81 - | -0.01 - | 4.33 | 0.936 - |
| classic | early | latest | 4 | v3 | overall | not_design | 1955 | 1.33 [1.11, 1.67] | +0.00 [0.00, 0.01] | 3.87 | 0.840 [0.776, 0.911] |
| classic | early | latest | 1 | v4 | overall | not_design | 334 | 1.69 - | +0.00 - | 3.99 | 0.941 - |
| classic | early | latest | 1 | v5 | overall | not_design | 334 | 1.60 - | +0.00 - | 3.44 | 0.940 - |
| classic | early | latest | 1 | stored nfl-ownership-fit-v1 | overall | not_design | 334 | 1.81 - | -0.01 - | 4.33 | 0.936 - |
| classic | early | preferred | 4 | v3 | overall | all | 2056 | 1.28 [1.11, 1.50] | +0.00 [0.00, 0.01] | 3.78 | 0.836 [0.776, 0.899] |
| classic | early | preferred | 4 | v4 | overall | all | 2056 | 1.28 [1.12, 1.49] | +0.00 [0.00, 0.01] | 3.74 | 0.837 [0.775, 0.900] |
| classic | early | preferred | 4 | v5 | overall | all | 2056 | 1.22 [1.07, 1.43] | +0.00 [0.00, 0.01] | 3.16 | 0.829 [0.776, 0.889] |
| classic | early | preferred | 1 | stored nfl-ownership-fit-v1 | overall | all | 435 | 1.40 - | -0.01 - | 3.72 | 0.920 - |
| classic | early | preferred | 4 | v3 | overall | not_design | 2056 | 1.28 [1.11, 1.50] | +0.00 [0.00, 0.01] | 3.78 | 0.836 [0.776, 0.899] |
| classic | early | preferred | 1 | v4 | overall | not_design | 435 | 1.33 - | +0.00 - | 3.58 | 0.926 - |
| classic | early | preferred | 1 | v5 | overall | not_design | 435 | 1.28 - | +0.00 - | 3.07 | 0.917 - |
| classic | early | preferred | 1 | stored nfl-ownership-fit-v1 | overall | not_design | 435 | 1.40 - | -0.01 - | 3.72 | 0.920 - |
| showdown | canonical | latest | 13 | v3 | CPT | all | 477 | 1.47 [1.26, 1.70] | +0.01 [-0.00, 0.02] | 3.40 | 0.859 [0.803, 0.904] |
| showdown | canonical | latest | 13 | v4 | CPT | all | 477 | 1.55 [1.34, 1.78] | +0.01 [-0.00, 0.02] | 3.64 | 0.857 [0.794, 0.905] |
| showdown | canonical | latest | 13 | v5 | CPT | all | 477 | 1.28 [1.14, 1.44] | -0.04 [-0.08, -0.00] | 2.47 | 0.852 [0.797, 0.893] |
| showdown | canonical | latest | 3 | stored v2 | CPT | all | 114 | 1.71 [1.37, 2.23] | +0.01 [0.00, 0.02] | 3.66 | 0.805 [0.740, 0.851] |
| showdown | canonical | latest | 10 | v3 | CPT | not_design | 366 | 1.37 [1.16, 1.65] | +0.01 [-0.00, 0.01] | 3.25 | 0.893 [0.860, 0.924] |
| showdown | canonical | latest | 4 | v4 | CPT | not_design | 158 | 1.21 [1.15, 1.27] | +0.01 [0.01, 0.02] | 2.92 | 0.931 [0.910, 0.951] |
| showdown | canonical | latest | 2 | v5 | CPT | not_design | 75 | 1.08 [1.05, 1.12] | +0.01 [0.01, 0.02] | 1.98 | 0.917 [0.906, 0.927] |
| showdown | canonical | latest | 3 | stored v2 | CPT | not_design | 114 | 1.71 [1.37, 2.23] | +0.01 [0.00, 0.02] | 3.66 | 0.805 [0.740, 0.851] |
| showdown | canonical | latest | 13 | v3 | FLEX | all | 536 | 5.39 [4.66, 6.11] | +0.07 [-0.00, 0.18] | 9.38 | 0.861 [0.811, 0.904] |
| showdown | canonical | latest | 13 | v4 | FLEX | all | 536 | 5.47 [4.87, 6.09] | +0.08 [0.01, 0.18] | 8.83 | 0.819 [0.766, 0.868] |
| showdown | canonical | latest | 13 | v5 | FLEX | all | 536 | 4.20 [3.75, 4.67] | +0.06 [-0.03, 0.18] | 6.67 | 0.870 [0.827, 0.907] |
| showdown | canonical | latest | 3 | stored v2 | FLEX | all | 124 | 5.90 [4.21, 7.81] | +0.06 [0.01, 0.11] | 11.20 | 0.844 [0.811, 0.867] |
| showdown | canonical | latest | 10 | v3 | FLEX | not_design | 407 | 5.31 [4.39, 6.23] | +0.03 [-0.02, 0.07] | 9.34 | 0.889 [0.855, 0.921] |
| showdown | canonical | latest | 4 | v4 | FLEX | not_design | 165 | 4.62 [3.87, 5.41] | +0.06 [0.02, 0.09] | 7.14 | 0.908 [0.874, 0.943] |
| showdown | canonical | latest | 2 | v5 | FLEX | not_design | 75 | 3.59 [3.05, 4.17] | +0.08 [0.08, 0.09] | 5.03 | 0.950 [0.945, 0.955] |
| showdown | canonical | latest | 3 | stored v2 | FLEX | not_design | 124 | 5.90 [4.21, 7.81] | +0.06 [0.01, 0.11] | 11.20 | 0.844 [0.811, 0.867] |
| showdown | canonical | preferred | 13 | v3 | CPT | all | 483 | 1.44 [1.23, 1.68] | +0.00 [-0.01, 0.01] | 3.35 | 0.854 [0.800, 0.897] |
| showdown | canonical | preferred | 13 | v4 | CPT | all | 483 | 1.51 [1.30, 1.75] | +0.00 [-0.00, 0.01] | 3.58 | 0.851 [0.789, 0.898] |
| showdown | canonical | preferred | 13 | v5 | CPT | all | 483 | 1.28 [1.14, 1.43] | -0.04 [-0.09, -0.01] | 2.47 | 0.850 [0.795, 0.889] |
| showdown | canonical | preferred | 1 | stored v1 | CPT | all | 37 | 2.25 - | -0.01 - | 4.74 | 0.470 - |
| showdown | canonical | preferred | 3 | stored v2 | CPT | all | 114 | 1.76 [1.49, 2.23] | +0.01 [0.00, 0.02] | 3.74 | 0.805 [0.740, 0.851] |
| showdown | canonical | preferred | 10 | v3 | CPT | not_design | 369 | 1.34 [1.12, 1.62] | +0.01 [-0.00, 0.01] | 3.19 | 0.887 [0.856, 0.916] |
| showdown | canonical | preferred | 4 | v4 | CPT | not_design | 158 | 1.17 [1.13, 1.24] | +0.01 [0.01, 0.02] | 2.79 | 0.925 [0.892, 0.951] |
| showdown | canonical | preferred | 2 | v5 | CPT | not_design | 75 | 1.08 [1.05, 1.12] | +0.01 [0.01, 0.02] | 1.98 | 0.917 [0.906, 0.927] |
| showdown | canonical | preferred | 1 | stored v1 | CPT | not_design | 37 | 2.25 - | -0.01 - | 4.74 | 0.470 - |
| showdown | canonical | preferred | 3 | stored v2 | CPT | not_design | 114 | 1.76 [1.49, 2.23] | +0.01 [0.00, 0.02] | 3.74 | 0.805 [0.740, 0.851] |
| showdown | canonical | preferred | 13 | v3 | FLEX | all | 543 | 5.43 [4.71, 6.12] | +0.03 [-0.01, 0.06] | 9.48 | 0.855 [0.804, 0.898] |
| showdown | canonical | preferred | 13 | v4 | FLEX | all | 543 | 5.50 [4.92, 6.11] | +0.03 [0.01, 0.06] | 8.92 | 0.809 [0.753, 0.862] |
| showdown | canonical | preferred | 13 | v5 | FLEX | all | 543 | 4.27 [3.84, 4.70] | +0.01 [-0.05, 0.06] | 6.77 | 0.864 [0.821, 0.902] |
| showdown | canonical | preferred | 1 | stored v1 | FLEX | all | 46 | 8.47 - | +0.00 - | 15.80 | 0.502 - |
| showdown | canonical | preferred | 3 | stored v2 | FLEX | all | 124 | 5.84 [4.21, 7.81] | +0.06 [0.01, 0.11] | 11.22 | 0.845 [0.812, 0.867] |
| showdown | canonical | preferred | 10 | v3 | FLEX | not_design | 410 | 5.36 [4.43, 6.24] | +0.03 [-0.02, 0.07] | 9.46 | 0.884 [0.850, 0.917] |
| showdown | canonical | preferred | 4 | v4 | FLEX | not_design | 165 | 4.88 [3.87, 5.93] | +0.06 [0.02, 0.09] | 7.64 | 0.893 [0.834, 0.943] |
| showdown | canonical | preferred | 2 | v5 | FLEX | not_design | 75 | 3.59 [3.05, 4.17] | +0.08 [0.08, 0.09] | 5.03 | 0.950 [0.945, 0.955] |
| showdown | canonical | preferred | 1 | stored v1 | FLEX | not_design | 46 | 8.47 - | +0.00 - | 15.80 | 0.502 - |
| showdown | canonical | preferred | 3 | stored v2 | FLEX | not_design | 124 | 5.84 [4.21, 7.81] | +0.06 [0.01, 0.11] | 11.22 | 0.845 [0.812, 0.867] |
| showdown | early | latest | 13 | v3 | CPT | all | 483 | 1.44 [1.23, 1.68] | +0.01 [-0.00, 0.02] | 3.35 | 0.847 [0.780, 0.896] |
| showdown | early | latest | 13 | v4 | CPT | all | 483 | 1.51 [1.30, 1.76] | +0.01 [-0.00, 0.02] | 3.59 | 0.845 [0.771, 0.896] |
| showdown | early | latest | 13 | v5 | CPT | all | 483 | 1.29 [1.14, 1.47] | -0.04 [-0.08, -0.00] | 2.49 | 0.842 [0.776, 0.887] |
| showdown | early | latest | 1 | stored linestar,v2 | CPT | all | 40 | 1.71 - | +0.01 - | 3.75 | 0.826 - |
| showdown | early | latest | 2 | stored v2 | CPT | all | 74 | 1.79 [1.49, 2.23] | +0.01 [0.00, 0.02] | 3.73 | 0.795 [0.740, 0.851] |
| showdown | early | latest | 10 | v3 | CPT | not_design | 371 | 1.34 [1.12, 1.63] | +0.01 [-0.00, 0.01] | 3.20 | 0.887 [0.856, 0.914] |
| showdown | early | latest | 4 | v4 | CPT | not_design | 160 | 1.17 [1.08, 1.25] | +0.01 [0.01, 0.01] | 2.83 | 0.922 [0.890, 0.948] |
| showdown | early | latest | 2 | v5 | CPT | not_design | 77 | 1.09 [1.05, 1.13] | +0.01 [0.01, 0.02] | 1.98 | 0.910 [0.893, 0.927] |
| showdown | early | latest | 1 | stored linestar,v2 | CPT | not_design | 40 | 1.71 - | +0.01 - | 3.75 | 0.826 - |
| showdown | early | latest | 2 | stored v2 | CPT | not_design | 74 | 1.79 [1.49, 2.23] | +0.01 [0.00, 0.02] | 3.73 | 0.795 [0.740, 0.851] |
| showdown | early | latest | 13 | v3 | FLEX | all | 542 | 5.35 [4.62, 6.08] | +0.07 [-0.00, 0.18] | 9.27 | 0.850 [0.788, 0.899] |
| showdown | early | latest | 13 | v4 | FLEX | all | 542 | 5.43 [4.83, 6.06] | +0.08 [0.01, 0.18] | 8.75 | 0.810 [0.749, 0.864] |
| showdown | early | latest | 13 | v5 | FLEX | all | 542 | 4.26 [3.80, 4.75] | +0.06 [-0.03, 0.17] | 6.75 | 0.861 [0.810, 0.903] |
| showdown | early | latest | 1 | stored linestar,v2 | FLEX | all | 40 | 7.81 - | +0.07 - | 13.87 | 0.855 - |
| showdown | early | latest | 2 | stored v2 | FLEX | all | 84 | 4.91 [4.21, 5.55] | +0.06 [0.01, 0.11] | 9.72 | 0.839 [0.812, 0.867] |
| showdown | early | latest | 10 | v3 | FLEX | not_design | 412 | 5.24 [4.31, 6.15] | +0.03 [-0.02, 0.07] | 9.23 | 0.884 [0.850, 0.917] |
| showdown | early | latest | 4 | v4 | FLEX | not_design | 167 | 4.60 [3.77, 5.49] | +0.06 [0.02, 0.08] | 7.07 | 0.901 [0.856, 0.943] |
| showdown | early | latest | 2 | v5 | FLEX | not_design | 77 | 3.62 [3.13, 4.17] | +0.08 [0.08, 0.08] | 5.05 | 0.949 [0.945, 0.952] |
| showdown | early | latest | 1 | stored linestar,v2 | FLEX | not_design | 40 | 7.81 - | +0.07 - | 13.87 | 0.855 - |
| showdown | early | latest | 2 | stored v2 | FLEX | not_design | 84 | 4.91 [4.21, 5.55] | +0.06 [0.01, 0.11] | 9.72 | 0.839 [0.812, 0.867] |
| showdown | early | preferred | 13 | v3 | CPT | all | 487 | 1.43 [1.23, 1.66] | +0.00 [-0.01, 0.01] | 3.30 | 0.848 [0.782, 0.896] |
| showdown | early | preferred | 13 | v4 | CPT | all | 487 | 1.50 [1.29, 1.74] | +0.00 [-0.00, 0.01] | 3.53 | 0.844 [0.770, 0.896] |
| showdown | early | preferred | 13 | v5 | CPT | all | 487 | 1.29 [1.14, 1.45] | -0.04 [-0.09, -0.01] | 2.48 | 0.842 [0.776, 0.886] |
| showdown | early | preferred | 1 | stored v1 | CPT | all | 37 | 2.25 - | -0.01 - | 4.74 | 0.470 - |
| showdown | early | preferred | 3 | stored v2 | CPT | all | 114 | 1.76 [1.49, 2.23] | +0.01 [0.00, 0.02] | 3.74 | 0.805 [0.740, 0.851] |
| showdown | early | preferred | 10 | v3 | CPT | not_design | 372 | 1.34 [1.13, 1.63] | +0.01 [-0.00, 0.01] | 3.20 | 0.886 [0.855, 0.915] |
| showdown | early | preferred | 4 | v4 | CPT | not_design | 161 | 1.18 [1.10, 1.25] | +0.01 [0.01, 0.01] | 2.84 | 0.919 [0.887, 0.950] |
| showdown | early | preferred | 2 | v5 | CPT | not_design | 78 | 1.12 [1.06, 1.19] | +0.01 [0.01, 0.02] | 2.01 | 0.906 [0.900, 0.912] |
| showdown | early | preferred | 1 | stored v1 | CPT | not_design | 37 | 2.25 - | -0.01 - | 4.74 | 0.470 - |
| showdown | early | preferred | 3 | stored v2 | CPT | not_design | 114 | 1.76 [1.49, 2.23] | +0.01 [0.00, 0.02] | 3.74 | 0.805 [0.740, 0.851] |
| showdown | early | preferred | 13 | v3 | FLEX | all | 547 | 5.35 [4.65, 6.05] | +0.02 [-0.01, 0.05] | 9.31 | 0.846 [0.783, 0.895] |
| showdown | early | preferred | 13 | v4 | FLEX | all | 547 | 5.44 [4.85, 6.05] | +0.03 [0.00, 0.05] | 8.82 | 0.802 [0.741, 0.859] |
| showdown | early | preferred | 13 | v5 | FLEX | all | 547 | 4.25 [3.81, 4.72] | +0.01 [-0.05, 0.05] | 6.73 | 0.858 [0.806, 0.901] |
| showdown | early | preferred | 1 | stored v1 | FLEX | all | 46 | 8.47 - | +0.00 - | 15.80 | 0.502 - |
| showdown | early | preferred | 3 | stored v2 | FLEX | all | 124 | 5.85 [4.21, 7.81] | +0.06 [0.01, 0.11] | 11.23 | 0.845 [0.812, 0.867] |
| showdown | early | preferred | 10 | v3 | FLEX | not_design | 413 | 5.21 [4.30, 6.09] | +0.03 [-0.02, 0.07] | 9.22 | 0.881 [0.849, 0.913] |
| showdown | early | preferred | 4 | v4 | FLEX | not_design | 168 | 4.58 [3.85, 5.28] | +0.06 [0.03, 0.08] | 7.09 | 0.890 [0.834, 0.945] |
| showdown | early | preferred | 2 | v5 | FLEX | not_design | 78 | 3.56 [3.24, 3.85] | +0.07 [0.05, 0.09] | 4.91 | 0.942 [0.922, 0.963] |
| showdown | early | preferred | 1 | stored v1 | FLEX | not_design | 46 | 8.47 - | +0.00 - | 15.80 | 0.502 - |
| showdown | early | preferred | 3 | stored v2 | FLEX | not_design | 124 | 5.85 [4.21, 7.81] | +0.06 [0.01, 0.11] | 11.23 | 0.845 [0.812, 0.867] |

### Source disagreement at the canonical horizon (same slate, same version)

| wk | slate | slot | version | pool | run | ghost |
|---:|---|---|---|---|---|---|
| 2 | showdown NYG@LAR | CPT | v3 | 20:00: 1.55 / 0.604 | 20:02: 1.55 / 0.604 | - |
| 2 | showdown NYG@LAR | CPT | v4 | 20:00: 1.51 / 0.559 | 20:02: 1.51 / 0.559 | - |
| 2 | showdown NYG@LAR | FLEX | v3 | 20:00: 6.30 / 0.629 | 20:02: 6.30 / 0.629 | - |
| 2 | showdown NYG@LAR | FLEX | v4 | 20:00: 6.35 / 0.603 | 20:02: 6.35 / 0.603 | - |
| 3 | classic 13 games | overall | v3 | 00:11: 1.13 / 0.826 | 16:32: 1.11 / 0.821 | - |
| 3 | classic 13 games | overall | v4 | 00:11: 1.12 / 0.827 | 16:32: 1.09 / 0.822 | - |
| 3 | showdown ATL@GB | CPT | v3 | 18:05: 2.08 / 0.841 | 23:34: 2.08 / 0.841 | - |
| 3 | showdown ATL@GB | CPT | v4 | 18:05: 2.16 / 0.843 | 23:34: 2.16 / 0.843 | - |
| 3 | showdown ATL@GB | FLEX | v3 | 18:05: 5.34 / 0.891 | 23:34: 5.34 / 0.891 | - |
| 3 | showdown ATL@GB | FLEX | v4 | 18:05: 4.93 / 0.868 | 23:34: 4.93 / 0.868 | - |
| 3 | showdown PHI@CHI | CPT | v3 | 21:01: 1.68 / 0.783 | 22:24: 1.75 / 0.793 | - |
| 3 | showdown PHI@CHI | CPT | v4 | 21:01: 1.74 / 0.758 | 22:24: 1.81 / 0.776 | - |
| 3 | showdown PHI@CHI | FLEX | v3 | 21:01: 5.22 / 0.754 | 22:24: 5.26 / 0.785 | - |
| 3 | showdown PHI@CHI | FLEX | v4 | 21:01: 5.91 / 0.651 | 22:24: 6.00 / 0.704 | - |
| 4 | classic 12 games | overall | v3 | 16:04: 3.57 / 0.652 | 16:07: 3.57 / 0.655 | - |
| 4 | classic 12 games | overall | v4 | 16:04: 3.58 / 0.657 | 16:07: 3.57 / 0.659 | - |
| 4 | showdown ATL@NO | CPT | v3 | 21:52: 1.30 / 0.818 | 21:55: 1.30 / 0.818 | - |
| 4 | showdown ATL@NO | CPT | v4 | 21:52: 1.43 / 0.841 | 21:55: 1.43 / 0.841 | - |
| 4 | showdown ATL@NO | FLEX | v3 | 21:52: 6.81 / 0.799 | 21:55: 6.81 / 0.799 | - |
| 4 | showdown ATL@NO | FLEX | v4 | 21:52: 6.54 / 0.746 | 21:55: 6.54 / 0.746 | - |
| 4 | showdown DET@CAR | CPT | v3 | 22:13: 1.52 / 0.871 | 22:18: 1.52 / 0.871 | - |
| 4 | showdown DET@CAR | CPT | v4 | 22:13: 1.65 / 0.877 | 22:18: 1.65 / 0.877 | - |
| 4 | showdown DET@CAR | FLEX | v3 | 22:13: 7.54 / 0.869 | 22:18: 7.54 / 0.869 | - |
| 4 | showdown DET@CAR | FLEX | v4 | 22:13: 7.38 / 0.815 | 22:18: 7.38 / 0.815 | - |
| 4 | showdown IND@WAS | CPT | v3 | 08:25: 1.11 / 0.808 | 08:25: 1.07 / 0.808 | - |
| 4 | showdown IND@WAS | CPT | v4 | 08:25: 1.10 / 0.798 | 08:25: 1.12 / 0.798 | - |
| 4 | showdown IND@WAS | FLEX | v3 | 08:25: 4.88 / 0.825 | 08:25: 5.04 / 0.827 | - |
| 4 | showdown IND@WAS | FLEX | v4 | 08:25: 4.32 / 0.762 | 08:25: 4.50 / 0.765 | - |
| 4 | showdown PIT@CLE | CPT | v3 | 22:13: 1.87 / 0.893 | 22:13: 1.87 / 0.893 | - |
| 4 | showdown PIT@CLE | CPT | v4 | 22:13: 1.98 / 0.911 | 22:13: 1.98 / 0.911 | - |
| 4 | showdown PIT@CLE | FLEX | v3 | 22:13: 3.19 / 0.879 | 22:13: 3.19 / 0.879 | - |
| 4 | showdown PIT@CLE | FLEX | v4 | 22:13: 4.72 / 0.814 | 22:13: 4.72 / 0.814 | - |
| 5 | classic 11 games | overall | v3 | 15:07: 1.64 / 0.848 | 15:37: 1.64 / 0.848 | - |
| 5 | classic 11 games | overall | v4 | 15:07: 1.63 / 0.850 | 15:37: 1.63 / 0.850 | - |
| 5 | showdown BUF@LAR | CPT | v3 | 22:35: 1.01 / 0.920 | 23:13: 1.23 / 0.948 | - |
| 5 | showdown BUF@LAR | CPT | v4 | 22:35: 1.07 / 0.917 | 23:13: 1.29 / 0.950 | - |
| 5 | showdown BUF@LAR | FLEX | v3 | 22:35: 7.01 / 0.825 | 23:13: 7.43 / 0.849 | - |
| 5 | showdown BUF@LAR | FLEX | v4 | 22:35: 6.96 / 0.791 | 23:13: 7.41 / 0.806 | - |
| 6 | classic 10 games | overall | v3 | 13:12: 2.95 / 0.801 | 14:02: 2.95 / 0.801 | 15:15: 2.94 / 0.801 |
| 6 | classic 10 games | overall | v4 | 13:12: 2.95 / 0.807 | 14:02: 2.95 / 0.807 | 15:15: 2.95 / 0.807 |
| 6 | showdown SEA@DEN | CPT | v3 | 13:54: 1.27 / 0.947 | 22:12: 1.27 / 0.947 | - |
| 6 | showdown SEA@DEN | CPT | v4 | 13:54: 1.28 / 0.956 | 22:12: 1.28 / 0.956 | - |
| 6 | showdown SEA@DEN | FLEX | v3 | 13:54: 3.28 / 0.954 | 22:12: 3.28 / 0.954 | - |
| 6 | showdown SEA@DEN | FLEX | v4 | 13:54: 3.87 / 0.938 | 22:12: 3.87 / 0.938 | - |
| 6 | showdown WAS@SF | CPT | v3 | 23:03: 0.97 / 0.891 | 23:13: 1.11 / 0.916 | 22:30: 1.07 / 0.892 |
| 6 | showdown WAS@SF | CPT | v4 | 23:03: 1.13 / 0.875 | 23:13: 1.27 / 0.898 | 22:30: 1.23 / 0.876 |
| 6 | showdown WAS@SF | FLEX | v3 | 23:03: 5.98 / 0.867 | 23:13: 5.11 / 0.895 | 22:30: 5.19 / 0.870 |
| 6 | showdown WAS@SF | FLEX | v4 | 23:03: 5.99 / 0.799 | 23:13: 5.05 / 0.860 | 22:30: 5.20 / 0.829 |
| 7 | classic 10 games | overall | v3 | - | 13:23: 3.39 / 0.769 | 15:55: 3.36 / 0.780 |
| 7 | classic 10 games | overall | v4 | - | 13:23: 3.33 / 0.773 | 15:55: 3.30 / 0.786 |
| 7 | showdown DAL@PHI | CPT | v3 | 23:50: 1.94 / 0.940 | 23:33: 1.94 / 0.940 | 23:13: 1.86 / 0.940 |
| 7 | showdown DAL@PHI | CPT | v4 | 23:50: 2.18 / 0.917 | 23:33: 2.18 / 0.917 | 23:13: 2.09 / 0.918 |
| 7 | showdown DAL@PHI | FLEX | v3 | 23:50: 6.18 / 0.898 | 23:33: 6.18 / 0.898 | 23:13: 5.87 / 0.877 |
| 7 | showdown DAL@PHI | FLEX | v4 | 23:50: 6.48 / 0.800 | 23:33: 6.48 / 0.800 | 23:13: 6.21 / 0.782 |
| 7 | showdown NE@CHI | CPT | v3 | 23:50: 1.03 / 0.952 | 23:20: 1.03 / 0.952 | 22:30: 0.93 / 0.941 |
| 7 | showdown NE@CHI | CPT | v4 | 23:50: 1.14 / 0.946 | 23:20: 1.14 / 0.946 | 22:30: 1.03 / 0.932 |
| 7 | showdown NE@CHI | FLEX | v3 | 23:50: 3.70 / 0.955 | 23:20: 3.70 / 0.955 | 22:30: 3.43 / 0.954 |
| 7 | showdown NE@CHI | FLEX | v4 | 23:50: 3.87 / 0.948 | 23:20: 3.87 / 0.948 | 22:30: 3.65 / 0.949 |
| 8 | classic 11 games | overall | v3 | - | 16:12: 1.67 / 0.934 | 16:55: 1.69 / 0.940 |
| 8 | classic 11 games | overall | v4 | - | 16:12: 1.67 / 0.935 | 16:55: 1.69 / 0.941 |
| 8 | showdown CAR@GB | CPT | v3 | 23:50: 1.12 / 0.927 | 22:50: 1.04 / 0.911 | 23:10: 1.12 / 0.927 |
| 8 | showdown CAR@GB | CPT | v4 | 23:50: 1.17 / 0.922 | 22:50: 1.08 / 0.903 | 23:10: 1.17 / 0.922 |
| 8 | showdown CAR@GB | FLEX | v3 | 23:50: 6.19 / 0.952 | 22:50: 5.29 / 0.923 | 23:10: 6.19 / 0.952 |
| 8 | showdown CAR@GB | FLEX | v4 | 23:50: 5.86 / 0.888 | 22:50: 5.26 / 0.853 | 23:10: 5.86 / 0.888 |

## 4. Diagnostics (outcome-defined, never a gate) — reference, canonical, primary, summed over slates

| format | slot | population | v3 n | v3 MAE | v3 bias | v4 MAE | v4 bias |
|---|---|---|---:|---:|---:|---:|---:|
| classic | overall | actual_ge_1 | 509 | 4.64 | -0.38 | 4.65 | -0.38 |
| classic | overall | position:DST | 96 | 3.87 | +0.00 | 3.87 | +0.00 |
| classic | overall | position:QB | 207 | 1.22 | +0.00 | 1.22 | +0.00 |
| classic | overall | position:RB | 466 | 1.38 | +0.16 | 1.32 | -0.06 |
| classic | overall | position:TE | 462 | 0.71 | -0.21 | 0.81 | +0.05 |
| classic | overall | position:WR | 724 | 1.38 | +0.04 | 1.37 | +0.01 |
| classic | overall | pred_chalk_ge_15 | 50 | 14.22 | +11.06 | 13.48 | +11.26 |
| classic | overall | salary_tier:0-3k | 419 | 0.57 | -0.04 | 0.58 | -0.02 |
| classic | overall | salary_tier:3-4k | 624 | 0.61 | -0.08 | 0.64 | -0.02 |
| classic | overall | salary_tier:4-5k | 596 | 0.68 | -0.24 | 0.67 | -0.18 |
| classic | overall | salary_tier:5-6k | 160 | 2.95 | -0.85 | 2.98 | -0.90 |
| classic | overall | salary_tier:6-7k | 101 | 4.97 | -0.66 | 5.04 | -0.89 |
| classic | overall | salary_tier:7-8k | 37 | 8.39 | +4.85 | 8.40 | +4.47 |
| classic | overall | salary_tier:8k+ | 18 | 15.99 | +13.25 | 14.95 | +11.79 |
| showdown | CPT | actual_ge_1 | 164 | 3.92 | -0.06 | 4.16 | +0.06 |
| showdown | CPT | position:DST | 24 | 1.04 | -1.03 | 1.50 | -1.50 |
| showdown | CPT | position:K | 27 | 1.02 | +0.92 | 0.37 | -0.33 |
| showdown | CPT | position:QB | 26 | 5.98 | +4.74 | 6.92 | +5.74 |
| showdown | CPT | position:RB | 104 | 1.55 | -0.29 | 1.60 | -0.09 |
| showdown | CPT | position:TE | 107 | 0.71 | -0.59 | 0.80 | -0.77 |
| showdown | CPT | position:WR | 189 | 1.34 | -0.14 | 1.37 | -0.04 |
| showdown | CPT | pred_chalk_ge_10 | 47 | 6.95 | +5.25 | 8.10 | +6.82 |
| showdown | CPT | salary_tier:0-2.5k | 236 | 0.09 | -0.04 | 0.09 | -0.04 |
| showdown | CPT | salary_tier:2.5-3k | 13 | 0.52 | -0.50 | 0.53 | -0.53 |
| showdown | CPT | salary_tier:3-4k | 43 | 0.56 | -0.22 | 0.62 | -0.26 |
| showdown | CPT | salary_tier:4-5k | 44 | 1.17 | -0.72 | 1.16 | -1.07 |
| showdown | CPT | salary_tier:5-6k | 30 | 1.36 | +0.14 | 1.11 | -0.80 |
| showdown | CPT | salary_tier:6-7k | 11 | 2.82 | -2.82 | 3.00 | -3.00 |
| showdown | CPT | salary_tier:7-8k | 20 | 2.36 | -1.79 | 2.38 | -2.16 |
| showdown | CPT | salary_tier:8k+ | 80 | 5.96 | +1.56 | 6.46 | +2.25 |
| showdown | FLEX | actual_ge_1 | 320 | 8.41 | -0.38 | 7.90 | -1.11 |
| showdown | FLEX | position:DST | 26 | 5.71 | -3.73 | 5.97 | -4.35 |
| showdown | FLEX | position:K | 27 | 8.03 | -1.16 | 9.78 | -9.34 |
| showdown | FLEX | position:QB | 26 | 13.87 | +9.97 | 11.49 | +7.41 |
| showdown | FLEX | position:RB | 120 | 5.27 | +1.10 | 5.42 | +1.41 |
| showdown | FLEX | position:TE | 129 | 3.59 | -1.68 | 3.69 | -0.54 |
| showdown | FLEX | position:WR | 208 | 5.14 | -0.04 | 5.22 | +0.55 |
| showdown | FLEX | pred_chalk_ge_25 | 91 | 14.35 | +12.80 | 12.38 | +10.31 |
| showdown | FLEX | salary_tier:0-2.5k | 291 | 1.63 | -1.00 | 2.13 | +0.66 |
| showdown | FLEX | salary_tier:2.5-3k | 14 | 7.11 | -6.78 | 5.02 | -3.82 |
| showdown | FLEX | salary_tier:3-4k | 45 | 5.94 | -3.37 | 6.12 | -4.22 |
| showdown | FLEX | salary_tier:4-5k | 45 | 7.84 | -6.08 | 8.98 | -8.19 |
| showdown | FLEX | salary_tier:5-6k | 30 | 8.94 | -1.82 | 9.45 | -6.61 |
| showdown | FLEX | salary_tier:6-7k | 11 | 7.01 | -6.82 | 8.50 | -8.31 |
| showdown | FLEX | salary_tier:7-8k | 20 | 8.42 | +3.63 | 7.96 | +1.84 |
| showdown | FLEX | salary_tier:8k+ | 80 | 14.79 | +11.33 | 12.81 | +8.93 |

Note: the predicted-chalk rows are selected on each version's own prediction, so their n differ by version.

### Calibration by predicted bin (summed over reference slates with complete labels, canonical, primary)

| format | slot | version | bin | n | mean predicted | mean actual |
|---|---|---|---|---:|---:|---:|
| classic | overall | v3 | 0-1 | 1431 | 0.12 | 0.21 |
| classic | overall | v3 | 1-5 | 318 | 2.44 | 3.66 |
| classic | overall | v3 | 5-10 | 106 | 6.85 | 7.31 |
| classic | overall | v3 | 10-20 | 69 | 13.53 | 12.02 |
| classic | overall | v3 | 20-35 | 24 | 27.30 | 15.80 |
| classic | overall | v3 | 35+ | 7 | 48.60 | 20.39 |
| classic | overall | v4 | 0-1 | 1431 | 0.12 | 0.20 |
| classic | overall | v4 | 1-5 | 311 | 2.44 | 3.68 |
| classic | overall | v4 | 5-10 | 116 | 6.90 | 7.49 |
| classic | overall | v4 | 10-20 | 66 | 13.74 | 11.40 |
| classic | overall | v4 | 20-35 | 24 | 26.53 | 16.24 |
| classic | overall | v4 | 35+ | 7 | 46.40 | 20.39 |
| showdown | CPT | v3 | 0-1 | 350 | 0.15 | 0.46 |
| showdown | CPT | v3 | 1-5 | 61 | 2.39 | 4.48 |
| showdown | CPT | v3 | 5-10 | 19 | 6.85 | 7.14 |
| showdown | CPT | v3 | 10-20 | 29 | 13.68 | 12.03 |
| showdown | CPT | v3 | 20-35 | 14 | 28.04 | 18.33 |
| showdown | CPT | v3 | 35+ | 4 | 44.94 | 29.17 |
| showdown | CPT | v4 | 0-1 | 361 | 0.12 | 0.47 |
| showdown | CPT | v4 | 1-5 | 53 | 2.39 | 5.22 |
| showdown | CPT | v4 | 5-10 | 19 | 7.37 | 8.41 |
| showdown | CPT | v4 | 10-20 | 23 | 14.17 | 11.50 |
| showdown | CPT | v4 | 20-35 | 16 | 27.30 | 18.11 |
| showdown | CPT | v4 | 35+ | 5 | 44.64 | 26.27 |
| showdown | FLEX | v3 | 0-1 | 248 | 0.16 | 1.36 |
| showdown | FLEX | v3 | 1-5 | 79 | 2.77 | 6.80 |
| showdown | FLEX | v3 | 5-10 | 51 | 7.04 | 12.41 |
| showdown | FLEX | v3 | 10-20 | 49 | 14.41 | 18.21 |
| showdown | FLEX | v3 | 20-35 | 36 | 26.16 | 26.52 |
| showdown | FLEX | v3 | 35-50 | 24 | 43.31 | 34.01 |
| showdown | FLEX | v3 | 50+ | 49 | 65.05 | 46.60 |
| showdown | FLEX | v4 | 0-1 | 128 | 0.19 | 1.12 |
| showdown | FLEX | v4 | 1-5 | 170 | 2.83 | 3.87 |
| showdown | FLEX | v4 | 5-10 | 89 | 7.11 | 10.07 |
| showdown | FLEX | v4 | 10-20 | 52 | 14.61 | 19.18 |
| showdown | FLEX | v4 | 20-35 | 29 | 27.10 | 28.36 |
| showdown | FLEX | v4 | 35-50 | 27 | 43.04 | 33.68 |
| showdown | FLEX | v4 | 50+ | 41 | 64.62 | 49.41 |

### Allocation by position (predicted sum vs recorded sum, canonical, primary, reference)

| wk | slate | slot | position | actual | v3 | v4 |
|---:|---|---|---|---:|---:|---:|
| 2 | classic 13 games | overall | DST | 99.9 | 100.0 | 100.0 |
| 2 | classic 13 games | overall | QB | 99.9 | 100.0 | 100.0 |
| 2 | classic 13 games | overall | RB | 250.5 | 255.0 | 230.0 |
| 2 | classic 13 games | overall | TE | 115.1 | 105.0 | 135.0 |
| 2 | classic 13 games | overall | WR | 333.3 | 340.1 | 335.0 |
| 2 | showdown NYG@LAR | CPT | DST | 1.8 | 0.4 | 0.1 |
| 2 | showdown NYG@LAR | CPT | K | 0.8 | 0.7 | 0.2 |
| 2 | showdown NYG@LAR | CPT | QB | 29.8 | 37.3 | 39.3 |
| 2 | showdown NYG@LAR | CPT | RB | 22.7 | 24.9 | 26.2 |
| 2 | showdown NYG@LAR | CPT | TE | 14.9 | 13.9 | 10.3 |
| 2 | showdown NYG@LAR | CPT | WR | 29.8 | 21.8 | 22.9 |
| 2 | showdown NYG@LAR | FLEX | DST | 17.2 | 7.1 | 7.2 |
| 2 | showdown NYG@LAR | FLEX | K | 27.7 | 10.5 | 6.2 |
| 2 | showdown NYG@LAR | FLEX | QB | 114.8 | 109.1 | 106.7 |
| 2 | showdown NYG@LAR | FLEX | RB | 117.8 | 157.0 | 155.0 |
| 2 | showdown NYG@LAR | FLEX | TE | 82.6 | 84.2 | 85.9 |
| 2 | showdown NYG@LAR | FLEX | WR | 139.4 | 132.2 | 139.1 |
| 3 | classic 13 games | overall | DST | 100.0 | 100.0 | 100.0 |
| 3 | classic 13 games | overall | QB | 99.8 | 100.0 | 100.0 |
| 3 | classic 13 games | overall | RB | 230.7 | 255.0 | 230.0 |
| 3 | classic 13 games | overall | TE | 137.7 | 105.0 | 134.9 |
| 3 | classic 13 games | overall | WR | 327.6 | 340.0 | 335.0 |
| 3 | showdown ATL@GB | CPT | DST | 3.6 | 0.4 | 0.1 |
| 3 | showdown ATL@GB | CPT | K | 1.9 | 4.8 | 1.2 |
| 3 | showdown ATL@GB | CPT | QB | 13.4 | 19.6 | 20.7 |
| 3 | showdown ATL@GB | CPT | RB | 26.9 | 33.2 | 34.9 |
| 3 | showdown ATL@GB | CPT | TE | 15.3 | 3.6 | 2.7 |
| 3 | showdown ATL@GB | CPT | WR | 38.6 | 38.2 | 40.3 |
| 3 | showdown ATL@GB | FLEX | DST | 30.6 | 14.8 | 12.4 |
| 3 | showdown ATL@GB | FLEX | K | 45.8 | 77.4 | 46.0 |
| 3 | showdown ATL@GB | FLEX | QB | 90.6 | 94.9 | 93.7 |
| 3 | showdown ATL@GB | FLEX | RB | 95.4 | 90.5 | 93.9 |
| 3 | showdown ATL@GB | FLEX | TE | 74.8 | 74.2 | 90.5 |
| 3 | showdown ATL@GB | FLEX | WR | 162.1 | 148.2 | 163.5 |
| 3 | showdown PHI@CHI | CPT | DST | 5.1 | 1.2 | 0.3 |
| 3 | showdown PHI@CHI | CPT | K | 1.1 | 2.8 | 0.7 |
| 3 | showdown PHI@CHI | CPT | QB | 23.9 | 29.0 | 30.0 |
| 3 | showdown PHI@CHI | CPT | RB | 34.3 | 38.6 | 40.0 |
| 3 | showdown PHI@CHI | CPT | TE | 5.5 | 0.8 | 0.6 |
| 3 | showdown PHI@CHI | CPT | WR | 27.1 | 27.2 | 28.1 |
| 3 | showdown PHI@CHI | FLEX | DST | 32.7 | 17.6 | 14.8 |
| 3 | showdown PHI@CHI | FLEX | K | 40.6 | 32.5 | 18.1 |
| 3 | showdown PHI@CHI | FLEX | QB | 63.3 | 80.2 | 78.3 |
| 3 | showdown PHI@CHI | FLEX | RB | 125.0 | 156.7 | 166.0 |
| 3 | showdown PHI@CHI | FLEX | TE | 51.4 | 15.3 | 23.8 |
| 3 | showdown PHI@CHI | FLEX | WR | 161.4 | 197.8 | 199.0 |
| 4 | classic 12 games | overall | DST | 100.0 | 100.0 | 100.0 |
| 4 | classic 12 games | overall | QB | 99.3 | 100.0 | 100.0 |
| 4 | classic 12 games | overall | RB | 228.5 | 244.3 | 220.4 |
| 4 | classic 12 games | overall | TE | 134.1 | 102.2 | 131.5 |
| 4 | classic 12 games | overall | WR | 332.8 | 330.6 | 325.8 |
| 4 | showdown ATL@NO | CPT | DST | 1.7 | 0.3 | 0.1 |
| 4 | showdown ATL@NO | CPT | K | 1.2 | 1.3 | 0.3 |
| 4 | showdown ATL@NO | CPT | QB | 20.8 | 31.0 | 32.0 |
| 4 | showdown ATL@NO | CPT | RB | 31.1 | 29.0 | 29.9 |
| 4 | showdown ATL@NO | CPT | TE | 13.0 | 6.4 | 4.6 |
| 4 | showdown ATL@NO | CPT | WR | 31.8 | 32.0 | 33.0 |
| 4 | showdown ATL@NO | FLEX | DST | 21.3 | 10.4 | 8.7 |
| 4 | showdown ATL@NO | FLEX | K | 36.0 | 23.5 | 12.5 |
| 4 | showdown ATL@NO | FLEX | QB | 88.0 | 114.9 | 108.8 |
| 4 | showdown ATL@NO | FLEX | RB | 114.5 | 94.9 | 96.1 |
| 4 | showdown ATL@NO | FLEX | TE | 78.7 | 82.1 | 85.3 |
| 4 | showdown ATL@NO | FLEX | WR | 160.5 | 166.9 | 184.7 |
| 4 | showdown DET@CAR | CPT | DST | 2.2 | 0.9 | 0.2 |
| 4 | showdown DET@CAR | CPT | K | 1.5 | 1.3 | 0.3 |
| 4 | showdown DET@CAR | CPT | QB | 16.0 | 27.0 | 27.8 |
| 4 | showdown DET@CAR | CPT | RB | 40.0 | 44.8 | 46.1 |
| 4 | showdown DET@CAR | CPT | TE | 12.8 | 3.6 | 2.6 |
| 4 | showdown DET@CAR | CPT | WR | 26.9 | 22.3 | 23.0 |
| 4 | showdown DET@CAR | FLEX | DST | 19.1 | 18.7 | 13.1 |
| 4 | showdown DET@CAR | FLEX | K | 40.8 | 25.0 | 13.0 |
| 4 | showdown DET@CAR | FLEX | QB | 92.9 | 163.0 | 153.3 |
| 4 | showdown DET@CAR | FLEX | RB | 86.5 | 86.8 | 82.1 |
| 4 | showdown DET@CAR | FLEX | TE | 103.0 | 56.6 | 81.4 |
| 4 | showdown DET@CAR | FLEX | WR | 154.9 | 149.9 | 157.2 |
| 4 | showdown IND@WAS | CPT | DST | 2.5 | 0.3 | 0.1 |
| 4 | showdown IND@WAS | CPT | K | 3.1 | 11.4 | 3.2 |
| 4 | showdown IND@WAS | CPT | QB | 17.3 | 19.7 | 22.1 |
| 4 | showdown IND@WAS | CPT | RB | 36.6 | 41.7 | 46.9 |
| 4 | showdown IND@WAS | CPT | TE | 12.2 | 7.6 | 6.0 |
| 4 | showdown IND@WAS | CPT | WR | 27.5 | 19.3 | 21.7 |
| 4 | showdown IND@WAS | FLEX | DST | 18.8 | 6.7 | 5.2 |
| 4 | showdown IND@WAS | FLEX | K | 49.0 | 82.3 | 48.5 |
| 4 | showdown IND@WAS | FLEX | QB | 93.9 | 121.0 | 118.8 |
| 4 | showdown IND@WAS | FLEX | RB | 98.8 | 76.0 | 79.3 |
| 4 | showdown IND@WAS | FLEX | TE | 67.6 | 60.8 | 75.5 |
| 4 | showdown IND@WAS | FLEX | WR | 167.2 | 153.3 | 172.7 |
| 4 | showdown PIT@CLE | CPT | DST | 6.0 | 4.6 | 1.6 |
| 4 | showdown PIT@CLE | CPT | K | 2.4 | 7.4 | 2.1 |
| 4 | showdown PIT@CLE | CPT | QB | 19.5 | 32.7 | 37.6 |
| 4 | showdown PIT@CLE | CPT | RB | 35.7 | 18.5 | 21.3 |
| 4 | showdown PIT@CLE | CPT | TE | 13.4 | 14.5 | 11.7 |
| 4 | showdown PIT@CLE | CPT | WR | 22.8 | 22.3 | 25.7 |
| 4 | showdown PIT@CLE | FLEX | DST | 40.0 | 31.4 | 34.1 |
| 4 | showdown PIT@CLE | FLEX | K | 50.1 | 46.6 | 26.0 |
| 4 | showdown PIT@CLE | FLEX | QB | 108.1 | 124.7 | 116.0 |
| 4 | showdown PIT@CLE | FLEX | RB | 91.0 | 87.1 | 90.5 |
| 4 | showdown PIT@CLE | FLEX | TE | 80.8 | 76.6 | 80.0 |
| 4 | showdown PIT@CLE | FLEX | WR | 129.7 | 133.7 | 153.4 |
| 5 | classic 11 games | overall | DST | 100.0 | 100.0 | 100.0 |
| 5 | classic 11 games | overall | QB | 100.0 | 100.0 | 100.0 |
| 5 | classic 11 games | overall | RB | 228.5 | 255.0 | 229.9 |
| 5 | classic 11 games | overall | TE | 137.1 | 105.0 | 135.0 |
| 5 | classic 11 games | overall | WR | 332.2 | 340.0 | 335.0 |
| 5 | showdown BUF@LAR | CPT | K | 0.2 | 0.6 | 0.1 |
| 5 | showdown BUF@LAR | CPT | QB | 35.8 | 45.4 | 46.5 |
| 5 | showdown BUF@LAR | CPT | RB | 20.8 | 18.4 | 18.9 |
| 5 | showdown BUF@LAR | CPT | TE | 7.2 | 4.1 | 2.9 |
| 5 | showdown BUF@LAR | CPT | WR | 36.0 | 30.4 | 31.1 |
| 5 | showdown BUF@LAR | FLEX | DST | 13.2 | 8.2 | 7.5 |
| 5 | showdown BUF@LAR | FLEX | K | 34.8 | 18.6 | 9.8 |
| 5 | showdown BUF@LAR | FLEX | QB | 120.2 | 139.6 | 129.6 |
| 5 | showdown BUF@LAR | FLEX | RB | 82.4 | 123.8 | 116.5 |
| 5 | showdown BUF@LAR | FLEX | TE | 84.2 | 44.5 | 62.3 |
| 5 | showdown BUF@LAR | FLEX | WR | 165.2 | 165.2 | 174.3 |
| 5 | showdown TB@DAL | CPT | DST | 4.0 | 0.2 | 0.0 |
| 5 | showdown TB@DAL | CPT | K | 3.3 | 4.0 | 1.0 |
| 5 | showdown TB@DAL | CPT | QB | 25.4 | 21.8 | 22.6 |
| 5 | showdown TB@DAL | CPT | RB | 20.1 | 18.5 | 19.2 |
| 5 | showdown TB@DAL | CPT | TE | 7.1 | 2.2 | 1.6 |
| 5 | showdown TB@DAL | CPT | WR | 40.0 | 53.3 | 55.4 |
| 5 | showdown TB@DAL | FLEX | DST | 23.2 | 8.1 | 6.0 |
| 5 | showdown TB@DAL | FLEX | K | 51.1 | 66.2 | 39.8 |
| 5 | showdown TB@DAL | FLEX | QB | 103.8 | 84.2 | 83.4 |
| 5 | showdown TB@DAL | FLEX | RB | 90.8 | 136.9 | 149.0 |
| 5 | showdown TB@DAL | FLEX | TE | 62.7 | 46.5 | 57.5 |
| 5 | showdown TB@DAL | FLEX | WR | 168.0 | 158.1 | 164.3 |
| 6 | classic 10 games | overall | DST | 100.0 | 100.0 | 100.0 |
| 6 | classic 10 games | overall | QB | 100.0 | 100.0 | 100.0 |
| 6 | classic 10 games | overall | RB | 227.1 | 246.9 | 222.7 |
| 6 | classic 10 games | overall | TE | 139.4 | 96.7 | 124.3 |
| 6 | classic 10 games | overall | WR | 328.5 | 331.9 | 327.1 |
| 6 | showdown SEA@DEN | CPT | DST | 4.9 | 4.3 | 1.4 |
| 6 | showdown SEA@DEN | CPT | K | 2.7 | 7.0 | 2.1 |
| 6 | showdown SEA@DEN | CPT | QB | 13.8 | 20.3 | 24.8 |
| 6 | showdown SEA@DEN | CPT | RB | 24.5 | 9.7 | 11.9 |
| 6 | showdown SEA@DEN | CPT | TE | 4.5 | 1.8 | 1.6 |
| 6 | showdown SEA@DEN | CPT | WR | 49.4 | 56.8 | 58.3 |
| 6 | showdown SEA@DEN | FLEX | DST | 37.9 | 47.7 | 46.3 |
| 6 | showdown SEA@DEN | FLEX | K | 58.0 | 62.6 | 35.7 |
| 6 | showdown SEA@DEN | FLEX | QB | 99.0 | 133.7 | 126.9 |
| 6 | showdown SEA@DEN | FLEX | RB | 106.6 | 98.0 | 102.5 |
| 6 | showdown SEA@DEN | FLEX | TE | 56.6 | 30.7 | 47.8 |
| 6 | showdown SEA@DEN | FLEX | WR | 141.6 | 127.3 | 140.9 |
| 6 | showdown WAS@SF | CPT | DST | 3.6 | 1.0 | 0.3 |
| 6 | showdown WAS@SF | CPT | K | 1.4 | 1.8 | 0.5 |
| 6 | showdown WAS@SF | CPT | QB | 33.9 | 54.4 | 57.4 |
| 6 | showdown WAS@SF | CPT | RB | 22.2 | 22.3 | 23.5 |
| 6 | showdown WAS@SF | CPT | TE | 18.8 | 10.6 | 7.8 |
| 6 | showdown WAS@SF | CPT | WR | 19.4 | 9.9 | 10.5 |
| 6 | showdown WAS@SF | FLEX | DST | 20.6 | 17.6 | 18.0 |
| 6 | showdown WAS@SF | FLEX | K | 39.7 | 27.6 | 15.1 |
| 6 | showdown WAS@SF | FLEX | QB | 110.0 | 135.6 | 132.6 |
| 6 | showdown WAS@SF | FLEX | RB | 130.1 | 122.6 | 129.8 |
| 6 | showdown WAS@SF | FLEX | TE | 73.0 | 79.1 | 81.0 |
| 6 | showdown WAS@SF | FLEX | WR | 123.1 | 117.5 | 123.5 |
| 7 | classic 10 games | overall | DST | 100.0 | 100.0 | 100.0 |
| 7 | classic 10 games | overall | QB | 99.8 | 100.0 | 100.0 |
| 7 | classic 10 games | overall | RB | 226.0 | 250.9 | 226.3 |
| 7 | classic 10 games | overall | TE | 140.6 | 102.8 | 132.2 |
| 7 | classic 10 games | overall | WR | 331.3 | 337.2 | 332.2 |
| 7 | showdown DAL@PHI | CPT | DST | 2.5 | 0.1 | 0.0 |
| 7 | showdown DAL@PHI | CPT | K | 1.3 | 1.6 | 0.4 |
| 7 | showdown DAL@PHI | CPT | QB | 16.4 | 27.6 | 28.7 |
| 7 | showdown DAL@PHI | CPT | RB | 24.1 | 15.7 | 16.2 |
| 7 | showdown DAL@PHI | CPT | TE | 9.6 | 8.0 | 5.8 |
| 7 | showdown DAL@PHI | CPT | WR | 45.9 | 46.7 | 48.6 |
| 7 | showdown DAL@PHI | FLEX | DST | 23.1 | 4.1 | 3.0 |
| 7 | showdown DAL@PHI | FLEX | K | 36.1 | 21.1 | 11.3 |
| 7 | showdown DAL@PHI | FLEX | QB | 98.6 | 114.0 | 108.7 |
| 7 | showdown DAL@PHI | FLEX | RB | 78.8 | 106.3 | 107.1 |
| 7 | showdown DAL@PHI | FLEX | TE | 71.2 | 66.4 | 77.4 |
| 7 | showdown DAL@PHI | FLEX | WR | 188.8 | 187.2 | 183.8 |
| 7 | showdown NE@CHI | CPT | DST | 3.4 | 1.7 | 0.5 |
| 7 | showdown NE@CHI | CPT | K | 1.4 | 2.0 | 0.5 |
| 7 | showdown NE@CHI | CPT | QB | 35.7 | 54.9 | 57.4 |
| 7 | showdown NE@CHI | CPT | RB | 21.3 | 19.1 | 19.9 |
| 7 | showdown NE@CHI | CPT | TE | 9.4 | 4.9 | 3.6 |
| 7 | showdown NE@CHI | CPT | WR | 28.1 | 17.4 | 18.1 |
| 7 | showdown NE@CHI | FLEX | DST | 18.1 | 22.2 | 21.0 |
| 7 | showdown NE@CHI | FLEX | K | 31.6 | 24.5 | 13.7 |
| 7 | showdown NE@CHI | FLEX | QB | 117.3 | 135.1 | 132.7 |
| 7 | showdown NE@CHI | FLEX | RB | 99.4 | 122.5 | 125.5 |
| 7 | showdown NE@CHI | FLEX | TE | 65.8 | 49.7 | 56.5 |
| 7 | showdown NE@CHI | FLEX | WR | 164.3 | 146.0 | 150.7 |
| 8 | classic 11 games | overall | DST | 99.9 | 100.0 | 100.0 |
| 8 | classic 11 games | overall | QB | 99.8 | 100.0 | 100.0 |
| 8 | classic 11 games | overall | RB | 237.2 | 255.0 | 230.0 |
| 8 | classic 11 games | overall | TE | 125.0 | 105.0 | 135.0 |
| 8 | classic 11 games | overall | WR | 336.8 | 340.0 | 335.0 |
| 8 | showdown CAR@GB | CPT | DST | 1.9 | 0.6 | 0.2 |
| 8 | showdown CAR@GB | CPT | K | 0.7 | 1.6 | 0.4 |
| 8 | showdown CAR@GB | CPT | QB | 17.7 | 33.2 | 34.2 |
| 8 | showdown CAR@GB | CPT | RB | 35.0 | 22.7 | 23.3 |
| 8 | showdown CAR@GB | CPT | TE | 7.0 | 3.8 | 2.7 |
| 8 | showdown CAR@GB | CPT | WR | 37.3 | 38.0 | 39.1 |
| 8 | showdown CAR@GB | FLEX | DST | 24.3 | 9.4 | 9.5 |
| 8 | showdown CAR@GB | FLEX | K | 41.1 | 17.8 | 9.8 |
| 8 | showdown CAR@GB | FLEX | QB | 108.7 | 134.0 | 122.6 |
| 8 | showdown CAR@GB | FLEX | RB | 94.1 | 111.7 | 115.1 |
| 8 | showdown CAR@GB | FLEX | TE | 69.9 | 34.3 | 54.4 |
| 8 | showdown CAR@GB | FLEX | WR | 159.1 | 192.8 | 188.7 |

Recorded sums cover gate-class labels only (a synthesized captain zero is excluded, so a stat-api captain sum can fall short of 100).

## 5. The owner's four questions (descriptive; reference contest, canonical horizon, LATEST capture)

These use the latest frozen capture at or before the canonical cutoff (for WAS@SF the owner run that reproduces the pre-grade; for week-3 Classic the run at T-28 rather than the 17-hour-old pool capture), because the questions are about what the forecast knew closest to lock. Slates whose reference labels are incomplete (weeks 4 and 6 Classic) are included on their observed labels and marked.

### Q1. Value chalk: actual >= predicted + 10 points (flex in Showdown, overall in Classic)

| wk | slate | player | pos | salary | actual | v3 | v4 | absence commit | DK status (canonical / early) | availability | ourProj − DK avg | design (v4) |
|---:|---|---|---|---:|---:|---:|---:|---|---|---|---:|---|
| 2 | classic 13 games | Bijan Robinson | RB | 8,200 | 42.6 | 22.5 | 20.3 | no | - / - | UNKNOWN | -7.3 | design |
| 2 | classic 13 games | CeeDee Lamb | WR | 7,300 | 17.5 | 7.0 | 6.9 | no | - / - | UNKNOWN | +2.3 | design |
| 2 | classic 13 games | Aaron Jones Sr. | RB | 5,100 | 20.0 | 7.2 | 6.5 | no | - / - | UNKNOWN | +9.8 | design |
| 2 | classic 13 games | George Pickens | WR | 6,300 | 16.8 | 2.9 | 2.9 | no | - / - | UNKNOWN | +10.8 | design |
| 2 | classic 13 games | Emeka Egbuka | WR | 6,400 | 14.4 | 1.6 | 1.6 | no | - / - | UNKNOWN | -1.3 | design |
| 2 | classic 13 games | Matthew Golden | WR | 4,700 | 14.8 | 2.5 | 2.4 | no | - / - | UNKNOWN | -8.6 | design |
| 2 | classic 13 games | Mark Andrews | TE | 4,400 | 11.3 | 1.2 | 1.6 | no | - / - | UNKNOWN | -0.3 | design |
| 2 | classic 13 games | Buccaneers | DST | 3,600 | 13.9 | 2.4 | 2.4 | no | - / - | UNKNOWN | -3.5 | design |
| 2 | classic 13 games | Dalton Schultz | TE | 3,200 | 18.4 | 6.6 | 8.5 | no | - / - | UNKNOWN | +8.1 | design |
| 2 | showdown NYG@LAR | Harrison Mevis | K | 5,000 | 16.9 | 4.4 | 2.6 | no | - / - | ACTIVE | +7.3 | design |
| 2 | showdown NYG@LAR | Blake Corum | RB | 4,600 | 25.5 | 8.3 | 8.2 | no | - / - | ACTIVE | +2.4 | design |
| 2 | showdown NYG@LAR | Colby Parkinson | TE | 3,600 | 21.2 | 7.8 | 7.7 | no | - / - | ACTIVE | +7.9 | design |
| 2 | showdown NYG@LAR | Terrance Ferguson | TE | 1,800 | 18.1 | 1.5 | 2.8 | no | - / - | ACTIVE | +5.3 | design |
| 2 | showdown NYG@LAR | Davante Adams | WR | 8,800 | 47.5 | 26.0 | 25.4 | no | - / - | ACTIVE | +11.1 | design |
| 2 | showdown NYG@LAR | Malachi Fields | WR | 5,600 | 13.2 | 2.5 | 2.5 | no | - / - | ACTIVE | -0.1 | design |
| 2 | showdown NYG@LAR | Konata Mumpfield | WR | 1,000 | 11.3 | 0.2 | 1.2 | no | - / - | ACTIVE | +2.3 | design |
| 3 | showdown ATL@GB | Packers | DST | 5,000 | 18.0 | 3.6 | 4.7 | no | - / - | UNKNOWN | +0.6 | design |
| 3 | showdown ATL@GB | Michael Penix Jr. | QB | 9,000 | 30.2 | 18.7 | 18.6 | no | - / - | ACTIVE | +12.1 | design |
| 3 | showdown ATL@GB | Kaleb Johnson | RB | 4,000 | 19.4 | 0.4 | 0.4 | no | - / - | ACTIVE | -0.5 | design |
| 3 | showdown ATL@GB | Jahan Dotson | WR | 3,000 | 17.9 | 3.8 | 3.8 | no | - / - | ACTIVE | +0.2 | design |
| 3 | showdown ATL@GB | Skyy Moore | WR | 2,400 | 15.2 | 0.4 | 3.8 | no | - / - | ACTIVE | -0.1 | design |
| 3 | classic 13 games | Kenneth Walker III | RB | 7,400 | 44.4 | 32.4 | 29.2 | no | - / - | EXPECTED_ACTIVE | -10.8 | design |
| 3 | classic 13 games | Garrett Wilson | WR | 6,300 | 27.9 | 5.1 | 5.1 | no | - / - | EXPECTED_ACTIVE | -2.3 | design |
| 3 | classic 13 games | Ladd McConkey | WR | 5,900 | 13.0 | 2.8 | 2.8 | no | - / - | EXPECTED_ACTIVE | -2.5 | design |
| 3 | classic 13 games | Jaylen Warren | RB | 5,700 | 13.1 | 1.7 | 1.5 | no | Q / Q | QUESTIONABLE | +1.5 | design |
| 3 | classic 13 games | Rashod Bateman | WR | 4,700 | 15.5 | 1.8 | 1.8 | no | - / - | EXPECTED_ACTIVE | -3.0 | design |
| 3 | classic 13 games | Devaughn Vele | WR | 4,400 | 16.9 | 6.8 | 6.7 | no | - / - | EXPECTED_ACTIVE | -4.6 | design |
| 3 | classic 13 games | Dalton Schultz | TE | 4,200 | 27.0 | 11.1 | 14.3 | no | - / - | EXPECTED_ACTIVE | -4.9 | design |
| 3 | classic 13 games | Titans | DST | 2,400 | 26.9 | 0.3 | 0.3 | no | - / - | EXPECTED_ACTIVE | +0.5 | design |
| 3 | showdown PHI@CHI | Eagles | DST | 5,200 | 22.4 | 5.9 | 7.1 | no | - / - | EXPECTED_ACTIVE | +3.5 | design |
| 3 | showdown PHI@CHI | Jake Elliott | K | 4,800 | 21.4 | 13.3 | 7.5 | no | - / - | EXPECTED_ACTIVE | -0.0 | design |
| 3 | showdown PHI@CHI | Saquon Barkley | RB | 10,000 | 41.8 | 21.4 | 19.9 | no | - / - | EXPECTED_ACTIVE | +6.3 | design |
| 3 | showdown PHI@CHI | Colston Loveland | TE | 7,000 | 18.9 | 5.8 | 5.4 | no | - / - | EXPECTED_ACTIVE | +7.8 | design |
| 3 | showdown PHI@CHI | Zach Ertz | TE | 2,600 | 15.0 | 0.0 | 0.0 | no | - / - | UNKNOWN | - | design |
| 3 | showdown PHI@CHI | Makai Lemon | WR | 4,200 | 13.9 | 0.8 | 0.7 | no | - / - | EXPECTED_ACTIVE | -0.1 | design |
| 4 | showdown PIT@CLE | Browns | DST | 4,800 | 16.1 | 6.9 | 4.5 | no | - / - | EXPECTED_ACTIVE | +2.1 | design |
| 4 | showdown PIT@CLE | Chris Boswell | K | 5,200 | 29.2 | 24.8 | 13.8 | no | - / - | EXPECTED_ACTIVE | -0.1 | design |
| 4 | showdown PIT@CLE | Quinshon Judkins | RB | 8,800 | 30.7 | 20.1 | 18.7 | no | - / - | EXPECTED_ACTIVE | +0.4 | design |
| 4 | showdown PIT@CLE | KC Concepcion Jr. | WR | 6,400 | 27.6 | 9.0 | 8.3 | no | - / - | EXPECTED_ACTIVE | -0.7 | design |
| 4 | showdown IND@WAS | Colts | DST | 4,000 | 11.3 | 1.0 | 1.2 | no | - / - | EXPECTED_ACTIVE | +0.8 | design |
| 4 | showdown IND@WAS | Jacory Croskey-Merritt | RB | 6,800 | 35.2 | 19.9 | 19.6 | no | - / - | EXPECTED_ACTIVE | +0.5 | design |
| 4 | showdown IND@WAS | Chig Okonkwo | TE | 4,400 | 19.4 | 9.5 | 9.3 | no | - / - | EXPECTED_ACTIVE | +3.3 | design |
| 4 | showdown IND@WAS | Josh Downs | WR | 8,800 | 44.2 | 33.6 | 33.0 | no | - / - | EXPECTED_ACTIVE | -0.4 | design |
| 4 | showdown IND@WAS | Laquon Treadwell | WR | 3,200 | 18.2 | 2.2 | 2.2 | no | - / - | EXPECTED_ACTIVE | -1.8 | design |
| 4 | classic 12 games | Cardinals | DST | 2,500 | 12.0 | 0.1 | 0.1 | no | - / - | EXPECTED_ACTIVE | -1.2 | design |
| 4 | classic 12 games | Aaron Jones Sr. | RB | 6,000 | 26.8 | 5.1 | 4.6 | no | - / - | EXPECTED_ACTIVE | -0.2 | design |
| 4 | classic 12 games | Jeremiyah Love | RB | 5,800 | 29.3 | 8.7 | 7.8 | no | - / - | EXPECTED_ACTIVE | +0.4 | design |
| 4 | classic 12 games | Braelon Allen | RB | 5,200 | 14.6 | 0.2 | 0.2 | no | - / - | EXPECTED_ACTIVE | -1.3 | design |
| 4 | classic 12 games | T.J. Hockenson | TE | 3,300 | 15.9 | 1.3 | 1.6 | no | - / - | EXPECTED_ACTIVE | -0.1 | design |
| 4 | classic 12 games | Brenton Strange | TE | 3,200 | 13.3 | 1.8 | 2.2 | no | - / - | EXPECTED_ACTIVE | +2.2 | design |
| 4 | classic 12 games | Parker Washington | WR | 6,500 | 24.5 | 10.2 | 10.0 | no | - / - | EXPECTED_ACTIVE | +0.2 | design |
| 4 | classic 12 games | Jordan Addison | WR | 5,300 | 23.1 | 1.4 | 1.4 | no | - / - | EXPECTED_ACTIVE | +1.0 | design |
| 4 | classic 12 games | Dontayvion Wicks | WR | 4,300 | 20.2 | 2.1 | 2.0 | no | - / - | EXPECTED_ACTIVE | -3.7 | design |
| 4 | showdown DET@CAR | Jake Bates | K | 5,000 | 20.8 | 9.5 | 4.9 | no | - / - | EXPECTED_ACTIVE | +1.3 | design |
| 4 | showdown DET@CAR | Ryan Fitzgerald | K | 4,800 | 20.0 | 15.5 | 8.1 | no | - / - | EXPECTED_ACTIVE | -2.6 | design |
| 4 | showdown DET@CAR | Darren Waller | TE | 4,400 | 46.0 | 21.5 | 18.6 | no | - / - | EXPECTED_ACTIVE | -0.9 | design |
| 4 | showdown DET@CAR | Tommy Tremble | TE | 2,400 | 13.8 | 3.8 | 10.9 | no | - / - | EXPECTED_ACTIVE | +0.3 | design |
| 4 | showdown DET@CAR | Tetairoa McMillan | WR | 9,000 | 29.9 | 18.9 | 16.3 | no | - / - | EXPECTED_ACTIVE | +0.8 | design |
| 4 | showdown DET@CAR | John Metchie III | WR | 3,200 | 17.0 | 4.8 | 4.2 | no | - / - | EXPECTED_ACTIVE | +0.8 | design |
| 4 | showdown DET@CAR | Brycen Tremayne | WR | 2,600 | 34.5 | 1.6 | 7.9 | no | - / - | EXPECTED_ACTIVE | -1.0 | design |
| 4 | showdown ATL@NO | Daniel Carlson | K | 4,800 | 19.4 | 11.0 | 5.9 | no | - / - | EXPECTED_ACTIVE | -0.2 | design |
| 4 | showdown ATL@NO | Alvin Kamara | RB | 7,000 | 20.9 | 10.9 | 9.7 | no | - / - | EXPECTED_ACTIVE | +2.5 | design |
| 4 | showdown ATL@NO | Brian Robinson Jr. | RB | 3,800 | 23.1 | 10.0 | 8.9 | no | - / - | EXPECTED_ACTIVE | -1.7 | design |
| 4 | showdown ATL@NO | Kyle Pitts Sr. | TE | 5,800 | 17.8 | 5.0 | 4.5 | no | - / - | EXPECTED_ACTIVE | +6.9 | design |
| 4 | showdown ATL@NO | Oscar Delp | TE | 400 | 13.8 | 0.1 | 1.8 | no | - / - | EXPECTED_ACTIVE | - | design |
| 4 | showdown ATL@NO | Jahan Dotson | WR | 3,000 | 15.6 | 1.4 | 1.2 | no | - / - | EXPECTED_ACTIVE | +0.3 | design |
| 4 | showdown ATL@NO | Bryce Lance | WR | 1,800 | 23.9 | 1.4 | 5.8 | no | - / - | EXPECTED_ACTIVE | -0.2 | design |
| 5 | showdown TB@DAL | Cowboys | DST | 4,800 | 17.6 | 0.5 | 0.7 | no | - / - | EXPECTED_ACTIVE | +0.7 | design |
| 5 | showdown TB@DAL | Brandon Aubrey | K | 5,400 | 33.3 | 36.5 | 22.0 | no | - / - | EXPECTED_ACTIVE | +0.4 | design |
| 5 | showdown TB@DAL | Jalon Daniels | QB | 8,600 | 41.1 | 10.6 | 10.6 | no | - / - | EXPECTED_ACTIVE | +0.6 | design |
| 5 | showdown TB@DAL | Cade Otton | TE | 4,400 | 32.2 | 20.3 | 20.3 | no | - / - | EXPECTED_ACTIVE | +0.5 | design |
| 5 | classic 11 games | Jacoby Brissett | QB | 5,500 | 28.6 | 4.6 | 4.6 | no | - / - | EXPECTED_ACTIVE | +1.7 | design |
| 5 | classic 11 games | D'Andre Swift | RB | 6,200 | 41.4 | 15.6 | 13.9 | no | - / - | EXPECTED_ACTIVE | -2.2 | design |
| 5 | classic 11 games | Braelon Allen | RB | 5,400 | 15.2 | 1.3 | 1.2 | yes | - / - | EXPECTED_ACTIVE | +1.4 | design |
| 5 | classic 11 games | Emanuel Wilson | RB | 5,300 | 21.6 | 3.5 | 3.1 | no | - / - | EXPECTED_ACTIVE | +0.2 | design |
| 5 | classic 11 games | Trey McBride | TE | 6,700 | 36.6 | 10.2 | 13.2 | no | - / - | EXPECTED_ACTIVE | +0.3 | design |
| 5 | classic 11 games | Michael Wilson | WR | 6,200 | 45.6 | 11.3 | 11.2 | no | - / - | EXPECTED_ACTIVE | +2.4 | design |
| 5 | classic 11 games | Tre' Harris | WR | 4,100 | 18.3 | 1.5 | 1.5 | no | - / - | EXPECTED_ACTIVE | -1.8 | design |
| 5 | classic 11 games | Roman Wilson | WR | 3,700 | 18.6 | 4.2 | 4.2 | no | - / - | EXPECTED_ACTIVE | -2.9 | design |
| 5 | showdown BUF@LAR | Harrison Mevis | K | 5,000 | 20.4 | 9.4 | 4.9 | no | - / - | EXPECTED_ACTIVE | +1.6 | design |
| 5 | showdown BUF@LAR | Blake Corum | RB | 3,800 | 18.4 | 4.8 | 4.3 | no | - / - | EXPECTED_ACTIVE | +0.3 | design |
| 5 | showdown BUF@LAR | Colby Parkinson | TE | 2,400 | 22.8 | 2.4 | 9.1 | no | - / - | EXPECTED_ACTIVE | +4.2 | design |
| 5 | showdown BUF@LAR | Skyler Bell | WR | 200 | 38.2 | 0.0 | 0.0 | no | - / - | EXPECTED_ACTIVE | - | design |
| 6 | showdown SEA@DEN | Wil Lutz | K | 5,400 | 24.2 | 17.0 | 9.7 | no | - / - | EXPECTED_ACTIVE | +1.1 | not_design |
| 6 | showdown SEA@DEN | AJ Barner | TE | 4,200 | 26.3 | 12.7 | 12.0 | no | - / - | EXPECTED_ACTIVE | +0.7 | not_design |
| 6 | showdown SEA@DEN | Evan Engram | TE | 4,000 | 23.6 | 11.0 | 10.5 | no | - / - | EXPECTED_ACTIVE | +0.8 | not_design |
| 6 | classic 10 games | Panthers | DST | 2,500 | 31.8 | 11.1 | 11.1 | no | - / - | EXPECTED_ACTIVE | -1.1 | not_design |
| 6 | classic 10 games | Tyler Allgeier | RB | 4,800 | 12.4 | 1.0 | 0.9 | yes | - / - | EXPECTED_ACTIVE | +0.5 | not_design |
| 6 | classic 10 games | Tyler Higbee | TE | 3,400 | 14.9 | 3.0 | 3.9 | no | - / - | EXPECTED_ACTIVE | -0.5 | not_design |
| 6 | classic 10 games | Isaiah Likely | TE | 4,800 | 35.1 | 12.8 | 16.4 | no | - / - | EXPECTED_ACTIVE | -1.7 | not_design |
| 6 | classic 10 games | Xavier Worthy | WR | 4,600 | 21.3 | 6.4 | 6.3 | no | - / - | EXPECTED_ACTIVE | +5.2 | not_design |
| 6 | classic 10 games | Germie Bernard | WR | 4,000 | 21.2 | 3.7 | 3.7 | no | - / - | EXPECTED_ACTIVE | +6.2 | not_design |
| 6 | showdown WAS@SF | Eddy Pineiro | K | 5,200 | 23.4 | 12.3 | 6.7 | no | - / - | EXPECTED_ACTIVE | +0.3 | not_design |
| 6 | showdown WAS@SF | Rachaad White | RB | 6,600 | 34.6 | 25.5 | 23.2 | no | - / - | EXPECTED_ACTIVE | +3.9 | not_design |
| 6 | showdown WAS@SF | Austin Ekeler | RB | 3,600 | 19.4 | 5.4 | 4.9 | no | - / - | EXPECTED_ACTIVE | +2.3 | not_design |
| 6 | showdown WAS@SF | Chig Okonkwo | TE | 4,600 | 19.6 | 8.2 | 7.5 | no | - / - | EXPECTED_ACTIVE | +1.5 | not_design |
| 6 | showdown WAS@SF | Brandin Cooks | WR | 4,000 | 19.3 | 6.2 | 5.7 | no | - / - | EXPECTED_ACTIVE | +2.7 | not_design |
| 7 | showdown NE@CHI | Cairo Santos | K | 5,000 | 19.0 | 12.3 | 6.9 | no | - / - | EXPECTED_ACTIVE | -0.6 | not_design |
| 7 | showdown NE@CHI | Roschon Johnson | RB | 4,400 | 16.8 | 7.1 | 6.6 | no | - / - | EXPECTED_ACTIVE | +2.3 | not_design |
| 7 | showdown NE@CHI | Kyle Williams | WR | 1,800 | 15.0 | 4.0 | 6.8 | no | - / - | EXPECTED_ACTIVE | -0.2 | not_design |
| 7 | classic 10 games | Jordan Love | QB | 6,000 | 28.3 | 9.1 | 9.1 | no | - / - | EXPECTED_ACTIVE | +1.5 | not_design |
| 7 | classic 10 games | Christian McCaffrey | RB | 7,600 | 38.3 | 28.0 | 25.3 | no | - / - | EXPECTED_ACTIVE | +3.8 | not_design |
| 7 | classic 10 games | Woody Marks | RB | 5,600 | 23.2 | 6.5 | 5.9 | no | - / - | EXPECTED_ACTIVE | +7.1 | not_design |
| 7 | classic 10 games | Braelon Allen | RB | 5,900 | 19.5 | 7.7 | 7.0 | no | - / - | EXPECTED_ACTIVE | +6.9 | not_design |
| 7 | classic 10 games | Tyler Higbee | TE | 3,500 | 18.2 | 4.5 | 5.8 | yes | - / - | EXPECTED_ACTIVE | +2.2 | not_design |
| 7 | classic 10 games | George Kittle | TE | 5,700 | 26.0 | 13.8 | 17.7 | no | - / - | EXPECTED_ACTIVE | +3.7 | not_design |
| 7 | classic 10 games | Cade Otton | TE | 3,100 | 13.4 | 3.2 | 4.1 | no | - / - | EXPECTED_ACTIVE | +4.0 | not_design |
| 7 | classic 10 games | Tucker Kraft | TE | 4,700 | 20.5 | 5.3 | 6.8 | no | - / - | EXPECTED_ACTIVE | +4.4 | not_design |
| 7 | classic 10 games | Matthew Golden | WR | 6,000 | 39.4 | 20.9 | 20.6 | no | - / - | EXPECTED_ACTIVE | +0.2 | not_design |
| 7 | classic 10 games | Ted Hurst III | WR | 3,000 | 19.1 | 2.7 | 2.6 | no | - / - | EXPECTED_ACTIVE | +3.3 | not_design |
| 7 | showdown DAL@PHI | Cowboys | DST | 4,000 | 14.6 | 0.8 | 0.9 | no | - / - | EXPECTED_ACTIVE | +0.6 | not_design |
| 7 | showdown DAL@PHI | Brandon Aubrey | K | 5,400 | 21.2 | 14.8 | 7.9 | no | - / - | EXPECTED_ACTIVE | +0.4 | not_design |
| 7 | showdown DAL@PHI | Jake Elliott | K | 4,800 | 14.9 | 6.3 | 3.4 | no | - / - | EXPECTED_ACTIVE | +0.1 | not_design |
| 7 | showdown DAL@PHI | Makai Lemon | WR | 3,800 | 36.0 | 20.9 | 18.6 | no | - / - | EXPECTED_ACTIVE | +6.0 | not_design |
| 7 | showdown DAL@PHI | Darius Cooper | WR | 3,200 | 36.5 | 25.5 | 22.7 | no | - / - | EXPECTED_ACTIVE | +2.1 | not_design |
| 8 | showdown CAR@GB | Packers | DST | 4,000 | 18.2 | 6.3 | 7.5 | no | - / - | EXPECTED_ACTIVE | +0.2 | not_design |
| 8 | showdown CAR@GB | Trey Smack | K | 5,000 | 21.5 | 8.3 | 4.5 | no | - / - | EXPECTED_ACTIVE | -0.0 | not_design |
| 8 | showdown CAR@GB | Ryan Fitzgerald | K | 4,800 | 19.6 | 9.6 | 5.2 | no | - / - | EXPECTED_ACTIVE | -1.4 | not_design |
| 8 | showdown CAR@GB | Tommy Tremble | TE | 2,600 | 21.7 | 5.9 | 14.0 | no | - / - | EXPECTED_ACTIVE | -1.5 | not_design |
| 8 | showdown CAR@GB | Savion Williams | WR | 4,400 | 25.4 | 8.5 | 7.8 | no | - / - | EXPECTED_ACTIVE | -1.1 | not_design |
| 8 | classic 11 games | Broncos | DST | 2,300 | 19.6 | 9.2 | 9.2 | no | - / - | EXPECTED_ACTIVE | +0.3 | not_design |
| 8 | classic 11 games | Rhamondre Stevenson | RB | 5,600 | 34.4 | 10.1 | 9.1 | no | - / - | EXPECTED_ACTIVE | +4.4 | not_design |
| 8 | classic 11 games | Bhayshul Tuten | RB | 5,900 | 23.4 | 9.6 | 8.6 | no | - / - | EXPECTED_ACTIVE | +1.1 | not_design |
| 8 | classic 11 games | Jonah Coleman | RB | 4,300 | 15.0 | 4.8 | 4.3 | no | - / - | EXPECTED_ACTIVE | +5.1 | not_design |
| 8 | classic 11 games | Hunter Henry | TE | 4,300 | 21.1 | 5.6 | 7.2 | no | - / - | EXPECTED_ACTIVE | +3.0 | not_design |
| 8 | classic 11 games | Omar Cooper Jr. | WR | 4,800 | 21.6 | 9.2 | 9.1 | no | - / - | EXPECTED_ACTIVE | -8.0 | not_design |

126 player-slates by position: DST 12, K 15, QB 4, RB 28, TE 28, WR 39. With an absence commit 3; DK status changed between early and canonical 0; ourProj at least 2 above DK average 40.

Shape count over 126 player-slates: cheap (< $5,000 Classic / < $4,500 Showdown) AND a news/role marker (absence commit, ourProj at least 2 above DK average, or DK status changed between early and canonical) 21; cheap without a marker 42; not cheap with a marker 21; neither 42. The marker is a crude proxy: role growth that the projection did not absorb (e.g. Cooks/Ekeler on WAS@SF, ourProj close to DK average) does not show in it.

### Q2. Showdown captain share by position (sum of captain %, gate-class labels only)

| wk | slate | position | actual | v3 | v4 | tag (v4) |
|---:|---|---|---:|---:|---:|---|
| 2 | showdown NYG@LAR | DST | 1.8 | 0.4 | 0.1 | design |
| 2 | showdown NYG@LAR | K | 0.8 | 0.7 | 0.2 | design |
| 2 | showdown NYG@LAR | QB | 29.8 | 37.3 | 39.3 | design |
| 2 | showdown NYG@LAR | RB | 22.7 | 24.9 | 26.2 | design |
| 2 | showdown NYG@LAR | TE | 14.9 | 13.9 | 10.3 | design |
| 2 | showdown NYG@LAR | WR | 29.8 | 21.8 | 22.9 | design |
| 3 | showdown ATL@GB | DST | 3.6 | 0.4 | 0.1 | design |
| 3 | showdown ATL@GB | K | 1.9 | 4.8 | 1.2 | design |
| 3 | showdown ATL@GB | QB | 13.4 | 19.6 | 20.7 | design |
| 3 | showdown ATL@GB | RB | 26.9 | 33.2 | 34.9 | design |
| 3 | showdown ATL@GB | TE | 15.3 | 3.6 | 2.7 | design |
| 3 | showdown ATL@GB | WR | 38.6 | 38.2 | 40.3 | design |
| 3 | showdown PHI@CHI | DST | 5.1 | 1.2 | 0.3 | design |
| 3 | showdown PHI@CHI | K | 1.1 | 2.8 | 0.7 | design |
| 3 | showdown PHI@CHI | QB | 23.9 | 29.0 | 30.0 | design |
| 3 | showdown PHI@CHI | RB | 34.3 | 38.6 | 40.0 | design |
| 3 | showdown PHI@CHI | TE | 5.5 | 0.8 | 0.6 | design |
| 3 | showdown PHI@CHI | WR | 27.1 | 27.2 | 28.1 | design |
| 4 | showdown ATL@NO | DST | 1.7 | 0.3 | 0.1 | design |
| 4 | showdown ATL@NO | K | 1.2 | 1.3 | 0.3 | design |
| 4 | showdown ATL@NO | QB | 20.8 | 31.0 | 32.0 | design |
| 4 | showdown ATL@NO | RB | 31.1 | 29.0 | 29.9 | design |
| 4 | showdown ATL@NO | TE | 13.0 | 6.4 | 4.6 | design |
| 4 | showdown ATL@NO | WR | 31.8 | 32.0 | 33.0 | design |
| 4 | showdown DET@CAR | DST | 2.2 | 0.9 | 0.2 | design |
| 4 | showdown DET@CAR | K | 1.5 | 1.3 | 0.3 | design |
| 4 | showdown DET@CAR | QB | 16.0 | 27.0 | 27.8 | design |
| 4 | showdown DET@CAR | RB | 40.0 | 44.8 | 46.1 | design |
| 4 | showdown DET@CAR | TE | 12.8 | 3.6 | 2.6 | design |
| 4 | showdown DET@CAR | WR | 26.9 | 22.3 | 23.0 | design |
| 4 | showdown IND@WAS | DST | 2.5 | 0.3 | 0.1 | design |
| 4 | showdown IND@WAS | K | 3.1 | 11.4 | 3.2 | design |
| 4 | showdown IND@WAS | QB | 17.3 | 19.7 | 22.1 | design |
| 4 | showdown IND@WAS | RB | 36.6 | 41.7 | 46.9 | design |
| 4 | showdown IND@WAS | TE | 12.2 | 7.6 | 6.0 | design |
| 4 | showdown IND@WAS | WR | 27.5 | 19.3 | 21.7 | design |
| 4 | showdown PIT@CLE | DST | 6.0 | 4.6 | 1.6 | design |
| 4 | showdown PIT@CLE | K | 2.4 | 7.4 | 2.1 | design |
| 4 | showdown PIT@CLE | QB | 19.5 | 32.7 | 37.6 | design |
| 4 | showdown PIT@CLE | RB | 35.7 | 18.5 | 21.3 | design |
| 4 | showdown PIT@CLE | TE | 13.4 | 14.5 | 11.6 | design |
| 4 | showdown PIT@CLE | WR | 22.8 | 22.3 | 25.7 | design |
| 5 | showdown BUF@LAR | K | 0.2 | 0.6 | 0.1 | design |
| 5 | showdown BUF@LAR | QB | 35.8 | 45.4 | 46.5 | design |
| 5 | showdown BUF@LAR | RB | 20.8 | 18.4 | 18.9 | design |
| 5 | showdown BUF@LAR | TE | 7.2 | 4.1 | 2.9 | design |
| 5 | showdown BUF@LAR | WR | 36.0 | 30.4 | 31.1 | design |
| 5 | showdown TB@DAL | DST | 4.0 | 0.2 | 0.0 | design |
| 5 | showdown TB@DAL | K | 3.3 | 4.0 | 1.0 | design |
| 5 | showdown TB@DAL | QB | 25.4 | 21.8 | 22.6 | design |
| 5 | showdown TB@DAL | RB | 20.1 | 18.5 | 19.3 | design |
| 5 | showdown TB@DAL | TE | 7.1 | 2.2 | 1.6 | design |
| 5 | showdown TB@DAL | WR | 40.0 | 53.3 | 55.4 | design |
| 6 | showdown SEA@DEN | DST | 4.9 | 4.3 | 1.4 | not_design |
| 6 | showdown SEA@DEN | K | 2.7 | 7.0 | 2.1 | not_design |
| 6 | showdown SEA@DEN | QB | 13.8 | 20.3 | 24.8 | not_design |
| 6 | showdown SEA@DEN | RB | 24.5 | 9.7 | 11.9 | not_design |
| 6 | showdown SEA@DEN | TE | 4.5 | 1.8 | 1.6 | not_design |
| 6 | showdown SEA@DEN | WR | 49.4 | 56.8 | 58.3 | not_design |
| 6 | showdown WAS@SF | DST | 3.6 | 1.0 | 0.3 | not_design |
| 6 | showdown WAS@SF | K | 1.4 | 1.8 | 0.5 | not_design |
| 6 | showdown WAS@SF | QB | 33.9 | 54.4 | 57.4 | not_design |
| 6 | showdown WAS@SF | RB | 22.2 | 22.3 | 23.5 | not_design |
| 6 | showdown WAS@SF | TE | 18.8 | 10.6 | 7.8 | not_design |
| 6 | showdown WAS@SF | WR | 19.4 | 10.0 | 10.5 | not_design |
| 7 | showdown DAL@PHI | DST | 2.5 | 0.1 | 0.0 | not_design |
| 7 | showdown DAL@PHI | K | 1.3 | 1.6 | 0.4 | not_design |
| 7 | showdown DAL@PHI | QB | 16.4 | 27.6 | 28.7 | not_design |
| 7 | showdown DAL@PHI | RB | 24.1 | 15.7 | 16.2 | not_design |
| 7 | showdown DAL@PHI | TE | 9.6 | 8.0 | 5.8 | not_design |
| 7 | showdown DAL@PHI | WR | 45.9 | 46.7 | 48.6 | not_design |
| 7 | showdown NE@CHI | DST | 3.4 | 1.7 | 0.5 | not_design |
| 7 | showdown NE@CHI | K | 1.4 | 2.0 | 0.5 | not_design |
| 7 | showdown NE@CHI | QB | 35.7 | 54.9 | 57.4 | not_design |
| 7 | showdown NE@CHI | RB | 21.3 | 19.1 | 19.9 | not_design |
| 7 | showdown NE@CHI | TE | 9.4 | 4.9 | 3.6 | not_design |
| 7 | showdown NE@CHI | WR | 28.1 | 17.4 | 18.2 | not_design |
| 8 | showdown CAR@GB | DST | 1.9 | 0.6 | 0.2 | not_design |
| 8 | showdown CAR@GB | K | 0.7 | 1.6 | 0.4 | not_design |
| 8 | showdown CAR@GB | QB | 17.7 | 33.2 | 34.2 | not_design |
| 8 | showdown CAR@GB | RB | 35.0 | 22.7 | 23.3 | not_design |
| 8 | showdown CAR@GB | TE | 7.0 | 3.8 | 2.7 | not_design |
| 8 | showdown CAR@GB | WR | 37.3 | 38.0 | 39.1 | not_design |

Totals over 14 Showdown slates (sum of captain % across slates; divide by slates for a per-slate share):

| position | scope | actual | v3 | v4 |
|---|---|---:|---:|---:|
| DST | all | 43.2 | 16.1 | 4.8 |
| DST | week6 | 16.2 | 7.8 | 2.3 |
| K | all | 23.1 | 48.2 | 13.3 |
| K | week6 | 7.5 | 14.0 | 4.0 |
| QB | all | 319.4 | 454.0 | 481.0 |
| QB | week6 | 117.4 | 190.5 | 202.4 |
| RB | all | 395.4 | 356.9 | 378.2 |
| RB | week6 | 127.1 | 89.4 | 94.9 |
| TE | all | 150.8 | 85.9 | 64.4 |
| TE | week6 | 49.4 | 29.1 | 21.5 |
| WR | all | 460.7 | 435.7 | 455.9 |
| WR | week6 | 180.2 | 168.8 | 174.6 |

### Q3. Showdown kickers, flex ownership

| wk | slate | kicker | salary | actual | v3 | v4 | tag (v4) |
|---:|---|---|---:|---:|---:|---:|---|
| 2 | showdown NYG@LAR | Harrison Mevis | 5,000 | 16.9 | 4.4 | 2.6 | design |
| 2 | showdown NYG@LAR | Dominic Zvada | 4,800 | 10.8 | 6.1 | 3.6 | design |
| 3 | showdown ATL@GB | Trey Smack | 5,200 | 27.5 | 54.4 | 32.3 | design |
| 3 | showdown ATL@GB | Lenny Krieg | 5,200 | 0.0 | 0.0 | 0.0 | design |
| 3 | showdown ATL@GB | Nick Folk | 4,800 | 18.4 | 23.0 | 13.7 | design |
| 3 | showdown PHI@CHI | Jake Elliott | 4,800 | 21.4 | 13.3 | 7.5 | design |
| 3 | showdown PHI@CHI | Cairo Santos | 4,600 | 19.2 | 19.1 | 10.7 | design |
| 4 | showdown PIT@CLE | Chris Boswell | 5,200 | 29.2 | 24.8 | 13.8 | design |
| 4 | showdown PIT@CLE | Andre Szmyt | 4,600 | 20.8 | 21.9 | 12.2 | design |
| 4 | showdown IND@WAS | Spencer Shrader | 5,000 | 30.9 | 54.2 | 31.9 | design |
| 4 | showdown IND@WAS | Drew Stevens | 4,800 | 18.1 | 28.1 | 16.5 | design |
| 4 | showdown DET@CAR | Jake Bates | 5,000 | 20.8 | 9.5 | 4.9 | design |
| 4 | showdown DET@CAR | Ryan Fitzgerald | 4,800 | 20.0 | 15.5 | 8.1 | design |
| 4 | showdown ATL@NO | Nick Folk | 5,400 | 16.6 | 12.4 | 6.6 | design |
| 4 | showdown ATL@NO | Daniel Carlson | 4,800 | 19.4 | 11.0 | 5.9 | design |
| 5 | showdown TB@DAL | Brandon Aubrey | 5,400 | 33.3 | 36.5 | 22.0 | design |
| 5 | showdown TB@DAL | Chase McLaughlin | 5,000 | 17.8 | 29.7 | 17.8 | design |
| 5 | showdown BUF@LAR | Harrison Mevis | 5,000 | 20.4 | 9.4 | 4.9 | design |
| 5 | showdown BUF@LAR | Tyler Bass | 4,800 | 14.4 | 9.2 | 4.8 | design |
| 6 | showdown SEA@DEN | Wil Lutz | 5,400 | 24.2 | 17.0 | 9.7 | not_design |
| 6 | showdown SEA@DEN | Jason Myers | 5,000 | 33.8 | 45.6 | 26.0 | not_design |
| 6 | showdown WAS@SF | Eddy Pineiro | 5,200 | 23.4 | 12.3 | 6.7 | not_design |
| 6 | showdown WAS@SF | Drew Stevens | 5,000 | 16.3 | 15.3 | 8.4 | not_design |
| 7 | showdown NE@CHI | Cairo Santos | 5,000 | 19.0 | 12.3 | 6.9 | not_design |
| 7 | showdown NE@CHI | Andy Borregales | 4,800 | 12.6 | 12.2 | 6.8 | not_design |
| 7 | showdown DAL@PHI | Brandon Aubrey | 5,400 | 21.2 | 14.8 | 7.9 | not_design |
| 7 | showdown DAL@PHI | Jake Elliott | 4,800 | 14.9 | 6.3 | 3.4 | not_design |
| 8 | showdown CAR@GB | Trey Smack | 5,000 | 21.5 | 8.3 | 4.5 | not_design |
| 8 | showdown CAR@GB | Ryan Fitzgerald | 4,800 | 19.6 | 9.6 | 5.2 | not_design |

29 kickers: mean actual 20.1, v3 18.5, v4 10.5; MAE v3 8.0, v4 10.0.

### Q4. Concentration: top-5 predicted vs everyone else (mean predicted − actual per player)

| wk | slate | slot | version | top-5 pred sum | top-5 actual sum | top-5 mean bias | rest mean bias |
|---:|---|---|---|---:|---:|---:|---:|
| 2 | classic 13 games | overall | v3 | 173.1 | 86.4 | +17.35 | -0.14 |
| 2 | classic 13 games | overall | v4 | 164.3 | 86.4 | +15.59 | -0.13 |
| 2 | showdown NYG@LAR | CPT | v3 | 70.0 | 52.5 | +3.51 | -0.54 |
| 2 | showdown NYG@LAR | CPT | v4 | 69.9 | 52.5 | +3.48 | -0.54 |
| 2 | showdown NYG@LAR | FLEX | v3 | 266.5 | 198.3 | +13.64 | -1.65 |
| 2 | showdown NYG@LAR | FLEX | v4 | 260.9 | 198.3 | +12.52 | -1.51 |
| 3 | classic 13 games | overall | v3 | 208.9 | 123.4 | +17.10 | -0.14 |
| 3 | classic 13 games | overall | v4 | 198.6 | 94.7 | +20.78 | -0.17 |
| 3 | showdown ATL@GB | CPT | v3 | 91.4 | 60.2 | +6.24 | -0.94 |
| 3 | showdown ATL@GB | CPT | v4 | 94.3 | 71.1 | +4.64 | -0.70 |
| 3 | showdown ATL@GB | FLEX | v3 | 300.9 | 213.6 | +17.47 | -2.41 |
| 3 | showdown ATL@GB | FLEX | v4 | 285.2 | 221.4 | +12.77 | -1.75 |
| 3 | showdown PHI@CHI | CPT | v3 | 82.7 | 61.2 | +4.30 | -0.66 |
| 3 | showdown PHI@CHI | CPT | v4 | 85.5 | 61.2 | +4.87 | -0.75 |
| 3 | showdown PHI@CHI | FLEX | v3 | 298.4 | 229.1 | +13.86 | -1.18 |
| 3 | showdown PHI@CHI | FLEX | v4 | 285.3 | 229.1 | +11.24 | -0.83 |
| 4 | classic 12 games | overall | v3 | 177.1 | 86.9 | +18.05 | -0.61 |
| 4 | classic 12 games | overall | v4 | 173.5 | 73.8 | +19.94 | -0.66 |
| 4 | showdown ATL@NO | CPT | v3 | 90.4 | 75.9 | +2.90 | -0.46 |
| 4 | showdown ATL@NO | CPT | v4 | 91.9 | 71.0 | +4.17 | -0.66 |
| 4 | showdown ATL@NO | FLEX | v3 | 323.1 | 224.0 | +19.82 | -2.85 |
| 4 | showdown ATL@NO | FLEX | v4 | 307.3 | 224.0 | +16.67 | -2.33 |
| 4 | showdown DET@CAR | CPT | v3 | 89.9 | 65.3 | +4.92 | -0.69 |
| 4 | showdown DET@CAR | CPT | v4 | 92.5 | 65.3 | +5.44 | -0.76 |
| 4 | showdown DET@CAR | FLEX | v3 | 325.4 | 206.9 | +23.69 | -3.30 |
| 4 | showdown DET@CAR | FLEX | v4 | 309.6 | 206.9 | +20.53 | -2.85 |
| 4 | showdown IND@WAS | CPT | v3 | 79.0 | 58.2 | +4.16 | -0.51 |
| 4 | showdown IND@WAS | CPT | v4 | 85.0 | 66.5 | +3.70 | -0.45 |
| 4 | showdown IND@WAS | FLEX | v3 | 296.6 | 214.7 | +16.38 | -1.98 |
| 4 | showdown IND@WAS | FLEX | v4 | 281.5 | 227.3 | +10.83 | -1.26 |
| 4 | showdown PIT@CLE | CPT | v3 | 70.7 | 62.2 | +1.71 | -0.34 |
| 4 | showdown PIT@CLE | CPT | v4 | 77.7 | 62.2 | +3.10 | -0.62 |
| 4 | showdown PIT@CLE | FLEX | v3 | 283.0 | 231.6 | +10.28 | -1.46 |
| 4 | showdown PIT@CLE | FLEX | v4 | 263.3 | 231.6 | +6.35 | -0.90 |
| 5 | classic 11 games | overall | v3 | 204.4 | 94.9 | +21.91 | -0.25 |
| 5 | classic 11 games | overall | v4 | 198.1 | 94.9 | +20.63 | -0.23 |
| 5 | showdown BUF@LAR | CPT | v3 | 84.7 | 74.8 | +1.99 | -0.55 |
| 5 | showdown BUF@LAR | CPT | v4 | 86.8 | 74.8 | +2.40 | -0.62 |
| 5 | showdown BUF@LAR | FLEX | v3 | 335.9 | 227.6 | +21.66 | -3.38 |
| 5 | showdown BUF@LAR | FLEX | v4 | 310.6 | 227.6 | +16.60 | -2.60 |
| 5 | showdown TB@DAL | CPT | v3 | 89.5 | 62.5 | +5.39 | -0.96 |
| 5 | showdown TB@DAL | CPT | v4 | 92.9 | 64.8 | +5.62 | -1.00 |
| 5 | showdown TB@DAL | FLEX | v3 | 274.1 | 205.5 | +13.72 | -2.01 |
| 5 | showdown TB@DAL | FLEX | v4 | 269.3 | 195.8 | +14.69 | -2.15 |
| 6 | classic 10 games | overall | v3 | 171.4 | 135.7 | +7.13 | -0.35 |
| 6 | classic 10 games | overall | v4 | 170.9 | 135.7 | +7.02 | -0.36 |
| 6 | showdown SEA@DEN | CPT | v3 | 81.3 | 68.8 | +2.50 | -0.38 |
| 6 | showdown SEA@DEN | CPT | v4 | 86.1 | 72.6 | +2.70 | -0.41 |
| 6 | showdown SEA@DEN | FLEX | v3 | 267.4 | 220.0 | +9.47 | -1.18 |
| 6 | showdown SEA@DEN | FLEX | v4 | 246.9 | 211.3 | +7.12 | -0.88 |
| 6 | showdown WAS@SF | CPT | v3 | 88.8 | 75.5 | +2.66 | -0.32 |
| 6 | showdown WAS@SF | CPT | v4 | 90.4 | 75.5 | +2.99 | -0.36 |
| 6 | showdown WAS@SF | FLEX | v3 | 325.3 | 229.7 | +19.12 | -2.30 |
| 6 | showdown WAS@SF | FLEX | v4 | 311.0 | 229.7 | +16.27 | -1.95 |
| 7 | classic 10 games | overall | v3 | 165.0 | 125.0 | +8.01 | -0.28 |
| 7 | classic 10 games | overall | v4 | 153.7 | 125.0 | +5.75 | -0.22 |
| 7 | showdown DAL@PHI | CPT | v3 | 81.3 | 64.6 | +3.34 | -1.05 |
| 7 | showdown DAL@PHI | CPT | v4 | 82.9 | 61.6 | +4.27 | -1.33 |
| 7 | showdown DAL@PHI | FLEX | v3 | 309.0 | 228.6 | +16.09 | -3.24 |
| 7 | showdown DAL@PHI | FLEX | v4 | 288.3 | 228.6 | +11.94 | -2.71 |
| 7 | showdown NE@CHI | CPT | v3 | 79.5 | 67.5 | +2.41 | -0.34 |
| 7 | showdown NE@CHI | CPT | v4 | 83.0 | 67.5 | +3.11 | -0.44 |
| 7 | showdown NE@CHI | FLEX | v3 | 282.0 | 231.1 | +10.18 | -1.39 |
| 7 | showdown NE@CHI | FLEX | v4 | 269.6 | 231.1 | +7.69 | -1.03 |
| 8 | classic 11 games | overall | v3 | 151.4 | 83.5 | +13.59 | -0.20 |
| 8 | classic 11 games | overall | v4 | 146.7 | 83.5 | +12.64 | -0.19 |
| 8 | showdown CAR@GB | CPT | v3 | 67.5 | 59.6 | +1.57 | -0.24 |
| 8 | showdown CAR@GB | CPT | v4 | 69.4 | 59.6 | +1.96 | -0.30 |
| 8 | showdown CAR@GB | FLEX | v3 | 290.4 | 207.0 | +16.68 | -2.60 |
| 8 | showdown CAR@GB | FLEX | v4 | 265.7 | 207.0 | +11.74 | -1.80 |

Slates where the top 5 were over-predicted AND the rest under-predicted on average:

- classic overall v3: 7 of 7
- classic overall v4: 7 of 7
- showdown CPT v3: 14 of 14
- showdown CPT v4: 14 of 14
- showdown FLEX v3: 14 of 14
- showdown FLEX v4: 14 of 14

## 7. Reproduction checks

- **WAS@SF (contest 196786695), owner run `21bf2279` (canonical, created 23:13 UTC, T-62):** flex v3 MAE flex v3 5.11 / 0.895, flex v4 5.05 / 0.860, cpt v3 1.11 / 0.916, cpt v4 1.27 / 0.898. The pre-grade result (flex v3 5.11 / 0.895, v4 5.05 / 0.860; captain v3 1.11 / 0.916, v4 1.27 / 0.898) is reproduced from this source.
- **Same contest, canonical POOL capture (23:03 UTC, the upload-instant capture, code 1963fcd7, v3):** flex v3 5.98 / 0.867, flex v4 5.99 / 0.799, cpt v3 0.97 / 0.891, cpt v4 1.13 / 0.875. It differs because at 23:03 Mike Evans and Stefon Diggs carried no Q status (the run at 23:13 has Q for both) and Brandin Cooks / Jacob Cowing were not yet market-anchored; v3 then put Evans at 67.3% flex (run: 43.9%, actual 26.8%). The owner resolved the source question on 2026-10-20 (scorecard v3): the canonical forecast is the latest capture of any source, here the run. The v1/v2 preference pick is still pooled as `preferred`.
- **Week-4 Millionaire (196151357), frozen stored v2 forecast:** 183 observed-label players including 2 OUT in the input (Case Keenum 0.69%, DeVonta Smith 0.05% actual) give MAE 3.540 (the review's 3.538); the scorecard's active-player population is 181 at MAE 3.572 (n 181). The contest's labels are not complete (mass 0.99493 < 0.995), so it is not gate-eligible; the 629-player 1.065 figure zero-fills 446 unresolved labels and is not reproduced as a gate number.
- **Recomputation vs stored ownership:** 61 frozen inputs carried a stored forecast of a recomputable version; the largest absolute difference on any player was 0.000 percentage points.

