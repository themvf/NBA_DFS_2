# NFL DFS ownership — scorecard regrade (nfl-ownership-scorecard-v4)

Generated 2026-11-07 21:27 UTC by `python -m research.nfl_ownership_scorecard` (grader `nfl-ownership-scorecard-grader-v1`). Read-only except the append-only `nfl_ownership_scorecard_rows` (8356 new rows written this run; 8356 rows under this scorecard and grader version in total, one grade per forecast (ON CONFLICT DO NOTHING)).

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
| 6 | showdown WAS@SF | 10-20 00:15 | 196786690 | 87,465 | yes | pool 10-19 23:03; optimizer 10-19 23:13; ghost 10-19 22:30 | pool 10-19 21:57; optimizer 10-19 21:57; ghost 10-19 22:30 | SF |
| 7 | showdown NE@CHI | 10-23 00:15 | 196947072 | 73,529 | yes | pool 10-22 23:50; optimizer 10-22 23:20; ghost 10-22 22:30 | pool 10-22 00:15; optimizer 10-22 21:01; ghost 10-22 22:30 | CHI |
| 7 | classic 10 games | 10-25 17:00 | 196905290 | 147,058 | yes | optimizer 10-25 13:23; ghost 10-25 15:55 | optimizer 10-25 00:41; ghost 10-25 15:55 | - |
| 7 | showdown DAL@PHI | 10-27 00:15 | 197055073 | 73,529 | yes | pool 10-26 23:50; optimizer 10-26 23:33; ghost 10-26 23:13 | pool 10-26 17:14; optimizer 10-26 20:54; ghost 10-26 23:13 | DAL |
| 8 | showdown CAR@GB | 10-30 00:15 | 197208268 | 73,529 | yes | pool 10-29 23:50; optimizer 10-29 22:50; ghost 10-29 23:10 | pool 10-29 11:48; optimizer 10-29 11:49; ghost 10-29 23:10 | GB |
| 8 | classic 11 games | 11-01 18:00 | 197220774 | 198,176 | yes | optimizer 11-01 16:12; ghost 11-01 16:55 | optimizer 11-01 01:46; ghost 11-01 16:55 | - |
| 8 | showdown PHI@WAS | 11-02 01:20 | 197302803 | 3,567 | yes | pool 11-02 00:55; optimizer 11-01 22:43; ghost 11-02 00:10 | pool 11-01 22:43; optimizer 11-01 22:43; ghost 11-02 00:10 | WAS |
| 8 | showdown CHI@SEA | 11-03 01:15 | 197302895 | 73,529 | yes | pool 11-03 00:50; optimizer 11-02 23:46; ghost 11-03 00:10 | pool 11-02 20:22; optimizer 11-02 23:32; ghost 11-03 00:10 | SEA |
| 9 | showdown JAX@BAL | 11-06 01:15 | 197450244 | 73,105 | yes | pool 11-06 00:50; optimizer 11-06 00:50; ghost 11-06 00:10 | pool 11-06 00:42; optimizer 11-06 00:42; ghost 11-06 00:10 | JAX |

- Not graded: contest 195677825 (week 2, NFL Showdown $1.5M Thursday Night Showdown [$500K to 1st] (D) has no salary upload (no_salary_upload (DET@BUF week 2)), so there is no slate, no frozen input and no `no_frozen_forecast` row.

`no_frozen_forecast` rows: 0.

## 2. Per-slate grades — reference contest, canonical horizon, primary source, full pool (gate population)

| wk | slate | contest | source / capture (min before) | stored | anchored/players | version | tag | slot | n | MAE | bias | RMSE | Spearman | Pearson | top-10 |
|---:|---|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 2 | classic 13 games | 195648006 | optimizer 09-20 16:03 (56) | none | 0/670 | v3 | not_design | overall | 593 | 1.12 | +0.00 | 3.13 | 0.754 | 0.725 | 4 |
| 2 | classic 13 games | 195648006 | optimizer 09-20 16:03 (56) | none | 0/670 | v4 | design | overall | 593 | 1.14 | +0.00 | 3.14 | 0.752 | 0.714 | 4 |
| 2 | classic 13 games | 195648006 | optimizer 09-20 16:03 (56) | none | 0/670 | v5 | design | overall | 593 | 1.12 | +0.00 | 2.70 | 0.756 | 0.745 | 4 |
| 2 | classic 13 games | 195648006 | optimizer 09-20 16:03 (56) | none | 0/670 | nfl-ownership-rank-shape-v1 | design | overall | 593 | 1.02 | -0.00 | 2.67 | 0.765 | 0.761 | 5 |
| 2 | showdown NYG@LAR | 195786068 | optimizer 09-21 20:02 (253) | none | 0/53 | v3 | design | CPT | 39 | 1.55 | -0.02 | 3.16 | 0.604 | 0.820 | 8 |
| 2 | showdown NYG@LAR | 195786068 | optimizer 09-21 20:02 (253) | none | 0/53 | v4 | design | CPT | 39 | 1.51 | -0.02 | 3.25 | 0.559 | 0.818 | 8 |
| 2 | showdown NYG@LAR | 195786068 | optimizer 09-21 20:02 (253) | none | 0/53 | v5 | design | CPT | 39 | 1.68 | -0.10 | 2.88 | 0.580 | 0.825 | 8 |
| 2 | showdown NYG@LAR | 195786068 | optimizer 09-21 20:02 (253) | none | 0/53 | nfl-ownership-rank-shape-v1 | design | CPT | 39 | 1.39 | -0.04 | 2.84 | 0.577 | 0.828 | 7 |
| 2 | showdown NYG@LAR | 195786068 | optimizer 09-21 20:02 (253) | none | 0/53 | v3 | design | FLEX | 46 | 6.30 | +0.01 | 10.64 | 0.629 | 0.810 | 7 |
| 2 | showdown NYG@LAR | 195786068 | optimizer 09-21 20:02 (253) | none | 0/53 | v4 | design | FLEX | 46 | 6.35 | +0.01 | 10.49 | 0.603 | 0.808 | 7 |
| 2 | showdown NYG@LAR | 195786068 | optimizer 09-21 20:02 (253) | none | 0/53 | v5 | design | FLEX | 46 | 5.64 | +0.01 | 9.03 | 0.692 | 0.841 | 7 |
| 2 | showdown NYG@LAR | 195786068 | optimizer 09-21 20:02 (253) | none | 0/53 | nfl-ownership-rank-shape-v1 | design | FLEX | 46 | 5.38 | -0.03 | 9.20 | 0.689 | 0.835 | 7 |
| 3 | classic 13 games | 195905122 | optimizer 09-27 16:32 (27) | none | 0/671 | v3 | not_design | overall | 586 | 1.11 | +0.01 | 3.61 | 0.821 | 0.701 | 4 |
| 3 | classic 13 games | 195905122 | optimizer 09-27 16:32 (27) | none | 0/671 | v4 | design | overall | 586 | 1.09 | +0.01 | 3.54 | 0.822 | 0.703 | 4 |
| 3 | classic 13 games | 195905122 | optimizer 09-27 16:32 (27) | none | 0/671 | v5 | design | overall | 586 | 1.03 | +0.01 | 2.87 | 0.805 | 0.742 | 4 |
| 3 | classic 13 games | 195905122 | optimizer 09-27 16:32 (27) | none | 0/671 | nfl-ownership-rank-shape-v1 | design | overall | 586 | 0.93 | +0.00 | 2.65 | 0.798 | 0.780 | 4 |
| 3 | showdown ATL@GB | 195943234 | optimizer 09-24 23:34 (40) | none | 0/53 | v3 | design | CPT | 38 | 2.08 | +0.00 | 4.48 | 0.841 | 0.838 | 7 |
| 3 | showdown ATL@GB | 195943234 | optimizer 09-24 23:34 (40) | none | 0/53 | v4 | design | CPT | 38 | 2.16 | +0.00 | 4.84 | 0.843 | 0.836 | 7 |
| 3 | showdown ATL@GB | 195943234 | optimizer 09-24 23:34 (40) | none | 0/53 | v5 | design | CPT | 38 | 1.25 | -0.04 | 2.25 | 0.886 | 0.903 | 8 |
| 3 | showdown ATL@GB | 195943234 | optimizer 09-24 23:34 (40) | none | 0/53 | nfl-ownership-rank-shape-v1 | design | CPT | 38 | 1.06 | -0.03 | 1.79 | 0.879 | 0.937 | 8 |
| 3 | showdown ATL@GB | 195943234 | optimizer 09-24 23:34 (40) | none | 0/53 | v3 | design | FLEX | 41 | 5.34 | +0.02 | 8.76 | 0.891 | 0.918 | 8 |
| 3 | showdown ATL@GB | 195943234 | optimizer 09-24 23:34 (40) | none | 0/53 | v4 | design | FLEX | 41 | 4.93 | +0.02 | 7.37 | 0.868 | 0.926 | 8 |
| 3 | showdown ATL@GB | 195943234 | optimizer 09-24 23:34 (40) | none | 0/53 | v5 | design | FLEX | 41 | 4.38 | +0.02 | 6.53 | 0.897 | 0.916 | 8 |
| 3 | showdown ATL@GB | 195943234 | optimizer 09-24 23:34 (40) | none | 0/53 | nfl-ownership-rank-shape-v1 | design | FLEX | 41 | 4.14 | -0.03 | 6.23 | 0.893 | 0.918 | 8 |
| 3 | showdown PHI@CHI | 196036238 | optimizer 09-28 22:24 (110) | none | 0/56 | v3 | design | CPT | 34 | 1.75 | +0.07 | 3.82 | 0.793 | 0.829 | 8 |
| 3 | showdown PHI@CHI | 196036238 | optimizer 09-28 22:24 (110) | none | 0/56 | v4 | design | CPT | 34 | 1.81 | +0.07 | 3.97 | 0.776 | 0.831 | 8 |
| 3 | showdown PHI@CHI | 196036238 | optimizer 09-28 22:24 (110) | none | 0/56 | v5 | design | CPT | 34 | 1.74 | -0.02 | 2.95 | 0.762 | 0.854 | 7 |
| 3 | showdown PHI@CHI | 196036238 | optimizer 09-28 22:24 (110) | none | 0/56 | nfl-ownership-rank-shape-v1 | design | CPT | 34 | 1.34 | +0.05 | 2.94 | 0.752 | 0.849 | 7 |
| 3 | showdown PHI@CHI | 196036238 | optimizer 09-28 22:24 (110) | none | 0/56 | v3 | design | FLEX | 42 | 5.26 | +0.61 | 8.79 | 0.785 | 0.898 | 8 |
| 3 | showdown PHI@CHI | 196036238 | optimizer 09-28 22:24 (110) | none | 0/56 | v4 | design | FLEX | 42 | 6.00 | +0.61 | 8.68 | 0.704 | 0.881 | 7 |
| 3 | showdown PHI@CHI | 196036238 | optimizer 09-28 22:24 (110) | none | 0/56 | v5 | design | FLEX | 42 | 5.32 | +0.61 | 7.11 | 0.796 | 0.898 | 8 |
| 3 | showdown PHI@CHI | 196036238 | optimizer 09-28 22:24 (110) | none | 0/56 | nfl-ownership-rank-shape-v1 | design | FLEX | 42 | 5.08 | +0.56 | 7.21 | 0.786 | 0.893 | 8 |
| 4 | classic 12 games | 196151357 | optimizer 10-04 16:07 (52) | v2 | 0/629 | v3 | not_design | overall | 181 | 3.57 | -0.10 | 6.50 | 0.655 | 0.562 | 3 | **labels incomplete: observed only, not gate-eligible**
| 4 | classic 12 games | 196151357 | optimizer 10-04 16:07 (52) | v2 | 0/629 | v4 | design | overall | 181 | 3.57 | -0.09 | 6.42 | 0.659 | 0.561 | 3 | **labels incomplete: observed only, not gate-eligible**
| 4 | classic 12 games | 196151357 | optimizer 10-04 16:07 (52) | v2 | 0/629 | v5 | design | overall | 181 | 3.18 | -0.23 | 5.23 | 0.660 | 0.591 | 3 | **labels incomplete: observed only, not gate-eligible**
| 4 | classic 12 games | 196151357 | optimizer 10-04 16:07 (52) | v2 | 0/629 | nfl-ownership-rank-shape-v1 | design | overall | 181 | 3.08 | -0.13 | 4.92 | 0.693 | 0.628 | 2 | **labels incomplete: observed only, not gate-eligible**
| 4 | classic 12 games | 196151357 | optimizer 10-04 16:07 (52) | v2 | 0/629 | stored v2 | stored_forecast | overall | 181 | 3.57 | -0.10 | 6.50 | 0.655 | 0.562 | 3 | **labels incomplete: observed only, not gate-eligible**
| 4 | showdown ATL@NO | 196285155 | optimizer 10-05 21:55 (140) | v3 | 0/56 | v3 | not_design | CPT | 36 | 1.30 | +0.01 | 2.63 | 0.818 | 0.951 | 7 |
| 4 | showdown ATL@NO | 196285155 | optimizer 10-05 21:55 (140) | v3 | 0/56 | v4 | design | CPT | 36 | 1.43 | +0.01 | 2.87 | 0.841 | 0.947 | 8 |
| 4 | showdown ATL@NO | 196285155 | optimizer 10-05 21:55 (140) | v3 | 0/56 | v5 | design | CPT | 36 | 0.87 | -0.03 | 1.42 | 0.872 | 0.974 | 9 |
| 4 | showdown ATL@NO | 196285155 | optimizer 10-05 21:55 (140) | v3 | 0/56 | nfl-ownership-rank-shape-v1 | design | CPT | 36 | 0.82 | -0.01 | 1.47 | 0.863 | 0.966 | 8 |
| 4 | showdown ATL@NO | 196285155 | optimizer 10-05 21:55 (140) | v3 | 0/56 | v3 | not_design | FLEX | 42 | 6.81 | -0.15 | 10.23 | 0.799 | 0.896 | 7 |
| 4 | showdown ATL@NO | 196285155 | optimizer 10-05 21:55 (140) | v3 | 0/56 | v4 | design | FLEX | 42 | 6.54 | -0.07 | 9.17 | 0.746 | 0.896 | 7 |
| 4 | showdown ATL@NO | 196285155 | optimizer 10-05 21:55 (140) | v3 | 0/56 | v5 | design | FLEX | 42 | 3.34 | -0.30 | 5.33 | 0.798 | 0.938 | 7 |
| 4 | showdown ATL@NO | 196285155 | optimizer 10-05 21:55 (140) | v3 | 0/56 | nfl-ownership-rank-shape-v1 | design | FLEX | 42 | 3.62 | -0.41 | 6.16 | 0.797 | 0.918 | 7 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | v3 | not_design | CPT | 40 | 1.52 | +0.01 | 3.09 | 0.871 | 0.927 | 9 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | v4 | design | CPT | 40 | 1.65 | +0.01 | 3.29 | 0.877 | 0.925 | 9 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | v5 | design | CPT | 40 | 1.18 | +0.01 | 2.48 | 0.872 | 0.904 | 9 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | nfl-ownership-rank-shape-v1 | design | CPT | 40 | 1.12 | +0.00 | 2.42 | 0.877 | 0.904 | 9 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | stored v2 | stored_forecast | CPT | 40 | 1.71 | +0.01 | 3.75 | 0.826 | 0.929 | 7 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | v3 | not_design | FLEX | 40 | 7.54 | +0.07 | 13.41 | 0.869 | 0.806 | 8 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | v4 | design | FLEX | 40 | 7.38 | +0.07 | 12.69 | 0.815 | 0.793 | 8 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | v5 | design | FLEX | 40 | 4.58 | +0.07 | 8.13 | 0.870 | 0.862 | 8 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | nfl-ownership-rank-shape-v1 | design | FLEX | 40 | 4.13 | +0.02 | 8.01 | 0.872 | 0.867 | 8 |
| 4 | showdown DET@CAR | 196285055 | optimizer 10-04 22:18 (122) | v2 | 0/50 | stored v2 | stored_forecast | FLEX | 40 | 7.81 | +0.07 | 13.87 | 0.855 | 0.793 | 7 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | v3 | not_design | CPT | 44 | 1.07 | +0.02 | 2.54 | 0.808 | 0.926 | 9 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | v4 | design | CPT | 44 | 1.12 | +0.02 | 2.98 | 0.798 | 0.929 | 9 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | v5 | design | CPT | 44 | 1.00 | +0.02 | 1.90 | 0.812 | 0.943 | 8 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | nfl-ownership-rank-shape-v1 | design | CPT | 44 | 0.90 | +0.01 | 1.71 | 0.814 | 0.947 | 8 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | stored v2 | stored_forecast | CPT | 44 | 1.37 | +0.02 | 3.40 | 0.740 | 0.880 | 9 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | v3 | not_design | FLEX | 44 | 5.04 | +0.11 | 8.13 | 0.827 | 0.921 | 9 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | v4 | design | FLEX | 44 | 4.50 | +0.11 | 6.99 | 0.765 | 0.922 | 9 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | v5 | design | FLEX | 44 | 4.20 | +0.11 | 6.00 | 0.836 | 0.929 | 9 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | nfl-ownership-rank-shape-v1 | design | FLEX | 44 | 3.75 | +0.07 | 5.77 | 0.821 | 0.934 | 9 |
| 4 | showdown IND@WAS | 196204783 | optimizer 10-04 08:25 (304) | v2 | 0/56 | stored v2 | stored_forecast | FLEX | 44 | 5.70 | +0.11 | 11.04 | 0.811 | 0.859 | 9 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | v3 | not_design | CPT | 30 | 1.87 | +0.00 | 3.50 | 0.893 | 0.792 | 7 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | v4 | design | CPT | 30 | 1.98 | +0.00 | 3.71 | 0.911 | 0.792 | 8 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | v5 | design | CPT | 30 | 1.61 | -0.07 | 2.74 | 0.859 | 0.893 | 7 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | nfl-ownership-rank-shape-v1 | design | CPT | 30 | 1.61 | -0.02 | 2.60 | 0.858 | 0.887 | 7 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | stored v2 | stored_forecast | CPT | 30 | 2.23 | +0.00 | 3.91 | 0.851 | 0.748 | 6 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | v3 | not_design | FLEX | 40 | 3.19 | +0.01 | 6.28 | 0.879 | 0.943 | 8 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | v4 | design | FLEX | 40 | 4.72 | +0.01 | 7.03 | 0.814 | 0.911 | 8 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | v5 | design | FLEX | 40 | 4.50 | +0.01 | 6.50 | 0.887 | 0.946 | 8 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | nfl-ownership-rank-shape-v1 | design | FLEX | 40 | 3.05 | -0.04 | 4.85 | 0.883 | 0.959 | 8 |
| 4 | showdown PIT@CLE | 196187074 | optimizer 10-01 22:13 (121) | v2 | 0/51 | stored v2 | stored_forecast | FLEX | 40 | 4.21 | +0.01 | 7.92 | 0.867 | 0.918 | 7 |
| 5 | classic 11 games | 196403752 | optimizer 10-11 15:37 (82) | v3 | 0/587 | v3 | not_design | overall | 442 | 1.64 | +0.00 | 4.80 | 0.848 | 0.612 | 3 |
| 5 | classic 11 games | 196403752 | optimizer 10-11 15:37 (82) | v3 | 0/587 | v4 | design | overall | 442 | 1.63 | +0.00 | 4.71 | 0.850 | 0.616 | 3 |
| 5 | classic 11 games | 196403752 | optimizer 10-11 15:37 (82) | v3 | 0/587 | v5 | design | overall | 442 | 1.55 | +0.00 | 4.02 | 0.840 | 0.664 | 3 |
| 5 | classic 11 games | 196403752 | optimizer 10-11 15:37 (82) | v3 | 0/587 | nfl-ownership-rank-shape-v1 | design | overall | 442 | 1.45 | -0.00 | 3.85 | 0.831 | 0.693 | 4 |
| 5 | showdown BUF@LAR | 196480476 | optimizer 10-12 23:13 (62) | v3 | 18/49 | v3 | not_design | CPT | 25 | 1.23 | -0.05 | 2.66 | 0.948 | 0.946 | 9 |
| 5 | showdown BUF@LAR | 196480476 | optimizer 10-12 23:13 (62) | v3 | 18/49 | v4 | design | CPT | 25 | 1.29 | -0.02 | 2.82 | 0.950 | 0.946 | 9 |
| 5 | showdown BUF@LAR | 196480476 | optimizer 10-12 23:13 (62) | v3 | 18/49 | v5 | design | CPT | 25 | 1.43 | -0.36 | 2.52 | 0.898 | 0.929 | 9 |
| 5 | showdown BUF@LAR | 196480476 | optimizer 10-12 23:13 (62) | v3 | 18/49 | nfl-ownership-rank-shape-v1 | design | CPT | 25 | 1.24 | -0.18 | 2.13 | 0.871 | 0.940 | 9 |
| 5 | showdown BUF@LAR | 196480476 | optimizer 10-12 23:13 (62) | v3 | 18/49 | v3 | not_design | FLEX | 37 | 7.43 | -0.00 | 12.88 | 0.849 | 0.839 | 8 |
| 5 | showdown BUF@LAR | 196480476 | optimizer 10-12 23:13 (62) | v3 | 18/49 | v4 | design | FLEX | 37 | 7.41 | -0.00 | 11.70 | 0.806 | 0.829 | 9 |
| 5 | showdown BUF@LAR | 196480476 | optimizer 10-12 23:13 (62) | v3 | 18/49 | v5 | design | FLEX | 37 | 4.87 | -0.00 | 9.04 | 0.850 | 0.850 | 8 |
| 5 | showdown BUF@LAR | 196480476 | optimizer 10-12 23:13 (62) | v3 | 18/49 | nfl-ownership-rank-shape-v1 | design | FLEX | 37 | 4.89 | -0.06 | 9.31 | 0.825 | 0.840 | 8 |
| 5 | showdown TB@DAL | 196438550 | pool 10-08 00:15 (1439) | v3 | 0/52 | v3 | not_design | CPT | 33 | 2.42 | +0.00 | 6.16 | 0.849 | 0.765 | 7 |
| 5 | showdown TB@DAL | 196438550 | pool 10-08 00:15 (1439) | v3 | 0/52 | v4 | design | CPT | 33 | 2.56 | +0.00 | 6.49 | 0.858 | 0.766 | 7 |
| 5 | showdown TB@DAL | 196438550 | pool 10-08 00:15 (1439) | v3 | 0/52 | v5 | design | CPT | 33 | 1.43 | -0.03 | 2.60 | 0.886 | 0.858 | 7 |
| 5 | showdown TB@DAL | 196438550 | pool 10-08 00:15 (1439) | v3 | 0/52 | nfl-ownership-rank-shape-v1 | design | CPT | 33 | 1.52 | -0.01 | 2.67 | 0.879 | 0.860 | 7 |
| 5 | showdown TB@DAL | 196438550 | pool 10-08 00:15 (1439) | v3 | 0/52 | v3 | not_design | FLEX | 39 | 5.25 | +0.01 | 10.03 | 0.914 | 0.862 | 8 |
| 5 | showdown TB@DAL | 196438550 | pool 10-08 00:15 (1439) | v3 | 0/52 | v4 | design | FLEX | 39 | 4.89 | +0.01 | 9.84 | 0.897 | 0.847 | 8 |
| 5 | showdown TB@DAL | 196438550 | pool 10-08 00:15 (1439) | v3 | 0/52 | v5 | design | FLEX | 39 | 4.36 | +0.01 | 7.41 | 0.927 | 0.891 | 8 |
| 5 | showdown TB@DAL | 196438550 | pool 10-08 00:15 (1439) | v3 | 0/52 | nfl-ownership-rank-shape-v1 | design | FLEX | 39 | 4.23 | -0.04 | 6.70 | 0.921 | 0.905 | 8 |
| 6 | classic 10 games | 196653900 | ghost 10-18 15:15 (104) | v3 | 128/526 | v3 | not_design | overall | 162 | 2.94 | -0.12 | 5.05 | 0.801 | 0.764 | 5 | **labels incomplete: observed only, not gate-eligible**
| 6 | classic 10 games | 196653900 | ghost 10-18 15:15 (104) | v3 | 128/526 | v4 | not_design | overall | 162 | 2.95 | -0.13 | 4.95 | 0.807 | 0.768 | 5 | **labels incomplete: observed only, not gate-eligible**
| 6 | classic 10 games | 196653900 | ghost 10-18 15:15 (104) | v3 | 128/526 | v5 | design | overall | 162 | 2.82 | -0.26 | 4.66 | 0.811 | 0.772 | 5 | **labels incomplete: observed only, not gate-eligible**
| 6 | classic 10 games | 196653900 | ghost 10-18 15:15 (104) | v3 | 128/526 | nfl-ownership-rank-shape-v1 | design | overall | 162 | 2.80 | -0.21 | 4.78 | 0.816 | 0.757 | 5 | **labels incomplete: observed only, not gate-eligible**
| 6 | showdown SEA@DEN | 196689718 | optimizer 10-15 22:12 (122) | v3 | 15/56 | v3 | not_design | CPT | 38 | 1.27 | +0.00 | 3.03 | 0.947 | 0.942 | 7 |
| 6 | showdown SEA@DEN | 196689718 | optimizer 10-15 22:12 (122) | v3 | 15/56 | v4 | not_design | CPT | 38 | 1.28 | +0.00 | 3.04 | 0.956 | 0.947 | 8 |
| 6 | showdown SEA@DEN | 196689718 | optimizer 10-15 22:12 (122) | v3 | 15/56 | v5 | design | CPT | 38 | 1.50 | -0.04 | 3.68 | 0.929 | 0.855 | 8 |
| 6 | showdown SEA@DEN | 196689718 | optimizer 10-15 22:12 (122) | v3 | 15/56 | nfl-ownership-rank-shape-v1 | design | CPT | 38 | 1.44 | -0.01 | 3.17 | 0.921 | 0.878 | 8 |
| 6 | showdown SEA@DEN | 196689718 | optimizer 10-15 22:12 (122) | v3 | 15/56 | v3 | not_design | FLEX | 45 | 3.28 | +0.01 | 5.81 | 0.954 | 0.950 | 8 |
| 6 | showdown SEA@DEN | 196689718 | optimizer 10-15 22:12 (122) | v3 | 15/56 | v4 | not_design | FLEX | 45 | 3.87 | +0.01 | 5.99 | 0.938 | 0.926 | 7 |
| 6 | showdown SEA@DEN | 196689718 | optimizer 10-15 22:12 (122) | v3 | 15/56 | v5 | design | FLEX | 45 | 3.40 | +0.01 | 5.45 | 0.954 | 0.932 | 8 |
| 6 | showdown SEA@DEN | 196689718 | optimizer 10-15 22:12 (122) | v3 | 15/56 | nfl-ownership-rank-shape-v1 | design | FLEX | 45 | 2.42 | -0.04 | 3.81 | 0.949 | 0.967 | 7 |
| 6 | showdown WAS@SF | 196786690 | optimizer 10-19 23:13 (62) | v3 | 13/60 | v3 | not_design | CPT | 38 | 1.42 | +0.00 | 3.16 | 0.877 | 0.895 | 9 |
| 6 | showdown WAS@SF | 196786690 | optimizer 10-19 23:13 (62) | v3 | 13/60 | v4 | not_design | CPT | 38 | 1.60 | +0.00 | 3.68 | 0.850 | 0.867 | 8 |
| 6 | showdown WAS@SF | 196786690 | optimizer 10-19 23:13 (62) | v3 | 13/60 | v5 | design | CPT | 38 | 1.18 | -0.02 | 2.41 | 0.837 | 0.920 | 8 |
| 6 | showdown WAS@SF | 196786690 | optimizer 10-19 23:13 (62) | v3 | 13/60 | nfl-ownership-rank-shape-v1 | design | CPT | 38 | 0.96 | -0.01 | 2.05 | 0.835 | 0.921 | 8 |
| 6 | showdown WAS@SF | 196786690 | optimizer 10-19 23:13 (62) | v3 | 13/60 | v3 | not_design | FLEX | 45 | 5.03 | +0.02 | 8.68 | 0.858 | 0.923 | 7 |
| 6 | showdown WAS@SF | 196786690 | optimizer 10-19 23:13 (62) | v3 | 13/60 | v4 | not_design | FLEX | 45 | 4.97 | +0.02 | 8.28 | 0.817 | 0.911 | 7 |
| 6 | showdown WAS@SF | 196786690 | optimizer 10-19 23:13 (62) | v3 | 13/60 | v5 | design | FLEX | 45 | 2.78 | +0.02 | 3.93 | 0.868 | 0.967 | 7 |
| 6 | showdown WAS@SF | 196786690 | optimizer 10-19 23:13 (62) | v3 | 13/60 | nfl-ownership-rank-shape-v1 | design | FLEX | 45 | 3.20 | -0.03 | 4.57 | 0.870 | 0.954 | 8 |
| 7 | classic 10 games | 196905290 | ghost 10-25 15:55 (64) | v5,v5+nfl-ownership-bands-v1 | 133/518 | v3 | not_design | overall | 389 | 1.50 | +0.01 | 3.65 | 0.883 | 0.775 | 5 |
| 7 | classic 10 games | 196905290 | ghost 10-25 15:55 (64) | v5,v5+nfl-ownership-bands-v1 | 133/518 | v4 | not_design | overall | 389 | 1.48 | +0.01 | 3.59 | 0.884 | 0.777 | 5 |
| 7 | classic 10 games | 196905290 | ghost 10-25 15:55 (64) | v5,v5+nfl-ownership-bands-v1 | 133/518 | v5 | not_design | overall | 389 | 1.46 | +0.01 | 3.38 | 0.872 | 0.788 | 5 |
| 7 | classic 10 games | 196905290 | ghost 10-25 15:55 (64) | v5,v5+nfl-ownership-bands-v1 | 133/518 | nfl-ownership-rank-shape-v1 | design | overall | 389 | 1.43 | -0.01 | 3.39 | 0.864 | 0.787 | 4 |
| 7 | classic 10 games | 196905290 | ghost 10-25 15:55 (64) | v5,v5+nfl-ownership-bands-v1 | 133/518 | stored v5,v5+nfl-ownership-bands-v1 | stored_forecast | overall | 389 | 1.69 | +0.37 | 3.42 | 0.863 | 0.785 | 5 |
| 7 | showdown DAL@PHI | 197055073 | pool 10-26 23:50 (24) | v5 | 20/55 | v3 | not_design | CPT | 30 | 1.36 | +0.00 | 3.08 | 0.951 | 0.910 | 10 |
| 7 | showdown DAL@PHI | 197055073 | pool 10-26 23:50 (24) | v5 | 20/55 | v4 | not_design | CPT | 30 | 1.53 | +0.00 | 3.33 | 0.947 | 0.905 | 10 |
| 7 | showdown DAL@PHI | 197055073 | pool 10-26 23:50 (24) | v5 | 20/55 | v5 | not_design | CPT | 30 | 1.36 | -0.12 | 2.42 | 0.955 | 0.894 | 10 |
| 7 | showdown DAL@PHI | 197055073 | pool 10-26 23:50 (24) | v5 | 20/55 | nfl-ownership-rank-shape-v1 | design | CPT | 30 | 1.35 | -0.06 | 2.45 | 0.952 | 0.892 | 10 |
| 7 | showdown DAL@PHI | 197055073 | pool 10-26 23:50 (24) | v5 | 20/55 | v3 | not_design | FLEX | 38 | 4.74 | +0.09 | 8.25 | 0.920 | 0.931 | 10 |
| 7 | showdown DAL@PHI | 197055073 | pool 10-26 23:50 (24) | v5 | 20/55 | v4 | not_design | FLEX | 38 | 5.18 | +0.09 | 7.89 | 0.838 | 0.914 | 10 |
| 7 | showdown DAL@PHI | 197055073 | pool 10-26 23:50 (24) | v5 | 20/55 | v5 | not_design | FLEX | 38 | 3.54 | +0.09 | 5.13 | 0.920 | 0.958 | 10 |
| 7 | showdown DAL@PHI | 197055073 | pool 10-26 23:50 (24) | v5 | 20/55 | nfl-ownership-rank-shape-v1 | design | FLEX | 38 | 3.35 | +0.04 | 5.25 | 0.918 | 0.954 | 10 |
| 7 | showdown NE@CHI | 196947072 | pool 10-22 23:50 (24) | v5 | 17/54 | v3 | not_design | CPT | 34 | 1.41 | +0.00 | 3.29 | 0.945 | 0.880 | 10 |
| 7 | showdown NE@CHI | 196947072 | pool 10-22 23:50 (24) | v5 | 17/54 | v4 | not_design | CPT | 34 | 1.48 | +0.00 | 3.54 | 0.961 | 0.879 | 9 |
| 7 | showdown NE@CHI | 196947072 | pool 10-22 23:50 (24) | v5 | 17/54 | v5 | not_design | CPT | 34 | 1.37 | -0.04 | 2.24 | 0.924 | 0.924 | 10 |
| 7 | showdown NE@CHI | 196947072 | pool 10-22 23:50 (24) | v5 | 17/54 | nfl-ownership-rank-shape-v1 | design | CPT | 34 | 0.99 | -0.01 | 1.81 | 0.925 | 0.933 | 10 |
| 7 | showdown NE@CHI | 196947072 | pool 10-22 23:50 (24) | v5 | 17/54 | v3 | not_design | FLEX | 39 | 3.91 | +0.01 | 6.63 | 0.930 | 0.945 | 9 |
| 7 | showdown NE@CHI | 196947072 | pool 10-22 23:50 (24) | v5 | 17/54 | v4 | not_design | FLEX | 39 | 4.02 | +0.01 | 6.14 | 0.924 | 0.941 | 9 |
| 7 | showdown NE@CHI | 196947072 | pool 10-22 23:50 (24) | v5 | 17/54 | v5 | not_design | FLEX | 39 | 3.53 | +0.01 | 5.04 | 0.929 | 0.958 | 9 |
| 7 | showdown NE@CHI | 196947072 | pool 10-22 23:50 (24) | v5 | 17/54 | nfl-ownership-rank-shape-v1 | design | FLEX | 39 | 3.43 | -0.04 | 5.06 | 0.921 | 0.951 | 9 |
| 8 | classic 11 games | 197220774 | ghost 11-01 16:55 (64) | nfl-ownership-fit-v1 | 245/562 | v3 | not_design | overall | 334 | 1.69 | +0.00 | 4.03 | 0.940 | 0.705 | 3 |
| 8 | classic 11 games | 197220774 | ghost 11-01 16:55 (64) | nfl-ownership-fit-v1 | 245/562 | v4 | not_design | overall | 334 | 1.69 | +0.00 | 3.99 | 0.941 | 0.705 | 2 |
| 8 | classic 11 games | 197220774 | ghost 11-01 16:55 (64) | nfl-ownership-fit-v1 | 245/562 | v5 | not_design | overall | 334 | 1.60 | +0.00 | 3.44 | 0.940 | 0.749 | 3 |
| 8 | classic 11 games | 197220774 | ghost 11-01 16:55 (64) | nfl-ownership-fit-v1 | 245/562 | nfl-ownership-rank-shape-v1 | design | overall | 334 | 1.53 | -0.01 | 3.37 | 0.934 | 0.762 | 3 |
| 8 | classic 11 games | 197220774 | ghost 11-01 16:55 (64) | nfl-ownership-fit-v1 | 245/562 | stored nfl-ownership-fit-v1 | stored_forecast | overall | 334 | 1.81 | -0.01 | 4.33 | 0.936 | 0.638 | 3 |
| 8 | showdown CAR@GB | 197208268 | pool 10-29 23:50 (24) | v5 | 24/53 | v3 | not_design | CPT | 29 | 1.72 | +0.01 | 3.62 | 0.939 | 0.775 | 9 |
| 8 | showdown CAR@GB | 197208268 | pool 10-29 23:50 (24) | v5 | 24/53 | v4 | not_design | CPT | 29 | 1.80 | +0.01 | 3.70 | 0.926 | 0.773 | 10 |
| 8 | showdown CAR@GB | 197208268 | pool 10-29 23:50 (24) | v5 | 24/53 | v5 | not_design | CPT | 29 | 1.41 | -0.04 | 2.83 | 0.929 | 0.867 | 9 |
| 8 | showdown CAR@GB | 197208268 | pool 10-29 23:50 (24) | v5 | 24/53 | nfl-ownership-rank-shape-v1 | design | CPT | 29 | 1.37 | -0.01 | 2.69 | 0.930 | 0.871 | 9 |
| 8 | showdown CAR@GB | 197208268 | pool 10-29 23:50 (24) | v5 | 24/53 | v3 | not_design | FLEX | 36 | 6.30 | +0.03 | 9.05 | 0.959 | 0.924 | 9 |
| 8 | showdown CAR@GB | 197208268 | pool 10-29 23:50 (24) | v5 | 24/53 | v4 | not_design | FLEX | 36 | 5.75 | +0.03 | 7.98 | 0.890 | 0.911 | 9 |
| 8 | showdown CAR@GB | 197208268 | pool 10-29 23:50 (24) | v5 | 24/53 | v5 | not_design | FLEX | 36 | 3.25 | +0.03 | 4.81 | 0.952 | 0.955 | 9 |
| 8 | showdown CAR@GB | 197208268 | pool 10-29 23:50 (24) | v5 | 24/53 | nfl-ownership-rank-shape-v1 | design | FLEX | 36 | 2.75 | -0.03 | 4.55 | 0.950 | 0.957 | 9 |
| 8 | showdown CHI@SEA | 197302895 | pool 11-03 00:50 (24) | nfl-ownership-fit-showdown-v1 | 21/55 | v3 | not_design | CPT | 23 | 2.04 | +0.04 | 3.31 | 0.823 | 0.970 | 7 |
| 8 | showdown CHI@SEA | 197302895 | pool 11-03 00:50 (24) | nfl-ownership-fit-showdown-v1 | 21/55 | v4 | not_design | CPT | 23 | 2.23 | +0.04 | 3.72 | 0.860 | 0.968 | 9 |
| 8 | showdown CHI@SEA | 197302895 | pool 11-03 00:50 (24) | nfl-ownership-fit-showdown-v1 | 21/55 | v5 | not_design | CPT | 23 | 2.21 | +0.01 | 4.88 | 0.842 | 0.787 | 8 |
| 8 | showdown CHI@SEA | 197302895 | pool 11-03 00:50 (24) | nfl-ownership-fit-showdown-v1 | 21/55 | nfl-ownership-rank-shape-v1 | design | CPT | 23 | 2.35 | +0.01 | 4.60 | 0.843 | 0.801 | 8 |
| 8 | showdown CHI@SEA | 197302895 | pool 11-03 00:50 (24) | nfl-ownership-fit-showdown-v1 | 21/55 | stored nfl-ownership-fit-showdown-v1 | stored_forecast | CPT | 23 | 1.40 | +0.03 | 2.42 | 0.912 | 0.954 | 8 |
| 8 | showdown CHI@SEA | 197302895 | pool 11-03 00:50 (24) | nfl-ownership-fit-showdown-v1 | 21/55 | v3 | not_design | FLEX | 28 | 8.21 | +0.32 | 11.94 | 0.894 | 0.867 | 8 |
| 8 | showdown CHI@SEA | 197302895 | pool 11-03 00:50 (24) | nfl-ownership-fit-showdown-v1 | 21/55 | v4 | not_design | FLEX | 28 | 8.09 | +0.32 | 10.66 | 0.858 | 0.852 | 7 |
| 8 | showdown CHI@SEA | 197302895 | pool 11-03 00:50 (24) | nfl-ownership-fit-showdown-v1 | 21/55 | v5 | not_design | FLEX | 28 | 5.04 | +0.32 | 6.58 | 0.907 | 0.902 | 8 |
| 8 | showdown CHI@SEA | 197302895 | pool 11-03 00:50 (24) | nfl-ownership-fit-showdown-v1 | 21/55 | nfl-ownership-rank-shape-v1 | design | FLEX | 28 | 4.22 | +0.22 | 6.19 | 0.909 | 0.916 | 8 |
| 8 | showdown CHI@SEA | 197302895 | pool 11-03 00:50 (24) | nfl-ownership-fit-showdown-v1 | 21/55 | stored nfl-ownership-fit-showdown-v1 | stored_forecast | FLEX | 28 | 6.21 | +0.32 | 8.51 | 0.878 | 0.874 | 7 |
| 8 | showdown PHI@WAS | 197302803 | pool 11-02 00:55 (24) | nfl-ownership-fit-showdown-v1 | 22/58 | v3 | not_design | CPT | 32 | 1.19 | +0.01 | 2.65 | 0.916 | 0.970 | 9 |
| 8 | showdown PHI@WAS | 197302803 | pool 11-02 00:55 (24) | nfl-ownership-fit-showdown-v1 | 22/58 | v4 | not_design | CPT | 32 | 1.22 | +0.01 | 2.68 | 0.896 | 0.970 | 9 |
| 8 | showdown PHI@WAS | 197302803 | pool 11-02 00:55 (24) | nfl-ownership-fit-showdown-v1 | 22/58 | v5 | not_design | CPT | 32 | 1.73 | +0.01 | 4.15 | 0.907 | 0.873 | 9 |
| 8 | showdown PHI@WAS | 197302803 | pool 11-02 00:55 (24) | nfl-ownership-fit-showdown-v1 | 22/58 | nfl-ownership-rank-shape-v1 | design | CPT | 32 | 1.35 | -0.00 | 3.10 | 0.907 | 0.915 | 9 |
| 8 | showdown PHI@WAS | 197302803 | pool 11-02 00:55 (24) | nfl-ownership-fit-showdown-v1 | 22/58 | stored nfl-ownership-fit-showdown-v1 | stored_forecast | CPT | 32 | 1.28 | +0.01 | 3.13 | 0.965 | 0.935 | 9 |
| 8 | showdown PHI@WAS | 197302803 | pool 11-02 00:55 (24) | nfl-ownership-fit-showdown-v1 | 22/58 | v3 | not_design | FLEX | 32 | 3.98 | +0.08 | 6.46 | 0.931 | 0.947 | 10 |
| 8 | showdown PHI@WAS | 197302803 | pool 11-02 00:55 (24) | nfl-ownership-fit-showdown-v1 | 22/58 | v4 | not_design | FLEX | 32 | 5.24 | +0.07 | 6.92 | 0.804 | 0.922 | 10 |
| 8 | showdown PHI@WAS | 197302803 | pool 11-02 00:55 (24) | nfl-ownership-fit-showdown-v1 | 22/58 | v5 | not_design | FLEX | 32 | 5.90 | +0.08 | 7.26 | 0.927 | 0.930 | 10 |
| 8 | showdown PHI@WAS | 197302803 | pool 11-02 00:55 (24) | nfl-ownership-fit-showdown-v1 | 22/58 | nfl-ownership-rank-shape-v1 | design | FLEX | 32 | 4.01 | +0.00 | 5.51 | 0.931 | 0.955 | 10 |
| 8 | showdown PHI@WAS | 197302803 | pool 11-02 00:55 (24) | nfl-ownership-fit-showdown-v1 | 22/58 | stored nfl-ownership-fit-showdown-v1 | stored_forecast | FLEX | 32 | 5.01 | +0.08 | 7.29 | 0.951 | 0.911 | 10 |
| 9 | showdown JAX@BAL | 197450244 | optimizer 11-06 00:50 (24) | nfl-ownership-fit-showdown-v1 | 24/54 | v3 | not_design | CPT | 24 | 1.76 | +0.01 | 3.34 | 0.870 | 0.906 | 8 |
| 9 | showdown JAX@BAL | 197450244 | optimizer 11-06 00:50 (24) | nfl-ownership-fit-showdown-v1 | 24/54 | v4 | not_design | CPT | 24 | 1.91 | +0.01 | 3.69 | 0.862 | 0.902 | 8 |
| 9 | showdown JAX@BAL | 197450244 | optimizer 11-06 00:50 (24) | nfl-ownership-fit-showdown-v1 | 24/54 | v5 | not_design | CPT | 24 | 1.62 | -0.12 | 2.43 | 0.873 | 0.913 | 8 |
| 9 | showdown JAX@BAL | 197450244 | optimizer 11-06 00:50 (24) | nfl-ownership-fit-showdown-v1 | 24/54 | nfl-ownership-rank-shape-v1 | design | CPT | 24 | 1.51 | -0.02 | 2.43 | 0.871 | 0.914 | 8 |
| 9 | showdown JAX@BAL | 197450244 | optimizer 11-06 00:50 (24) | nfl-ownership-fit-showdown-v1 | 24/54 | stored nfl-ownership-fit-showdown-v1 | stored_forecast | CPT | 24 | 1.87 | +0.00 | 2.89 | 0.862 | 0.872 | 8 |
| 9 | showdown JAX@BAL | 197450244 | optimizer 11-06 00:50 (24) | nfl-ownership-fit-showdown-v1 | 24/54 | v3 | not_design | FLEX | 32 | 7.26 | +0.43 | 12.68 | 0.916 | 0.838 | 9 |
| 9 | showdown JAX@BAL | 197450244 | optimizer 11-06 00:50 (24) | nfl-ownership-fit-showdown-v1 | 24/54 | v4 | not_design | FLEX | 32 | 7.80 | +0.43 | 11.63 | 0.830 | 0.820 | 9 |
| 9 | showdown JAX@BAL | 197450244 | optimizer 11-06 00:50 (24) | nfl-ownership-fit-showdown-v1 | 24/54 | v5 | not_design | FLEX | 32 | 4.96 | +0.43 | 6.39 | 0.932 | 0.927 | 9 |
| 9 | showdown JAX@BAL | 197450244 | optimizer 11-06 00:50 (24) | nfl-ownership-fit-showdown-v1 | 24/54 | nfl-ownership-rank-shape-v1 | design | FLEX | 32 | 4.53 | +0.37 | 6.55 | 0.935 | 0.922 | 9 |
| 9 | showdown JAX@BAL | 197450244 | optimizer 11-06 00:50 (24) | nfl-ownership-fit-showdown-v1 | 24/54 | stored nfl-ownership-fit-showdown-v1 | stored_forecast | FLEX | 32 | 5.03 | +0.43 | 7.70 | 0.921 | 0.901 | 8 |

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
| 6 | showdown WAS@SF | CPT | 38 | 1.42 / 0.877 | 1.60 / 0.850 | ghost 10-19 22:30 | 1.38 / 0.854 | 1.55 / 0.833 |
| 6 | showdown WAS@SF | FLEX | 45 | 5.03 / 0.858 | 4.97 / 0.817 | ghost 10-19 22:30 | 5.11 / 0.830 | 5.14 / 0.781 |
| 7 | classic 10 games | overall | 102 | 5.00 / 0.566 | 4.89 / 0.589 | ghost 10-25 15:55 | 1.50 / 0.883 | 1.48 / 0.884 |
| 7 | showdown DAL@PHI | CPT | 30 | 1.36 / 0.951 | 1.53 / 0.947 | ghost 10-26 23:13 | 1.32 / 0.937 | 1.48 / 0.935 |
| 7 | showdown DAL@PHI | FLEX | 38 | 4.74 / 0.920 | 5.18 / 0.838 | ghost 10-26 23:13 | 4.57 / 0.890 | 5.02 / 0.811 |
| 7 | showdown NE@CHI | CPT | 34 | 1.41 / 0.945 | 1.48 / 0.961 | ghost 10-22 22:30 | 1.32 / 0.956 | 1.37 / 0.957 |
| 7 | showdown NE@CHI | FLEX | 39 | 3.91 / 0.930 | 4.02 / 0.924 | ghost 10-22 22:30 | 3.65 / 0.934 | 3.77 / 0.931 |
| 8 | classic 11 games | overall | 100 | 4.89 / 0.597 | 4.92 / 0.596 | ghost 11-01 16:55 | 1.69 / 0.940 | 1.69 / 0.941 |
| 8 | showdown CAR@GB | CPT | 29 | 1.72 / 0.939 | 1.80 / 0.926 | ghost 10-29 23:10 | 1.72 / 0.939 | 1.80 / 0.926 |
| 8 | showdown CAR@GB | FLEX | 36 | 6.30 / 0.959 | 5.75 / 0.890 | ghost 10-29 23:10 | 6.30 / 0.959 | 5.75 / 0.890 |
| 8 | showdown CHI@SEA | CPT | 23 | 2.04 / 0.823 | 2.23 / 0.860 | ghost 11-03 00:10 | 1.97 / 0.832 | 2.16 / 0.859 |
| 8 | showdown CHI@SEA | FLEX | 28 | 8.21 / 0.894 | 8.09 / 0.858 | ghost 11-03 00:10 | 7.95 / 0.891 | 7.84 / 0.846 |
| 8 | showdown PHI@WAS | CPT | 32 | 1.19 / 0.916 | 1.22 / 0.896 | ghost 11-02 00:10 | 1.20 / 0.916 | 1.23 / 0.898 |
| 8 | showdown PHI@WAS | FLEX | 32 | 3.98 / 0.931 | 5.24 / 0.804 | ghost 11-02 00:10 | 3.90 / 0.932 | 5.16 / 0.797 |
| 9 | showdown JAX@BAL | CPT | 24 | 1.76 / 0.870 | 1.91 / 0.862 | pool 11-06 00:42 | 1.76 / 0.870 | 1.91 / 0.862 |
| 9 | showdown JAX@BAL | FLEX | 32 | 7.26 / 0.916 | 7.80 / 0.830 | pool 11-06 00:42 | 7.26 / 0.916 | 7.80 / 0.830 |

## 3. Pooled (reference contests with complete labels, full pool, slate-clustered 95% interval)

| format | horizon | source selector | slates | version | slot | subset | n | MAE [95%] | bias [95%] | RMSE | mean Spearman [95%] |
|---|---|---|---:|---|---|---|---:|---|---|---:|---|
| classic | canonical | latest | 5 | v3 | overall | all | 2344 | 1.36 [1.17, 1.62] | +0.00 [0.00, 0.01] | 3.82 | 0.849 [0.794, 0.905] |
| classic | canonical | latest | 5 | v4 | overall | all | 2344 | 1.36 [1.18, 1.61] | +0.00 [0.00, 0.01] | 3.77 | 0.850 [0.794, 0.906] |
| classic | canonical | latest | 5 | v5 | overall | all | 2344 | 1.30 [1.13, 1.54] | +0.00 [0.00, 0.01] | 3.24 | 0.843 [0.790, 0.899] |
| classic | canonical | latest | 5 | nfl-ownership-rank-shape-v1 | overall | all | 2344 | 1.22 [1.03, 1.48] | -0.00 [-0.01, -0.00] | 3.14 | 0.838 [0.791, 0.892] |
| classic | canonical | latest | 1 | stored nfl-ownership-fit-v1 | overall | all | 334 | 1.81 - | -0.01 - | 4.33 | 0.936 - |
| classic | canonical | latest | 1 | stored v5,v5+nfl-ownership-bands-v1 | overall | all | 389 | 1.69 - | +0.37 - | 3.42 | 0.863 - |
| classic | canonical | latest | 5 | v3 | overall | not_design | 2344 | 1.36 [1.17, 1.62] | +0.00 [0.00, 0.01] | 3.82 | 0.849 [0.794, 0.905] |
| classic | canonical | latest | 2 | v4 | overall | not_design | 723 | 1.57 [1.48, 1.69] | +0.00 [0.00, 0.01] | 3.78 | 0.912 [0.884, 0.941] |
| classic | canonical | latest | 2 | v5 | overall | not_design | 723 | 1.53 [1.46, 1.60] | +0.00 [0.00, 0.01] | 3.41 | 0.906 [0.872, 0.940] |
| classic | canonical | latest | 1 | stored nfl-ownership-fit-v1 | overall | not_design | 334 | 1.81 - | -0.01 - | 4.33 | 0.936 - |
| classic | canonical | latest | 1 | stored v5,v5+nfl-ownership-bands-v1 | overall | not_design | 389 | 1.69 - | +0.37 - | 3.42 | 0.863 - |
| classic | canonical | preferred | 5 | v3 | overall | all | 2355 | 1.36 [1.18, 1.62] | +0.00 [0.00, 0.01] | 3.83 | 0.846 [0.796, 0.895] |
| classic | canonical | preferred | 5 | v4 | overall | all | 2355 | 1.36 [1.18, 1.61] | +0.00 [0.00, 0.01] | 3.79 | 0.847 [0.795, 0.896] |
| classic | canonical | preferred | 5 | v5 | overall | all | 2355 | 1.31 [1.14, 1.54] | +0.00 [0.00, 0.01] | 3.26 | 0.840 [0.790, 0.890] |
| classic | canonical | preferred | 5 | nfl-ownership-rank-shape-v1 | overall | all | 2355 | 1.23 [1.05, 1.46] | -0.00 [-0.01, -0.00] | 3.17 | 0.836 [0.793, 0.884] |
| classic | canonical | preferred | 1 | stored nfl-ownership-fit-v1 | overall | all | 347 | 1.76 - | -0.01 - | 4.27 | 0.934 - |
| classic | canonical | preferred | 1 | stored v5,v5+nfl-ownership-bands-v1 | overall | all | 402 | 1.66 - | +0.38 - | 3.38 | 0.849 - |
| classic | canonical | preferred | 5 | v3 | overall | not_design | 2355 | 1.36 [1.18, 1.62] | +0.00 [0.00, 0.01] | 3.83 | 0.846 [0.796, 0.895] |
| classic | canonical | preferred | 2 | v4 | overall | not_design | 749 | 1.56 [1.46, 1.67] | +0.00 [0.00, 0.01] | 3.79 | 0.903 [0.870, 0.935] |
| classic | canonical | preferred | 2 | v5 | overall | not_design | 749 | 1.51 [1.45, 1.59] | +0.00 [0.00, 0.01] | 3.42 | 0.895 [0.857, 0.933] |
| classic | canonical | preferred | 1 | stored nfl-ownership-fit-v1 | overall | not_design | 347 | 1.76 - | -0.01 - | 4.27 | 0.934 - |
| classic | canonical | preferred | 1 | stored v5,v5+nfl-ownership-bands-v1 | overall | not_design | 402 | 1.66 - | +0.38 - | 3.38 | 0.849 - |
| classic | early | latest | 5 | v3 | overall | all | 2344 | 1.36 [1.17, 1.63] | +0.00 [0.00, 0.01] | 3.83 | 0.848 [0.795, 0.905] |
| classic | early | latest | 5 | v4 | overall | all | 2344 | 1.36 [1.18, 1.62] | +0.00 [0.00, 0.01] | 3.78 | 0.849 [0.795, 0.906] |
| classic | early | latest | 5 | v5 | overall | all | 2344 | 1.31 [1.13, 1.55] | +0.00 [0.00, 0.01] | 3.25 | 0.842 [0.789, 0.899] |
| classic | early | latest | 5 | nfl-ownership-rank-shape-v1 | overall | all | 2344 | 1.22 [1.03, 1.48] | -0.00 [-0.01, -0.00] | 3.14 | 0.838 [0.791, 0.892] |
| classic | early | latest | 1 | stored nfl-ownership-fit-v1 | overall | all | 334 | 1.81 - | -0.01 - | 4.33 | 0.936 - |
| classic | early | latest | 1 | stored v5,v5+nfl-ownership-bands-v1 | overall | all | 389 | 1.69 - | +0.37 - | 3.42 | 0.863 - |
| classic | early | latest | 5 | v3 | overall | not_design | 2344 | 1.36 [1.17, 1.63] | +0.00 [0.00, 0.01] | 3.83 | 0.848 [0.795, 0.905] |
| classic | early | latest | 2 | v4 | overall | not_design | 723 | 1.57 [1.48, 1.69] | +0.00 [0.00, 0.01] | 3.78 | 0.912 [0.884, 0.941] |
| classic | early | latest | 2 | v5 | overall | not_design | 723 | 1.53 [1.46, 1.60] | +0.00 [0.00, 0.01] | 3.41 | 0.906 [0.872, 0.940] |
| classic | early | latest | 1 | stored nfl-ownership-fit-v1 | overall | not_design | 334 | 1.81 - | -0.01 - | 4.33 | 0.936 - |
| classic | early | latest | 1 | stored v5,v5+nfl-ownership-bands-v1 | overall | not_design | 389 | 1.69 - | +0.37 - | 3.42 | 0.863 - |
| classic | early | preferred | 5 | v3 | overall | all | 2454 | 1.31 [1.15, 1.52] | +0.01 [0.00, 0.02] | 3.76 | 0.843 [0.795, 0.893] |
| classic | early | preferred | 5 | v4 | overall | all | 2454 | 1.31 [1.16, 1.51] | +0.01 [0.00, 0.02] | 3.72 | 0.844 [0.795, 0.894] |
| classic | early | preferred | 5 | v5 | overall | all | 2454 | 1.26 [1.11, 1.46] | +0.01 [0.00, 0.02] | 3.19 | 0.835 [0.788, 0.882] |
| classic | early | preferred | 5 | nfl-ownership-rank-shape-v1 | overall | all | 2454 | 1.17 [1.02, 1.39] | +0.00 [-0.01, 0.01] | 3.09 | 0.829 [0.791, 0.871] |
| classic | early | preferred | 1 | stored nfl-ownership-fit-v1 | overall | all | 435 | 1.40 - | -0.01 - | 3.72 | 0.920 - |
| classic | early | preferred | 5 | v3 | overall | not_design | 2454 | 1.31 [1.15, 1.52] | +0.01 [0.00, 0.02] | 3.76 | 0.843 [0.795, 0.893] |
| classic | early | preferred | 2 | v4 | overall | not_design | 833 | 1.39 [1.33, 1.45] | +0.02 [0.00, 0.03] | 3.59 | 0.899 [0.872, 0.926] |
| classic | early | preferred | 2 | v5 | overall | not_design | 833 | 1.35 [1.28, 1.43] | +0.02 [0.00, 0.03] | 3.23 | 0.888 [0.858, 0.917] |
| classic | early | preferred | 1 | stored nfl-ownership-fit-v1 | overall | not_design | 435 | 1.40 - | -0.01 - | 3.72 | 0.920 - |
| showdown | canonical | latest | 17 | v3 | CPT | all | 567 | 1.57 [1.41, 1.76] | +0.01 [-0.00, 0.02] | 3.48 | 0.864 [0.820, 0.900] |
| showdown | canonical | latest | 17 | v4 | CPT | all | 567 | 1.66 [1.49, 1.86] | +0.01 [0.00, 0.02] | 3.72 | 0.863 [0.813, 0.902] |
| showdown | canonical | latest | 17 | v5 | CPT | all | 567 | 1.41 [1.27, 1.57] | -0.05 [-0.09, -0.02] | 2.80 | 0.860 [0.814, 0.895] |
| showdown | canonical | latest | 17 | nfl-ownership-rank-shape-v1 | CPT | all | 567 | 1.27 [1.14, 1.44] | -0.02 [-0.04, -0.00] | 2.56 | 0.856 [0.810, 0.892] |
| showdown | canonical | latest | 3 | stored nfl-ownership-fit-showdown-v1 | CPT | all | 79 | 1.49 [1.28, 1.87] | +0.01 [0.00, 0.03] | 2.87 | 0.913 [0.862, 0.965] |
| showdown | canonical | latest | 3 | stored v2 | CPT | all | 114 | 1.71 [1.37, 2.23] | +0.01 [0.00, 0.02] | 3.66 | 0.805 [0.740, 0.851] |
| showdown | canonical | latest | 14 | v3 | CPT | not_design | 456 | 1.51 [1.35, 1.73] | +0.01 [-0.00, 0.01] | 3.38 | 0.890 [0.863, 0.916] |
| showdown | canonical | latest | 8 | v4 | CPT | not_design | 248 | 1.59 [1.40, 1.81] | +0.01 [0.00, 0.02] | 3.42 | 0.907 [0.877, 0.937] |
| showdown | canonical | latest | 6 | v5 | CPT | not_design | 172 | 1.59 [1.42, 1.85] | -0.05 [-0.09, -0.01] | 3.25 | 0.905 [0.872, 0.933] |
| showdown | canonical | latest | 3 | stored nfl-ownership-fit-showdown-v1 | CPT | not_design | 79 | 1.49 [1.28, 1.87] | +0.01 [0.00, 0.03] | 2.87 | 0.913 [0.862, 0.965] |
| showdown | canonical | latest | 3 | stored v2 | CPT | not_design | 114 | 1.71 [1.37, 2.23] | +0.01 [0.00, 0.02] | 3.66 | 0.805 [0.740, 0.851] |
| showdown | canonical | latest | 17 | v3 | FLEX | all | 666 | 5.51 [4.86, 6.19] | +0.09 [0.01, 0.19] | 9.52 | 0.871 [0.828, 0.904] |
| showdown | canonical | latest | 17 | v4 | FLEX | all | 666 | 5.66 [5.13, 6.23] | +0.10 [0.02, 0.19] | 8.94 | 0.819 [0.779, 0.855] |
| showdown | canonical | latest | 17 | v5 | FLEX | all | 666 | 4.29 [3.86, 4.73] | +0.08 [-0.01, 0.19] | 6.59 | 0.879 [0.843, 0.908] |
| showdown | canonical | latest | 17 | nfl-ownership-rank-shape-v1 | FLEX | all | 666 | 3.88 [3.49, 4.30] | +0.02 [-0.07, 0.13] | 6.37 | 0.875 [0.838, 0.905] |
| showdown | canonical | latest | 3 | stored nfl-ownership-fit-showdown-v1 | FLEX | all | 92 | 5.38 [5.01, 6.21] | +0.27 [0.08, 0.43] | 7.82 | 0.917 [0.878, 0.951] |
| showdown | canonical | latest | 3 | stored v2 | FLEX | all | 124 | 5.90 [4.21, 7.81] | +0.06 [0.01, 0.11] | 11.20 | 0.844 [0.811, 0.867] |
| showdown | canonical | latest | 14 | v3 | FLEX | not_design | 537 | 5.47 [4.67, 6.37] | +0.06 [0.01, 0.13] | 9.53 | 0.893 [0.868, 0.916] |
| showdown | canonical | latest | 8 | v4 | FLEX | not_design | 295 | 5.43 [4.60, 6.54] | +0.11 [0.03, 0.22] | 8.21 | 0.862 [0.831, 0.896] |
| showdown | canonical | latest | 6 | v5 | FLEX | not_design | 205 | 4.28 [3.63, 5.15] | +0.15 [0.04, 0.29] | 5.85 | 0.928 [0.917, 0.939] |
| showdown | canonical | latest | 3 | stored nfl-ownership-fit-showdown-v1 | FLEX | not_design | 92 | 5.38 [5.01, 6.21] | +0.27 [0.08, 0.43] | 7.82 | 0.917 [0.878, 0.951] |
| showdown | canonical | latest | 3 | stored v2 | FLEX | not_design | 124 | 5.90 [4.21, 7.81] | +0.06 [0.01, 0.11] | 11.20 | 0.844 [0.811, 0.867] |
| showdown | canonical | preferred | 17 | v3 | CPT | all | 573 | 1.54 [1.38, 1.74] | +0.00 [-0.01, 0.01] | 3.43 | 0.861 [0.817, 0.896] |
| showdown | canonical | preferred | 17 | v4 | CPT | all | 573 | 1.63 [1.45, 1.84] | +0.00 [-0.00, 0.01] | 3.67 | 0.859 [0.808, 0.899] |
| showdown | canonical | preferred | 17 | v5 | CPT | all | 573 | 1.41 [1.27, 1.56] | -0.06 [-0.10, -0.03] | 2.80 | 0.858 [0.812, 0.893] |
| showdown | canonical | preferred | 17 | nfl-ownership-rank-shape-v1 | CPT | all | 573 | 1.27 [1.15, 1.43] | -0.02 [-0.04, -0.01] | 2.55 | 0.854 [0.808, 0.890] |
| showdown | canonical | preferred | 3 | stored nfl-ownership-fit-showdown-v1 | CPT | all | 79 | 1.49 [1.28, 1.87] | +0.01 [0.00, 0.03] | 2.87 | 0.913 [0.862, 0.965] |
| showdown | canonical | preferred | 1 | stored v1 | CPT | all | 37 | 2.25 - | -0.01 - | 4.74 | 0.470 - |
| showdown | canonical | preferred | 3 | stored v2 | CPT | all | 114 | 1.76 [1.49, 2.23] | +0.01 [0.00, 0.02] | 3.74 | 0.805 [0.740, 0.851] |
| showdown | canonical | preferred | 14 | v3 | CPT | not_design | 459 | 1.49 [1.32, 1.71] | +0.01 [-0.00, 0.01] | 3.33 | 0.886 [0.860, 0.912] |
| showdown | canonical | preferred | 8 | v4 | CPT | not_design | 248 | 1.56 [1.39, 1.79] | +0.01 [0.00, 0.02] | 3.34 | 0.905 [0.872, 0.936] |
| showdown | canonical | preferred | 6 | v5 | CPT | not_design | 172 | 1.59 [1.42, 1.85] | -0.05 [-0.09, -0.01] | 3.25 | 0.905 [0.872, 0.933] |
| showdown | canonical | preferred | 3 | stored nfl-ownership-fit-showdown-v1 | CPT | not_design | 79 | 1.49 [1.28, 1.87] | +0.01 [0.00, 0.03] | 2.87 | 0.913 [0.862, 0.965] |
| showdown | canonical | preferred | 1 | stored v1 | CPT | not_design | 37 | 2.25 - | -0.01 - | 4.74 | 0.470 - |
| showdown | canonical | preferred | 3 | stored v2 | CPT | not_design | 114 | 1.76 [1.49, 2.23] | +0.01 [0.00, 0.02] | 3.74 | 0.805 [0.740, 0.851] |
| showdown | canonical | preferred | 17 | v3 | FLEX | all | 673 | 5.54 [4.92, 6.17] | +0.05 [0.00, 0.11] | 9.59 | 0.865 [0.822, 0.901] |
| showdown | canonical | preferred | 17 | v4 | FLEX | all | 673 | 5.69 [5.18, 6.25] | +0.06 [0.02, 0.11] | 9.00 | 0.810 [0.767, 0.850] |
| showdown | canonical | preferred | 17 | v5 | FLEX | all | 673 | 4.34 [3.95, 4.75] | +0.04 [-0.02, 0.11] | 6.67 | 0.874 [0.836, 0.905] |
| showdown | canonical | preferred | 17 | nfl-ownership-rank-shape-v1 | FLEX | all | 673 | 3.91 [3.52, 4.31] | -0.01 [-0.08, 0.05] | 6.43 | 0.870 [0.830, 0.903] |
| showdown | canonical | preferred | 3 | stored nfl-ownership-fit-showdown-v1 | FLEX | all | 92 | 5.38 [5.01, 6.21] | +0.27 [0.08, 0.43] | 7.82 | 0.917 [0.878, 0.951] |
| showdown | canonical | preferred | 1 | stored v1 | FLEX | all | 46 | 8.47 - | +0.00 - | 15.80 | 0.502 - |
| showdown | canonical | preferred | 3 | stored v2 | FLEX | all | 124 | 5.84 [4.21, 7.81] | +0.06 [0.01, 0.11] | 11.22 | 0.845 [0.812, 0.867] |
| showdown | canonical | preferred | 14 | v3 | FLEX | not_design | 540 | 5.52 [4.73, 6.36] | +0.06 [0.01, 0.13] | 9.60 | 0.889 [0.862, 0.914] |
| showdown | canonical | preferred | 8 | v4 | FLEX | not_design | 295 | 5.58 [4.69, 6.63] | +0.11 [0.03, 0.22] | 8.43 | 0.854 [0.812, 0.894] |
| showdown | canonical | preferred | 6 | v5 | FLEX | not_design | 205 | 4.28 [3.63, 5.15] | +0.15 [0.04, 0.29] | 5.85 | 0.928 [0.917, 0.939] |
| showdown | canonical | preferred | 3 | stored nfl-ownership-fit-showdown-v1 | FLEX | not_design | 92 | 5.38 [5.01, 6.21] | +0.27 [0.08, 0.43] | 7.82 | 0.917 [0.878, 0.951] |
| showdown | canonical | preferred | 1 | stored v1 | FLEX | not_design | 46 | 8.47 - | +0.00 - | 15.80 | 0.502 - |
| showdown | canonical | preferred | 3 | stored v2 | FLEX | not_design | 124 | 5.84 [4.21, 7.81] | +0.06 [0.01, 0.11] | 11.22 | 0.845 [0.812, 0.867] |
| showdown | early | latest | 17 | v3 | CPT | all | 574 | 1.54 [1.38, 1.74] | +0.01 [-0.00, 0.02] | 3.43 | 0.856 [0.802, 0.897] |
| showdown | early | latest | 17 | v4 | CPT | all | 574 | 1.63 [1.45, 1.84] | +0.01 [-0.00, 0.02] | 3.67 | 0.854 [0.794, 0.898] |
| showdown | early | latest | 17 | v5 | CPT | all | 574 | 1.42 [1.28, 1.58] | -0.05 [-0.09, -0.02] | 2.80 | 0.851 [0.795, 0.890] |
| showdown | early | latest | 17 | nfl-ownership-rank-shape-v1 | CPT | all | 574 | 1.27 [1.14, 1.43] | -0.02 [-0.03, -0.00] | 2.55 | 0.845 [0.791, 0.885] |
| showdown | early | latest | 1 | stored linestar,v2 | CPT | all | 40 | 1.71 - | +0.01 - | 3.75 | 0.826 - |
| showdown | early | latest | 3 | stored nfl-ownership-fit-showdown-v1 | CPT | all | 80 | 1.47 [1.25, 1.87] | +0.00 [-0.00, 0.01] | 2.83 | 0.901 [0.862, 0.943] |
| showdown | early | latest | 2 | stored v2 | CPT | all | 74 | 1.79 [1.49, 2.23] | +0.01 [0.00, 0.02] | 3.73 | 0.795 [0.740, 0.851] |
| showdown | early | latest | 14 | v3 | CPT | not_design | 462 | 1.49 [1.31, 1.70] | +0.00 [-0.00, 0.01] | 3.33 | 0.886 [0.861, 0.912] |
| showdown | early | latest | 8 | v4 | CPT | not_design | 251 | 1.55 [1.38, 1.77] | +0.00 [0.00, 0.01] | 3.35 | 0.903 [0.872, 0.934] |
| showdown | early | latest | 6 | v5 | CPT | not_design | 175 | 1.57 [1.41, 1.81] | -0.06 [-0.10, -0.02] | 3.22 | 0.898 [0.861, 0.927] |
| showdown | early | latest | 1 | stored linestar,v2 | CPT | not_design | 40 | 1.71 - | +0.01 - | 3.75 | 0.826 - |
| showdown | early | latest | 3 | stored nfl-ownership-fit-showdown-v1 | CPT | not_design | 80 | 1.47 [1.25, 1.87] | +0.00 [-0.00, 0.01] | 2.83 | 0.901 [0.862, 0.943] |
| showdown | early | latest | 2 | stored v2 | CPT | not_design | 74 | 1.79 [1.49, 2.23] | +0.01 [0.00, 0.02] | 3.73 | 0.795 [0.740, 0.851] |
| showdown | early | latest | 17 | v3 | FLEX | all | 674 | 5.46 [4.81, 6.11] | +0.07 [-0.00, 0.17] | 9.42 | 0.861 [0.808, 0.900] |
| showdown | early | latest | 17 | v4 | FLEX | all | 674 | 5.61 [5.08, 6.17] | +0.07 [0.00, 0.17] | 8.85 | 0.809 [0.761, 0.850] |
| showdown | early | latest | 17 | v5 | FLEX | all | 674 | 4.32 [3.88, 4.77] | +0.06 [-0.03, 0.16] | 6.64 | 0.870 [0.827, 0.904] |
| showdown | early | latest | 17 | nfl-ownership-rank-shape-v1 | FLEX | all | 674 | 3.89 [3.48, 4.32] | +0.01 [-0.09, 0.11] | 6.41 | 0.863 [0.820, 0.898] |
| showdown | early | latest | 1 | stored linestar,v2 | FLEX | all | 40 | 7.81 - | +0.07 - | 13.87 | 0.855 - |
| showdown | early | latest | 3 | stored nfl-ownership-fit-showdown-v1 | FLEX | all | 93 | 5.20 [4.64, 6.00] | +0.20 [0.08, 0.43] | 7.63 | 0.910 [0.875, 0.933] |
| showdown | early | latest | 2 | stored v2 | FLEX | all | 84 | 4.91 [4.21, 5.55] | +0.06 [0.01, 0.11] | 9.72 | 0.839 [0.812, 0.867] |
| showdown | early | latest | 14 | v3 | FLEX | not_design | 544 | 5.40 [4.62, 6.26] | +0.04 [-0.01, 0.10] | 9.42 | 0.887 [0.861, 0.912] |
| showdown | early | latest | 8 | v4 | FLEX | not_design | 299 | 5.37 [4.51, 6.44] | +0.06 [-0.02, 0.16] | 8.12 | 0.853 [0.815, 0.894] |
| showdown | early | latest | 6 | v5 | FLEX | not_design | 209 | 4.23 [3.58, 5.07] | +0.07 [-0.04, 0.23] | 5.78 | 0.923 [0.907, 0.938] |
| showdown | early | latest | 1 | stored linestar,v2 | FLEX | not_design | 40 | 7.81 - | +0.07 - | 13.87 | 0.855 - |
| showdown | early | latest | 3 | stored nfl-ownership-fit-showdown-v1 | FLEX | not_design | 93 | 5.20 [4.64, 6.00] | +0.20 [0.08, 0.43] | 7.63 | 0.910 [0.875, 0.933] |
| showdown | early | latest | 2 | stored v2 | FLEX | not_design | 84 | 4.91 [4.21, 5.55] | +0.06 [0.01, 0.11] | 9.72 | 0.839 [0.812, 0.867] |
| showdown | early | preferred | 17 | v3 | CPT | all | 577 | 1.54 [1.39, 1.73] | +0.00 [-0.01, 0.01] | 3.41 | 0.853 [0.799, 0.893] |
| showdown | early | preferred | 17 | v4 | CPT | all | 577 | 1.62 [1.45, 1.82] | +0.00 [-0.00, 0.01] | 3.65 | 0.850 [0.790, 0.893] |
| showdown | early | preferred | 17 | v5 | CPT | all | 577 | 1.42 [1.28, 1.58] | -0.06 [-0.10, -0.03] | 2.80 | 0.849 [0.793, 0.888] |
| showdown | early | preferred | 17 | nfl-ownership-rank-shape-v1 | CPT | all | 577 | 1.27 [1.14, 1.43] | -0.02 [-0.04, -0.01] | 2.54 | 0.843 [0.789, 0.883] |
| showdown | early | preferred | 3 | stored nfl-ownership-fit-showdown-v1 | CPT | all | 79 | 1.52 [1.32, 1.87] | +0.01 [0.00, 0.03] | 2.91 | 0.912 [0.862, 0.964] |
| showdown | early | preferred | 1 | stored v1 | CPT | all | 37 | 2.25 - | -0.01 - | 4.74 | 0.470 - |
| showdown | early | preferred | 3 | stored v2 | CPT | all | 114 | 1.76 [1.49, 2.23] | +0.01 [0.00, 0.02] | 3.74 | 0.805 [0.740, 0.851] |
| showdown | early | preferred | 14 | v3 | CPT | not_design | 462 | 1.49 [1.33, 1.71] | +0.01 [-0.00, 0.01] | 3.36 | 0.881 [0.856, 0.907] |
| showdown | early | preferred | 8 | v4 | CPT | not_design | 251 | 1.57 [1.39, 1.80] | +0.01 [0.00, 0.02] | 3.41 | 0.893 [0.859, 0.927] |
| showdown | early | preferred | 6 | v5 | CPT | not_design | 175 | 1.60 [1.44, 1.86] | -0.06 [-0.09, -0.01] | 3.25 | 0.893 [0.858, 0.919] |
| showdown | early | preferred | 3 | stored nfl-ownership-fit-showdown-v1 | CPT | not_design | 79 | 1.52 [1.32, 1.87] | +0.01 [0.00, 0.03] | 2.91 | 0.912 [0.862, 0.964] |
| showdown | early | preferred | 1 | stored v1 | CPT | not_design | 37 | 2.25 - | -0.01 - | 4.74 | 0.470 - |
| showdown | early | preferred | 3 | stored v2 | CPT | not_design | 114 | 1.76 [1.49, 2.23] | +0.01 [0.00, 0.02] | 3.74 | 0.805 [0.740, 0.851] |
| showdown | early | preferred | 17 | v3 | FLEX | all | 678 | 5.48 [4.89, 6.11] | +0.05 [0.00, 0.11] | 9.48 | 0.855 [0.803, 0.895] |
| showdown | early | preferred | 17 | v4 | FLEX | all | 678 | 5.66 [5.14, 6.19] | +0.06 [0.01, 0.11] | 8.94 | 0.803 [0.752, 0.846] |
| showdown | early | preferred | 17 | v5 | FLEX | all | 678 | 4.35 [3.92, 4.80] | +0.04 [-0.02, 0.11] | 6.65 | 0.867 [0.823, 0.901] |
| showdown | early | preferred | 17 | nfl-ownership-rank-shape-v1 | FLEX | all | 678 | 3.93 [3.52, 4.35] | -0.01 [-0.08, 0.05] | 6.44 | 0.861 [0.816, 0.898] |
| showdown | early | preferred | 3 | stored nfl-ownership-fit-showdown-v1 | FLEX | all | 92 | 5.44 [5.03, 6.13] | +0.27 [0.08, 0.43] | 7.90 | 0.914 [0.874, 0.947] |
| showdown | early | preferred | 1 | stored v1 | FLEX | all | 46 | 8.47 - | +0.00 - | 15.80 | 0.502 - |
| showdown | early | preferred | 3 | stored v2 | FLEX | all | 124 | 5.85 [4.21, 7.81] | +0.06 [0.01, 0.11] | 11.23 | 0.845 [0.812, 0.867] |
| showdown | early | preferred | 14 | v3 | FLEX | not_design | 544 | 5.41 [4.65, 6.24] | +0.06 [0.01, 0.13] | 9.45 | 0.882 [0.857, 0.906] |
| showdown | early | preferred | 8 | v4 | FLEX | not_design | 299 | 5.45 [4.62, 6.48] | +0.10 [0.03, 0.21] | 8.19 | 0.848 [0.807, 0.890] |
| showdown | early | preferred | 6 | v5 | FLEX | not_design | 209 | 4.31 [3.59, 5.19] | +0.14 [0.03, 0.29] | 5.85 | 0.917 [0.902, 0.931] |
| showdown | early | preferred | 3 | stored nfl-ownership-fit-showdown-v1 | FLEX | not_design | 92 | 5.44 [5.03, 6.13] | +0.27 [0.08, 0.43] | 7.90 | 0.914 [0.874, 0.947] |
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
| 6 | showdown WAS@SF | CPT | v3 | 23:03: 1.25 / 0.852 | 23:13: 1.42 / 0.877 | 22:30: 1.38 / 0.854 |
| 6 | showdown WAS@SF | CPT | v4 | 23:03: 1.43 / 0.830 | 23:13: 1.60 / 0.850 | 22:30: 1.55 / 0.833 |
| 6 | showdown WAS@SF | FLEX | v3 | 23:03: 5.96 / 0.827 | 23:13: 5.03 / 0.858 | 22:30: 5.11 / 0.830 |
| 6 | showdown WAS@SF | FLEX | v4 | 23:03: 5.97 / 0.748 | 23:13: 4.97 / 0.817 | 22:30: 5.14 / 0.781 |
| 7 | classic 10 games | overall | v3 | - | 13:23: 1.48 / 0.869 | 15:55: 1.50 / 0.883 |
| 7 | classic 10 games | overall | v4 | - | 13:23: 1.46 / 0.870 | 15:55: 1.48 / 0.884 |
| 7 | showdown DAL@PHI | CPT | v3 | 23:50: 1.36 / 0.951 | 23:33: 1.36 / 0.951 | 23:13: 1.32 / 0.937 |
| 7 | showdown DAL@PHI | CPT | v4 | 23:50: 1.53 / 0.947 | 23:33: 1.53 / 0.947 | 23:13: 1.48 / 0.935 |
| 7 | showdown DAL@PHI | FLEX | v3 | 23:50: 4.74 / 0.920 | 23:33: 4.74 / 0.920 | 23:13: 4.57 / 0.890 |
| 7 | showdown DAL@PHI | FLEX | v4 | 23:50: 5.18 / 0.838 | 23:33: 5.18 / 0.838 | 23:13: 5.02 / 0.811 |
| 7 | showdown NE@CHI | CPT | v3 | 23:50: 1.41 / 0.945 | 23:20: 1.41 / 0.945 | 22:30: 1.32 / 0.956 |
| 7 | showdown NE@CHI | CPT | v4 | 23:50: 1.48 / 0.961 | 23:20: 1.48 / 0.961 | 22:30: 1.37 / 0.957 |
| 7 | showdown NE@CHI | FLEX | v3 | 23:50: 3.91 / 0.930 | 23:20: 3.91 / 0.930 | 22:30: 3.65 / 0.934 |
| 7 | showdown NE@CHI | FLEX | v4 | 23:50: 4.02 / 0.924 | 23:20: 4.02 / 0.924 | 22:30: 3.77 / 0.931 |
| 8 | classic 11 games | overall | v3 | - | 16:12: 1.67 / 0.934 | 16:55: 1.69 / 0.940 |
| 8 | classic 11 games | overall | v4 | - | 16:12: 1.67 / 0.935 | 16:55: 1.69 / 0.941 |
| 8 | showdown CAR@GB | CPT | v3 | 23:50: 1.72 / 0.939 | 22:50: 1.65 / 0.935 | 23:10: 1.72 / 0.939 |
| 8 | showdown CAR@GB | CPT | v4 | 23:50: 1.80 / 0.926 | 22:50: 1.71 / 0.922 | 23:10: 1.80 / 0.926 |
| 8 | showdown CAR@GB | FLEX | v3 | 23:50: 6.30 / 0.959 | 22:50: 5.40 / 0.913 | 23:10: 6.30 / 0.959 |
| 8 | showdown CAR@GB | FLEX | v4 | 23:50: 5.75 / 0.890 | 22:50: 5.20 / 0.834 | 23:10: 5.75 / 0.890 |
| 8 | showdown CHI@SEA | CPT | v3 | 00:50: 2.04 / 0.823 | 23:46: 2.02 / 0.809 | 00:10: 1.97 / 0.832 |
| 8 | showdown CHI@SEA | CPT | v4 | 00:50: 2.23 / 0.860 | 23:46: 2.24 / 0.828 | 00:10: 2.16 / 0.859 |
| 8 | showdown CHI@SEA | FLEX | v3 | 00:50: 8.21 / 0.894 | 23:46: 7.98 / 0.886 | 00:10: 7.95 / 0.891 |
| 8 | showdown CHI@SEA | FLEX | v4 | 00:50: 8.09 / 0.858 | 23:46: 7.99 / 0.878 | 00:10: 7.84 / 0.846 |
| 8 | showdown PHI@WAS | CPT | v3 | 00:55: 1.19 / 0.916 | 22:43: 1.15 / 0.911 | 00:10: 1.20 / 0.916 |
| 8 | showdown PHI@WAS | CPT | v4 | 00:55: 1.22 / 0.896 | 22:43: 1.17 / 0.894 | 00:10: 1.23 / 0.898 |
| 8 | showdown PHI@WAS | FLEX | v3 | 00:55: 3.98 / 0.931 | 22:43: 4.13 / 0.931 | 00:10: 3.90 / 0.932 |
| 8 | showdown PHI@WAS | FLEX | v4 | 00:55: 5.24 / 0.804 | 22:43: 5.43 / 0.799 | 00:10: 5.16 / 0.797 |
| 9 | showdown JAX@BAL | CPT | v3 | 00:50: 1.76 / 0.870 | 00:50: 1.76 / 0.870 | 00:10: 1.76 / 0.870 |
| 9 | showdown JAX@BAL | CPT | v4 | 00:50: 1.91 / 0.862 | 00:50: 1.91 / 0.862 | 00:10: 1.91 / 0.862 |
| 9 | showdown JAX@BAL | FLEX | v3 | 00:50: 7.26 / 0.916 | 00:50: 7.26 / 0.916 | 00:10: 7.26 / 0.916 |
| 9 | showdown JAX@BAL | FLEX | v4 | 00:50: 7.80 / 0.830 | 00:50: 7.80 / 0.830 | 00:10: 7.80 / 0.830 |

## 4. Diagnostics (outcome-defined, never a gate) — reference, canonical, primary, summed over slates

| format | slot | population | v3 n | v3 MAE | v3 bias | v4 MAE | v4 bias |
|---|---|---|---:|---:|---:|---:|---:|
| classic | overall | actual_ge_1 | 627 | 4.62 | -0.38 | 4.61 | -0.37 |
| classic | overall | position:DST | 116 | 3.98 | +0.00 | 3.98 | +0.00 |
| classic | overall | position:QB | 227 | 1.41 | +0.00 | 1.41 | +0.00 |
| classic | overall | position:RB | 556 | 1.40 | +0.18 | 1.33 | -0.04 |
| classic | overall | position:TE | 561 | 0.75 | -0.23 | 0.83 | +0.03 |
| classic | overall | position:WR | 884 | 1.36 | +0.04 | 1.35 | +0.02 |
| classic | overall | pred_chalk_ge_15 | 65 | 13.20 | +9.79 | 12.54 | +9.53 |
| classic | overall | salary_tier:0-3k | 512 | 0.58 | -0.07 | 0.59 | -0.06 |
| classic | overall | salary_tier:3-4k | 767 | 0.66 | -0.12 | 0.67 | -0.07 |
| classic | overall | salary_tier:4-5k | 683 | 0.69 | -0.25 | 0.68 | -0.19 |
| classic | overall | salary_tier:5-6k | 192 | 2.94 | -0.88 | 2.95 | -0.90 |
| classic | overall | salary_tier:6-7k | 119 | 5.13 | -0.46 | 5.17 | -0.72 |
| classic | overall | salary_tier:7-8k | 48 | 8.13 | +4.94 | 8.29 | +4.65 |
| classic | overall | salary_tier:8k+ | 23 | 15.03 | +12.88 | 13.78 | +11.31 |
| showdown | CPT | actual_ge_1 | 215 | 3.82 | -0.05 | 4.08 | +0.07 |
| showdown | CPT | position:DST | 32 | 1.21 | -1.18 | 1.61 | -1.61 |
| showdown | CPT | position:K | 34 | 0.98 | +0.91 | 0.36 | -0.32 |
| showdown | CPT | position:QB | 34 | 6.25 | +5.16 | 7.12 | +6.08 |
| showdown | CPT | position:RB | 124 | 1.59 | -0.45 | 1.63 | -0.25 |
| showdown | CPT | position:TE | 125 | 0.73 | -0.63 | 0.86 | -0.83 |
| showdown | CPT | position:WR | 218 | 1.45 | -0.14 | 1.48 | -0.02 |
| showdown | CPT | pred_chalk_ge_10 | 58 | 7.14 | +5.48 | 7.95 | +6.73 |
| showdown | CPT | salary_tier:0-2.5k | 251 | 0.10 | -0.05 | 0.10 | -0.05 |
| showdown | CPT | salary_tier:2.5-3k | 17 | 0.56 | -0.53 | 0.57 | -0.55 |
| showdown | CPT | salary_tier:3-4k | 57 | 0.63 | -0.36 | 0.68 | -0.39 |
| showdown | CPT | salary_tier:4-5k | 55 | 1.15 | -0.72 | 1.14 | -1.07 |
| showdown | CPT | salary_tier:5-6k | 39 | 1.41 | -0.06 | 1.20 | -0.95 |
| showdown | CPT | salary_tier:6-7k | 17 | 2.10 | -2.09 | 2.33 | -2.32 |
| showdown | CPT | salary_tier:7-8k | 29 | 2.27 | -1.79 | 2.36 | -2.11 |
| showdown | CPT | salary_tier:8k+ | 102 | 5.88 | +1.72 | 6.36 | +2.41 |
| showdown | FLEX | actual_ge_1 | 412 | 8.39 | -0.26 | 7.95 | -1.04 |
| showdown | FLEX | position:DST | 34 | 6.69 | -5.10 | 6.86 | -5.56 |
| showdown | FLEX | position:K | 34 | 7.02 | -1.54 | 10.10 | -9.75 |
| showdown | FLEX | position:QB | 34 | 13.61 | +8.75 | 11.31 | +6.23 |
| showdown | FLEX | position:RB | 150 | 5.80 | +1.53 | 5.90 | +1.75 |
| showdown | FLEX | position:TE | 163 | 3.37 | -1.63 | 3.62 | -0.38 |
| showdown | FLEX | position:WR | 251 | 5.27 | +0.10 | 5.31 | +0.69 |
| showdown | FLEX | pred_chalk_ge_25 | 122 | 13.93 | +12.06 | 12.32 | +10.14 |
| showdown | FLEX | salary_tier:0-2.5k | 345 | 1.56 | -0.97 | 2.19 | +0.91 |
| showdown | FLEX | salary_tier:2.5-3k | 18 | 6.87 | -6.49 | 4.94 | -2.57 |
| showdown | FLEX | salary_tier:3-4k | 60 | 6.27 | -4.20 | 6.67 | -5.13 |
| showdown | FLEX | salary_tier:4-5k | 56 | 7.93 | -6.51 | 9.11 | -8.48 |
| showdown | FLEX | salary_tier:5-6k | 39 | 8.00 | -2.25 | 9.47 | -7.07 |
| showdown | FLEX | salary_tier:6-7k | 17 | 5.23 | -4.04 | 6.36 | -5.73 |
| showdown | FLEX | salary_tier:7-8k | 29 | 8.27 | +2.90 | 7.63 | +0.98 |
| showdown | FLEX | salary_tier:8k+ | 102 | 15.15 | +11.75 | 12.89 | +9.05 |

Note: the predicted-chalk rows are selected on each version's own prediction, so their n differ by version.

### Calibration by predicted bin (summed over reference slates with complete labels, canonical, primary)

| format | slot | version | bin | n | mean predicted | mean actual |
|---|---|---|---|---:|---:|---:|
| classic | overall | v3 | 0-1 | 1702 | 0.11 | 0.21 |
| classic | overall | v3 | 1-5 | 382 | 2.50 | 3.67 |
| classic | overall | v3 | 5-10 | 131 | 6.82 | 7.65 |
| classic | overall | v3 | 10-20 | 90 | 13.69 | 11.34 |
| classic | overall | v3 | 20-35 | 31 | 26.80 | 17.29 |
| classic | overall | v3 | 35+ | 8 | 49.08 | 22.27 |
| classic | overall | v4 | 0-1 | 1703 | 0.12 | 0.20 |
| classic | overall | v4 | 1-5 | 375 | 2.53 | 3.66 |
| classic | overall | v4 | 5-10 | 140 | 6.87 | 7.90 |
| classic | overall | v4 | 10-20 | 87 | 13.92 | 11.02 |
| classic | overall | v4 | 20-35 | 31 | 26.04 | 17.13 |
| classic | overall | v4 | 35+ | 8 | 46.51 | 22.27 |
| showdown | CPT | v3 | 0-1 | 395 | 0.16 | 0.49 |
| showdown | CPT | v3 | 1-5 | 86 | 2.34 | 4.33 |
| showdown | CPT | v3 | 5-10 | 28 | 7.15 | 7.52 |
| showdown | CPT | v3 | 10-20 | 35 | 13.60 | 11.80 |
| showdown | CPT | v3 | 20-35 | 17 | 28.18 | 18.25 |
| showdown | CPT | v3 | 35+ | 6 | 46.35 | 32.03 |
| showdown | CPT | v4 | 0-1 | 414 | 0.14 | 0.53 |
| showdown | CPT | v4 | 1-5 | 69 | 2.38 | 5.09 |
| showdown | CPT | v4 | 5-10 | 27 | 7.25 | 8.27 |
| showdown | CPT | v4 | 10-20 | 31 | 13.87 | 11.20 |
| showdown | CPT | v4 | 20-35 | 19 | 27.74 | 18.03 |
| showdown | CPT | v4 | 35+ | 7 | 46.17 | 29.55 |
| showdown | FLEX | v3 | 0-1 | 296 | 0.18 | 1.40 |
| showdown | FLEX | v3 | 1-5 | 102 | 2.78 | 6.79 |
| showdown | FLEX | v3 | 5-10 | 59 | 7.06 | 12.20 |
| showdown | FLEX | v3 | 10-20 | 64 | 14.42 | 18.76 |
| showdown | FLEX | v3 | 20-35 | 50 | 25.97 | 26.75 |
| showdown | FLEX | v3 | 35-50 | 31 | 43.44 | 35.85 |
| showdown | FLEX | v3 | 50+ | 64 | 65.19 | 46.21 |
| showdown | FLEX | v4 | 0-1 | 145 | 0.19 | 1.25 |
| showdown | FLEX | v4 | 1-5 | 208 | 2.89 | 3.81 |
| showdown | FLEX | v4 | 5-10 | 113 | 7.13 | 9.38 |
| showdown | FLEX | v4 | 10-20 | 71 | 14.48 | 19.93 |
| showdown | FLEX | v4 | 20-35 | 40 | 26.36 | 28.02 |
| showdown | FLEX | v4 | 35-50 | 34 | 43.01 | 35.29 |
| showdown | FLEX | v4 | 50+ | 55 | 63.93 | 48.44 |

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
| 6 | showdown WAS@SF | CPT | DST | 2.7 | 1.0 | 0.3 |
| 6 | showdown WAS@SF | CPT | K | 1.1 | 1.8 | 0.5 |
| 6 | showdown WAS@SF | CPT | QB | 33.1 | 54.4 | 57.4 |
| 6 | showdown WAS@SF | CPT | RB | 22.5 | 22.3 | 23.5 |
| 6 | showdown WAS@SF | CPT | TE | 20.9 | 10.6 | 7.8 |
| 6 | showdown WAS@SF | CPT | WR | 19.6 | 9.9 | 10.5 |
| 6 | showdown WAS@SF | FLEX | DST | 20.2 | 17.6 | 18.0 |
| 6 | showdown WAS@SF | FLEX | K | 36.4 | 27.6 | 15.1 |
| 6 | showdown WAS@SF | FLEX | QB | 112.0 | 135.6 | 132.6 |
| 6 | showdown WAS@SF | FLEX | RB | 128.5 | 122.6 | 129.8 |
| 6 | showdown WAS@SF | FLEX | TE | 70.7 | 79.1 | 81.0 |
| 6 | showdown WAS@SF | FLEX | WR | 131.5 | 117.5 | 123.5 |
| 7 | classic 10 games | overall | DST | 100.0 | 100.0 | 100.0 |
| 7 | classic 10 games | overall | QB | 99.8 | 100.0 | 100.0 |
| 7 | classic 10 games | overall | RB | 226.0 | 255.0 | 230.1 |
| 7 | classic 10 games | overall | TE | 140.6 | 105.0 | 135.0 |
| 7 | classic 10 games | overall | WR | 331.3 | 340.0 | 334.9 |
| 7 | showdown DAL@PHI | CPT | DST | 2.5 | 0.1 | 0.0 |
| 7 | showdown DAL@PHI | CPT | K | 1.3 | 1.6 | 0.4 |
| 7 | showdown DAL@PHI | CPT | QB | 16.4 | 27.6 | 28.7 |
| 7 | showdown DAL@PHI | CPT | RB | 24.1 | 15.7 | 16.3 |
| 7 | showdown DAL@PHI | CPT | TE | 9.6 | 8.0 | 5.8 |
| 7 | showdown DAL@PHI | CPT | WR | 45.9 | 46.7 | 48.6 |
| 7 | showdown DAL@PHI | FLEX | DST | 23.1 | 4.1 | 3.0 |
| 7 | showdown DAL@PHI | FLEX | K | 36.1 | 21.1 | 11.3 |
| 7 | showdown DAL@PHI | FLEX | QB | 98.6 | 114.0 | 108.7 |
| 7 | showdown DAL@PHI | FLEX | RB | 78.8 | 106.8 | 111.1 |
| 7 | showdown DAL@PHI | FLEX | TE | 71.2 | 66.8 | 81.0 |
| 7 | showdown DAL@PHI | FLEX | WR | 188.8 | 187.2 | 184.9 |
| 7 | showdown NE@CHI | CPT | DST | 1.5 | 1.7 | 0.5 |
| 7 | showdown NE@CHI | CPT | K | 0.7 | 2.0 | 0.5 |
| 7 | showdown NE@CHI | CPT | QB | 33.6 | 54.9 | 57.4 |
| 7 | showdown NE@CHI | CPT | RB | 21.2 | 19.1 | 19.9 |
| 7 | showdown NE@CHI | CPT | TE | 10.1 | 4.9 | 3.6 |
| 7 | showdown NE@CHI | CPT | WR | 32.9 | 17.3 | 18.1 |
| 7 | showdown NE@CHI | FLEX | DST | 16.8 | 22.2 | 21.0 |
| 7 | showdown NE@CHI | FLEX | K | 28.9 | 24.5 | 13.7 |
| 7 | showdown NE@CHI | FLEX | QB | 122.7 | 135.1 | 132.7 |
| 7 | showdown NE@CHI | FLEX | RB | 96.4 | 122.5 | 125.5 |
| 7 | showdown NE@CHI | FLEX | TE | 67.8 | 49.7 | 56.5 |
| 7 | showdown NE@CHI | FLEX | WR | 166.9 | 146.0 | 150.7 |
| 8 | classic 11 games | overall | DST | 99.9 | 100.0 | 100.0 |
| 8 | classic 11 games | overall | QB | 99.8 | 100.0 | 100.0 |
| 8 | classic 11 games | overall | RB | 237.2 | 255.0 | 230.0 |
| 8 | classic 11 games | overall | TE | 125.0 | 105.0 | 135.0 |
| 8 | classic 11 games | overall | WR | 336.8 | 340.0 | 335.0 |
| 8 | showdown CAR@GB | CPT | DST | 2.0 | 0.6 | 0.2 |
| 8 | showdown CAR@GB | CPT | K | 0.8 | 1.6 | 0.4 |
| 8 | showdown CAR@GB | CPT | QB | 14.4 | 33.2 | 34.2 |
| 8 | showdown CAR@GB | CPT | RB | 37.8 | 22.6 | 23.3 |
| 8 | showdown CAR@GB | CPT | TE | 8.0 | 3.8 | 2.7 |
| 8 | showdown CAR@GB | CPT | WR | 36.8 | 38.0 | 39.1 |
| 8 | showdown CAR@GB | FLEX | DST | 25.7 | 9.4 | 9.5 |
| 8 | showdown CAR@GB | FLEX | K | 36.8 | 17.8 | 9.8 |
| 8 | showdown CAR@GB | FLEX | QB | 102.9 | 134.0 | 122.6 |
| 8 | showdown CAR@GB | FLEX | RB | 96.2 | 111.7 | 115.1 |
| 8 | showdown CAR@GB | FLEX | TE | 72.9 | 34.3 | 54.4 |
| 8 | showdown CAR@GB | FLEX | WR | 164.4 | 192.8 | 188.7 |
| 8 | showdown CHI@SEA | CPT | DST | 4.1 | 1.7 | 0.5 |
| 8 | showdown CHI@SEA | CPT | K | 2.2 | 3.4 | 0.9 |
| 8 | showdown CHI@SEA | CPT | QB | 13.7 | 24.6 | 26.2 |
| 8 | showdown CHI@SEA | CPT | RB | 16.1 | 11.7 | 12.5 |
| 8 | showdown CHI@SEA | CPT | TE | 10.0 | 2.8 | 2.1 |
| 8 | showdown CHI@SEA | CPT | WR | 53.1 | 55.7 | 57.9 |
| 8 | showdown CHI@SEA | FLEX | DST | 36.9 | 28.4 | 27.3 |
| 8 | showdown CHI@SEA | FLEX | K | 50.1 | 44.8 | 24.2 |
| 8 | showdown CHI@SEA | FLEX | QB | 97.6 | 142.4 | 135.1 |
| 8 | showdown CHI@SEA | FLEX | RB | 76.9 | 106.3 | 103.1 |
| 8 | showdown CHI@SEA | FLEX | TE | 74.9 | 40.2 | 52.7 |
| 8 | showdown CHI@SEA | FLEX | WR | 154.7 | 137.9 | 157.5 |
| 8 | showdown PHI@WAS | CPT | DST | 1.9 | 0.4 | 0.1 |
| 8 | showdown PHI@WAS | CPT | K | 0.8 | 2.9 | 0.8 |
| 8 | showdown PHI@WAS | CPT | QB | 42.7 | 58.3 | 59.4 |
| 8 | showdown PHI@WAS | CPT | RB | 18.0 | 9.2 | 10.4 |
| 8 | showdown PHI@WAS | CPT | TE | 12.8 | 10.7 | 8.4 |
| 8 | showdown PHI@WAS | CPT | WR | 23.4 | 18.4 | 20.8 |
| 8 | showdown PHI@WAS | FLEX | DST | 31.5 | 9.7 | 10.5 |
| 8 | showdown PHI@WAS | FLEX | K | 42.7 | 35.4 | 19.4 |
| 8 | showdown PHI@WAS | FLEX | QB | 103.1 | 104.8 | 99.6 |
| 8 | showdown PHI@WAS | FLEX | RB | 90.2 | 92.5 | 93.4 |
| 8 | showdown PHI@WAS | FLEX | TE | 78.4 | 87.8 | 101.8 |
| 8 | showdown PHI@WAS | FLEX | WR | 151.7 | 169.9 | 175.3 |
| 9 | showdown JAX@BAL | CPT | DST | 10.9 | 1.3 | 0.4 |
| 9 | showdown JAX@BAL | CPT | K | 1.6 | 3.2 | 0.8 |
| 9 | showdown JAX@BAL | CPT | QB | 25.1 | 33.3 | 34.8 |
| 9 | showdown JAX@BAL | CPT | RB | 30.6 | 29.9 | 31.3 |
| 9 | showdown JAX@BAL | CPT | TE | 4.7 | 3.9 | 2.9 |
| 9 | showdown JAX@BAL | CPT | WR | 26.6 | 28.2 | 29.6 |
| 9 | showdown JAX@BAL | FLEX | DST | 43.8 | 16.5 | 18.2 |
| 9 | showdown JAX@BAL | FLEX | K | 30.5 | 26.9 | 14.8 |
| 9 | showdown JAX@BAL | FLEX | QB | 84.2 | 62.2 | 60.6 |
| 9 | showdown JAX@BAL | FLEX | RB | 137.3 | 171.8 | 166.0 |
| 9 | showdown JAX@BAL | FLEX | TE | 60.3 | 43.1 | 59.7 |
| 9 | showdown JAX@BAL | FLEX | WR | 130.1 | 179.6 | 180.7 |

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
| 6 | showdown WAS@SF | Eddy Pineiro | K | 5,200 | 20.9 | 12.3 | 6.7 | no | - / - | EXPECTED_ACTIVE | +0.3 | not_design |
| 6 | showdown WAS@SF | Rachaad White | RB | 6,600 | 34.2 | 25.5 | 23.2 | no | - / - | EXPECTED_ACTIVE | +3.9 | not_design |
| 6 | showdown WAS@SF | Austin Ekeler | RB | 3,600 | 20.5 | 5.4 | 4.9 | no | - / - | EXPECTED_ACTIVE | +2.3 | not_design |
| 6 | showdown WAS@SF | Chig Okonkwo | TE | 4,600 | 19.5 | 8.2 | 7.5 | no | - / - | EXPECTED_ACTIVE | +1.5 | not_design |
| 6 | showdown WAS@SF | Brandin Cooks | WR | 4,000 | 21.4 | 6.2 | 5.7 | no | - / - | EXPECTED_ACTIVE | +2.7 | not_design |
| 7 | showdown NE@CHI | Cairo Santos | K | 5,000 | 17.2 | 12.3 | 6.9 | no | - / - | EXPECTED_ACTIVE | -0.6 | not_design |
| 7 | showdown NE@CHI | Roschon Johnson | RB | 4,400 | 19.0 | 7.1 | 6.6 | no | - / - | EXPECTED_ACTIVE | +2.3 | not_design |
| 7 | showdown NE@CHI | Kyle Williams | WR | 1,800 | 17.9 | 4.0 | 6.8 | no | - / - | EXPECTED_ACTIVE | -0.2 | not_design |
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
| 8 | showdown CAR@GB | Packers | DST | 4,000 | 18.8 | 6.3 | 7.5 | no | - / - | EXPECTED_ACTIVE | +0.2 | not_design |
| 8 | showdown CAR@GB | Trey Smack | K | 5,000 | 18.8 | 8.3 | 4.5 | no | - / - | EXPECTED_ACTIVE | -0.0 | not_design |
| 8 | showdown CAR@GB | Ryan Fitzgerald | K | 4,800 | 18.1 | 9.6 | 5.2 | no | - / - | EXPECTED_ACTIVE | -1.4 | not_design |
| 8 | showdown CAR@GB | Tommy Tremble | TE | 2,600 | 19.4 | 5.9 | 14.0 | no | - / - | EXPECTED_ACTIVE | -1.5 | not_design |
| 8 | showdown CAR@GB | Savion Williams | WR | 4,400 | 24.6 | 8.5 | 7.8 | no | - / - | EXPECTED_ACTIVE | -1.1 | not_design |
| 8 | classic 11 games | Broncos | DST | 2,300 | 19.6 | 9.2 | 9.2 | no | - / - | EXPECTED_ACTIVE | +0.3 | not_design |
| 8 | classic 11 games | Rhamondre Stevenson | RB | 5,600 | 34.4 | 10.1 | 9.1 | no | - / - | EXPECTED_ACTIVE | +4.4 | not_design |
| 8 | classic 11 games | Bhayshul Tuten | RB | 5,900 | 23.4 | 9.6 | 8.6 | no | - / - | EXPECTED_ACTIVE | +1.1 | not_design |
| 8 | classic 11 games | Jonah Coleman | RB | 4,300 | 15.0 | 4.8 | 4.3 | no | - / - | EXPECTED_ACTIVE | +5.1 | not_design |
| 8 | classic 11 games | Hunter Henry | TE | 4,300 | 21.1 | 5.6 | 7.2 | no | - / - | EXPECTED_ACTIVE | +3.0 | not_design |
| 8 | classic 11 games | Omar Cooper Jr. | WR | 4,800 | 21.6 | 9.2 | 9.1 | no | - / - | EXPECTED_ACTIVE | -8.0 | not_design |
| 8 | showdown PHI@WAS | Commanders | DST | 4,000 | 24.5 | 7.8 | 9.3 | no | - / - | UNKNOWN | +0.4 | not_design |
| 8 | showdown PHI@WAS | Drew Stevens | K | 4,600 | 29.8 | 26.5 | 14.5 | no | - / - | EXPECTED_ACTIVE | -0.1 | not_design |
| 8 | showdown CHI@SEA | Jason Myers | K | 5,400 | 27.0 | 25.2 | 13.7 | no | - / - | EXPECTED_ACTIVE | +0.6 | not_design |
| 8 | showdown CHI@SEA | Cairo Santos | K | 5,000 | 23.1 | 19.6 | 10.6 | no | - / - | EXPECTED_ACTIVE | -0.0 | not_design |
| 8 | showdown CHI@SEA | Zach Charbonnet | RB | 7,000 | 16.2 | 3.8 | 3.4 | no | - / - | EXPECTED_ACTIVE | +8.3 | not_design |
| 8 | showdown CHI@SEA | AJ Barner | TE | 4,600 | 29.9 | 14.2 | 12.8 | no | - / - | EXPECTED_ACTIVE | +2.6 | not_design |
| 8 | showdown CHI@SEA | Sam Roush | TE | 1,200 | 12.1 | 0.9 | 9.8 | no | - / - | EXPECTED_ACTIVE | +2.4 | not_design |
| 8 | showdown CHI@SEA | Cooper Kupp | WR | 4,800 | 22.3 | 11.8 | 10.7 | no | - / - | EXPECTED_ACTIVE | +1.6 | not_design |
| 8 | showdown CHI@SEA | Tory Horton | WR | 2,800 | 25.9 | 5.0 | 19.6 | no | - / - | EXPECTED_ACTIVE | +2.8 | not_design |
| 9 | showdown JAX@BAL | Jaguars | DST | 5,800 | 34.3 | 13.9 | 16.5 | no | - / - | EXPECTED_ACTIVE | -3.1 | not_design |
| 9 | showdown JAX@BAL | Cam Little | K | 5,400 | 30.5 | 26.9 | 14.8 | no | - / - | EXPECTED_ACTIVE | +0.1 | not_design |
| 9 | showdown JAX@BAL | Joe Fagnano | QB | 8,400 | 19.7 | 0.4 | 0.4 | no | - / - | EXPECTED_ACTIVE | - | not_design |
| 9 | showdown JAX@BAL | Chris Rodriguez Jr. | RB | 3,400 | 30.6 | 16.0 | 14.7 | no | - / - | EXPECTED_ACTIVE | +0.8 | not_design |
| 9 | showdown JAX@BAL | Justice Hill | RB | 3,200 | 33.3 | 16.4 | 15.1 | no | - / - | EXPECTED_ACTIVE | +2.2 | not_design |

140 player-slates by position: DST 14, K 19, QB 5, RB 31, TE 30, WR 41. With an absence commit 3; DK status changed between early and canonical 0; ourProj at least 2 above DK average 45.

Shape count over 140 player-slates: cheap (< $5,000 Classic / < $4,500 Showdown) AND a news/role marker (absence commit, ourProj at least 2 above DK average, or DK status changed between early and canonical) 24; cheap without a marker 44; not cheap with a marker 23; neither 49. The marker is a crude proxy: role growth that the projection did not absorb (e.g. Cooks/Ekeler on WAS@SF, ourProj close to DK average) does not show in it.

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
| 6 | showdown WAS@SF | DST | 2.7 | 1.0 | 0.3 | not_design |
| 6 | showdown WAS@SF | K | 1.1 | 1.8 | 0.5 | not_design |
| 6 | showdown WAS@SF | QB | 33.1 | 54.4 | 57.4 | not_design |
| 6 | showdown WAS@SF | RB | 22.5 | 22.3 | 23.5 | not_design |
| 6 | showdown WAS@SF | TE | 20.9 | 10.6 | 7.8 | not_design |
| 6 | showdown WAS@SF | WR | 19.6 | 9.9 | 10.5 | not_design |
| 7 | showdown DAL@PHI | DST | 2.5 | 0.1 | 0.0 | not_design |
| 7 | showdown DAL@PHI | K | 1.3 | 1.6 | 0.4 | not_design |
| 7 | showdown DAL@PHI | QB | 16.4 | 27.6 | 28.7 | not_design |
| 7 | showdown DAL@PHI | RB | 24.1 | 15.7 | 16.3 | not_design |
| 7 | showdown DAL@PHI | TE | 9.6 | 8.0 | 5.8 | not_design |
| 7 | showdown DAL@PHI | WR | 45.9 | 46.7 | 48.6 | not_design |
| 7 | showdown NE@CHI | DST | 1.5 | 1.7 | 0.5 | not_design |
| 7 | showdown NE@CHI | K | 0.7 | 2.0 | 0.5 | not_design |
| 7 | showdown NE@CHI | QB | 33.6 | 54.9 | 57.4 | not_design |
| 7 | showdown NE@CHI | RB | 21.2 | 19.1 | 19.9 | not_design |
| 7 | showdown NE@CHI | TE | 10.1 | 4.9 | 3.6 | not_design |
| 7 | showdown NE@CHI | WR | 32.9 | 17.3 | 18.1 | not_design |
| 8 | showdown CAR@GB | DST | 2.0 | 0.6 | 0.2 | not_design |
| 8 | showdown CAR@GB | K | 0.8 | 1.6 | 0.4 | not_design |
| 8 | showdown CAR@GB | QB | 14.4 | 33.2 | 34.2 | not_design |
| 8 | showdown CAR@GB | RB | 37.8 | 22.6 | 23.3 | not_design |
| 8 | showdown CAR@GB | TE | 8.0 | 3.8 | 2.7 | not_design |
| 8 | showdown CAR@GB | WR | 36.8 | 38.0 | 39.1 | not_design |
| 8 | showdown CHI@SEA | DST | 4.1 | 1.7 | 0.5 | not_design |
| 8 | showdown CHI@SEA | K | 2.2 | 3.4 | 0.9 | not_design |
| 8 | showdown CHI@SEA | QB | 13.7 | 24.6 | 26.2 | not_design |
| 8 | showdown CHI@SEA | RB | 16.1 | 11.7 | 12.5 | not_design |
| 8 | showdown CHI@SEA | TE | 10.0 | 2.8 | 2.1 | not_design |
| 8 | showdown CHI@SEA | WR | 53.1 | 55.7 | 57.9 | not_design |
| 8 | showdown PHI@WAS | DST | 1.9 | 0.4 | 0.2 | not_design |
| 8 | showdown PHI@WAS | K | 0.8 | 2.9 | 0.8 | not_design |
| 8 | showdown PHI@WAS | QB | 42.7 | 58.3 | 59.4 | not_design |
| 8 | showdown PHI@WAS | RB | 18.0 | 9.2 | 10.4 | not_design |
| 8 | showdown PHI@WAS | TE | 12.8 | 10.7 | 8.4 | not_design |
| 8 | showdown PHI@WAS | WR | 23.4 | 18.4 | 20.8 | not_design |
| 9 | showdown JAX@BAL | DST | 10.9 | 1.3 | 0.4 | not_design |
| 9 | showdown JAX@BAL | K | 1.6 | 3.2 | 0.8 | not_design |
| 9 | showdown JAX@BAL | QB | 25.1 | 33.3 | 34.8 | not_design |
| 9 | showdown JAX@BAL | RB | 30.6 | 29.9 | 31.3 | not_design |
| 9 | showdown JAX@BAL | TE | 4.7 | 3.9 | 2.9 | not_design |
| 9 | showdown JAX@BAL | WR | 26.6 | 28.2 | 29.6 | not_design |

Totals over 17 Showdown slates (sum of captain % across slates; divide by slates for a per-slate share):

| position | scope | actual | v3 | v4 |
|---|---|---:|---:|---:|
| DST | all | 57.5 | 19.6 | 5.9 |
| DST | week6 | 30.5 | 11.3 | 3.4 |
| K | all | 26.9 | 57.8 | 15.9 |
| K | week6 | 11.3 | 23.5 | 6.5 |
| QB | all | 394.6 | 570.2 | 601.5 |
| QB | week6 | 192.7 | 306.7 | 322.8 |
| RB | all | 463.0 | 407.8 | 432.3 |
| RB | week6 | 194.8 | 140.3 | 149.0 |
| TE | all | 181.9 | 103.3 | 77.8 |
| TE | week6 | 80.5 | 46.5 | 34.9 |
| WR | all | 568.2 | 537.9 | 564.0 |
| WR | week6 | 287.7 | 271.1 | 282.7 |

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
| 6 | showdown WAS@SF | Eddy Pineiro | 5,200 | 20.9 | 12.3 | 6.7 | not_design |
| 6 | showdown WAS@SF | Drew Stevens | 5,000 | 15.6 | 15.3 | 8.4 | not_design |
| 7 | showdown NE@CHI | Cairo Santos | 5,000 | 17.2 | 12.3 | 6.9 | not_design |
| 7 | showdown NE@CHI | Andy Borregales | 4,800 | 11.7 | 12.2 | 6.8 | not_design |
| 7 | showdown DAL@PHI | Brandon Aubrey | 5,400 | 21.2 | 14.8 | 7.9 | not_design |
| 7 | showdown DAL@PHI | Jake Elliott | 4,800 | 14.9 | 6.3 | 3.4 | not_design |
| 8 | showdown CAR@GB | Trey Smack | 5,000 | 18.8 | 8.3 | 4.5 | not_design |
| 8 | showdown CAR@GB | Ryan Fitzgerald | 4,800 | 18.1 | 9.6 | 5.2 | not_design |
| 8 | showdown PHI@WAS | Jake Elliott | 5,000 | 12.9 | 8.9 | 4.9 | not_design |
| 8 | showdown PHI@WAS | Drew Stevens | 4,600 | 29.8 | 26.5 | 14.5 | not_design |
| 8 | showdown CHI@SEA | Jason Myers | 5,400 | 27.0 | 25.2 | 13.7 | not_design |
| 8 | showdown CHI@SEA | Cairo Santos | 5,000 | 23.1 | 19.6 | 10.6 | not_design |
| 9 | showdown JAX@BAL | Cam Little | 5,400 | 30.5 | 26.9 | 14.8 | not_design |

34 kickers: mean actual 20.5, v3 18.9, v4 10.7; MAE v3 7.0, v4 10.1.

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
| 6 | showdown WAS@SF | CPT | v3 | 88.8 | 76.4 | +2.48 | -0.37 |
| 6 | showdown WAS@SF | CPT | v4 | 90.4 | 76.4 | +2.81 | -0.42 |
| 6 | showdown WAS@SF | FLEX | v3 | 325.3 | 230.2 | +19.01 | -2.36 |
| 6 | showdown WAS@SF | FLEX | v4 | 311.0 | 230.2 | +16.16 | -2.00 |
| 7 | classic 10 games | overall | v3 | 165.0 | 125.0 | +8.01 | -0.10 |
| 7 | classic 10 games | overall | v4 | 153.7 | 125.0 | +5.75 | -0.07 |
| 7 | showdown DAL@PHI | CPT | v3 | 81.3 | 64.6 | +3.34 | -0.67 |
| 7 | showdown DAL@PHI | CPT | v4 | 82.9 | 61.6 | +4.27 | -0.85 |
| 7 | showdown DAL@PHI | FLEX | v3 | 309.0 | 228.6 | +16.09 | -2.33 |
| 7 | showdown DAL@PHI | FLEX | v4 | 288.3 | 228.6 | +11.94 | -1.71 |
| 7 | showdown NE@CHI | CPT | v3 | 79.5 | 68.9 | +2.12 | -0.36 |
| 7 | showdown NE@CHI | CPT | v4 | 83.0 | 68.9 | +2.82 | -0.49 |
| 7 | showdown NE@CHI | FLEX | v3 | 282.0 | 234.8 | +9.45 | -1.38 |
| 7 | showdown NE@CHI | FLEX | v4 | 269.6 | 234.8 | +6.97 | -1.01 |
| 8 | classic 11 games | overall | v3 | 151.4 | 83.5 | +13.59 | -0.20 |
| 8 | classic 11 games | overall | v4 | 146.7 | 83.5 | +12.64 | -0.19 |
| 8 | showdown CAR@GB | CPT | v3 | 67.5 | 57.9 | +1.92 | -0.39 |
| 8 | showdown CAR@GB | CPT | v4 | 69.4 | 57.9 | +2.31 | -0.47 |
| 8 | showdown CAR@GB | FLEX | v3 | 290.4 | 205.4 | +16.98 | -2.71 |
| 8 | showdown CAR@GB | FLEX | v4 | 265.7 | 205.4 | +12.05 | -1.91 |
| 8 | showdown CHI@SEA | CPT | v3 | 87.3 | 68.9 | +3.69 | -0.98 |
| 8 | showdown CHI@SEA | CPT | v4 | 91.4 | 68.9 | +4.51 | -1.21 |
| 8 | showdown CHI@SEA | FLEX | v3 | 305.4 | 209.7 | +19.16 | -3.77 |
| 8 | showdown CHI@SEA | FLEX | v4 | 285.1 | 209.7 | +15.10 | -2.89 |
| 8 | showdown PHI@WAS | CPT | v3 | 81.1 | 71.3 | +1.97 | -0.35 |
| 8 | showdown PHI@WAS | CPT | v4 | 82.4 | 71.3 | +2.22 | -0.40 |
| 8 | showdown PHI@WAS | FLEX | v3 | 272.7 | 236.0 | +7.33 | -1.27 |
| 8 | showdown PHI@WAS | FLEX | v4 | 253.1 | 236.0 | +3.41 | -0.54 |
| 9 | showdown JAX@BAL | CPT | v3 | 83.8 | 67.9 | +3.19 | -0.82 |
| 9 | showdown JAX@BAL | CPT | v4 | 87.8 | 67.9 | +3.99 | -1.03 |
| 9 | showdown JAX@BAL | FLEX | v3 | 327.4 | 213.7 | +22.75 | -3.70 |
| 9 | showdown JAX@BAL | FLEX | v4 | 303.7 | 213.7 | +18.00 | -2.82 |

Slates where the top 5 were over-predicted AND the rest under-predicted on average:

- classic overall v3: 7 of 7
- classic overall v4: 7 of 7
- showdown CPT v3: 17 of 17
- showdown CPT v4: 17 of 17
- showdown FLEX v3: 17 of 17
- showdown FLEX v4: 17 of 17

## 7. Reproduction checks

- **WAS@SF (contest 196786695), owner run `21bf2279` (canonical, created 23:13 UTC, T-62):** flex v3 MAE flex v3 5.11 / 0.895, flex v4 5.05 / 0.860, cpt v3 1.11 / 0.916, cpt v4 1.27 / 0.898. The pre-grade result (flex v3 5.11 / 0.895, v4 5.05 / 0.860; captain v3 1.11 / 0.916, v4 1.27 / 0.898) is reproduced from this source.
- **Same contest, canonical POOL capture (23:03 UTC, the upload-instant capture, code 1963fcd7, v3):** flex v3 5.98 / 0.867, flex v4 5.99 / 0.799, cpt v3 0.97 / 0.891, cpt v4 1.13 / 0.875. It differs because at 23:03 Mike Evans and Stefon Diggs carried no Q status (the run at 23:13 has Q for both) and Brandin Cooks / Jacob Cowing were not yet market-anchored; v3 then put Evans at 67.3% flex (run: 43.9%, actual 26.8%). The owner resolved the source question on 2026-10-20 (scorecard v3): the canonical forecast is the latest capture of any source, here the run. The v1/v2 preference pick is still pooled as `preferred`.
- **Week-4 Millionaire (196151357), frozen stored v2 forecast:** 183 observed-label players including 2 OUT in the input (Case Keenum 0.69%, DeVonta Smith 0.05% actual) give MAE 3.540 (the review's 3.538); the scorecard's active-player population is 181 at MAE 3.572 (n 181). The contest's labels are not complete (mass 0.99493 < 0.995), so it is not gate-eligible; the 629-player 1.065 figure zero-fills 446 unresolved labels and is not reproduced as a gate number.
- **Recomputation vs stored ownership:** 61 frozen inputs carried a stored forecast of a recomputable version; the largest absolute difference on any player was 0.000 percentage points.

