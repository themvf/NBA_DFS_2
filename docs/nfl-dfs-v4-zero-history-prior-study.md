# NFL DFS projection v4 — zero-history prior study (registered 2026-09-22)

Pre-registered before any week-4+ slate is graded. Implements WP1b of
[the 2026-09-22 implementation handoff](nfl-dfs-implementation-handoff-2026-09-22.md).
Nothing in this study changes production; `nfl-dfs-historical-v3` stays the
live model until the verdict below is PASS and the shadow study is re-pinned.

## The defect being tested

`model/nfl_dfs_historical.py::_peer_rows` builds a player's fallback from the
400 most recent stat rows of his position. Stat rows exist only for players
who recorded stats, so the "position prior" is a **starter** prior, and
`project_player` gives it full weight (`player_strength = 0`) for anyone with
fewer than two games. A backup with no history is projected as an average
starter.

Measured on the slate-scoped report card (`nfl-dfs-slate-report-v1`,
DraftKings' listed + game final + no stat line = 0 rule), weeks 1–2 of 2026,
`hist_0` cohort, actual − projected with weeks-clustered 95% intervals:

| position | n | bias | interval |
|---|---:|---:|---|
| QB | 63 | −14.0 | [−14.2, −13.6] |
| RB | 125 | −5.9 | [−6.6, −5.4] |
| WR | 245 | −5.2 | [−5.6, −4.9] |
| TE | 119 | −4.7 | [−5.1, −4.5] |

Those two weeks are the discovery sample. **They are inspected and are
excluded from the confirmation window.**

## Hypothesis (frozen)

**H — first-appearance cohort prior.** Replacing the starter-derived peer set
with the walk-forward *first-appearance* cohort — for every player in the
position, the stat line of his first recorded game before the cutoff —
reduces slate-scoped MAE for players with 0 prior games, without worsening
players with 6+ prior games.

Falsifiable prediction: on weeks 4–10 of 2026, the paired v4 − v3 MAE for
the `hist_0` cohort has a weeks-clustered 95% interval entirely below zero
at every skill position that reaches the sample floor, and the `hist_6_plus`
cohort's paired interval does not lie entirely above zero at any position.

## Construction (frozen — `model/nfl_dfs_zero_history_prior.py`)

| element | rule |
|---|---|
| model version | `nfl-dfs-historical-v4` |
| cohort | one row per other player in the position: his chronologically first `ff_player_week_stats` row strictly before the cutoff; at most the 400 most recent such rows |
| 0 prior games | resample cohort stat lines only (`player_strength = 0`), same environment adjustment and draw mechanics as v3 |
| 1+ prior games | **delegates to v3 unchanged**, so `hist_1_5` and `hist_6_plus` are identical by construction; both are still graded and reported |

**Rejected on the discovery weeks, before registration:** a first draft also
shrank 1–5 game players toward the cohort with `w = n/(n+4)`. On weeks 1–2
it made that cohort *worse* at every position (paired ΔMAE QB +0.18, RB
+0.52, WR +0.99, TE +0.83, all intervals above zero) while the 0-game gain
held. Players with a game or two of history are mostly rookies and new-role
players with real usage, not scratches; a first-game prior is too
pessimistic for them. The registered construction therefore touches 0-game
players only. Any prior for 1–5 game players is a separate study.
| K / DST | delegate to v3 unchanged |
| Sleeper depth stratification | **not used** in this registration (family stays at 1); recorded here so a stratified variant is visibly a new study |
| environment | the `team_implied_total` frozen in the production run's `feature_snapshot`, so v3 and v4 differ only in the prior |
| seed | the production run's seed |

## Population, grader, metrics (frozen)

- **Population:** every completed classic and showdown upload whose
  projection run is week 4–10 of 2026 (kickoffs from 2026-09-24 ET); one
  upload per slate signature (the latest). Rows with no `ff_player_id` or no
  projection row in the run are excluded and counted.
- **Grader:** `model/nfl_dfs_slate_reportcard.py::build_slate_report` with
  both v3 and v4 recomputed from the same point-in-time history
  (`ingest.nfl_dfs_projections._history` at the run's cutoff) and graded as
  alternative streams on the identical player set; the slate's stored
  projection is reported alongside as a sanity reference only.
- **Primary metric:** paired per-row MAE difference (v4 − v3), per position ×
  cohort, weeks-clustered bootstrap (2,000 draws, seed 20260922). Bias is
  reported alongside, not gated.
- **Floors (conjunctive):** ≥ 5 distinct weeks graded, and ≥ 40 scored
  `hist_0` rows at a position for that position to receive a verdict. A
  position below floor is reported as `insufficient`, never pooled into
  another position.
- **Verdict per position, computed once after week 10 is scorable:**
  - PASS: `hist_0` paired MAE interval upper bound < 0 **and** neither the
    `hist_1_5` nor the `hist_6_plus` interval has a lower bound above 0
    (both are identical to v3 by construction, so this is a tripwire
    against a construction drift, not an expected event).
  - FAIL: otherwise.
- **Promotion:** only if PASS at QB, RB, WR and TE. Ship path: bump
  `MODEL_VERSION`, re-pin the shadow study per the 2026-09-20 handoff, then
  flip production. A partial pass (some positions) is a FAIL for this
  registration; a position-specific variant is a new study.

## Kill rule and non-negotiables

- No verdict before week 10 is scorable, and no efficacy peeking. The script
  prints weeks 1–3 results labelled `INSPECTED — excluded` for mechanical
  checks only.
- k is not tuned. The cohort definition, floors and window do not move. If
  accrual is slower than expected, the verdict waits.
- If FAIL: the starter prior stays, the defect is recorded as unresolved by
  this construction, and any different prior (Sleeper-stratified,
  salary-stratified, shrink-to-zero) is a separately registered study.
- Do not compare against the roster report card; the two streams are never
  pooled.

## Honest prior

The `hist_0` gain is expected to be large and real: the discovery bias is
5–14 points against a cohort-mean first-game outcome of 3–5 points, and the
effect is mechanical rather than a market claim. The risk is on the other
side — the first-appearance cohort may be *too* pessimistic for a backup
promoted by injury (the very players DFS cares about), which the `hist_1_5`
cohort and the bias column exist to show. This study cannot distinguish a
promoted backup from a healthy scratch; that is the QB1/depth gate's job,
not the prior's.

## Commands

```bash
python -m model.nfl_dfs_zero_history_prior_study --season 2026            # grade every scorable upload
python -m model.nfl_dfs_zero_history_prior_study --season 2026 --week 2   # one week, mechanical check
python -m pytest tests/test_nfl_dfs_zero_history_prior.py -q
```
