# NFL DFS ownership scorecard — `nfl-ownership-scorecard-v2` (v1 registered 2026-10-19; v2 2026-10-20)

One fixed definition of "better ownership", so every forecast version (the
production prior, a fitted model, a field simulation) is graded the same way
and the meaning of the gate cannot drift between evaluations. Companion to
`docs/nfl-ownership-model.md` (the Phase 2 promotion gate, unchanged) and
`docs/nfl-ownership-review-2026-10-19.md` (the evidence this answers).

Status: REGISTERED 2026-10-19 (owner accepted the proposed constants as
written, before any grade under this scorecard was computed). Frozen values:

| constant | value |
|---|---|
| `N_RELEVANT` (population 2) | Classic QB 12 / RB 20 / WR 30 / TE 12 / DST 12; Showdown every player |
| Predicted-chalk thresholds | Classic >= 15%; Showdown flex >= 25%; Showdown captain >= 10% |
| Salary tiers | < $3,000 / 3-4 / 4-5 / 5-6 / 6-7 / 7-8 / 8+ ($ thousands); Showdown punt tier < $2,500 |
| Reconciled-zero mass floor (stat-api) | recorded mass >= 0.995 of roster slots x 100, AND full standings fetched |
| Calibration bins | 0-1, 1-5, 5-10, 10-20, 20-35, 35+; Showdown flex adds 50+ |

Any change to a target, population, label rule, metric, holdout or one of
these constants is a new scorecard version with its own first-grade date, and
earlier grades are kept as written.

**v2 (2026-10-20, owner):** the 50,000-entry field-size floor is removed. It
was a forecasting heuristic, not a grading rule. The only grade read under v1
(WAS@SF contest 196786695, 47,562 entries, `docs/nfl-ownership-pregrade-2026-10-19-was-sf.md`)
was tagged `below_target`; under v2 it is an ordinary graded slate. No other
constant changed, and no grade was written to `nfl_ownership_scorecard_rows`
under v1.

## 1. Target contests

| | rule |
|---|---|
| Format | Classic and Showdown graded separately; nothing is pooled across formats |
| Field size | no minimum. Entry count is recorded with every grade but does not include or exclude a contest (v2; v1 required 50,000) |
| Reference contest per slate | the largest-field GPP on that slate that passes label provenance (section 3); other same-slate contests are `secondary` and reported, not gated. The Bijan Robinson case (42.57% vs 47.26% on one slate) shows contests differ by audience, so a model is graded on its reference contest only |
| Contest attributes recorded | contest id, entry count, entry fee, max entries per user, slate upload id, format, first kickoff (ET and UTC) |

## 2. Forecast cutoff and provenance

- A graded forecast is one FROZEN before lock, never a rebuild. Sources, in
  order of preference: the pregame pool capture (`nfl_dfs_pool_captures`,
  `poolCaptureDue`: one capture inside 24 h of first kickoff, then every tick
  in the final 20 minutes), the saved optimizer run's `input_snapshot`, the
  ghost freeze. Each grade names the source row and its `captured_at`.
- Canonical decision horizon: the LAST capture at or before `first kickoff
  minus 20 minutes`, so Sunday inactives (released about 90 minutes before
  kickoff) are inside the forecast's information. A second horizon, the
  first capture inside 24 h, is graded as `early` and reported beside it.
  Both horizons are stated per grade; they are never averaged.
  Capture schedule note (2026-10-19, not a scorecard change): the task-1
  inventory found every final-window pool capture falls after this cutoff,
  so the canonical capture was often the 24-hour one (TB@DAL's was 1,439
  minutes out). The owner kept the rule and added one pool capture in the
  20-25 minute window (`PRE_LOCK_CAPTURE_MINUTES`, `pool-audit.ts`) from the
  next slate on; earlier slates keep the captures they have. The owner also
  kept the 0.995 mass floor after the inventory showed week 4's Millionaire
  at 0.99493, so its 446 missing labels stay `unresolved`.
- A forecast records: ownership version, projection source for each player
  (`model`, `market_anchor`, `absence_commit`), availability as of capture,
  DK status as of capture, salary file digest. A capture missing any of these
  is graded `provenance_incomplete` and excluded from gates.
- The market anchor changed the prior's input from 2026-10-12, but coverage
  varies within a slate (144 of 407 Classic players on week 5). The split is
  therefore by the anchor evidence STORED in each forecast (`marketAnchor`
  per player), reported as anchored / unanchored player counts per slate,
  alongside capture date and ownership version. The date alone does not
  establish that a player was anchored.
- A forecast is never backfilled with later news. If the needed capture does
  not exist, the slate has no graded forecast for that horizon; it is listed
  as `no_frozen_forecast`, not rebuilt.

## 3. Label provenance

Every label row carries one of these classes, and the gate uses only the
first two:

| class | meaning | source paths |
|---|---|---|
| `observed` | the player's share was read directly from the contest record | DraftKings export rows; stat-api seats for players any fetched user rostered; stat-api CPT seats for captain share |
| `reconciled_zero` | the player is salary-listed, the contest record is complete, and the player appears in no entry | DraftKings full export only (every drafted player appears); stat-api only when `mass >= 0.995` AND the full standings were fetched |
| `synthesized_zero` | a zero the importer filled in without evidence | stat-api captain share for a player no fetched user captained (`captain.get(key, 0.0)` in `model/statapi_contest.py`); any slot split derived from a combined number |
| `unresolved` | no row and the record is not complete enough to call it zero | stat-api imports below the mass floor; LineStar-style combined feeds for a slot they never claimed |

Rules:
- `synthesized_zero` and `unresolved` rows are excluded from MAE, bias and
  rank metrics and from training targets. They are counted and reported.
- A contest's label completeness (`observed + reconciled_zero` mass over
  roster slots × 100) is reported with every grade. Week 4 Millionaire:
  895.44 of 900%, so zero-filling can move full-pool MAE by at most ~0.007;
  that bound is recomputed per contest rather than assumed.
- Showdown captain and flex are separate labels. A combined percentage is
  never split without a slot-level source.
- Availability at grading time is the slate's own pinned availability at the
  forecast capture, never today's.

## 4. Populations

Three populations, each reported on every grade; the gate uses the first.

1. **Full pool (gate).** Every salary-listed player on the slate with an
   `observed` or `reconciled_zero` label. This is the Phase 2 gate population
   and it does not change. Week 4 Millionaire: 629 players.
2. **Relevant players (pregame-defined).** Defined only from information in
   the frozen forecast: the top `N_RELEVANT` by predicted ownership plus the
   top `N_RELEVANT` by salary-adjusted field points (DK average blend) per
   position, union. Registered `N_RELEVANT`: Classic QB 12 / RB 20 / WR 30 /
   TE 12 / DST 12; Showdown every player (the pool is one game). Fixed
   before grading; does not look at realized ownership.
3. **Diagnostics (outcome-defined, never a gate).** Actual ownership >= 1%;
   predicted-chalk (predicted >= 15% Classic, >= 25% Showdown flex, >= 10%
   captain); per position; per Showdown slot; per salary tier
   (< $3,000 / 3-4 / 4-5 / 5-6 / 6-7 / 7-8 / 8+ ; Showdown < $2,500 as the
   punt tier). The 1% cut is kept because it exposed 3.54 vs 1.07 on week 4;
   it is excluded from gating because it is selected on the answer.

## 5. Metrics

Per contest, per horizon, per population:

- MAE in percentage points; bias (predicted minus actual, positive = we
  over-project ownership); RMSE.
- Spearman with AVERAGED tied ranks (the current calibration script does not
  average ties; the 0.80 figures in the model doc are withdrawn as headlines).
  Tie-correct results so far, each on its own population and not
  interchangeable: week 2 full-pool retrospective replay 0.663; frozen week 4
  and week 5 forecasts 0.661 and 0.710 on their observed-label populations.
  None is a full-pool gate metric for a frozen forecast yet. Also Pearson.
- Calibration by predicted-ownership bin (0-1, 1-5, 5-10, 10-20, 20-35, 35+;
  Showdown flex adds 50+): mean predicted vs mean actual and count.
- Chalk: top-10 overlap by predicted vs actual; largest 10 absolute misses
  named.
- Allocation: predicted vs recorded ownership SUM per position (Classic) and
  per slot (Showdown). The recorded TE total minus 100 is the realized TE
  FLEX share (weeks 3-5: 37.71 / 34.09 / 37.12%, prior 5%); predicted sums are
  compared to it. Players above the forecast's own cap (60% Classic, 50%
  captain, 90% flex) are listed with their actual share.
- Pooled over slates: per-slate values and the pooled value, with a
  slate-clustered bootstrap 95% interval. Slates are the cluster; contests on
  one slate never count twice.

The portfolio measure (`model/nfl_showdown_ownership_eval.py`: projected
points kept at equal real-field duplication) is reported for Showdown as a
secondary outcome. It is not part of the accuracy gate.

## 6. Chronological holdouts

- Holdout is by SLATE, never by row or contest. Every contest from one slate
  stays on the same side of the split.
- A fitted model graded on slate N may use labels only from slates whose
  labels were available before slate N's canonical capture
  (`labels_available_at <= captured_at`). Standings become available Monday
  night; a model fitted Tuesday may use the prior week.
- Walk-forward: fit through week N-1, grade week N, for every N with a
  frozen forecast. Prior-only folds are labelled and do not count as trained
  forward folds (the 2026-10-04 artifact had zero).
- The Phase 2 gate thresholds (Spearman >= 0.70, MAE <= 2.0, |bias| <= 0.5,
  >= 4 held-out Classic slates) are read on population 1 with averaged ties.
  They are not changed here. Showdown has no registered gate yet; this
  scorecard reports it and a Showdown gate is a separate registration.
- The stated prior is graded on the same slates descriptively. It has no
  fit, so every slate is "held out" in form but its Classic constants were
  judged before any grade and its Showdown exponent was read off weeks 2-3;
  those two slates are tagged `design` for v3 Showdown.

## 7. Reporting

One row per (contest, horizon, forecast version, population) in an
append-only table (`nfl_ownership_scorecard_rows`, proposed), with the
scorecard version, label completeness, counts by provenance class, and the
capture row id. A grade is written once per forecast; a re-grade under a new
scorecard version appends. The Tuesday post-week job runs it after the
flagship import; a slate with no frozen forecast writes a `no_frozen_forecast`
row so silence is visible.

## 8. What this scorecard does not decide

- Which model is better for the OPTIMIZER. Accuracy here is marginal
  ownership accuracy; duplication, stacking and payout are graded by the
  ghost factorial and the portfolio objective study, with their own
  registrations.
- Whether the market-implied projection is "public consensus". It is one
  separately recorded point source; its coefficient and sign are learned.
- The cause of any miss. Wilson (11.35% predicted vs 45.59%) and Lance (1.45%
  vs 23.87%) are verified misses; "news chalk" is a hypothesis until the
  status timeline at the forecast's capture is set beside it.

## 9. Before the first registered grade

1. Fix the calibration script: averaged tied ranks; missing labels classified
   by section 3, never a blanket zero.
2. Inventory and deduplicate optimizer snapshots and pool captures: dates,
   contents (projections, availability, DK status, ownership version, source
   timestamps) and DISTINCT slates. More captures of one slate improve the
   timing evidence; they do not add validation slates. Whether this enlarges
   the gradable history is a hypothesis until the inventory says so.
3. Regrade weeks 2-6 once under this spec, split at the anchor, and publish
   per-slate rows. That run is the baseline every later version is compared
   to.
4. Record the stack-label provenance for the 90% QB-stack figure: contest
   ids, whether the full standings were fetched, stat-api's label vocabulary
   (`model/nfl_dfs_field_stacks.py`), per-slate rates. Five Millionaires are
   five slate environments.
