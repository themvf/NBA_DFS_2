# Prop field lag: is the field's ownership set against the Wednesday lines? (pre-registered 2026-10-28)

Study id `nfl-prop-field-lag-v1`. Registered **before any Wednesday prop line
existed in the database**: the first Wednesday capture is the evening this
registration is pushed, at the earliest. Implementation:
`model/nfl_prop_field_lag_study.py`; capture jobs `nfl-prop-capture-wed` and
`nfl-prop-capture-fri` in `web/src/lib/cron-dispatch.ts`; roles `wed`, `fri`
and `sat` in `nfl_prop_implied_lines` (`research/nfl_prop_implied.py`).

## Why

Line movement does not help our projection. We hold the closing line at lock
and can late-swap anyone whose game has not started, and this repository's
studies say the close is efficient on the mean (MLB execution timing and
opener-vs-closer were null; the NFL west-coast study died; the totals-walking
fade is accruing with a prior of nothing). The market anchor
(`nfl-market-anchor-v1`) already projects from the close.

What an early-week line can tell us is about the **field**. DraftKings salaries
are set Monday. Most of the field builds Wednesday to Saturday from the
projections of that time, and a 150-entry book set on Friday mostly stays as
set. The ownership we face is therefore decided against something like the
Wednesday lines, while the points are scored against the closing lines. If
that is true, a player whose lines rose during the week is owned less than his
closing number deserves (free leverage), a player whose lines fell is owned
more (chalk to avoid), and the ownership prior, which reads the same close we
do, cannot see either. The Wednesday line is the best proxy we can capture for
the information the field held when it decided.

This is a question about the ownership model only. The projection stays on the
close whatever the answer.

## What the stored data could not answer

Checked before registering (2026-10-28, `nfl_prop_implied_lines` and
`prop_odds_history`):

- 2026 weeks 1-4 have one capture per game, on game day; the stored `open` and
  `close` rows are the same capture.
- Weeks 6-7 have two: Saturday 22:07 UTC (18 hours before a Sunday 1 PM ET
  kickoff) and about an hour before kickoff. In those 18 hours yardage lines
  changed on 63-91% of players by a median of 1-2 yards (0.1-0.2 DK points,
  max 12 yards); receptions and touchdown lines were essentially frozen.
- No Wednesday line exists anywhere.

So the hypothesis needs forward capture, and the last day before kickoff is
not where the field's information gap would live.

As an end-to-end check of the join and the analysis, the Saturday read stood
in for Wednesday on weeks 6-7 (`EARLY_ROLE = "sat"`, never the study):
265 player-slates (131 and 134 per Classic flagship), move sd 0.46 points,
MAE gap +0.09 with 95% CI [-0.06, +0.20], best blend weight w = 0, control
correlation +0.07 [-0.06, +0.18]. That is an 18-hour move on two weeks that
are not in the population; it says the pipeline runs and that about 130
players per slate carry a full set of lines, nothing else.

## Capture (frozen)

Four reads a week per Sunday/Monday game, two per Thursday game:

| read | when (UTC) | job | markets | window |
|---|---|---|---|---|
| Wednesday | Wed 22:07 | `nfl-prop-capture-wed` (new) | main + volume, 12 | games 30-130 h out |
| Friday | Fri 22:07 | `nfl-prop-capture-fri` (new) | main + volume, 12 | games 30-100 h out |
| Saturday | day before, 22:07 | `nfl-prop-capture-day-ahead` (existing) | all 22 | tomorrow's Eastern date |
| close | T-60 to T-75 min | `nfl-prop-capture` (existing) | all 22 | that kickoff group |

The 30-hour floor leaves tomorrow's game to the day-ahead run, so a Thursday
game's Wednesday read is its day-ahead capture. The windows stop short of the
following Thursday. Each new round is about 12 markets x 14-16 games, 170-190
credits, under 400 credits a week on a 100k/month plan; the midweek runs skip
the game-leader pricing steps.

`research/nfl_prop_implied.py build` stores, per player-market-week, `open`
(first capture) and `close` (last) as before, plus `wed`, `fri` and `sat`: the
last pregame capture made on that Eastern weekday.

## Population (frozen)

- Season 2026, every week from the first with a Wednesday read (week 8 if the
  capture fires on 2026-10-28, else week 9) through week 18.
- One contest per week: the largest Classic DraftKings contest with imported
  ownership and a linked slate (`nfl_dfs_field_contests`, the weekly flagship
  import). Showdowns are excluded from the verdict.
- QB, RB, WR and TE on that slate, not ruled out, with a salary, and with a
  calibrated DK-points read (`model/nfl_prop_implied.py`, `CALIBRATION_V1`)
  from BOTH the `wed` and the `close` rows. The conversion needs the
  position's required markets at both reads.
- A player with a Wednesday read and no closing read is a **pulled line**: counted
  and reported, never scored as a zero move.
- Ownership is the contest's `drafted_pct` (0 for a slate player nobody
  drafted); actual points are the contest's own `fpts`.

## Metrics (frozen)

`delta` = closing implied DK points minus Wednesday implied DK points. Salary
is fixed Monday, so `delta` is also the change in value.

**Primary, H1 (field lag).** Two ownership models of identical form,
`log(1 + own) ~ points + points per $1K + position`, one fed the Wednesday
read and one the close, each scored leave-one-week-out in ownership points.
The statistic is `MAE(Wednesday model) - MAE(close model)`. H1 passes if its
95% bootstrap CI lies below zero: Wednesday's lines explain the field better
than the closing lines do.

**Control, H0 (market efficiency).** Correlation of `delta` with actual minus
closing implied points. Prediction: CI includes zero. If it does not, the
market under-reacts to its own moves; that is a different claim and gets its
own registration rather than being folded into this one.

**Descriptive, never gating.** The blend weight w in `w x Wednesday + (1 - w)
x close` that best explains ownership out of sample (the field's effective
read); the ownership residual from the close-based model by |delta| tercile
(big moves are teammate injuries the field does see, so any lag should sit in
the small and medium terciles; saying so now stops it being discovered later);
the count of pulled lines.

**Bootstrap.** Weeks are the dependence unit, so the honest interval is
week-clustered; with eight clusters that interval is also anti-conservative,
so the reported CI is the wider of the week-clustered and the row-level
bootstrap (2,000 draws each, fixed seed).

## Floors and verdict (frozen)

- At least 8 Classic flagships and at least 700 player-slates, both.
- The verdict is computed once, at the first run that meets both floors
  (`--verdict` writes `artifacts/nfl_prop_field_lag_verdict.json` and refuses
  to overwrite). Weekly runs before that print descriptives only.
- Kill: the MAE gap's CI includes zero or favours the close. No re-slicing by
  position, market or move size to rescue it. A variant is a new registration.

## What a pass licenses

A lag term in the ownership prior, as a new prior version: ownership predicted
from the Wednesday read plus a fitted share of the move, with the projection
unchanged from the close. It would be graded in 2027 by the existing
`calibrate:nfl-ownership` harness against the gate registered in
`docs/nfl-ownership-model.md`. The study's inputs are the same captures the
page would read on a Sunday, so a pass needs no new data at run time.

A pass never changes a projection, a star rating or the market anchor.

## Honest prior

The lag is real but small. The large-entry players who set most of the
ownership mass re-optimize on Sunday morning, and the casual mass that does
not is the part of the field we beat anyway. The surprising result would be
the control failing.

## Disclosures

- The first draft of the primary metric was the coefficient on `delta` in a
  nested model (close-based ownership model plus `delta`). On a synthetic
  field with NO lag that coefficient came out reliably negative, because
  `delta` absorbs misspecification of the close-based model. It was replaced
  by the paired model comparison above before any Wednesday line existed.
- The weeks 6-7 Saturday stand-in above was looked at before registration. Those
  weeks have no Wednesday read and are outside the population.
- The Friday and Saturday reads are captured so the move can be dated; they
  take no part in the verdict.
