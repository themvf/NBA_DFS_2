# CFB DFS field-ownership model

Spec written 2026-11-01, before any CFB ownership estimate existed, with two
contests of actual ownership in hand (one Classic, one Showdown). Nothing here
is built yet.

## Why

`/cfb/dfs` has no estimate of how the field will draft. The optimizer
maximises projection with randomness, and the results panel can only compare
our exposure to the field's after the game. The OSU @ USC Showdown
(2026-10-31) showed the gap: the field had Bo Jackson in 76% of lineups and we
had him in 45%, and nothing on the page said so before lock.

The model has three uses, in order:

1. **Show it before lock**: a "Field own" column beside our exposure, so an
   under- or over-weight is visible while it can still be changed.
2. **Grade it after**: every forecast frozen pregame and scored against the
   contest the owner imports.
3. **Leverage, later**: an ownership term in the CFB optimizer, only after the
   grade earns it (Phase 3). Not part of v1.

## Data

**Labels.** DraftKings contest standings, imported on the CFB page, already
store each player's ownership by slot (`cfb_dfs_contest_players.drafted_by_slot`).
That is the only source:

- stat-api has no college football (its public API covers NFL, NBA, MLB, NHL
  and golf; checked 2026-11-01).
- dfsdb caps a contest's athletes at 50 rows and reports Showdown ownership as
  flex only, which made its NFL rows unusable for calibration (see the dfsdb
  section in `CLAUDE.md`). Not a label source.
- DraftKings exports standings only for contests you entered.

So labels accrue at the rate the owner enters and exports contests: about one to
three a week until the bowls. **Import the standings for every CFB contest
entered; that is the training set.**

**Labels so far (design contests, used below):**

| contest | format | entries | what it shows |
|---|---|---:|---|
| 196055904 (2026-09-26, 3 games) | Classic | 5,142 | QB 185.5 / RB 253.2 / WR 361.2 of 800; 85.5 of the 100 Super Flex points went to a second QB; the remaining 114.5 flex points split RB 53 / WR 61 |
| 197268067 (2026-10-31, OSU @ USC) | Showdown | 1,127 | Captain by position WR 52 / RB 29 / QB 17 / K 1; players under $3,000 drew 34.6 of the 600 points |

**Inputs available before lock**, per player: our projection (or the owner's
manual projection), DraftKings' printed season average, salary (and Captain
salary), position, team, the team's implied total, status (salary file and the
live DraftKings poll). There is no consensus projection source for college
(FantasyPros has none; checked 2026-09-26).

## Phase 1: stated prior, `cfb-ownership-prior-v1`

A pure function, `web/src/lib/cfb-dfs/ownership-prior.ts`. It ports the shape
of the NFL v5 prior (`web/src/lib/nfl-dfs/ownership-prior.ts`), which was
chosen on 16 NFL slates and graded on later ones. **Every constant is ported
unchanged from NFL v5 except the three marked CFB**, so the two CFB design
contests above shape only what NFL cannot supply (the roster structure). They
are not used to tune anything else, and the prior is not graded on them.

```
field points  = 0.5 x our projection + 0.5 x DK average          [CFB]
                our projection = the manual projection when the owner set one
                either missing -> the other; both missing -> not drafted
value         = field points / (max(salary, $3,000) / 1000)
score         = field points^2 x value^v x status                 (Q 0.6, D 0.25, OUT 0)
weight        = score^alpha
share         = weight / sum(weight within the group) x group budget,
                capped per player; excess re-shared (NFL allocateBudget)
```

**Classic** (alpha 0.75, value exponent 1.5, cap 60%)

| group | budget | source |
|---|---:|---|
| QB | 185 | CFB: 100 QB slot + 85 of the Super Flex, from contest 196055904 |
| RB | 255 | CFB: 200 + 55 of the remaining flex points |
| WR | 360 | CFB: 300 + 60 of the remaining flex points |

The budgets sum to 800, the eight roster slots.

**Showdown** (alpha 0.45, value exponent 0.5)

- UTIL: 500 points, cap 90%. Punt tier as NFL v5: players under $3,000 share a
  reserved 60 of the 500 on points^1, cap 30; any unused part returns to the
  main pool.
- Captain: 100 points, split by position, then within a position by the
  Captain score (score^1.5 at the Captain salary) raised to alpha, cap 50%.
  Position shares are NFL v5's with TE and DST removed and renormalised
  [CFB]: **QB 26.7 / RB 33.8 / WR 37.4 / K 2.1**. The one CFB Showdown read
  WR 52 / RB 29 / QB 17 / K 1; that is one game, so it is recorded here, not used.
- A player's Captain + UTIL share is capped at 95% (one slot per lineup).
- Kickers: the baseline does not project them, so their field points come
  from the DK average alone (or the owner's manual number). No kicker cut.

**Why 0.5/0.5 and not NFL's weighting.** NFL's first grade found the field
follows the market's projection, not ours. In college the DK average is the
only market-like number on the page, and our projection is a young,
unvalidated baseline. Equal weights is a stated judgement, not a fit.

**What v1 cannot see**: news (a backup named the starter an hour before lock,
as Fahey was), hype, brand-name schools, and stacking. The manual projection
rule partly covers news: when the owner hand-sets a player, the prior assumes
the field knows what the owner knows.

## Where it shows and how it is frozen

- Player pool: a "Field own" column (total, plus Captain % in Showdown) beside
  the projection, labelled "Uncalibrated estimate" with the version.
- Every saved lineup run stores the forecast it was built beside
  (`settings.ownership`: version, per-player percents, inputs digest). That is
  the frozen pregame forecast.
- Results panel: the field's actual ownership beside the forecast and our
  exposure, per player.

## Grading

`npm run calibrate:cfb-ownership` (to build) scores the forecast frozen on the
newest run saved before the slate's first kickoff against the imported
contest. A slate with no pre-lock run is not graded. Metrics, per format:

- Classic: Spearman, MAE and bias over every slate player, in points.
- Showdown: the same for UTIL and for Captain separately.

Counted slates are those whose standings are imported **after this spec's
commit**; the two design contests are reported, labelled design, and never
counted.

## Phase 2: fitted model (gate frozen now)

When enough labels exist, port the NFL fit (`nfl-ownership-fit-v1`: regression
on log ownership with slate-relative rank features, one model per role,
leave-one-slate-out). It replaces the prior only if it clears, on held-out
slates, every bar below and beats the prior on the same rows:

| | Classic | Showdown |
|---|---|---|
| held-out slates, by slate never by row | 6 or more | 8 or more |
| pooled Spearman | 0.70 or higher | 0.70 or higher (UTIL) |
| pooled MAE, points | 2.0 or lower | must beat the prior's UTIL and Captain MAE |
| pooled bias | within +/- 0.5 | within +/- 0.5 |

Classic needs 6 slates rather than NFL's 4 because college slates vary more
(2 to 15 games, and a team's whole season can sit outside the slate). At the
current entry rate this is unlikely before 2027. Thresholds do not move; if
the fit fails, the prior stays and the gate stays closed.

## Phase 3: leverage (decided later)

An ownership term in the CFB optimizer (`(1 - own)^k` on the objective, as
NFL GPP builds use) is a separate decision, made after Phase 1 has been graded
on at least 4 counted slates of the format. Until then the estimate is shown
and frozen, never optimized against.

## Build list (Phase 1)

1. `web/src/lib/cfb-dfs/ownership-prior.ts`: pure, constants as above, version
   `cfb-ownership-prior-v1`; reuse NFL's `allocateBudget` rather than copying it.
2. Tests (`web/scripts/test-cfb-ownership.ts`): budgets sum to 800 / 500 / 100,
   caps hold, OUT is 0, a manual projection replaces ours, kickers draft on the
   DK average, the punt tier returns unused points, and the two design contests
   produce a descriptive grade printed for the record.
3. Workspace read: attach the forecast to each player; Field own column.
4. Run snapshot: freeze the forecast with each saved run.
5. Results panel and `calibrate:cfb-ownership`.
6. Grade after each counted slate; append results below, never edit above.
