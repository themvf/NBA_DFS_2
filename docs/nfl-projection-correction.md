# NFL DFS projection corrections — `nfl-projection-correction-v1` (2026-11-03)

Owner's decision, 2026-11-03: four model changes read off the past slates and
shipped at once, after the owner asked for immediate improvements from past
Showdowns and Classics rather than another grader. Three change projections
(`web/src/lib/nfl-dfs/projection-correction.ts`, applied in the workspace
read after the market anchor); the fourth is an optimizer pool rule
(`POSITION_PRIOR` in `web/src/app/dfs/nfl/nfl-optimizer.ts`). A fifth change
makes the winners shape the default Classic GPP build.

## Evidence

Every served projection in the pregame pool captures (`nfl_dfs_pool_captures`,
the number the build actually used, latest capture before each game's
kickoff) joined to the slate report cards' actual DraftKings points:
3,933 player-games on 63 slates, 2026 weeks 2-8 (Classic 29 slates, 2,819
rows; Showdown 34 slates, 1,114 rows). Scratch scripts `imp_errors.py`,
`imp_errors2.py`, `imp_errors3.py` (read-only; this session's scratchpad).

| population | n | projected | scored | bias |
|---|---:|---:|---:|---:|
| Classic, all | 2,819 | | | -1.39 |
| Classic unanchored RB/WR/TE, history, proj 3+, weeks 6-8 | 275 | 7.62 | 5.72 | -1.90 |
| Classic TD-only-anchored RB/WR/TE, same filter, week 8 (the only such week) | 33 | 4.86 | 2.73 | -2.13 |
| Classic anchored RB/WR/TE, same weeks | | 10.03 | 9.50 | -0.5 |
| Classic QB not the team's top-projected QB | 66 | ~10 | 0.86 | -9.05 (88% scored 0) |
| Classic QB top-projected, unanchored | 120 | | 14.17 | -3.47 |
| Position-prior rows (both formats) | 160 | 6.83 | 0.76 | -6.1 (81-95% scored 0) |
| Showdown DST | 68 | 6.32 | 4.15 | -2.17 (negative in 5 of 7 weeks) |
| Classic DST | | | | +0.45 |

The shrink factor, fitted on each post-anchor week and graded on the others
(unanchored Classic RB/WR/TE with games of their own, projection 3+):

| held-out week | n | MAE at k=1 | MAE at fitted k | fitted k |
|---|---:|---:|---:|---:|
| 6 | 133 | 5.38 | 4.64 | 0.70 |
| 7 | 125 | 3.57 | 3.40 | 0.55 |
| 8 | 17 | 4.65 | 3.00 | 0.70 |

By position the fitted factors were TE 0.50-0.55, WR 0.55-0.80, RB
0.75-0.95; one factor, 0.70, is used for all three. The same fit on Showdown
made its held-out week worse (4.79 -> 5.23, n=74), so Showdown skill players
are not touched.

## The rules

1. **Classic RB/WR/TE without a full set of prop lines x 0.70.** Applies
   when the player has games of his own, no FULL market anchor, no absence
   commit, and a projection of 3 or more. A player anchored on the anytime-TD
   line alone (`mode: 'td_only'`, anchor v3, live from week 8) is INCLUDED:
   his volume and yards are still the model's, these players were inside the
   fitted population in weeks 6-7 (before TD-only anchors existed), and in
   week 8 they read the same way (4.86 projected, 2.73 scored, n=33; MAE
   3.50 -> 2.92 at 0.70, best k 0.50). Without them the rule is nearly inert
   under the current anchor: on the 2026-11-01 Classic 82 of 246 skill
   players were TD-only anchored and 1 was unanchored. Full-set anchors are
   exempt (bias -0.03 to -0.27 in weeks 7-8). Mean, floor, median and
   ceiling are scaled together; the boom rate is re-read off the scaled curve
   (the absence commit's own method) or dropped when the quantiles are
   missing.
2. **Classic backup quarterbacks zeroed.** Per team with two or more active
   quarterbacks, the expected starter is: the quarterback the user confirmed;
   else the one and only unblocked quarterback the depth chart lists QB1;
   else the top-projected active quarterback. Every other quarterback is
   zeroed, flagged `isOut` and given a block reason, exactly as a depth-chart
   backup is today, so the optimizer names the reason. The rule exists
   because the depth chart's own block misses the cases it cannot resolve
   (no depth number, stale chart, no identified QB1), and 66 such quarterbacks
   still carried starter-sized projections.
3. **Showdown DST x 0.65**, including the special-teams candidate block the
   optimizer substitutes at build time.
4. **Position-prior rows out of the pool unless locked** (both formats,
   independent of the observed-history setting). The existing history gate
   caps its requirement at the games the player's team has played, so a
   position-prior row survived it in week 2; this rule does not depend on the
   team's games.
5. **Default build shape**: `defaultBuildShape(format, mode)` returns
   `winners` for Classic GPP, `standard` otherwise. The user's own pick, a
   restored run's shape and a v4+ draft's shape all override it; a draft
   written before v4 stored `standard` whether or not anyone chose it, so it
   takes the page default (`formFromDraft`). The winners-shape constants
   themselves are unchanged (`nfl-winners-shape-v12`).

## Order in the workspace read

```
replacement upside -> absence commit -> market anchor
  -> projection correction (this)           <- new
  -> long-memory ranges -> ownership model -> optimizer
```

After the anchor so an anchored player is never touched; before the ranges,
ownership and the optimizer so every downstream number builds from the
corrected projection. Note the ownership models (`nfl-ownership-fit-v1`,
`nfl-ownership-fit-showdown-v1`) were fitted on stored upload-time
projections, not on these; their inputs move with this change, as they
already did with the anchor.

## What is recorded

Each changed row carries `projectionCorrection` with `version`, `rule`,
`factor`, `before` (mean, p10, median, p90, boom) and `after`, a note, and
for a zeroed quarterback the starter he lost to and how he was identified.
The pool capture records the whole row, and the saved-run snapshot
(`snapshotPlayer`) keeps the field, so every week from here can be graded
corrected-versus-uncorrected from captures alone. The page shows a
"Corrections" status line above the pool and a chip on each changed row.

## Seen on real slates (local dev server, 2026-11-03)

| slate | rule 1 scaled | QBs zeroed | DSTs scaled | default shape |
|---|---:|---:|---:|---|
| Nov 1 Classic, 11 games (246 active RB/WR/TE, 242 of 308 anchored, 82 TD-only) | 24 | 0 | | winners |
| Oct 25 Classic, 10 games (261 active RB/WR/TE) | 35 | 0 | | winners |
| Nov 1 Showdown PHI @ WAS | | | 2 (Commanders 4.9 -> 3.2, Eagles 3.0 -> 2.0) | standard |

Zero quarterbacks were zeroed on both Classics: the depth chart's own block
already covered every backup there, so rule 2 is the net under it, not a
weekly event. Chips: "TD line only x0.70 · 8.9 -> 6.2" (J.K. Dobbins),
"DST x0.65 · 4.9 -> 3.2". The Commanders' displayed 3.7 is the scaled
special-teams candidate the page substitutes, as it did before.

## What this is not

Not validated forward. The factor is fitted on one season's weeks 6-8; the
QB and position-prior rules are read off the same weeks. Review after six
weeks against `before`, the way the absence commit is reviewed. Change a
constant and bump the version. Not applied: Showdown skill players, Classic
DSTs, anchored players, ceiling widths (interval coverage was near the
claimed 10% except QB floors, explained by the backups, and RB ceilings,
slightly generous).

Tests: `npm run test:nfl-projection-correction` (rules, pool rule, default
shape, draft upgrade); `test:nfl-backup-qb-gate` updated for the pool rule.
