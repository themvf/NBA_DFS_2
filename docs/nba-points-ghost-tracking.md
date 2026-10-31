# NBA player-points ghost trial v1

Purpose: evaluate experimental forecasts of actual individual points alongside an unchanged market baseline. No production projections, odds, or probability claims change. A local artifact is not an automatic daily capture service.

## Fixed trial and acceptance criteria

Freeze before the first scheduled game start using the common cutoff already established in the forward recorder. Ghost creation time, market observation time and historical-input availability time are separate. A model created after lock remains retrospective even when the market input was captured before lock.

Three columns per player:

- `market_line`: unchanged consensus betting line; a benchmark, not an estimated expected value.
- `history20`: arithmetic mean of the last 20 available regular-season appearances, with at least 10 required. Includes prior seasons and trades, excludes playoffs and non-appearances. No opening-season reset is implied.
- `blend90_10`: market line plus 10% of the difference between history20 and the line, capped at a two-point change in either direction. The 90/10 weights and cap are fixed research assumptions, not fitted or validated parameters.

Record raw historical rows, player identities, source fetched times, original per-book quotes, method versions and a content digest. Include only games before the target date and observations fetched before the ghost decision time. Later data is not allowed to enter an existing file. Reject duplicate history identities; name collisions between player IDs leave ghost forecasts unresolved. Missing history does not silently fall back to the market under the ghost label.

Grade every market candidate using verified final boxes, not only selected trios. Pushes count for points error; DNPs and unmatched/ambiguous/missing results remain explicit and excluded. Preserve actual minutes as an outcome diagnostic, never as an input. Report MAE, RMSE and signed bias (prediction minus actual) on identical paired player cohorts. Primary cohort: frozen market line >=20; secondary: all eligible players. Negative MAE change means the ghost did better. Coverage and exclusions must accompany errors.

The report excludes retrospective reconstructions and rejects multiple snapshots on one date, preventing repeated early/late versions from inflating sample size. Do not select a day's version based on results. For this first trial use one early pregame recording per date and keep the methods fixed. Revisions start a new version and a fresh trial; do not pool versions.

Initial review after 30 completed slate dates is an operational checkpoint, not sufficient evidence by itself. Before promotion require lower paired MAE in the primary cohort, date-clustered uncertainty that supports improvement, no material RMSE deterioration, and confirmation on a subsequent untouched period. Do not promote based on a winning night, simulation count, secondary cohort alone or post-hoc weights. The current report provides descriptive errors only; clustered inference is a later review requirement.

## Commands

First create a market snapshot with the existing `research.nba_points_forward freeze` command and verified common first-game cutoff. Then:

```powershell
python -m research.nba_points_ghost freeze --snapshot MARKET.json --output GHOST.json
python -m research.nba_points_ghost grade --snapshot GHOST.json --output GRADE.json
python -m research.nba_points_ghost report --grades GRADE1.json GRADE2.json --output REPORT.json
```

All files use exclusive creation; nothing overwrites a frozen forecast or previous grading attempt. If final boxes are incomplete, a later grading attempt uses a new output path. Supply only the latest complete grading attempt per chosen daily snapshot to the report. The freeze and grade commands read the warehouse without schema initialization or writes, and request no paid sportsbook data.

## Material limits

Available player histories do not establish current roster, injury, starting-lineup, role or minutes-restriction knowledge. These ghosts test historical shrinkage, not availability-aware forecasting. History can be stale or missing, and market captures can be hours old; those times remain visible. The observed market pool can omit players. Historical inputs fetched after an old game's lock cannot support a prospective reconstruction of that game. Future daily recording and grading require running the commands; no scheduler or live dashboard integration is installed by this local implementation.

The first October 24 recording has 78 market candidates, 74 with sufficient history and four unresolved. Its season-log inputs end in April 2026: current-season completed games exist in the separate box-score source but are not supplemented into these histories. Thus v1 deliberately measures blending with prior-season history. Do not describe it as a current-form or opportunity-aware model. Before a new version uses box-score supplementation, verify regular-season classification, player IDs across providers, duplicate-game exclusion and availability at decision time. This gap is visible evidence of why the collection path needs improvement before more complex forecasts.
