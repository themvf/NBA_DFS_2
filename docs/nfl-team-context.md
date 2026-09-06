# NFL team context and role projection validation

The existing `/dfs/nfl/model/team-context` page joins current roster evidence, historical play distributions and a paired DFS projection diagnostic. Historical references and manual allocations are not activated optimizer projections.

## Coaching evidence

All 32 teams have source-linked current head coaches and offensive coordinators, plus current offensive play-caller assignments. Nineteen caller assignments have direct team confirmation; thirteen are explicitly labeled reported from Mike Clay's September 2, 2026 ESPN coaching directory (page 74). Titles and calling responsibility are separate evidence. Caller evidence has its own season, source and checked timestamp, and expires after 30 days. A reported assignment must not be presented as independently team-confirmed.

The hash-verified 2025 schedule supplies each team's 17-game head-coach history, including midseason changes. Selected caller-change events are recorded for CLE (week 10), DET (week 10), LV (week 13), and TEN (week 4). These event records are retrospective attribution, not complete caller timelines or archived pregame observations. Historical coordinator/caller coverage remains incomplete. In particular, do not apply 2026 coaching assignments to 2024/2025 replay rows or treat a full-season team's rates as one coach's rates.

## Historical opportunity accounting

The CLI reads hash-verified prior-season regular-season play-by-play, weekly statistics, participation and schedule caches. Eligible plays are pass/run plays excluding kneels, spikes, no-play and two-point plays. Designed dropbacks include sacks and scrambles; neither becomes a receiver target or designed running-back carry.

Situation views: neutral is within seven points in Q1â€“Q3; leading/trailing is beyond seven points in any quarter. Overall play counts retain unknown score states. These are descriptive game situations, not causal coaching effects.

Charts show shotgun, no-huddle, red-zone and inside-five dropbacks with recorded/eligible denominators. Positional target, designed-carry and inside-five carry splits match player identity and position in the same historical game. Unknown identities stay in the denominator rather than being redistributed. Positions are never borrowed from the current roster. Duplicate identity joins fail.

Clock spacing uses adjacent eligible snaps in the same drive and quarter with a nonnegative game-clock delta no greater than 60 seconds. Intervening no-play rows break a pair. It includes play duration and excludes outlying gaps; it is not wall-clock pace or a full-game play forecast. Formation, personnel, route and coverage counts are an availability audit; route labels do not establish routes run by every receiver or individual matchup advantages.

## Current roles and allocations

The read-only roster query uses `ff_players`. Team aliases, position and capture age (72 hours) must match. Out/IR/PUP/NFI/suspended/inactive players cannot drive allocations. Questionable does not mean absent. Rookie flags require provider rookie-year evidence. Prior-team history never transfers a target or carry share automatically to a newcomer.

The allocation reference uses the last eight prior-season team games. Non-participation contributes zero usage within known team games. Departures, missing identities and unavailable players leave unallocated opportunity. Shares can be overridden explicitly; sums above 100% fail with the required reduction. Save scenario exports assumptions, roster evidence and hashes. This is not an automatic rookie role forecast or official game-day confirmation.

## Role-context projection diagnostic

`model/nfl_dfs_role_context.py` tests team-game workload denominators and residual ranges conditioned on prior target workload and the observed primary QB in the prior four team games. Tied or missing QB evidence stays unknown. It does not infer injuries, use target-game starters, or apply a coaching multiplier. Forecasts within a week finish before that week's outcomes enter residual history.

On exactly paired recorded WR games against the frozen production-algorithm replay (market inputs disabled):

| Season | Games | Existing / candidate MAE | Existing / candidate interval score | Existing / candidate 25-point Brier |
|---|---:|---:|---:|---:|
| 2024 | 1,794 | 5.126 / 5.228 | 24.275 / 25.216 | 0.05332 / 0.05170 |
| 2025 | 1,821 | 4.844 / 4.913 | 21.895 / 23.013 | 0.04185 / 0.04197 |

A matched ablation keeps the workload estimates but pools residuals across all contexts: interval scores worsen to 26.974 (2024) and 25.312 (2025), versus 25.216 / 23.013 with workload-plus-QB conditioning. Conditioning helps this candidate's ranges, but does not rescue it against production. This does not isolate QB history from workload conditioning.

Lower is better for all three measures. The candidate fails the joint accuracy screen and remains disabled. Both seasons were previously inspected; this is a diagnostic, not untouched validation. Missing/DNP evaluation outcomes, players without four prior recorded games, and unmatched baseline rows are excluded. There is no historical salary-multiple, contest payout or lineup-distribution validation.

Reproduce from the worktree:

```powershell
python -m ingest.nfl_dfs_team_context --source-root "C:/Docs/_AI Python Projects/NBADFS_v2" --season 2026
python -m ingest.nfl_dfs_role_context --source-root "C:/Docs/_AI Python Projects/NBADFS_v2"
python -m pytest tests/test_nfl_dfs_team_context.py tests/test_nfl_dfs_role_context.py -q
```

The role replay archives prediction draws' summaries, hashes sources/recipes and verifies the frozen comparator digest. UI data is a generated snapshot, not an automated refresh service.

Remaining work: independently confirm the reported callers and complete historical coordinator/caller timelines; improve the workload-to-points component rather than merely widening ranges; then validate current-role forecasts prospectively, especially rookies, arrivals and injury replacements, before optimizer activation.

## Receiving-component diagnostic

The receiving-component replay holds projected targets fixed and estimates catches, yards per catch and touchdowns per target separately. Rates shrink toward strictly earlier WR rates using 30 target, 20 reception and 50 target prior weights. Historical bonuses/other scoring remain separate; mean yardage does not automatically earn a bonus. Prior-week residuals supply ranges and an explicit mean correction.

| Season | Paired games | Component MAE | Interval score | 25-point Brier |
|---|---:|---:|---:|---:|
| 2024 | 1,794 | 5.163 | 24.590 | 0.05075 |
| 2025 | 1,821 | 4.856 | 22.327 | 0.04161 |

All three metrics improve over the previous role experiment in both seasons, but point and interval error still trail production. Optimizer activation remains disabled. Whole-week bootstrap intervals accompany the comparison; the 2025 probability improvement is uncertain. Previously inspected seasons are diagnostic, not untouched validation. No salary-multiple or contest-winning improvement is established.

The team-context page adds historical receiver selection, component expectations, points contribution bars and floor/median/ceiling estimates. Examples are selected by projected targets, never actual performance. Source, recipe and paired-prediction hashes are archived. Current coaching and roster observations never enter historical replay.

Reproduce with `python -m ingest.nfl_dfs_receiving_components --source-root "C:/Docs/_AI Python Projects/NBADFS_v2"`. Verify with `python -m pytest tests/test_nfl_dfs_receiving_components.py tests/test_nfl_dfs_role_context.py -q`.

Next: investigate residual calibration and receiving efficiency errors before prospective validation. Rookies, arrivals and injury replacements still require separately validated opportunity estimates.
