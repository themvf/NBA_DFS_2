# Current roster and team opportunity foundation

`/dfs/nfl/model/team-context` joins the existing volume/share research navigation. It contains 2025 play distributions for 32 teams and a frozen current roster snapshot. It does not change optimizer projections.

## Play accounting

Read hash-verified 2025 regular-season play-by-play and weekly-stat sources. Retain pass/run plays; exclude kneels, spikes, two-point attempts and no-play rows. `qb_dropback` defines designed dropbacks, including sacks and scrambles. Remaining eligible plays are designed runs. Targets require a receiver identity on a non-sack, non-scramble dropback.

Neutral means score differential within seven points and Q1–Q3. Leading/trailing means beyond seven points in any quarter. These descriptive states are not causal coaching effects; situation-specific play counts must not substitute for full-game volume. Unknown score states are retained in overall data but excluded from situation profiles.

For a user-supplied total play count: designed runs = plays × run rate; dropbacks = plays − runs; attempts = dropbacks × (1 − scramble rate − sack rate); targets = attempts × receiver-identified target rate. A supplied carry share applies to designed runs, including designed quarterback runs. A target share applies to the allocatable target budget. The remainder belongs to other players. These are expected counts under explicit assumptions, not probabilistic forecasts. RB receiving work and QB fantasy production are not estimated by this first calculator.

## Roster and movement

Read `ff_players` directly using read-only psycopg2, retaining ff identity even without GSIS history. Match team aliases, position and roster retrieval age (72 hours). Out/IR/PUP/NFI/suspended/inactive records cannot drive calculator output. Questionable does not mean absent. Provider rookie year identifies rookies; missing NFL history alone does not. Compare recorded 2025 teams to the current team to identify movement. Never transfer prior target/carry shares automatically to rookies or arrivals.

Verified examples on September 6: Kirk Cousins is listed QB1 for LV with ATL historical games; Mike Evans is listed WR1/Questionable for SF with TB historical games. The official Raiders starter announcement and 49ers signing announcement independently support these movements. This snapshot is not an automated news monitor or official game-day availability feed.

## Coaching

Source-bearing `web/src/data/nfl-coaching-evidence.json` records season, checked timestamp, staff identities and continuity evidence. Full continuity requires affirmative head-coach, coordinator and play-caller continuity; changed staff takes priority. Evidence expires after 30 days and cannot be used before capture or in another season.

First evidence coverage: LV changed head coach/coordinator, SF returning head coach/coordinator with play-calling continuity not independently established. The remaining 30 teams are unresolved. This is deliberately incomplete coaching coverage; do not describe it as all-team verified continuity. Historical rates are accessible as manual scenario references regardless, with no automatic scheme attribution or optimizer adjustment. Previous-team coordinator tendencies are not transferred to a new team in this increment.

Reproduce with `python -m ingest.nfl_dfs_team_context --source-root "C:/Docs/_AI Python Projects/NBADFS_v2" --season 2026`. The CLI reads existing caches/DB and writes the UI snapshot. Roster and model hashes accompany source hashes. Refresh coaching evidence only after checking its linked primary sources. Next work: complete attributable coaching/play-caller evidence, establish current-player role shares, then test component projections against the frozen benchmark before activation.
