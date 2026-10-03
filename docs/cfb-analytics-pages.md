# CFB Analytics pages

The Line Terminal remains the source-facing market view. The standalone
Analytics section has stable, directly addressable routes:

| Route | Role |
|---|---|
| `/cfb/analytics` | Upcoming canonical games and mapping/capture coverage |
| `/cfb/analytics/games/[id]` | One CFBD-identified game's market and football context |
| `/cfb/analytics/teams` | FBS team directory |
| `/cfb/analytics/teams/[id]` | Team context, upcoming games, and completed results |
| `/cfb/analytics/methods` | Definitions, planned foundation layers, and live source coverage |

The Terminal links to the selected game's Analytics route. The game page links
back to the Terminal with both `date` and `game` query parameters, so a direct
link opens the same matchup. Game pages link to both team profiles. The global
CFB navigation also exposes Analytics.

`web/src/db/cfb-analytics.ts` owns the shared page data contract. It reads
canonical `cfb_matchups`/`cfb_teams`, the latest accepted **pregame**
`game_odds_history` row, and the newest `cfb-team-context-v2` feature available
no later than the current time or game kickoff. Completed game pages therefore
cannot display a later feature as if it were pregame evidence. The team profile
shows its latest current snapshot, while links to older game pages recover the
game-specific boundary.

The market values are observed Odds API prices and lines. The CFBD feature
values are descriptive FBS-versus-FBS results, an opponent-adjusted margin
rating, and prospectively captured roster context. Neither side is relabeled
as a fair moneyline, spread, or total. Missing evidence remains blank. The
methods page reports stored play/PPA/drive coverage and research model validation.

The 2026 play/drive refresh is `.github/workflows/refresh_cfb_plays.yml`.
An initial bootstrap fetches every completed week. Scheduled Friday and Sunday
runs re-fetch the latest two completed weeks, bypassing caches for the current
season. `ingest/cfb_plays.py` compares the API payload with every completed
FBS-vs-FBS game in those weeks and fails the job if a game has no plays or
drives. The first live bootstrap on October 3, 2026 stored 59,316 plays
(44,578 with PPA) and 7,887 drives across 336 games, with no skipped rows or
missing completed-game feeds.

`research/cfb_forecast_v1.py` replays historical FBS games in kickoff order.
Each game's inputs use only earlier games: prior scoring, offensive and
defensive play PPA, and drives per game. Teams need at least two current-season
FBS games with usable feeds, and a near-term forecast needs at least 100 prior
current-season PPA plays for each team. The model is trained through 2025 and
publishes versioned pregame snapshots in `cfb_forecast_runs` and
`cfb_game_forecasts`. A frozen snapshot remains attached to its game after
kickoff; later results cannot rewrite its inputs.

The 2025 holdout contained 653 games. The retrospective 2026 comparison
currently has 67 games with accepted pregame market captures: model spread
mean absolute error 13.93 points versus market 11.28; total 12.14 versus
11.76; moneyline Brier 0.201 versus market 0.141 (66 prices). These are
retrospective comparisons, not prospective evidence. The model is therefore
`RESEARCH_ONLY` and its game-page estimates are clearly labeled as such.
Promotion requires at least 200 retrospectively matched games, 100 games
settled from pregame-frozen forecasts, and improvement on all three market
benchmarks in both samples. Even then it would not be an approved betting
signal. The workflow follows the active football season through January and
February bowls rather than switching years on New Year's Day.

Future efficiency, pace, matchup, and score-distribution layers should extend
this shared contract with a definition/version, `available_at`, coverage,
sample size, and validation state. Do not infer current-season PPA from
historical play storage, and do not interpret a research forecast as a fair
betting line.

## Forecast diagnosis and possession challenger

The v1 forecast run now stores an exact linear attribution for each upcoming
game in `report_json.explanations[game_id]`. The game page shows the scoring,
play-value, drive, and home-field contributions to the home margin and total,
plus the training-score baseline. It flags fewer than four eligible
current-season FBS games or 200 plays with PPA on either team, and market
captures older than six hours. The explanation is frozen with the forecast;
it is never recomputed from postgame data.

`research/cfb_forecast_v2.py` is a separate `cfb-score-possession-v2` research
challenger. For each earlier FBS game, it adjusts offense and defense points
per drive using the opponent's prior scoring profile available before the
forecasted kickoff. It blends current and prior seasons, estimates each team's
drives from its offense and the opposing defense, then fits a points-per-drive
regression. The same `cfb_forecast_runs` and `cfb_game_forecasts` tables hold
immutable versioned snapshots. Readers select each version explicitly; v2
cannot silently replace the v1 card. Both models' prospective graders use only
their own pregame-frozen forecasts and the most recent accepted odds capture
available at forecast time.

The first read-only v2 replay on October 3, 2026 found 2025 holdout spread
MAE 12.49 versus v1 12.77, total MAE 12.96 versus 12.80, and win Brier 0.187
versus 0.190 (653 games). In the 67 eligible 2026 retrospective games, v2
spread MAE was 13.11 versus v1 13.93 and market 11.28; total MAE 11.96 versus
v1 12.14 and market 11.76. On 66 moneylines, v2 Brier was 0.184 versus market
0.141. These results do not meet the market benchmark. They are not
prospective evidence or an approved betting signal. The methods page displays
the current frozen comparison after publication, including settled prospective
games once any exist. The Friday, Saturday, and Sunday refreshes publish both
versions; Saturday captures the main slate before kickoff.
