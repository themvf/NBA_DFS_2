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
| `/cfb/analytics/evaluation` | Prospective forecast grading, coverage, and same-capture comparisons |

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

## Frozen market comparisons and coverage audit

Each newly published pregame forecast now writes one append-only
`cfb_market_comparisons` row in the same transaction as the forecast. It
records the forecast/version, point-in-time explanation, exact odds history
ID available when the forecast was generated, and per-market eligibility.
The market reference is fixed at publication; a later backfill cannot change
it. If no capture existed, the source ID is null and all markets are
ineligible. Eligibility requires a stored market value, three selected books
quoting both sides, three books updated within five minutes of the capture,
and a capture no older than 90 minutes within 12 hours of kickoff or six
hours earlier. The exact failure reasons remain in the record. These are
research comparison rules, not claims that a quote was executable.

`/cfb/coverage` also audits due capture checkpoints over the prior 14 days,
separately for spread, total, and moneyline. It counts usable, captured but
thin, and missed windows; the gap list links back to each game. This audit
uses the checkpoint's linked history row and never treats a successful API
request as proof that all three markets were quoted.

The Methods page reports completed games from the latest frozen forecast per
game and version. Only eligible market pairs enter error metrics; excluded
games are counted. A new `cfb-score-opponent-ppa-v3` trial retains the v2
possession framework but adjusts prior play PPA for opponent profiles known
before each kickoff. It is trained through the prior season and compared
against v2 on the same 2025 holdout and 2026 retrospective games, then
frozen prospectively with the other models. It remains research-only and off
game pages until prospective comparisons support a change.

The first read-only v3 replay on October 3, 2026 improved on v2 in the
653-game 2025 holdout: spread MAE 12.46 versus 12.49, total MAE 12.81 versus
12.96, and win Brier 0.1863 versus 0.1865. On 67 completed 2026 games,
v3's spread MAE was 12.91 versus v2's 13.11 and market 11.28; total MAE was
11.89 versus 11.96 and market 11.76. On 66 market moneylines, v3 Brier was
0.180 versus v2 0.184 and market 0.141. This is retrospective and the market
still leads all three measures. The first frozen v3 forecasts will create a
prospective sample after those games finish.

## Prospective evaluation and market anchor

`research/cfb_prospective_evaluation.py` reads the latest pregame frozen
comparison for each game and model version. It grades only games with an
official completed result and only markets that met the frozen eligibility
rules. A verified close is joined by canonical matchup and scheduled kickoff;
missing closes remain missing. The report retains coverage and exclusion
reasons and is published append-only in `cfb_prospective_evaluation_runs`.
The standalone `/cfb/analytics/evaluation` page shows the latest report.

Each model is compared with the market quote frozen at that model's forecast
time. The strict v1/v2/v3 comparison also requires the same game, exact odds
history ID, and forecast times within 15 minutes. Spread and total use mean
absolute error in points. Moneyline uses Brier score, with log loss and
calibration bias retained in the report. Differences are model error minus
market error; negative favors the model. Confidence intervals resample game
dates and are withheld until 20 settled games span at least four dates.
Directional movement toward a verified close is descriptive only.

The fixed `cfb-market-anchor-v1` challenger blends 75% of each eligible
observed market value with 25% of the v3 football estimate. Its rule was fixed
before prospective outcomes and is frozen with new v3 comparisons. Historical
comparisons are not rewritten to add it. It remains research-only regardless
of an early favorable sample; no betting action is generated.

The refresh workflow publishes evaluation after new forecasts. A separate
Sunday/Monday workflow regrades official finals and verified closes without
calling CFBD or the paid odds API. Both runs preserve the underlying frozen
comparison and can be audited by report version and generation time.

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
