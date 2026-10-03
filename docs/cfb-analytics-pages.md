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
methods page reports stored play/PPA/drive coverage, not model readiness.

Future efficiency, pace, matchup, and score-distribution layers should extend
this shared contract with a definition/version, `available_at`, coverage,
sample size, and validation state. Any independent forecast must be evaluated
against contemporaneous market prices before appearing beside the observed
market. Do not infer current-season PPA from historical play storage.
