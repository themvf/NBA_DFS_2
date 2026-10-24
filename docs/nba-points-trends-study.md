# NBA points prediction: historical context study

Protocol recorded October 24, 2026, before fitting or inspecting final-period model results.

## Question and target

Predict a player's actual basketball points, conditional on appearing. Do rest, opponent defense/style/pace, home court, recent opportunity or prior head-to-head residuals improve forecasts beyond the player's recent scoring history?

The warehouse has 2025–26 only: 26,651 regular-season player-game rows and 2,460 team-game rows covering 1,230 games. Exclude the two non-appearance/missing-point rows, require ten previous player appearances and ten previous games for each team. Report all exclusions and join checks. Current-game minutes, attempts, efficiency, final margin, lineup membership or injury hindsight must never become pregame predictors. Historical source capture times are after the season: this is a lagged-statistical replay, not verified archived pre-lock inputs. Injury availability and late news are not reconstructed.

## Chronology and comparisons

- Development: games before January 1, 2026.
- Validation: January 1–31, 2026. Select naive window and model hyperparameters using only this period.
- Final evaluation: February 1–April 12, 2026. Inspect once; no adjustments after seeing results. Refitting through January 31 is permitted. Rolling features may incorporate earlier games during the evaluation period; target-game outcomes may not.
- Primary cohort: players whose pregame ten-appearance scoring mean is at least 20. Secondary: all eligible appearances. Cohort membership cannot depend on the target game's points.
- Primary metric: mean absolute error in basketball points. Also report RMSE and bias (predicted minus actual). A median-oriented forecast may improve MAE without improving expected-point accuracy; RMSE must accompany MAE.
- Naive candidates: previous 5-, 10-, 20-appearance arithmetic means and ten-appearance EWMA (alpha .25).
- Learned history baseline: lagged scoring means, variability and EWMA.
- Ablations add: recent minutes/shot/free-throw opportunity; home/away; rest and schedule density; opponent points allowed; pace and defense per possession; defense by scoring style; prior head-to-head scoring residuals. Also evaluate all features together and leave-one-family-out comparisons.
- Primary learned estimator: standardized Ridge with validation-selected alpha from 10/100/1000. Nonlinear sensitivity: histogram gradient boosting, 150 iterations, learning rate .05, leaves 7/15, L2 10/100, selected on validation. Every family and candidate's final result is disclosed; do not choose a production rule from final-period performance.

## Source and calculation contract

Player identity is player_id; game identity is game_id; team and opponent are the IDs on that dated game's records. Validate reciprocal team joins, unique game/player keys, two team records per game and player scoring sums matching team totals. Missing opponent counting fields in team logs must be reconstructed from the opposing team's own row, not replaced by current-season ratings.

All rolling features strictly exclude the target date. Back-to-back is a team schedule property (previous team game one calendar day earlier); days since the player's own last appearance is separate. Three-in-four and four-in-six count scheduled team games from prior dates plus the target game. Previous-minute workload may be interacted with rest, but actual current-game minutes may not enter.

Estimate historical game possessions from both sides' FGA + .44 FTA − offensive rebounds + turnovers, averaged across teams. Estimate game duration from the two teams' player-minute totals to normalize pace to 48 minutes. These are approximate descriptive possession estimates, not official possession counts. Lag all pace and defensive measures before using them. Compare raw points allowed with defense per 100 estimated possessions so fast pace is not silently called poor defense.

Scoring-style interactions use the player's earlier three-point attempt share and FT/FGA ratio with the opponent's earlier allowed three-point rate, percentage and FT/FGA ratio. No historical player-position source is verified here, so do not claim position-specific defensive effects.

Head-to-head features use prior scoring deviations from the player's baseline at each prior meeting, not full-season averages computed afterward. Report sparse coverage; one matchup is not a stable player-specific defensive effect.

## Trend and correlation interpretation

Compare raw points and deviations from a pregame baseline; player composition can confound raw rest/defense comparisons. Use held-out residuals for same-team and opposing-player correlations. Report minimum shared-game counts; teammates' correlated misses do not prove one player's score is a usable pregame predictor of another's score.

Bootstrap held-out dates, preserving all player observations on a date, to describe uncertainty in paired MAE differences. These intervals are exploratory, are not adjusted for the many feature comparisons, and do not establish causality or future profitability. This is one season and includes only players who appeared; rest-induced DNP selection is unmeasured. Examine late-season sensitivity rather than tune to April outcomes. No automatic point bonus, probability haircut or production promotion follows from this experiment.
