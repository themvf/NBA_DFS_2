# Anytime touchdown model comparison

Prepared 2026-11-06 for the proposed first prospective slate on Sunday,
2026-11-08. This is an experiment specification, not an implemented model,
saved forecast, scheduled job, or evidence of predictive improvement.

## Question and outcome

Compare two estimates of the probability that an individual scores at least
one rushing or receiving touchdown in the full game, including overtime.
Passing TD credit, conversions, defensive TDs and return TDs are excluded.
First-touchdown scorer is a separate outcome and is outside this experiment.

Use the same canonical game IDs, GSIS player identities, eligibility policy,
decision times and source captures for both models. Retain every forecast,
including low-probability players and players who do not score. Do not select
the evaluation population using realized touches or touchdowns. Preserve and
report omitted scorers and source/identity failures separately.

## Candidates

A: Regularized logistic regression trained directly on the binary outcome.
Use lagged workload, location-specific opportunity shares, shrunk TD history,
team offensive context, opponent context and position. Training-only scaling,
regularization selection and optional calibration must use time-ordered splits.

B: Hierarchical opportunity/conversion model. Predict distributions of team
opportunities and player shares, with separate rushing/receiving and field
position categories. Pool sparse conversion evidence toward comparable league
opportunities. Integrate over workload and conversion uncertainty. Preserve
an unallocated/new-player component and reconcile allocations to team budgets.
Do not equate the Poisson transform of mean TDs with uncertainty-integrated
anytime probability. Scope, priors and workload distribution choices must be
documented and frozen before prospective forecasts are generated.

Benchmark: Shrunk historical player TD-count mean transformed using a stated
Poisson assumption. Also capture an existing production anytime probability if
one exists with the identical outcome; expected TD counts alone are not that
probability. Both candidates must be compared against the simple benchmark.
Sportsbook prices may be a separately reported reference, not an input to these
initial candidates. Yes-only anytime odds cannot be normalized across players
to remove margin because player outcomes are not mutually exclusive.

## Data and validation before Sunday

- Use complete historical player sources, not a current-roster-filtered history.
  Verify counted scorer credits, source completion, no-TD games, canonical
  schedule/team joins and player identities. Missing observations are not zeros.
- Define the historical player population from evidence available before games;
  observed participation is not a substitute for historical eligibility.
- Audit source coverage first, then freeze training, tuning and untouched
  chronological holdout periods before comparing candidate results. Historical
  inputs captured later must be labeled retrospective; never assert that their
  current corrected values were available at the historical decision time.
- Inspect both Sleeper and FantasyPros depth evidence for each Sunday team with
  research.nfl_dual_depth_audit, plus week-matched FantasyPros injuries and
  official inactive lists when published. Preserve conflicts and timestamps.
  Depth rank is not projected workload. Unresolved availability/roles must be
  flagged or the affected game withheld under a shared predeclared policy.
- Test temporal leakage, duplicate identities, nullified TDs, overtime, zero-TD
  games, unknown outcomes, team-budget accounting, probability bounds and
  grading against hand-reconciled source examples.

## Sunday protocol

The canonical database schedule checked on November 6 contains 13 Sunday games
for Week 9. The first kickoff is November 8 at 09:30 America/New_York; the final
Sunday kickoff is 20:20 that evening (November 9 in UTC). Select the slate by
local Eastern date rather than UTC date. Revalidate schedule before forecasting.

Freeze each game's final research forecast 20 minutes before kickoff, beginning
at 09:10 ET for the early game. Save provisional earlier versions separately.
All data retrieval and fitting used by a forecast must finish by its decision
time. Retain source files/digests, capture times, features, availability evidence,
model settings, implementation digest, probabilities and exclusions. Never
overwrite forecasts or backdate a late run. If evidence or fitting is not ready,
record the game as missed rather than inventing a pregame prediction. This plan
does not itself schedule or execute these future captures.

## Grading and decision rule

Primary: paired player-game binary log loss, A versus B and each versus benchmark.
Secondary: Brier score, calibration plots with counts/uncertainty, calibration
intercept/slope, and position/workload diagnostics. Disclose numerical clipping
for log loss; it must not hide structurally omitted players.

Freeze the evaluation roster before kickoff. Retain inactive/DNP predictions in
the unconditional scoring-probability evaluation (zero if the verified complete
game has no qualifying TD for that player); report participation strata as
diagnostics. Conditional-on-playing forecasts require a distinct evaluation and
must not be mixed with unconditional probabilities. Unknown results remain
ungraded, with coverage reported; never silently remove losing predictions.

Sunday is the first forward observation, not the model-selection endpoint.
Report cumulative paired differences and uncertainty clustered by week once
multiple weeks exist; game-clustered Sunday summaries are descriptive. Do not
claim superiority from the day's number of correct picks. Decide the evaluation
horizon and any formal promotion rule before beginning the prospective study,
using historical uncertainty/power analysis rather than an arbitrary number of
Sundays. No production replacement or betting-edge claim follows automatically.

## Readiness

Still required: implement both candidates and the common capture/grading path;
audit historical eligibility and coverage; run chronological comparisons and
meaningful tests; freeze model choices and evaluation horizon; capture Sunday
availability and generate immutable pregame forecasts. The existing longest-TD
simulator is regulation-only and is not an interchangeable full-game anytime-TD
candidate without explicit scope changes and verification.
