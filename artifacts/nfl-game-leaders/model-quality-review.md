# Game-leader model: not ready for recommendations

## What went wrong

Across all 64 games in 2026 Weeks 1–4, the model failed to improve on picking
the player with the highest recent average.

| Pick the player with the most… | Model correct | Simple average correct |
| --- | ---: | ---: |
| Rushing yards | 43.8% | 50.0% |
| Receptions | 17.7% | 22.4% |
| Receiving yards | 25.0% | 25.0% |

These percentages grade one first-choice player in each game. Ties split credit.
The model's extra adjustments hurt rushing and reception picks and did not
help receiving-yard picks. More simulations would not fix demonstrated accuracy.

## What we tested next

We built a separate challenger that learns from actual game winners. It considers
recent carries, targets, catches, yards, workload changes, position, and what
the opponent previously allowed. It was trained on earlier games, and its
settings were chosen using 2024 games. The final test used 94 games in 2025
Weeks 13–18 that had not been used in earlier model evaluations.

| Outcome | Challenger correct | Simple average correct |
| --- | ---: | ---: |
| Rushing yards | 36.7% | 38.8% |
| Receptions | 28.8% | 31.3% |
| Receiving yards | 27.7% | 26.6% |

The small receiving-yard improvement could be noise. No category demonstrated
an advantage under the test's uncertainty check. This is a failed challenger,
and we preserved the result. These weeks cannot now be treated as a fresh test
for another round of tuning.

## What this means

Use the simple average as the reference; neither new model has earned trust
for recommendations or betting edges. Historical records reconstruct likely
participants from earlier usage and use later statistical corrections. They
do not prove what we would have known about injuries before kickoff.

The next useful work is to collect genuinely frozen pregame availability and
workload forecasts, check predicted carries and targets against actual usage,
and evaluate future games without changing settings after seeing the results.
That will distinguish bad workload estimates from ordinary game-to-game variation.

All existing forecasts remain unchanged. This research has not been pushed or
deployed. The detailed 64-game results are in `weekly-review-all-games.md`;
the reviewed challenger records are in `challenger-study-reviewed.json`.
