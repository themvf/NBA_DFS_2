# MLB Independent Moneyline v2 Candidate

Evaluation date: 2026-07-17
Ticket: SCRUM-29

## Outcome

The first genuinely independent MLB moneyline candidate was **not promoted**.
It excludes sportsbook probability from its features and uses the market only
as the final-window benchmark. It trails that benchmark on both required proper
scoring rules.

| Metric | Market | Independent candidate |
|---|---:|---:|
| Final-window log loss | 0.685976 | 0.710451 |
| Final-window Brier score | 0.246150 | 0.257875 |

Training covered 1,107 games through 2026-06-20. The untouched final window
contained 289 games from 2026-06-21 through 2026-07-17.

## Point-in-time construction

- 1,443 completed 2026 games were backfilled from official MLB boxscores.
- The raw table stores two immutable team outcome rows per game and labels them
  `retrospective_backfill`.
- Features use only outcomes from strictly earlier game dates. Same-date games,
  including doubleheaders, cannot feed one another.
- Offense uses rolling wOBA proxy, ISO, K%, BB%, and runs per game.
- Starter context uses rolling FIP, K/9, and BB/9 from actual starter outcomes.
- Bullpen FIP is derived from team pitching minus the starter line.
- Rest is derived from the prior team game date.
- Missing values are measured and handled with training-fold medians plus
  explicit missingness indicators; they are not labeled as observed league
  averages.

## Training-only group retention

Feature groups were forward-tested using chronological folds before the final
window was opened.

- Bullpen improved training-fold log loss from 0.692368 to 0.690502 and was retained.
- Adding starters improved only 0.000361, below the 0.0005 retention threshold.
- Offense and rest did not improve the retained candidate.
- The retained bullpen-only candidate failed on the untouched final window,
  indicating distribution instability rather than a releasable edge.

## Coverage

- Offense and bullpen features: 91.5% coverage.
- Starter features: 62.2% coverage.
- Rest: 99.4% coverage.
- Every feature has nontrivial variation; the prior constant-feature failure is resolved.

## Release decision

Keep `mlb-ml-v1` labeled as market-anchored. Do not relabel the movement-board
model as independent or data-informed. The next candidate needs more historical
seasons and stronger starter availability before another untouched-window test.
