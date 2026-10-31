# Pregame game totals and individual points leaders

Question: does the highest sportsbook game total identify the day's highest individual scorer, and do totals correlate with individual points? This exploratory audit does not change production forecasts.

## Timing and coverage

The warehouse contains 1,549 NBA game-odds observations dated October 2025 through June 2026, but collection began April 5 UTC. Earlier game dates therefore do not establish pregame evidence. Do not use those retrospective captures as though they were known before tip.

For the available regular-season window, select the latest non-null game total captured strictly before the earliest ESPN scheduled game start on that local slate date. This applies the user's common first-game lock, including later games. Match warehouse NBA game IDs through nba_matchups; verify home/away identities against canonical team names. Reconcile every covered game's final team scores independently against ESPN. Player totals come from the previously reconciled season player logs.

Coverage before first tip: April 5 0/11 games; April 6 5/5; April 7 9/10; April 8 6/7; April 9 6/6; April 10 15/15; April 12 14/15. The incomplete April 7, 8 and 12 dates lack Clippers games. Only complete slates with two or more games qualify for the daily-leader test. No missing game is treated as having a lower total.

## Results

| Date | Games | Highest pregame total | Actual individual leader | Leader's game total / rank |
|---|---:|---|---|---|
| April 6, 2026 | 5 | PHI at SAS, 237 | Nickeil Alexander-Walker, 36 | NYK at ATL, 226.5 / 4th |
| April 9, 2026 | 6 | CHI at WAS, 249 | Brandon Ingram, 38 | MIA at TOR, 236.5 / 2nd |
| April 10, 2026 | 15 | MEM at UTA, 248 | Amen Thompson, 41 | MIN at HOU, 224 / 12th |

The highest-total game produced the outright slate leader on 0/3 dates. It supplied 2 of the 9 top-three scorer slots. The equally weighted random-game baseline is 14.4% per date; 0/3 leaders is unsurprising given that baseline. These observations establish neither useful positive lift nor evidence of an inverse relationship. Ties receive fractional credit in the script.

Across the 55 games with eligible captures, Pearson correlations were:

- Pregame game total versus actual combined game points: +0.457.
- Pregame game total versus that game's highest individual points: +0.155.
- Pregame game total versus individual points among players with at least ten prior appearances: +0.114 (1,056 appearances).
- Pregame game total versus individual points above/below the player's prior 20-appearance average: +0.063.

Player appearances are clustered within only 55 games and six dates, not 1,056 independent game environments. The 84 appearances by players averaging at least 20 prior points showed negative correlations (-0.236 raw and -0.290 residual), another reason not to extrapolate the pooled positive coefficient to star selection. All results are descriptive and exploratory. No predictive coefficient was fitted or validated.

## Interpretation and next acceptance criteria

The combined scoring environment and the allocation of points to one player are different quantities. A higher team total can be distributed among several players. Player scoring history, expected minutes, shot opportunities, teammate availability, and spread must be examined alongside the total; their incremental contributions remain to be validated for this question.

This window is the final week of the regular season, with unusual rest and rotation risks. Three complete dates cannot establish a dependable highest-total rule or calibrate leader probabilities. Current-season October game-total snapshots are absent from this particular history table; player-prop captures are a separate source and are not substituted for totals.

A larger study must freeze the entire eligible slate before first tip, compare the top-total game and top-two-total games against slate-size baselines, handle ties, and separately evaluate individual-point prediction error after accounting for player ability, spread and availability. Promote game totals only if they improve chronological held-out points error or leader ranking versus a player-only baseline. A descriptive correlation alone does not meet that requirement.

Reproduction: `python -m research.nba_vegas_leader_audit`. Local frozen inputs, ESPN responses, row-level evidence and results are in `artifacts/nba-vegas-leaders-20261024/`. The script reads the database without schema initialization or writes. No paid odds API was called.
