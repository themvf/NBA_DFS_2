# NFL Market Studies

The NFL tab links to `/nfl/market-studies`. It lists regular-season games from
the current season and compares a selected week with season totals through
that week. The page reads the existing archive and grades as scores become
final. It makes no odds requests and does not require a pick’em card.

## Source and grading contract

- `nfl_season_games` supplies the canonical game ID, week, teams, rest,
  scheduled kickoff and completed full-game scores, including overtime.
- The matchup bridge requires a unique matchup ID and matching season, week,
  date and teams. The NFL `game_odds_history` row must match that same event,
  date and ordered pair of teams. An unresolved join leaves the game visible
  with missing quote coverage.
- Select each market independently. A quote must precede both scheduled
  kickoffs (canonical and provider), the recorded frozen boundary if present,
  and the read time. A supplied book update must be no later than its capture.
  A newer incomplete capture cannot erase an earlier complete market. Keep
  the quote ID and observation time visible. The NFL archive does not retain
  an immutable provider kickoff per ordinary history row, and the selected
  first and last archived quotes are proxies for the opener and close.
- Summary records are wins, losses and pushes. Unit return uses one unit
  risked per priced side, with a refund on a push. Two-way moneylines treat a
  game tie as a push. Moneyline calibration is conditional on a winner and
  compares Brier scores on the identical Pinnacle and other-book cohort.
- Prior seasons in the separate line-movement archive and existing pick’em
  studies use their own cohorts and capture rules. Those results are excluded
  from these summaries.

| ID | Definition |
| --- | --- |
| NFL-01 | Both sides ATS by absolute spread, with 3 and 7 isolated. |
| NFL-02 | Both sides ATS by favorite rest below seven, exactly seven or above seven days. |
| NFL-03 | Both sides ATS by favorite’s listed home/away designation, including listed designations at neutral sites. |
| NFL-04 | Both sides ATS by first-to-last movement, with favorite changes and single quotes separate. |
| NFL-05 | Over/under at the last quote, by total below 42, 42 to under 48 or 48+. First-versus-last rows use the same two-quote games. These bins are exploratory. |
| NFL-06 | Team ATS by market role. |
| NFL-07 | Paired first-versus-last mean absolute error for spreads and totals. |
| NFL-08 | Better prices at the exact same number and capture, strictly before scheduled kickoff minus 60 minutes. This is a game benchmark. Contest-lock comparisons require each contest’s actual lock. |
| NFL-09 | Pending original pregame totals forecasts. |
| NFL-10 | Two-way moneyline returns, no-vig reliability bins, and paired same-capture Pinnacle versus mean of other complete no-vig book pairs. |
| NFL-11 | Both sides ATS by which side has more rest. |
| NFL-12 | Pending matched timestamped injury and quote evidence. |
| NFL-13 | Pending paired first-half quotes and halftime scores. |

This is descriptive historical monitoring. A prospective study needs a frozen
selection rule, executable entry and price, review gates fixed before enrollment,
and an explicit reference quote policy. There is no enrollment on this page.

Run the grading checks from `web` with
`node --import tsx scripts/test-nfl-market-studies.ts`.
