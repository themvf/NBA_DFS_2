# NBA game leaders and the top-end correction (`nba-points-sim-recal-v1`)

2026-11-06. Exploratory, not pre-registered. Two things: a per-game "who scores the most"
panel on `/nba/top-scorers`, and a correction to the points simulation it uses.

## No market feed

The Odds API carries no NBA "most points in the game" market. Listing every market for one
event across 54 books (`/v4/sports/basketball_nba/events/{id}/markets`, 1 credit) shows first
basket, double-double, triple-double and quarter points, but no top scorer. Sportsbooks do post
it on their own apps; the chances here come from the points lines and alternate lines we capture.

## The problem: the simulation's top end was too fat

Each player's points are drawn from the books' exceedance curve (`research/nba_wta_trios.py`
`simulate_points`, also `web/src/lib/nba-contest-optimizer.ts`). Over 1,688 played
player-games (Oct 20 - Nov 5; pregame captures, ESPN box scores), each player's actual score
placed against his own curve (randomized PIT) was flat through the middle but thin at the top:
the top 5% of levels held 0.47 of their share, the top 1% 0.18.

| Outcome | Curve said | Happened |
|---|---:|---:|
| Line + 5 or more | 25.5% | 19.6% |
| Line + 10 or more | 10.5% | 6.3% |
| Line + 15 or more | 4.6% | 1.9% |

This matches the known ladder caveat: over-only rungs keep longshot margin, so top-end chances
read high (the tail de-vig itself is the separate registered `nba-points-ladder-v2` test; the
reader is unchanged). Overstated big nights give low-line players too much chance to lead.

## The correction

`RECAL_G` is the realized PIT CDF at fixed knots (0, .1, ..., .9, .95, .99, 1), shrunk toward
the identity with 200 pseudo-rows. A draw's level u is mapped to G^-1(u) before the player's
curve is inverted, so big nights become rarer and the middle barely moves. The same-game
correlation and sitting rates are unchanged.

Leader test, 123 games, G refitted leaving each night out:

| Simulation | Per-game log loss | Biggest line: said | led |
|---|---:|---:|---:|
| Original | 1.690 | 34.7% | 43.0% |
| Corrected | 1.657 | 38.7% | 43.0% |

Paired change -0.033 per game (95% CI -0.053 to -0.012, nights resampled), better in 79% of
games. A separate correction for lines at or above 18 or 22 points did worse (CIs crossing zero),
so one correction is used. Four variants were tried on the same games; read the gain as an
estimate, not a validated number.

Other facts from the same games: the leader was one of the top two lines 63% of the time,
the top three 80%; the leader had no line 0.4% of the time.

## Still low on favourites

Compared with one book's "Most Points" prices on four games (Nov 6, margin removed by the
power method over the players shown), the original simulation was 6-13 points lower on every
favourite (Gilgeous-Alexander 73% vs 59%, LaMelo Ball 33% vs 25%, Doncic 67% vs 61%,
Wembanyama 44% vs 38%). With the correction, on the evening capture: Gilgeous-Alexander 72%
(his line had moved to 33.5), Doncic 66%, Wembanyama 44%, LaMelo Ball 27%. Against what
happened on the 123 games it closes about half of the gap (39% said vs 43% led). Those
markets carried 113-117% on the players shown alone; the book's own +10000 cap prices players
with near-zero chance at 1%.

## Leading his own team (2026-11-06)

Each game card also shows, per team, the team's biggest line and his chance to outscore his
quoted teammates (same draws, ties split; an unquoted bench player can still lead, which the
chance does not count), beside this season's record: how often that player, and the team's
biggest line whoever held it, actually led the team (`getNbaTeamLeaderGames`, from the nightly
grade and ESPN box scores; games before the viewed date, a player who sat scores 0).

Oct 20 - Nov 5, 246 team-games: the team's biggest line led his team 54% (95% CI 48-60%). The
team-level range was wide (Golden State 8/9, Denver 1/7, Toronto 1.3/8), but with about eight
games a team a permutation test of between-team spread gave p = 0.36, so the record is shown
as history, not used to adjust anything. Clearer leaders led more often (line 10+ above the
next teammate 64%, line 30+ 74%); recent over/under form showed no reliable pattern.

## Why the favourite misses, and the tags (2026-11-07)

Over 132 games (Oct 20 - Nov 6) the favourite by chance led 42% against 39% said. In 82 misses
he never sat; he finished under his own line in 73% of them (median -3.5). The winner was usually
the 2nd-4th biggest line and beat his own line by a median +9.5. Games decided by 20+ cost the
favourite about 5 minutes (he led 37% of those). Pre-game signals tested against the simulation's
chance (offset logistic, nights resampled): last-3 points vs line, rising shots, rising minutes,
earlier over rate, missing teammates' shot volume, back-to-back, line movement. None picked the
winner reliably better than the lines; missing teammates are already in the lines. About 20 tests
on 17 nights, so treat the one near-miss (last-3 above the line) as a forward test, not a finding.
Players outside the top five lines were given 15.8 wins and won 10.8.

Teams: the lines sort teams by how clear their leader is (thirds by chance: said 42/52/65%, led
46/50/66%), but a team's own record does not repeat (alternate games of the same team, r = -0.04
for "biggest line led", 0.95 for the stated chance).

Tags on the panel restate the chances in bands that held up (they add no information beyond them):

| Tag | Rule | Oct 20 - Nov 6 |
|---|---|---|
| WIDE OPEN | game favourite under 30% | favourite led 26% of 29; top three by chance led 76% |
| CLEAR FAVOURITE | game favourite 50%+ | led 57% of 27 |
| TOSS-UP w/ X | team's biggest line under 45% to lead his team | led 47% of 81; he or X led 75% |
| CLEAR | team's biggest line 65%+ | led 65% of 46 |
| QUESTIONABLE / OUT / ... | latest observed official injury report | fact, not a model |
| 1 BOOK | one book quotes the line | such players sat ~35% |

## Spreads and the blowout question (2026-11-07)

`ingest/nba_game_lines.py` stores each game's spread and total from one bulk Odds API call
(`markets=spreads,totals`, six named books, 2 credits a run) inside the existing nba-points-props
job (the morning read at 15:07 UTC, then 35-50 minutes before each tip group; see docs/the-odds-api.md). `nba_game_lines` gets a row only when a game's lines changed since
its last row, so a quiet capture writes nothing; the line at tip is the newest row before
`commence_time`. /health reads freshness from the call's `odds_api_usage` row, which every run writes.
The card shows the favourite and spread (median of the books, to the nearest half point) and total;
BIG SPREAD marks a 10+ favourite and is not measured yet.

Registered now, before any outcome is looked at: on games tipping from 2026-11-08 with a stored
pre-tip spread, does the game favourite (by chance) lead less often than the simulation said when
the spread is 10 or more? Primary: led minus said for those games, 95% CI from resampling nights;
the tag earns a real meaning (and a correction is considered) only if the CI lies below zero with
at least 60 such games. Secondary, descriptive: the same for the team leader rows, and the
favourite's minutes against his season average.

## Where it is used

- `/nba/top-scorers` "Game leaders" panel: always corrected (`web/src/lib/nba-game-leaders.ts`).
- `/nba/contest-trios` custom runs: corrected by default (`optimizeContest`, `recal`).
- The registered `nba-wta-trios-v1` frozen entries and their grade: never corrected
  (`simulate_points(..., recal=False)`, the default), because a rule change starts a new version.

The constants are pinned between Python and TypeScript by `tests/fixtures/nba_wta_fixture.json`.
The panel is not graded night by night yet.
