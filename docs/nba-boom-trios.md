# NBA boom trio alternatives (`nba-boom-trios-v1`)

Registered 2026-10-31, before the rules were run on any slate. Implemented by
`research/nba_boom_trios.py`; constants pinned by `tests/test_nba_boom_trios.py`.

## Question

Keep the strongest-looking trio, then give a seat or two to players whose shot
volume is rising. Do those swaps raise the trio's actual points?

## Inputs

The frozen `nba-points-ghost-v2` snapshot of each slate (stored in
`nba_ghost_snapshots` before the first tip) and its latest ghost grade
(`nba_ghost_grades`). Nothing is recomputed from data fetched after the freeze.

## Rules (frozen)

- **Pool**: the existing `market_line` trio pool (`research.nba_leader_evaluation.eligible`):
  one quote per player, a market line, not reported Out.
- **Baseline**: the three highest market lines, same tie rule as the existing trio.
- **Boom score**: rising shot volume = (FGA + 0.44 x FTA) per game over the last
  3 games minus the last 20, from the snapshot's frozen `shots_growth` and
  `ft_growth`. It ranks replacements only; it is never added to projected points.
- **Boom candidate**: ready history and a boom score of at least 2.0.
- **Boom alternative**: keep baseline #1 and #2; replace #3 with the boom
  candidate (not already in the baseline) with the highest boom score whose
  line is at most 3.0 points below #3's. Ties: higher line, then player key.
- **Aggressive alternative**: keep baseline #1; fill the other two seats with
  the pair of boom candidates with the highest combined boom score whose three
  lines total at most 6.0 points below the baseline total. A baseline player
  who is a boom candidate may keep his seat. Ties: higher total, then player keys.
- **No candidate inside the limit**: no alternative that day. It is counted as
  "no alternative", never filled with someone else.
- Each alternative carries its reason, e.g. "Projected total drops by 2.5
  points, but X has taken 4.1 more shot attempts a game over his last 3 games
  than his last 20 (21.3 vs 17.2)."

## Tracking

Per slate: actual three-player totals of baseline, boom and aggressive; the
difference from the baseline; helped / hurt / tied. A trio with a DNP or
unresolved member is incomplete and left out, never scored as 0. Diagnostic
only: how often the swapped-in players beat their frozen historical P90
compared with the players they replaced.

Report: `python -m research.nba_boom_trios report` (counted dates only) or
`--all-dates` (adds pre-registration days, descriptive). `show` prints each
day's three trios and the reasons. Read-only; it writes nothing to the database.

## Live page (2026-10-31, owner: run it live and tracked)

`/nba/boom-trios` (nav: NBA > Boom Trios) shows each night's three trios with
their reasons and, once graded, every player's points and each alternative's
result against the baseline, plus the record table. It recomputes the same
rules in `web/src/lib/nba-boom-trios.ts` from the frozen snapshot (slimmed in
SQL to the fields the rules read, `web/src/db/nba-boom-trios.ts`), pinned to
the Python by `tests/fixtures/nba_boom_trios_fixture.json` (both sides tested).
The page reads; it writes nothing. The rules and the count are unchanged.

## What counts and when to look

A slate counts only if its snapshot is prospective and was created after
2026-10-31 19:30 UTC (first counted slate: 2026-11-01). Oct 26-31 are
pre-registration and stay descriptive. Review after 30 counted dates: mean
difference from the baseline with a date-bootstrap 95% interval, helped/hurt
counts and sizes, no-alternative days, and the P90 diagnostic. An alternative
is worth turning on only if its interval sits above zero there and holds on
the next untouched stretch. Any rule change is a new version and restarts the count.

## The boom lineup builder (`nba-boom-exposure-v1`)

Started by the Codex session on 2026-10-31 and finished the same day on the
same definitions as these rules: on `/nba/top-scorers`, pick a rising-volume
player (boom score at least 2.0) and require him in N of five unique trios
under a points limit. Lineup 1 is always this module's baseline trio; points
are market lines; nobody's projection changes; an infeasible request is
refused, never relaxed. `Save before the first tip` stores both lineup sets in
`nba_boom_lineup_records` (web-owned, append-only, insert refused by the
database clock after the first tip), and `/nba/boom-trios` grades each save
(sum of actual points across the five lineups against the five
highest-projected). Saves are discretionary choices and are reported
separately from these frozen rules; they never enter this module's count.
Code: `web/src/lib/nba-boom-lineups.ts`, `web/src/db/nba-boom-board.ts`,
`web/src/app/api/nba/boom-lineups/route.ts`,
`web/src/app/nba/top-scorers/boom-lineups-client.tsx`.
