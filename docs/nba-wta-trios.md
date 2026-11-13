# NBA winner-take-all trio builder (`nba-wta-trios-v1`)

Registered 2026-11-06, before any forward night was frozen. Page: `/nba/contest-trios`.
Code: `research/nba_wta_trios.py`; tables `nba_wta_portfolios`, `nba_wta_grades`
(`db/nba_wta_schema.py`); tests `tests/test_nba_wta_trios.py`.

## The contest

The owner's contest: each entry names three players; its score is the sum of their
points; the single highest total takes the whole prize; tied winners split it; many
entries per person are allowed; the field is 200+ entries. Nothing here has been
checked against the contest's written rules. In particular, a selected player who
does not play is assumed to score 0.

## Why

Top Scorers ranks by the books' lines, and the three biggest lines are the most
likely trio. In a winner-take-all field that is the wrong target: an entry pays only
when it beats every other entry, and the most likely trio is also the most
duplicated. On the 16 graded nights Oct 20 - Nov 5, the three biggest lines averaged
86 points against 112 for the night's best trio, were the best of the 1,140 trios
from the top-20 lines once, and ranked a median 52nd. Every reading of the books'
own numbers (line, ceiling, P(30+), simulated P(top 5)) picks almost the same
players, so the gain, if any, comes from beating the field, not from out-predicting
the line.

## Method (frozen; changing any of it is a new version)

1. **Pool.** The 20 biggest consensus lines from each game's latest capture before
   the freeze, read with `model/nba_points_ladder.py` (the reader the page and the
   nightly grade use). A name quoted in two games is dropped.
2. **Each player's points.** The books' exceedance curve (main line plus the
   alternate ladder) inside its range; linear to 0.995 at zero points below the
   first rung; exponential decay at the last segment's rate (bounded 0.05-1.0 per
   point) past the last rung. Drawn through a Gaussian copula: teammates -0.06,
   opponents +0.02, everyone else 0 (measured on standardized `actual - line`,
   graded nights Oct 20 - Nov 5, 5,061 and 5,802 pairs). Rounded to whole points.
   A player sits and scores 0 with probability 0.35 (one book quoting him), 0.03
   (two or three) or 0.002 (four or more), measured Oct 20-31. A player with no
   usable ladder (1% of top-20 lines) gets a normal with sd = 3.9 + 0.18 x line,
   the measured spread.
3. **Field.** 300 entries. 10% enter the exact three-biggest-lines trio; the rest
   pick three players with probability proportional to exp(line / 3). **These are
   stated priors**: there is no data on what this contest's entrants pick.
4. **Payout.** In each of 6,000 simulated nights an entry earns 1 / (number of
   entries tied at the top) when its total is the night's highest, else 0. Our own
   entries compete with each other and with the field. A fresh field is drawn for
   every 500 nights.
5. **Selection.** Ten distinct trios from the 1,140 in the pool, chosen greedily:
   each adds the most expected prize share to those already chosen. The first k are
   the builder's k-entry set. Seeded from the date and pool, so a freeze is
   reproducible.
6. **Baseline.** The ten trios with the largest line sums, scored the same way.
7. **Freeze.** After the 11:07 AM and 6:07 PM ET line captures
   (`capture_nba_points_props.yml`), only while every game on the date is still to
   tip; the table refuses a row created at or after the first tip. An unchanged
   capture is not re-frozen. The grade reads the latest freeze for the date.

## Grade (`nba-wta-grade-v1`, each morning after the box scores)

Actual points from ESPN's final box scores; a pool player with no row in his final
box score scored 0; a game without a final box score leaves the night pending.

- **Primary: realized prize share, five entries.** With the actual points, 2,000
  fields drawn from the frozen field model (seeded by the freeze), the share of the
  prize the builder's first five entries would have won, against the same for the
  baseline's first five. Paired by night; night-resampled 95% interval on the mean
  difference.
- **Guard: field-free percentile.** The best of the five entries' actual total,
  as a percentile among all 1,140 pool trios, builder against baseline. It shows
  whether the builder only "wins" by leaning on the assumed field.
- **Secondary:** both at ten entries; nights with a best entry in the top 1% of
  pool trios ("in contention").
- **Real results:** when the owner supplies a night's real winning score or field,
  that night is also scored against it. Real results outrank the modeled field.

**Review once, after 30 graded nights.** The builder is preferred only if the
primary interval is above zero **and** the guard's mean difference is not below
zero with its interval excluding zero. Otherwise the biggest-lines baseline stands.
Constants do not change during the 30 nights; a change starts a new version and a
new count.

## Discovery check (recorded, not counted)

The builder was run on the 16 nights Oct 20 - Nov 5 with the lines as they stood
one minute before each first tip, and graded with the rules above:

| | Builder (5 entries) | Baseline (5 entries) |
|---|---:|---:|
| Field-free percentile of best entry | 93.1% | 93.6% (diff -0.005, CI -0.061 to +0.051) |
| Nights in contention (top 1%) | 4 | 8 |
| Realized prize share, frozen field model | 1.48 prizes | 0.21 |
| ... field with no chalk, tau 5 | 0.88 | 0.44 |
| ... field with 25% chalk, tau 2 | 3.29 | 0.24 |

On the field-free score the builder is no better. Its advantage is all in avoiding
the crowd, it depends on how crowded the real field is, and Oct 29 alone supplies
1.00 of its 1.48. The realized-share grade also uses the same field model the
builder optimized against, so it partly measures the builder against its own
assumption. That is why the guard and real contest results exist, and why nothing
here is a demonstrated edge. These 16 nights informed the method and never count
toward the review.

## Known limits

- **The exact entries are not stable; the mix and the share are.** Re-running Nov 6 with
  only the random draws changed (measured 2026-11-06): the top five shared 0-1 entries with
  the original run at 6,000 simulated nights and 1-2 at 24,000, while the five-entry prize
  share stayed 5.9-6.6% and the same players dominated (Gilgeous-Alexander in most entries,
  Tatum and Wembanyama often, Luka rarely). Many trios are near-ties. Each night's frozen set
  is still a fair draw of the method, so the test is unaffected, but no single entry should be
  read as uniquely right. The 6,000-night share runs slightly high against 24,000 (greedy
  selection fits the draws it chose from).

- The field model is a guess; real entrant data is the single most valuable input.
- **Custom runs** (`/nba/contest-trios`, "Build entries for your contest"): the same rules
  ported to the browser (`web/src/lib/nba-contest-optimizer.ts`, pinned to this module by
  `tests/fixtures/nba_wta_fixture.json`; regenerate with `python -m research.nba_wta_trios
  fixture`) on the latest lines, for contests that leave games out or have another field
  size. Not part of the registered test; the frozen all-games set is that record.
- **Saved runs** (`nba-contest-run-v1`, 2026-11-10). The owner enters the custom builder's
  output (6 to 10 entries, locked at the first tip, games varying by night), so every
  Optimize run is stored in `nba_contest_runs` through `/api/nba/contest-runs`
  (`web/src/lib/nba-contest-runs.ts`, `web/src/db/nba-contest-runs.ts`; web-owned table,
  one row per distinct run by content digest, so a repeat press keeps the first save).
  The browser sends its settings and entries; the games' teams and tip times come from the
  stored lines, and the database clock decides "before the first tip". A late run is kept
  and marked, never refused: this is a record, not a trial, and nothing here is counted by
  the registered test. The page's "Your saved runs" scores each run from the final box
  scores by this module's `load_actuals` rule (no row or did not play = 0; a game that is
  not final or matches two box scores = unknown). Which run was entered, the winning score,
  the field size and what the field picked come from the contest's results export, which
  is not imported yet (the owner can download every entry; first file due after the
  2026-11-10 contest). That import is what can measure the field assumption.
- The books' 30+ and 40+ chances read high (registered `nba-points-ladder-v2` test);
  the builder uses the books' curve as is, which favors high-ceiling players.
- Pool is the top 20 lines; 5 of 48 actual top-three scorers in discovery came from
  outside it.
- Availability for the frozen v1 entries is the measured sitting rate by book count, not injury news.
  Custom runs read the official injury report (see "The injury report rule" below).

## The lock replay and the lead-time sitting rates (2026-11-12, exploratory)

Not pre-registered, and about a dozen cuts of the same 22 nights (Oct 20 - Nov 11): read the
smaller results as leads. Each night was rebuilt as it stood one minute before the first tip
(the owner's lock) with `load_pool`, and scored with `load_actuals`.

**What the owner reported (2026-11-12).** Almost every entry in his contests has the
biggest-line player, and anyone can change players until the lock. So the field guess
(84-99% of the field on the biggest line) points the right way, and a star ruled out during
the day does not leave early entrants stuck. The exact share is still unmeasured.

**Sitting chance depends on when the lines were read (`nba-points-sit-lead-v1`).**
`DNP_BY_BOOKS` was measured from each game's last capture before its own tip. The contest
locks at the first tip, so the lines an entry is built from are hours ahead of the late
games. Every pre-tip capture (10,720 player-captures, `python -m research.nba_sit_lead`):

| Books at the line | Lines read under 1.5 h before his tip | 1.5 to 6 h | 6 h or more |
|---|---:|---:|---:|
| 4 or more | 8 / 3,999 = 0.2% | 76 / 3,603 = 2.1% | 82 / 2,111 = 3.9% |
| 2-3 | 4 / 404 = 1.0% | 5 / 209 = 2.4% | 10 / 223 = 4.5% |
| 1 | 49 / 171 = 29% (too few to split) | | |

Both halves of the window agree on the order, and the later one is higher (4+ books: 0.2 / 1.4 /
2.3% on Oct 20-31, 0.2 / 2.7 / 5.8% on Nov 1-11), so the pooled rates should be measured again
as the season goes on. One name the box scores spell differently was set aside
(Carlton Carrington). Custom runs use these rates (`sit_prob`, `SIT_BY_BOOKS_LEAD`; the web
`sitProb`, pinned by the fixture) from each player's own lead time; the saved run records the
rule and each player's chance. The frozen v1 entries keep `DNP_BY_BOOKS`. The page warns when
the lines are over an hour old, and tags a player at 1.5% or more.

The rates barely move which entries are picked. On the 22 nights, entries built with the lead-time
rates instead of the book-count rates (both corrected, registered field) were worth 7.75 against
7.64 expected prizes per 100 nights at 8 entries, took 61% of their picks from games tipping 90+
minutes after the capture against 64%, and carried a 4.2% chance per entry of holding a sitter
against 4.4%. What changes is the stated risk (about 4% per entry, where the book-count rates
implied 0.6%) and the advice to run close to the lock.

The tracked test freezes around 22:07 UTC. Anthony Davis (Nov 6), Austin Reaves (Nov 9) and
Tyler Herro (Nov 10) were in its pool, had their lines pulled before the first tip, and scored
0. A run made at the lock would not have held them; the tracked grade does.

**The field guess decides which build is best.** Entries built against three fields and scored,
with an independent corrected simulation, against each (expected prizes per 100 nights, 8
entries, field of 300, fair share 2.6; a one-off scratch replay, not in the repo):

| Built for | Uses the biggest line | Field piles on (83% hold him) | Mixed (52%) | Spread (30%) |
|---|---:|---:|---:|---:|
| Piles on (today's custom builder) | 7% of entries | 7.8 | 4.0 | 4.6 |
| Biggest line sums (the baseline) | 98% | 1.7 | 4.1 | 9.2 |
| All three fields at once | 49% | 6.2 | 4.7 | 7.7 |

With the owner's report the default stays as it is. The fields here pick by line size alone;
a field with skilled entrants would cut every number.

**What a winning trio looked like.** The best pool trio averaged 110 points, 22 above the sum
of the three biggest lines; the top 1% of trios started 12.5 above it. It held at least one of
the three biggest lines on 20 of 22 nights, all three once, and a player ranked 11th-20th on
11. The biggest line was in it on 9 nights.

**The uncorrected simulation aims too high.** The actual best trio sat at the 36th percentile
of the v1 simulation's best-trio distribution on average (median simulated 116, actual mean
110); with the top-end correction, the 54th (median 108). The correction was fitted on most of
the same nights, so this is a consistency check, not a second test.

**Dropped.** First game of the night (+3.7 points over the line among 37 pool players; +0.14,
95% CI -0.37 to +0.64, on all 2,366 graded players). The ghost model's ceiling-watch flag
(flagged players finished 0.9 below unflagged against the line; line + 10 reached 7.2% vs
7.1%, 1,399 rows). Morning-to-lock line moves (no pattern). Small slates (the three biggest
lines ranked higher on 2-5 game nights, six nights only).

## The injury report rule (`nba-points-sit-injury-v1`, 2026-11-13, exploratory)

The lead-time rates above are an average over two very different groups. The league's official
injury report (`nba_injury_observations`, read from the PDFs at
`ak-static.cms.nba.com/referee/injury/`) names nearly everyone who sits. At the lock on Nov 2-12
(10 nights, 1,086 players with a line at two or more books, the last report read 0.1-3.9 hours
before the first tip; `python -m research.nba_sit_injury --start 2026-11-02 --end 2026-11-12`):

| Status on the report | Sat |
|---|---:|
| Out | 7 of 7 |
| Doubtful | 1 of 1 |
| Questionable | 9 of 21 (43%) |
| Probable | 0 of 16 |
| Available | 0 of 36 |
| Not listed, both teams' reports in | 3 of 982 (0.3%) |
| Not listed, a team not yet filed | 0 of 23 |

17 of the 20 players who sat were flagged beforehand. Books still quoted a line for every one
of the eight Out or Doubtful players at the lock capture (Scottie Barnes 23.5 at five books on
Nov 9), so the pool held players the league had already ruled out.

With the day-before report instead (`--morning`, read 20-26 hours before the lock):
Questionable 17 of 33, not listed 7 of 305 with both teams in and 12 of 316 with a team not
filed. So a status is worth acting on early, but "not listed" clears a player only on a fresh
report.

The rule, for custom runs only (`sit_prob_injury` / `sitProbInjury`, fixture-pinned):

- **Out or Doubtful:** left out of the pool, listed under the entries as left out.
- **Questionable:** stays in at a 43% chance of sitting, tagged.
- **Cleared** (Probable, Available, or not listed with both teams filed) by a report read within
  4 hours of the lock: 0.3%.
- **Anyone else** (no report, a report older than that, a team not filed, one-book players):
  the lead-time rate.

The lock is the first tip among the games ticked. The saved run records the rule, when the
report was read, each listed status and who was left out. 21 Questionable players is a small
sample (a 95% interval of roughly 24% to 63%); measure again as reports accumulate. The frozen
v1 entries do not use the report.

**Reads timed to the tips.** The report was read at 11:37 AM, 6:37 PM and 6:52 PM Eastern,
which suits a 7 PM lock and nothing else. The dispatcher's `nba-injury-refresh` job now also
fires on every tick in the 50 minutes before the night's first tip (three reads, the last 5 to
20 minutes before it) and on the one tick 5 to 20 minutes before each later tip, for a contest
that leaves the early games out (`nbaTipPickupDue`; tips come from the stored lines through an
index-only read). The league posts on the quarter hour and the ticks run seven minutes after.
If the tip times cannot be read the two fixed evening reads still run and the tick says so.
Each read stores about 70 KB. `nba_injury_observations` is on /health (36 h).

Limits: the report only, no beat-writer news; a Questionable player in a late game is usually
settled after a first-tip lock; and a team that has not filed leaves its players unknown, not
healthy.

