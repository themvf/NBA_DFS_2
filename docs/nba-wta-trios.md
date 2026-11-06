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
  size. Never graded; the frozen all-games set is the record.
- The books' 30+ and 40+ chances read high (registered `nba-points-ladder-v2` test);
  the builder uses the books' curve as is, which favors high-ceiling players.
- Pool is the top 20 lines; 5 of 48 actual top-three scorers in discovery came from
  outside it.
- Availability is the measured sitting rate by book count, not injury news.
