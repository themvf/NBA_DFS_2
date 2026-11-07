# NFL DFS archetype theses on 2023-25 outcomes — `nfl-archetype-thesis-v1`

Registered 2026-11-07, before any 2023-25 game was scored, after the owner asked
whether the proposed Showdown and Classic archetypes could be checked against
prior seasons. Script: `research/nfl_archetype_thesis.py`. Output:
`artifacts/nfl-archetype-thesis/thesis-2023-25.md`.

## What this can and cannot answer

We hold no DraftKings salaries, field ownership or payout curves before
September 2026, so no lineup can be built or placed for a 2023-25 week. What we
do hold is every player's weekly stat line (nflverse, scored under DraftKings
rules here), every defense's and kicker's weekly line, and the closing spread
and total of all 816 regular-season games. Each archetype is a claim about how
the players of one game score together, and those claims are testable on
outcomes alone. This study grades the claims, not lineups. Lineup results for
any archetype come from the 2026 ghost factorial and the weekly winners review.

Descriptive by design: no promotion rule, no star, no edge claim. It tells us
which archetype theses the games support, so quotas are set on the ones that
describe real co-movement rather than on folklore.

## Population (frozen)

- 2023, 2024 and 2025 regular-season games with a closing spread and total
  (`nfl_season_games`, nflverse: `spread_line` is the home team's expected
  margin, positive = home favored; `total_line` the closing total).
- Every player with a stat line in `ff_player_week_stats` for that game, scored
  under DraftKings Classic rules from the nflverse row (passing 0.04/yard, 4 per
  TD, -1 per interception, +3 at 300 yards; rushing and receiving 0.1/yard,
  6 per TD, +3 at 100 yards; 1 per reception; -1 per fumble lost; 2 per
  two-point conversion). Defenses from the stored components (sacks 1,
  interceptions 2, fumble recoveries 2, safeties 2, defensive and return TDs 6,
  points-allowed tiers 10/7/4/1/0/-1/-4), which match DraftKings' DST scoring
  except blocked kicks, which the source does not carry. Kickers 3/4/5 by
  distance and 1 per PAT.
- A "starting QB" is the team's leader in pass attempts that week; "lead RB"
  the carries leader; pass catchers are WR/TE ranked by targets.
- Game top-6: the six highest DraftKings scorers across both teams, any
  position, the outcome a Showdown roster is paid on.

## Theses (frozen; metric and slice fixed before scoring)

| id | archetype | claim | metric | slices |
|---|---|---|---|---|
| T1 | Shootout | Both starting QBs score big together | P(both QBs >= 20) against the product of the single rates (independence) | closing total terciles |
| T2 | Favorite onslaught | The favorite supplies most of the game's top scorers | mean favorite share of the top 6; P(>= 4 of 6) and P(>= 5 of 6) | spread 0-3, 3.5-6.5, 7+ |
| T3 | Underdog comeback | The underdog QB still lands in the top 3 | P(underdog QB in top 3); P(game top scorer is an underdog) | same spread buckets |
| T4 | Blowout | Favorite lead RB and favorite DST boom together | within-game correlation of their points; P(both above their position's 75th percentile) against independence | spread buckets |
| T5 | Captain stack | A pass-catcher top scorer comes with his own QB | when the top scorer is a WR/TE: P(own QB in top 3), P(in top 6); when a QB: P(an own WR/TE in top 6) | all games |
| T6 | Captain position | Which position produces the game's top scorer | distribution of the top scorer's position | all games; by total terciles |
| T7 | Low-scoring K/DST | Kickers and defenses reach the top 6 in low-total games | P(at least one K or DST in top 6); mean K+DST count in top 6 | total terciles; actual total under 38 / 38-48 / over 48 |
| T8 | Team stack 4+/5+ | One team dominates the top 6 | P(some team has >= 4 of top 6), P(>= 5) | spread buckets; total terciles |
| T9 | Classic double stack | A QB's points move with his own catchers | within-game correlation of QB points with own WR1, WR2, TE1; P(QB >= 25 and catcher >= 20) against independence | all games |
| T10 | RB plus own DST | A team's lead RB and its DST move together | within-game correlation; P(both above 75th percentile) against independence | spread buckets; favorite only |

Independence baselines multiply the two marginal rates over the same games.
Every rate carries a game-clustered bootstrap 95% interval (1,000 draws over
games). Season-by-season figures are reported for stability, never pooled with
2026.

## What counts as support

An archetype thesis is supported when its conditional rate differs from its
independence or all-games baseline with the interval excluding the baseline, in
the direction the archetype asserts, in the pooled 2023-25 sample and with the
same sign in each season. Supported theses are candidates for quotas in the
2026 factorial; unsupported ones are not built. No constant here moves a live
build.

## Result — first and only run, 2026-11-07 (816 games, 17,928 player-weeks, 1,000 draws)

| id | thesis | verdict | the number |
|---|---|---|---|
| T1 | Shootout: both QBs big together | NOT supported beyond the total | both QBs >= 20 in 13.5% of games [11.3, 15.8] against 11.8% under independence; inside each total tercile the rate sits on its own independence baseline (high total 20.8% vs 20.5%). QBs co-move through the closing total, not with each other |
| T2 | Favorite onslaught | SUPPORTED, scaling with the spread | favorite share of the top 6 is 55.8% [54.6, 57.0]: 52.3% at spreads 0-3, 61.6% at 7+. Favorite >= 4 of 6 in 44% of games (57% at 7+), >= 5 of 6 in 12.6% (22% at 7+). Same sign every season |
| T3 | Underdog comeback | Minority outcome, sized by the spread | underdog QB in the top 3 in 36% of games (39% at 0-3, 29% at 7+); the top scorer is an underdog in 40% [36.5, 43.1], 30% at 7+ |
| T4 | Blowout: favorite RB and DST together | NOT supported | within-game correlation for favorites -0.03 [-0.09, +0.04], -0.06 at spreads of 7+; co-boom 12.3% against 11.6% independence. The blowout correlation does not exist in these three seasons |
| T5 | Captain stack | SUPPORTED | a WR/TE top scorer has his own QB in the top 3 in 59% [53, 64] of games and in the top 6 in 84%; a QB top scorer has an own WR/TE in the top 6 in 94% [91, 97]. Every season 57-62% on the first figure |
| T6 | Captain position | SUPPORTED (RB/WR/QB) | the game's top scorer is a WR 31%, RB 30%, QB 29%, TE 5.6%, DST 3.6%, K 1.6%. In high-total games DST falls to 0.8%; in low-total games 5.2% |
| T7 | Low-scoring K/DST | SUPPORTED by the closing total | a K or DST is in the top 6 in 66% of low-total games [61, 71] against 44% of high-total games [38, 50]; 0.96 vs 0.52 such players on average. By actual total: 75% under 38 points, 38% over 48 |
| T8 | Team stack 4+ / 5+ | 4+ is the common outcome; 5+ is not | some team holds >= 4 of the top 6 in 63% of games (70% at spreads of 7+) and >= 5 in 16% (23% at 7+, 11% in high-total games) |
| T9 | Classic double stack | SUPPORTED, strongest thesis here | QB points against his own WR1 r = +0.41 [+0.36, +0.45], WR2 +0.34, TE1 +0.30, each season inside those bands; QB >= 25 with WR1 >= 20 in 9.3% of team-games against 5.4% under independence |
| T10 | RB plus own DST | NOT supported | all teams r = +0.07 [+0.02, +0.12], co-boom 8.3% against 7.2%; nothing for favorites (see T4) |

What this changes about the archetype list: the favorite onslaught, captain
stack, captain-position, low-scoring K/DST and Classic double-stack theses are
supported and keep (or earn) their quotas; the team-stack 5+ form is a one-in-
six outcome that should be weighted that way; the blowout "favorite RB plus
favorite DST" archetype and the RB-plus-own-DST Classic idea describe a
correlation the games do not show and are dropped; a shootout archetype adds
nothing a total slice does not already carry. None of this grades a lineup or
an edge; the lineup grade stays with the 2026 factorial.
