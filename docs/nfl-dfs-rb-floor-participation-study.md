# NFL DFS running-back floors: participation ledger (pre-registered)

Study id `nfl-dfs-participation-ledger-v1`. Registered 2026-10-20, before any
2024 or 2025 player-game was scored for this question and before week 7 of 2026
was played. Nothing here changes production projections. The candidate rule
below is fixed now; there are no constants to tune.

## Why

Our RB P10 is beaten from below far more often than 10% of the time. Measured
2026-10-20 over saved pregame builds: for each player, the latest non-research
optimizer run made before his game's kickoff (`load()` and `review_rows()` in
`model/nfl_week_projection_review.py`, branch `claude/nfl-week-skill-review`),
players projected 5 or more, not ruled out in the build. A player with no stat
line whose team's game has results is scored 0, as DraftKings scored him.
"Q" means DraftKings status Q/D or availability status QUESTIONABLE/DOUBTFUL.

Share finishing below our P10 (n in brackets):

| pos | week | all | model rows | anchored rows | played | did not play | Q/D | played, not Q/D |
|---|---|---|---|---|---|---|---|---|
| RB | 1 | 13% (95) | 13% (95) | - | 8% (49) | 17% (46) | 17% (18) | 10% (39) |
| RB | 2 | 15% (91) | 15% (91) | - | 12% (58) | 21% (33) | 50% (2) | 12% (57) |
| RB | 3 | 23% (57) | 23% (57) | - | 14% (49) | 75% (8) | 67% (3) | 13% (47) |
| RB | 4 | 22% (54) | 22% (54) | - | 15% (48) | 83% (6) | - | 15% (48) |
| RB | 5 | 32% (50) | 35% (46) | 0% (4) | 19% (42) | 100% (8) | 50% (4) | 18% (39) |
| RB | 6 | 23% (53) | 28% (18) | 20% (35) | 15% (48) | 100% (5) | 40% (5) | 14% (44) |
| TE | 3 | 13% (30) | 13% (30) | - | 10% (29) | 1 of 1 | 1 of 2 | 11% (28) |
| TE | 4 | 17% (36) | 17% (36) | - | 15% (34) | 1 of 2 | 0 of 1 | 15% (34) |
| TE | 5 | 14% (35) | 16% (32) | 0% (3) | 12% (34) | 1 of 1 | - | 12% (34) |
| TE | 6 | 22% (32) | 1 of 2 | 20% (30) | 20% (30) | 1 of 2 | 2 of 3 | 18% (28) |
| WR | 3 | 8% (85) | 8% (85) | - | 6% (77) | 25% (8) | 0% (7) | 7% (71) |
| WR | 4 | 15% (89) | 15% (89) | - | 10% (79) | 50% (10) | 2 of 3 | 9% (77) |
| WR | 5 | 9% (75) | 10% (71) | 0% (4) | 10% (69) | 0% (6) | 22% (9) | 8% (62) |
| WR | 6 | 1% (79) | 0% (17) | 2% (62) | 1% (74) | 0% (5) | 0% (6) | 1% (69) |

The weeks 3-6 figures reproduce the ones that prompted this (RB 23/22/32/23%,
TE 13/17/14/22%, WR 8/15/9/1%). Weeks 1-2 are a different regime: 45 and 33 of
the RB rows were `position_prior` projections for fringe backs with no usable
history, and most of them did not play.

Weeks 3-6 pooled, game-clustered 95% intervals (59 games):

| pos | all | played | played, not Q/D |
|---|---|---|---|
| RB | 25% [19, 30] (214) | 16% [10, 21] (187) | 15% [10, 20] (178) |
| TE | 17% [10, 23] (133) | 14% [8, 21] (127) | 14% [8, 20] (124) |
| WR | 9% [5, 12] (328) | 7% [4, 10] (299) | 6% [4, 9] (279) |

Of the 53 RBs below their P10 in weeks 3-6, **24 did not play at all**, 3 were
Questionable players who played, and 26 were healthy players who played. 21 of
the 24 were fringe backs projected under 9 with no RB1/RB2 depth label, and the
same names recur week after week: Salvon Ahmed (4 weeks), Sincere McCormick
(4), Michael Carter (3), Jawhar Jordan (3), Bam Knight (2), Audric Estime (3),
Devin Neal (2), Devin Singletary (2). In the nflverse weekly roster (cached
through week 5) Ahmed, McCormick and Jordan were on the practice squad (`DEV`)
every week, Neal in week 4; Knight (weeks 3-4) and Singletary and Carter
(week 5) were dressed (`ACT`) and recorded no stat; Singletary and Carter were
inactive (`INA`) in week 3.

So the RB problem is mostly **missing zero mass**, not a narrow tail. Among
RBs who played, the rate is 14-16%, above target but with an interval that
reaches 10%. TE has almost no non-players; its excess is in the played tail
and this study does not expect to fix it.

The market anchor is not the cause. On anchored rows, the share below the
model's own pre-anchor P10 equals the share below the anchored P10 (RB 18% vs
18%, n=39; TE 18% vs 18%, n=33), although the anchor raised RB means by 15% on
average.

## Root cause in code

1. **The history holds only games with a stat line.**
   `_history` (`ingest/nfl_dfs_projections.py:180`) reads
   `ff_player_week_stats` (`:209`), which by design stores a week with no stat
   line as an absent row, never a zero. A practice-squad week, a healthy
   scratch, or a dressed game without a touch leaves no trace.
2. **Recency is counted in stat lines, not in team games.**
   `project_player` keeps the player's last 34 stat rows
   (`model/nfl_dfs_historical.py:318-321`) and weights them by row position
   (`_recency_weights`, `:178`, half-life 3 rows, `:32`). A player's newest
   stat line gets full weight however long ago it was. Salvon Ahmed's newest
   line is 2023 week 11 with Miami; with half-life 3 that game and the two
   before it carry about 60% of his own-history weight, so he was projected 6-7 points every week
   on Chicago's practice squad. 7 stat rows also clear
   `minimum_historical_games` (`:324`), so he is treated as `historical`
   and given 64% own-history weight (`:334`).
3. **The draws can never produce a zero for not playing.**
   Each draw resamples one stat row (`:342`, `:348`) and the floor is the
   10th percentile of those draws (`:370`). With no zero rows to draw, P10 is
   a "if he gets touches" floor while the grade (and DraftKings) scores a
   non-player as 0.
4. **The anchor scales P10 with the mean** (`web/src/lib/nfl-dfs/market-anchor.ts:191-196`,
   `k = market mean / model mean`, every quantile times `k`). This moves floors
   up when the market is higher than the model, but it did not change coverage
   in weeks 5-6. A zero stays zero under it.
5. **Played-tail width, secondary.** Half-life 3 gives an effective sample of
   about 8.7 games out of 34 (half-life 6 gave 16.7), with half the weight on
   the last three. v5 chose half-life 3 on MAE alone
   (`model/nfl_dfs_half_life_screen.py`); no quantile was graded. Recent games
   in a stable role understate how often a back loses his role or leaves
   early: of 31 played RBs below P10 in weeks 1-6, 12 got under half their
   projected touches, against 17 of 164 above P10. This is not what the
   candidate addresses; it is a separate study.

## Candidate: the participation ledger

One change, to the history the model is given. The model code
(`project_player`) is unchanged.

For each QB/RB/WR/TE, build his history as a **ledger of team games** instead
of a list of stat lines: every regular-season game, before the cutoff, played
by the team he was on in the nflverse weekly roster that week, with his roster
status that week:

- `ACT` (dressed) or `DEV` (practice squad): the game enters the ledger. His
  stat line if he has one; otherwise an empty line, which scores 0 and has
  0 for every stat.
- `INA`, `RES`, `CUT`, `RET`, any other status, or not on a weekly roster:
  the game is left out, as today. An inactive game cannot be told apart from
  an injury absence, and the availability layer owns injuries.
- A stat line with no matching ledger week (a roster gap in the feed) is kept,
  so no game line he actually played is ever dropped.

Everything downstream is as in `nfl-dfs-historical-v5`: the last 34 ledger
rows (`max_player_games` now counts ledger rows), half-life 3 by row position,
`minimum_historical_games` counted on ledger rows, peer rows chosen by the
player's (now lower) mean, 2,000 draws, the same seed rule. Each ledger row
is a `HistoricalWeek` with empty `stats`, so `draftkings_points` is 0 with no
change to scoring.

Rows the market anchors: the anchor is applied to the candidate's distribution
exactly as today (`k` = market mean / candidate mean), so anchored means are
unchanged and only the anchored range moves. Prop lines are void when a player
does not play, so the market mean is a mean given that he plays; leaving
anchored means unchanged is a deliberate simplification, recorded as a known
limitation.

## Prediction stated before scoring

The candidate lowers the P10 (and the mean) of fringe and practice-squad backs
toward 0 and leaves established starters nearly unchanged. It should cut RB
P10 pinball loss and bring the below-P10 rate down, but not to 10%, because
the healthy played tail (about 15%) and game-day inactives (`INA`, not
zero-filled) remain. TE should barely move.

## Grading

**Metric (primary).** Paired P10 pinball loss at tau = 0.10, candidate minus
baseline, per player-game, on the same rows:
`loss = (0.10 - 1[actual < p10]) * (actual - p10)`. Mean difference with a
95% bootstrap interval, 10,000 resamples of whole games (both teams' players
together; seed 20261020).

**Also required (guards).**
- Below-P10 rate: the candidate's rate must be closer to 10% than the
  baseline's.
- MAE (paired, candidate minus baseline): 95% interval upper bound below
  +0.10 DK points. The candidate changes means; it may not buy floors with a
  worse projection.
- P90 pinball (tau = 0.90, paired): point estimate no worse than +0.01.

**Reported, not gated:** the same for TE and WR (WR is a no-harm check),
played-only and did-not-play splits, splits by target-week roster status
(`ACT`/`INA`/`DEV`), by projection band (5-10, 10-15, 15+), and how many
players' projections moved by more than 1 point.

### Gate H: historical replay, held out

2024 and 2025 regular seasons, weeks 4-18, each graded separately. Built from
nflverse `stats_player_week`, `roster_weekly` and `games` files, cached with
SHA-256 digests recorded in the artifact. Both arms run `project_player` from
`model/nfl_dfs_historical.py` at the file's frozen hash (SHA-256 `63d0353099727a292ef2a1255baf312a6cadf74bc3da38bc507709e41e3afc30` on 2026-10-20), with history strictly
before the target week, team implied totals from the nflverse closing total
and spread (same context for both arms), and no market anchor.

Population: RB (primary), TE and WR, whose target-week roster status is
`ACT`, `INA` or `DEV` (the statuses a DraftKings pool lists and our pregame
layer can miss; `RES` is excluded because the availability layer removes
injured-reserve players), with a **baseline** projection of 5 or more (so
both arms are graded on the same rows). Outcome: DraftKings points from his
stat line, 0 if he has none.

These seasons were used by other studies (half-life, opponent and workload
screens) but no one has scored a P10 on them with non-players counted as 0.

### Gate F: forward, the production population

2026 weeks 7-14, never seen. Rows: the same selection as the table above
(latest non-research build before kickoff, projected 5+, not ruled out).
Both arms are recomputed at that run's history cutoff from the database
history, with the ledger built from `roster_weekly_2026.csv` restricted to
weeks before the cutoff, and anchored rows anchored identically. The saved
build's own P10 is reported beside them for continuity. Floor: at least 150
RB rows over at least 6 of the 8 weeks; short of that the gate waits for
weeks 15-18 and is graded once then.

### Verdict, decided now

- **KILL** if, for RB, the Gate H pinball interval's upper bound is at or
  above 0 in either 2024 or 2025. No rescue by slicing (practice squad only,
  starters excluded, one season), by adding `INA` weeks as zeros, or by a
  different half-life: each of those is a new version with its own
  registration.
- **PASS** only if Gate H's RB interval is wholly below 0 in both seasons,
  every guard holds in both, and Gate F's RB pinball point estimate is below 0
  with the below-P10 rate closer to 10% than the baseline's.
- Otherwise **INCONCLUSIVE**: production unchanged, nothing re-run on the
  same data.

A PASS licenses a proposal, not a release: it would ship as
`nfl-dfs-historical-v6` only on the owner's decision, and it would change
means as well as floors, so every study that pins the DFS model version or
`baseline_config_hash` (the coherent-scenario study, the replacement-upside
grade's baseline) would need its pin re-registered at its next boundary.

## Out of scope, named so they are not lost

- **Played-tail width** (finding 5). RB and TE are about 14-16% below P10 even
  when they play. A quantile study of the half-life or of a role-loss mixture,
  graded on pinball rather than MAE, is a separate registration.
- **Questionable players.** 31% of Q/D RBs finished below P10 (n=32,
  weeks 1-6), many because they sat. An inactivity probability for Q players
  belongs to the availability layer.
- **Practice-squad players in the pool.** At least 10 of the 24 RB
  non-players in weeks 3-6 were on the practice squad (counted through week 5,
  where the roster cache stops), and the availability layer called
  them `EXPECTED_ACTIVE` from Sleeper. Excluding `DEV` players pregame would
  remove them without any model change. That is an availability fix and is
  not tested here; the ledger candidate handles them through zeros instead.

## Reproduction of the table

Scripts used for the table are in the session notes folder
(`coverage.py`, `tables.py`); they call `load()` from
`model/nfl_week_projection_review.py` on branch `claude/nfl-week-skill-review`
for each week 1-6 and position, then join each player's actual touches from
`ff_player_week_stats`. Data read 2026-10-20; nothing was written.
