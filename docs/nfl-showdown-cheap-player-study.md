# NFL Showdown sub-$3,000 players: field ownership, outcomes, our exposure (2026-11-08)

Descriptive study, not a registration. Asked after Charlie Woerner ($400, blocked
by the no-punts policy) was drafted at 10% in the CIN@ATL Showdown and scored a
touchdown. Question: does the field's ownership of cheap players carry
information our model lacks, and what pre-lock signals reproduce it?

Readable report: https://claude.ai/artifact/62GJm75qWwTnhV1ZuGmwRQ (private).

## Data

- 18 Showdowns with an imported contest, weeks 1-9 (`nfl_dfs_field_contests`,
  `format='showdown'`), one contest per game: the largest field with complete
  ownership. 11 games come from the DraftKings standings export (every drafted
  player observed; absent = 0%). 7 come from stat-api top-100 lineups, whose
  ownership rows exist only for players a top-100 lineup used, so those lean
  toward scorers. Every rate below uses the 11 unless it says "all 18".
- 545 player-games with flex salary under $3,000; 432 active (OUT/IR/doubtful
  removed by `nfl_dfs_slate_players.dk_status`/`is_out`); 265 active in the 11.
- Points: `nfl_dfs_field_ownership.fpts`. Our exposure: the last saved
  optimizer run for the game before kickoff. Prior usage: carries + targets in
  the team's two previous games from `nfl_pbp_play_participants`. DK anytime
  TD: `prop_odds_history`, last capture before kickoff (many cheap players have
  no line). Hit = 6+ DK points.

## Findings (11 complete-ownership games, n=265)

Correlation with points: field ownership 0.55, salary 0.46, DK average 0.28,
prior two-game touches 0.25, our projection 0.22.

| field ownership | n | mean pts | hit 6+ | Wilson 95% |
|---|---:|---:|---:|---|
| under 1% | 198 | 0.1 | 2 (1%) | 0-4% |
| 1-3% | 23 | 0.7 | 0 | 0-14% |
| 3-5% | 13 | 2.3 | 1 (8%) | 1-33% |
| 5-10% | 17 | 2.9 | 1 (6%) | 1-27% |
| 10-20% | 9 | 3.0 | 2 (22%) | 6-55% |
| 20%+ | 5 | 7.0 | 2 (40%) | 12-77% |

| salary band | n | field own | mean pts | hit 6+ | when field >= 5% |
|---|---:|---:|---:|---:|---|
| $200-1,000 | 201 | 0.5% | 0.2 | 2 (1%) | 0 of 6, mean 1.6 |
| $1,100-2,000 | 40 | 5.2% | 1.9 | 3 (8%) | 2 of 13 (15%); under 5%: 1 of 27 |
| $2,100-2,900 | 24 | 8.5% | 2.8 | 3 (12%) | 3 of 12 (25%); under 5%: 0 of 12 |

Field ownership adds to our projection: our projection >= 4 and field >= 5%
hit 3 of 9 (mean 5.9); our projection >= 4 and field under 2% hit 0 of 28
(mean 0.0). Our projections of 6+ on cheap players (n=18, 1 hit) are mostly
$200 backs with no history projected from the position prior (Connors 9.3,
Waters 8.9, Finley 8.0) and veterans projected from a role they no longer had
(Devin Neal 8.5, Higbee 8.3 with 0 touches in his last two games).

Pre-lock signals: DK anytime TD +800 or shorter 3 of 18 hit (17%); +800 to
+1300 0 of 20; +1300 and longer 2 of 49; no line 3 of 178. Touches in the last
two games: 0 -> 1 of 190; 1-2 -> 2 of 40; 3-5 -> 3 of 23 (13%); 6+ -> 2 of 12.

The cheapest active skill player on each team (the salary-relief slot): 25
cases, 0 hits, mean 0.1, field 2.2%. All 18 games: the field put 5%+ on a
sub-$1,000 player 8 times, none scored 6; Woerner today makes it 1 of 9.

Our builds (333 active cheap player-games with a saved run): we used 43
players, the field 101 at >= 1%; our mean exposure 1.9% vs field 2.1%;
exposure-weighted points per cheap slot 3.44 (ours) vs 3.62 (field). Of our 21
exposures at 10%+, 9 scored 0, and 7 of those 9 were players the field held
under 2.5% (Horton wk1 56%, Higbee 48%, Alie-Cox 25%, Chism 25%, Strong 20%,
Adkins 11%, Blue 10%). All four of our hits (Ferguson 17.4, Hooper 9.1,
Boerkircher 8.1, Horton wk8 7.6) were players the field also had at 5%+.

Winners: 130 of 413 matched top-20 lineups (31%) carried a sub-$3,000 flex;
slots 94 at $1,100-2,000, 31 at $200-1,000, 5 at $2,100-2,900. 306 of 719
top-20 rosters did not match our slate names and are excluded here only.

## Candidate rule (in-sample; NOT shipped)

Keep the block under $1,100. Above it, eligible only if the DK anytime-TD line
is +800 or shorter OR the player had 3+ carries plus targets in his team's last
two games; cap 15% exposure, flex only. On the same 11 games:

| class | n | field own | mean pts | hit 6+ | our exposure |
|---|---:|---:|---:|---:|---:|
| eligible | 35 | 9.6% | 3.53 | 6 (17%) | 10.0% |
| blocked: no market or usage signal | 29 | 2.6% | 0.62 | 0 | 1.4% |
| blocked: under $1,100 | 201 | 0.5% | 0.19 | 2 (1%) | 0.0% |
| current policy (all $1,100+) | 64 | 6.4% | 2.21 | 6 (9%) | 6.1% |

Keeps all six hits above $1,000; drops Roush 14.6% (0), Juszczyk 12.2% (2.0),
Ekeler 8.1% (5.0), Melton (0), and our Alie-Cox 25% (0). It does not remove
Higbee, Chism, Strong or Cowing, which qualify on touches: it filters the pool,
it does not fix the projections behind those picks. Thresholds were chosen after
seeing these games and 6 hits in 35 is small. Shipping it requires a
registration with the thresholds frozen and a forward test from week 10:
eligible vs blocked hit rate and mean points, Wilson intervals, one verdict.

## Scripts

Scratch only (not committed): `cheap_study_1..6.py` in the session scratchpad
built the dataset from `nfl_dfs_field_ownership`, `nfl_dfs_slate_players`,
`nfl_dfs_field_top_entries`, `nfl_dfs_optimizer_runs`/`nfl_dfs_lineups`,
`prop_odds_history` and `nfl_pbp_play_participants`. Rebuild from those tables
if the study is re-run; the method is fully described above.
