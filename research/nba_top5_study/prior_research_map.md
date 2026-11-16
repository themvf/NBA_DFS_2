# NBA trio research map: picking the night's five best 3-player lineups

The contest: three players per entry, scored on their summed actual points, winner takes all, 200+ entries. Readings marked **interim** come from read-only database queries in the source reports. They sit below each trial's registered floor and settle nothing.

## 1) What is firmly known

**The points line is the best mean we have.** Measured on 1,543 played player-games (Oct 26 – Nov 12), mean absolute error:

| Source | Error (points) |
|---|---|
| Late consensus line | 5.02 |
| Our 11 AM line | 5.03 |
| stat-api | 5.06 (correlation 0.993 with the line) |
| Opportunity model | 5.30 |
| Last-20-game average | 5.42 |

The main line is calibrated: overs hit 540 of 1,098 (49.18%).

**The books' high-end chances are too high, but they still rank players.**
- 25+ points: stated 18.68%, happened 13.65% (n=835). 30+: 13.68% vs 9.26% (n=551).
- Line+10: 10.5% vs 6.3%. Line+15: 4.6% vs 1.9% (1,688 player-games).
- Even so, the tails beat a constant forecast at every threshold.
- The top-end correction improved game-leader log loss by 0.033 (CI 0.012–0.053). It was fitted on mostly the same nights, so the improvement is not independently confirmed.

**What a winning trio looks like**
- On 22 nights replayed at the lock, the best trio in the pool averaged 110 points, 22 more than the three biggest lines combined. The top 1% of trios started 12.5 points above that sum.
- On 16 discovery nights, the three biggest lines averaged 86 points against 112 for the night's best trio. They were the best of the 1,140 possible trios once; their median rank was 52nd.
- The best trio held at least one of the three biggest lines on 20 of 22 nights, all three on 1, the #1 line on 9, and a player ranked 11th–20th on 11.
- 5 of 48 actual top-three scorers came from outside the top-20 lines.

**Who leads a game**
- The leader had one of the top two lines 63% of the time and one of the top three 80%.
- The usual winner is the 2nd–4th line beating his own line by a median +9.5.
- The favourite led 42% of the time against 39% stated.
- Players outside the top-five lines were given 15.8 wins and won 10.8.

**Same-game links are near zero.** Teammates −0.06 (5,061 pairs; −0.021 historically). Opponents +0.02 (5,802 pairs; +0.056 historically).

**Who sits can mostly be known at the lock** (official injury report, 10 nights, 1,086 players)
- Out 7 of 7 sat, Doubtful 1 of 1, Questionable 9 of 21 (43%, interval roughly 24–63%).
- Not listed, with both teams' reports filed: 3 of 982 (0.3%).
- 17 of the 20 players who sat were flagged beforehand.
- Books still quoted lines for all 8 Out/Doubtful players at the lock.
- By lead time (players quoted at 4+ books): under 1.5 h before tip, 0.2% sat; 1.5–6 h, 2.1%; 6 h or more, 3.9%. Players quoted at only one book sat 29% of the time (49/171).
- On Nov 8 a morning-only read put Luka Doncic (#1 line, scored 0) in all five baseline entries.

**The best build depends on how crowded the field is.** Expected prizes per 100 nights, 8 entries, against fields where 83% / 52% / 30% of entries hold the biggest line:

| Build | 83% | 52% | 30% |
|---|---|---|---|
| Built for a pile-on field | 7.8 | 4.0 | 4.6 |
| Biggest line sums | 1.7 | 4.1 | 9.2 |
| Built for all three fields | 6.2 | 4.7 | 7.7 |

This was a one-off scratch replay that is not in the repo.

**The winner-take-all builder scores no higher than the biggest line sums**
- Discovery: percentile of the best entry, ignoring the field, 93.1% vs 93.6% (CI −0.061 to +0.051).
- Live, 8 nights (interim): 90.6% vs 91.4%. Mean best of five: 92.6 vs 97.4 points, against 111.4 for the night's best pool trio.
- The builder's only gain is avoiding the crowd under an assumed field. Its first five entries held the biggest-line player 4 times in 40; the baseline held him 40 times in 40.

**The exact five entries are unstable.** Rerunning with only the random draws changed, the top five shared 0–1 entries at 6,000 simulated nights and 1–2 at 24,000. The player mix and the five-entry prize share (5.9–6.6%) stayed stable.

**The pool limit costs about as much as selection (interim, 8 dates, all but one provisional)**
- Best trio in the full pool minus best in the top 12: about 5.4 points on average.
- Best trio in the top 12 minus the baseline's best entry: about 5.6 (total gap about 11.7).
- No five-entry set ever held the night's exact top three.

## 2) Dead ends (do not retry)

- **Different readings of the books' numbers** (line, ceiling, P(30+), simulated P(top 5)) pick nearly the same players.
- **Pregame signals for the game leader**: last-3 games vs the line, rising shots, rising minutes, over rate, missing teammates, back-to-back, line moves. About 20 tests on 17 nights; none beat the lines.
- **Context features for stars** (rest, opponent defense, pace, home/away, head-to-head): every confidence interval includes zero. Gradient boosting was worse (6.354 vs 6.297).
- **Short windows**: the last-5 average was worse than the last-20 by 0.538 points (CI 0.351–0.718).
- **Highest game total**: produced the slate leader on 0 of 3 dates. Among players averaging 20+, its correlation with points was −0.236 (84 appearances).
- **Back-to-back penalty**: back-to-back games averaged 23.0 points vs 22.4.
- **Weak-defense boost**: none (22.46 vs 22.54 points).
- **First game of the night**: +0.14 over the line (CI −0.37 to +0.64), 2,366 players.
- **Ceiling-watch flag**: flagged players finished 0.9 below unflagged against the line; line+10 reached 7.2% vs 7.1%.
- **Morning-to-lock line moves**: no pattern.
- **Small slates**: only 6 nights; dropped.
- **Separate top-end corrections for lines ≥18 or ≥22**: worse than one correction.
- **Team leader records**: do not repeat (alternate-game r = −0.04, p = 0.36).
- **Sit rates by lead time instead of by book count**: 7.75 vs 7.64 prizes per 100 nights. They change the stated risk, not the picks.
- **v3 joint draws**: understate 40+ nights; the Oct 27 reconstruction was worse than the market.
- **Aggressive two-seat boom swap**: −9.09 over all 19 dates (CI −17.36 to −1.55). Interim; it is not formally closed on counted dates.
- **"Most points in the game" market**: none across 54 books.

## 3) Pending registered trials

| Trial | Floor | Progress |
|---|---|---|
| nba-wta-trios-v1 | 30 graded nights | 8 graded (Nov 6–13). Freeze moved to 35–50 min before first tip on Nov 13; the two periods are reported separately |
| Trio math (best of five vs line sums) | 30 fully paired dates + 10 confirmation | 5 paired. Interim: joint −6.6 and robust −3.8 points on best of five; other dates incomplete because of non-playing picks |
| Boom trios | 30 counted dates | 12 counted. Interim: boom swap +2.44 (CI −10.67 to +14.0), n=9 |
| Ladder-v2 tail de-vig | 30 nights + 1,000 player-games with a 35+ value | Floors expected about early December; INCONCLUSIVE if unmet by 2027-04-12 |
| Big spread (10+) | ≥60 games from Nov 8 | Accruing |
| Three v3 experiments | 30 dates + 100 alerts | Pending |
| Role challenger | 30 dates + 100 conflicts | Pending |
| Ghost v2 | 30 dates | Pending |

## 4) Open leads worth testing

1. **Measure the real field.** Import the contest results export. The field's share on the biggest line flips which build wins. The owner reports almost every entry holds him.
2. **Widen the pool.** Go past the top 12/20: the pool gap is about 5 points, and an 11th–20th player sat in the best trio on half the nights.
3. **Anchor plus upside.** One top-3 line plus 2nd–20th-ranked upside matches what winning trios looked like. This has not been tested as a rule.
4. **A corrected tail.** Options are the top-end correction, ladder-v2, or stat-api's 90th percentile (8.8% above, pinball 1.262 vs the books' 1.283, the best-calibrated we hold).
5. **A field-robust build.** The build made for all three fields scored 6.2 / 4.7 / 7.7.
6. **Build at the lock** with a fresh injury report. Sit rates are rising: players read 6 h or more before tip sat 2.3% (Oct 20–31) vs 5.8% (Nov 1–11).
7. **Smaller leads:** the boom swap's 90th-percentile hits (6/9 added vs 1/9 replaced); last-3 above the line (a near-miss, forward test only); a blowout discount (the favourite lost about 5 minutes in 20+ point games).

## 5) Reusable data inventory

| Source | Contents | Coverage |
|---|---|---|
| prop_odds_history (nba) | Main and alternate points lines, 6 books; near-tip T-5/T-1 rows | From Oct 20; tip-timed captures from Nov 13 |
| nba_points_calibration | Last pre-tip line, P(over), floor/ceiling, P(20+…40+), actual points, status | From Oct 20 |
| nba_box_score_games/players (ESPN) | Points, minutes, shots, played flag | From Oct 20 |
| nba_wta_portfolios / grades | Top-20 pool, 10 entries + baseline, actual points for every pool player, best pool trio, percentile, prize share | 22 frozen / 8 graded, Nov 6–14 |
| nba_trio_math_snapshots / grades; nba_trio_pool_audits | Top-12 pool, 3 portfolios, verified actuals; full-pool vs top-12 best trio | From Nov 4 |
| nba_research_archives | v3 originals with 5,000 joint draws for the whole modeled pool (25–172 players); review archives with verified box lines | From about Oct 28 |
| nba_ghost_snapshots / grades | Frozen ghost v2 pools (line, history, opportunity, P90, shot growth) and actuals | From Oct 24/25 |
| nba_injury_observations | Official reports with publish and observe times | Analyzed from Nov 2 |
| nba_game_lines | Spreads and totals, row on change | From about Nov 7 |
| statapi_nba_player_projections | Per-game projections and quantiles | Oct 20 – Nov 12, 6,090 rows; manual ingest |
| nba_player_game_logs | 2025-26 regular season | 26,651 rows |
| nba_contest_runs; nba_boom_lineup_records | Owner's saved runs; boom saves | 0 rows each |

Code: research/nba_wta_trios.py (`load_pool`/`load_actuals` replay at any timestamp, corrected simulation, sit rules), research/nba_sit_lead.py, research/nba_sit_injury.py, research/nba_trio_pool_report.py.

## 6) Gaps nobody has examined that matter

- **The contest rules are unchecked.** Whether a player who does not play scores 0, or can be swapped, has never been confirmed.
- **The real field and winning score have never been observed.** nba_contest_runs has 0 rows although its save code has been on main since Nov 10. It is unverified whether no runs were made or saves are failing silently.
- **No study scores against "the night's best five trios" directly.** Grades use the best of N, the single best pool trio, or prize share. Nobody has measured overlap with, or the score of, the night's actual top-five trios.
- **Spotting who blows past his line is unproven.** Telling which high-line player will exceed his line by 10+ is not established; the conditional tail checks for lines ≥20 are pending.
- **How the five entries relate to each other** (one repeated anchor vs varied anchors) has not been studied as its own question.
- **The full lock-time package has never been graded on actual outcomes**: injury rule + corrected tail + wider pool together, ignoring the field. The frozen v3 joint draws and the WTA pools would allow a retrospective replay, labelled as retrospective.
- **Every finding is exploratory.** They rest on at most 22 nights, with roughly a dozen cuts of the same nights.