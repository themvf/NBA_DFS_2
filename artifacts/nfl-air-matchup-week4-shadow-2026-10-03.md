# Week 4 air-yard matchup shadow check

Saved DraftKings Classic upload `0759c396-6ae9-4ce0-beca-d35506f736af`, linked projection run `1130e058-d07e-5d2d-a8b3-51f09b67d4a3`, cutoff `2026-10-03T01:07:48.097Z`. The 12 games kick off October 4. The companion JSON contains all 124 eligible player records, selected market quote IDs, assumptions, and paired simulation results.

The player explanation now shows prior three-game targets, air yards, shares, opponent air yards allowed per target, the regressed depth factor, projected team attempts, and the pre-cutoff market quote. Of 379 WR/TE rows, 124 met the two-game/four-target minimum; 255 did not. All 124 eligible players had a market quote captured before both the projection cutoff and kickoff. Six carried the stricter `AIR_MATCHUP` lineup chip.

The 10,000-draw paired shadow test holds the same simulated attempts, targets, and target depth for neutral and matchup arms, then applies the bounded opponent depth factor. Attempt variation (15%), depth variation (0.3 log standard deviation), and leading/trailing budgets (0.9/1.1 times attempts) are explicit assumptions. Under the neutral script, 59 players gained and 65 lost mean target air yards relative to a league-average opponent. Largest positive changes:

| Player | Opponent | Matchup factor | Mean target air-yard change | Change in chance of 100+ target air yards |
| --- | --- | ---: | ---: | ---: |
| Tee Higgins | JAX | 1.161 | +16.5 | +12.6 pp |
| Ja'Marr Chase | JAX | 1.161 | +11.7 | +10.3 pp |
| Malachi Fields | ARI | 1.137 | +8.8 | +6.9 pp |
| Parker Washington | CIN | 1.066 | +8.7 | +4.5 pp |
| Xavier Hutchinson | DAL | 1.098 | +8.6 | +7.0 pp |

The simulation measures target air-yard opportunity only. It does not simulate catches, yards gained, touchdowns, QB correlation, legal lineups, ownership, duplication, or GPP payout. A favorable number is not established leverage. Market quotes are displayed as context; the leading/trailing scenarios are not assigned market-derived probabilities. Participant rows lack an as-of timestamp, so reconstructed history can include later participant corrections. This test cannot promote the feature into fantasy-point projections or GPP optimizer scoring; the coherent game/lineup scenario and walk-forward holdout remain the next gates.
