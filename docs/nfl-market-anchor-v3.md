# NFL DFS market anchor v3: anchoring on whatever lines exist (2026-10-25)

Owner's priority (2026-10-25): put sportsbook lines on more players. v2
(`nfl-market-anchor-v2`) anchors a player only when every required market is
priced (QB pass yards + pass TDs; RB rush yards + receptions + anytime TD;
WR/TE receptions + receiving yards + anytime TD). This note records what the
books post for everyone else, the rule chosen, and its grade. Exploratory: the
rule was chosen after looking at weeks 1-6, not pre-registered.

## What the books post for players v2 leaves on the model

Main Classic slate each week, players projected 5+ and not ruled out
(`nfl_dfs_slate_players`), lines = latest pregame capture per market, books in
the web's order, matched by name and kickoff within 30 minutes. Week 7 was read
on 2026-10-25 before the 1 PM games, so later captures are not in it.

| week | QB v2 -> v3 / with a line / all | RB | WR | TE |
|---|---|---|---|---|
| 5 | 22 -> 22 / 22 / 46 | 26 -> 37 / 37 / 40 | 54 -> 56 / 56 / 61 | 26 -> 27 / 27 / 27 |
| 6 | 20 -> 20 / 21 / 39 | 27 -> 34 / 34 / 36 | 47 -> 55 / 55 / 58 | 27 -> 28 / 28 / 28 |
| 7 | 20 -> 20 / 20 / 42 | 25 -> 34 / 34 / 35 | 46 -> 47 / 47 / 48 | 23 -> 26 / 26 / 26 |

With v3 every RB, WR and TE who has any line is anchored; the remaining
unanchored ones have no line at all (5 RBs and 8 WRs in weeks 5-6, several of
them practice-squad players). QBs without lines are backups. v3 can lose a few
to the absence-commit rule below, which this table does not apply. (The
handover's week-6 counts, RB 35/53 and so on, used a different denominator; the
direction is the same.)

Of the RB/WR/TE v2 missed in weeks 5-7, almost all had touchdown markets only
(anytime TD, 2+ TDs, first TD). A handful of RBs also had rush yards or rush
attempts without a receptions line; one RB had receptions without rush yards.

## Rule (`nfl-market-anchor-v3`)

* A full set: unchanged from v2.
* Otherwise, an RB/WR/TE with an anytime-TD line and the model's own TD means:
  `projection = model + 6 x (calibrated line TD rate - model rushing + receiving TDs)`.
  Only the touchdown term moves (no yardage bonus changes); every other stat
  stays the model's. The range is rescaled to the new mean as in v2.
  `mode: 'td_only'`, `fromMarket: ['player_anytime_td']`, `modelTds` kept.
* A partial set of volume or yardage lines is NOT used without a full set
  (graded no better, below). Such a player gets the TD-only anchor if he has
  an anytime-TD line.
* A TD-only anchor yields to an absence commit (the grade was against the
  model's own number, not the starter-aware one); the workspace counts these
  (`tdOnlyYieldedToCommit`). QBs are never TD-only.

Code: `web/src/lib/nfl-dfs/market-anchor.ts` (`tdOnlyDelta`, `anchorPlayer`),
`model/nfl_prop_implied.py` (`td_only_delta`), pinned to each other by
`npm run test:nfl-market-anchor` and `tests/test_nfl_prop_implied.py`.

## Grade (2026 weeks 1-6, `python -m research.nfl_market_anchor_v3_grade --weeks 1-6`)

Population: players v3 newly anchors (on them v2 = the model), scored on the
week's main Classic slate report card, model projection over 0.5. Paired MAE
in DK points, game-clustered bootstrap 95% CI.

| slice | n | games | MAE model | MAE v3 | v3 - model |
|---|---:|---:|---:|---:|---|
| all | 665 | 67 | 3.18 | 2.99 | -0.196 [-0.248, -0.142] |
| RB | 214 | 67 | 3.72 | 3.49 | -0.228 [-0.336, -0.111] |
| WR | 230 | 66 | 3.22 | 3.06 | -0.158 [-0.235, -0.086] |
| TE | 221 | 67 | 2.62 | 2.41 | -0.205 [-0.264, -0.150] |
| projected 5+ | 176 | 55 | 5.62 | 4.97 | -0.648 [-0.784, -0.506] |
| scored > 0 | 277 | 67 | 3.06 | 2.93 | -0.122 [-0.200, -0.044] |
| weeks 5-6 | 171 | 20 | 2.77 | 2.63 | -0.134 [-0.229, -0.037] |
| RB, weeks 5-6 | 57 | 20 | 3.34 | 3.29 | -0.047 [-0.230, +0.165] |

Every week improves (week 5 -0.055 [-0.209, +0.092]; the other five exclude 0).

Is it the line, or would any TD cut do? A placebo shrinking every model TD by
one constant (mean line TD / mean model TD = 0.77, fitted on this same
population, which favours the placebo) is beaten too: -0.092 [-0.133, -0.049]
overall, and at each position (RB -0.115, WR -0.059, TE -0.105, all CIs below
0). The model over-projects these backups (bias +1.70 points; they average 0.113
model TDs against 0.087 from the lines), and the line says which ones.

Partial volume/yardage lines (64 player-games, 61 of them RBs): replacing each
priced stat with the line and keeping the model's rest scored +0.050
[-0.338, +0.434] against the model, and TD-only did as well as it
(-0.079 [-0.315, +0.151]). No evidence for the extra machinery, so v3 does not
use them. WR/TE partial volume sets were too rare to grade (3).

Caveats: chosen after seeing these weeks; the RB effect rests on weeks 1-4 (the
weeks 5-6 RB slice is flat); the anytime-TD calibration (0.998) was fitted on
weeks 1-4 full-set players, so it is close to uncalibrated for this population;
only the mean is graded, while the rescaled P10/P90 are not (the range study is
separate work). Regrade with the script as weeks accrue; if a position's slice
turns positive, drop it from `TD_ONLY_POSITIONS` and bump the version.
