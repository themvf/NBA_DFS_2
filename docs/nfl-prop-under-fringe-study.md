# Flagged prop unders on fringe receivers (`nfl-prop-under-fringe-v1`)

**Registered 2026-10-22 11:45 UTC**, before any week-7 game kicked off. Constants are frozen in `docs/nfl-prop-under-fringe-study.json` and
pinned by `tests/test_nfl_prop_under_fringe.py`; a change after this point is a new
version. Graded Tuesday and Wednesday by `refresh_nfl_dfs_postweek.yml` (job
`grade-prop-under`) into the append-only `nfl_prop_under_fringe_runs`; the one
verdict goes to `nfl_prop_under_fringe_verdicts`.

## Why

On the Monday night game of 2026 week 6 (WAS at SF), the game-leaders model's
flagged unders did best on depth receivers: Brandin Cooks, Jacob Cowing and Kyle
Juszczyk all stayed under. The idea was "fringe receivers and backups are a target".

Measured first, on role alone, it is wrong. Every DraftKings closing line for
receiving yards and receptions in weeks 1-6 whose player ranked third or lower on his
team's board for that market (1,419 lines, 93 games): the under hit 49.0% against
50.3% fair at the price, ROI **-7.7%** per $1, game-clustered 95% interval
[-13.8%, -1.5%]. DraftKings prices depth receivers correctly and the under pays the
margin, which is large on small catch lines (-150 to -190). Fading a role is not an
edge.

What the Monday game suggests instead is narrower: when the model disagrees with
DraftKings, its low reads may be more reliable on depth receivers, whose usage is
small and stable, than on top targets, whose games swing. This study asks that.

## Question

Among the unders that `nfl-prop-under-v2` flags on receiving yards and receptions, do
the ones on **fringe** receivers return a different flat-stake ROI from the ones on
the team's **top two**?

## Population (frozen)

- The graded bets of `nfl-prop-under-v2`, exactly as its own grader selects and
  settles them (`research/nfl_prop_under_study.py`, imported unchanged): DraftKings
  under, flagged when the model's P(over) is at least 10 points below DraftKings'
  de-vigged P(over), latest pregame price, $1 flat, voids excluded, pushes 0.
- Receiving yards and receptions only. RBs and TEs are included as pass catchers.
  Rushing-yards flags are reported, never tested.
- Only games kicking off after this study's `registered_at`. Week 6, including the
  Monday game that prompted it, is discovery and never counts.
- If the parent study is amended or superseded, rows that leave its cohort leave this
  study too. Following a new parent version needs a new version of this study.

## The tag (frozen)

The tag uses only DraftKings' own board at pricing time, so it is known before
kickoff and needs no judgment.

- Among the parent's chosen pregame rows (the latest per player) for one game, one
  team and one market, a player's **rank** is 1 plus the number of teammates whose
  line is strictly higher. Tied lines share a rank.
- **Fringe** (the tested arm): rank 3 or worse.
- **Top** (the comparison arm): rank 1 or 2.
- A row without a team is `unranked` and in neither arm.

## Metric and verdict

- **Primary:** ROI(fringe) minus ROI(top), with a 95% interval from a bootstrap
  that resamples **game weeks**, drawing both arms from the same weeks (2,000
  resamples, seed 20261022).
- **Floors:** at least 400 graded fringe bets and 200 graded top bets, over at
  least 8 game weeks.
- **One look:** the verdict is computed once, on the first grade run after every
  regular-season game through week 18 is graded. Nothing earlier is a verdict.
  - **FRINGE_BETTER** (interval wholly above zero): if fringe's own ROI interval
    is also above zero, it licenses showing the model's fringe-receiver unders as a
    tracked signal and a small-stakes live period. It does not change the parent
    study, the threshold or the model. If fringe's own ROI interval includes zero,
    it is recorded as "better than top, not shown to be profitable" and licenses
    nothing.
  - **FRINGE_WORSE** or **NO_DIFFERENCE:** the fringe idea is retired. Role on the
    board adds nothing to the model's flag.
  - **INCONCLUSIVE** (floors not met): nothing; a 2027 version may re-ask.
- **Reported every week, gating nothing:** both arms' records and ROI, fringe ROI
  with its own interval, fringe by market and by rank (3 vs 4+), rushing flags,
  and the unranked count.

## Size and power, stated before any result

Counts of flagged rows by tag from `sizes --week 6` (14 games, no results read): 130
fringe, 97 top, 51 rushing. That is about 9 fringe and 7 top a game. Weeks 7-18 give
roughly 1,500 fringe and 1,100 top bets; the floors sit at about a quarter of that,
because a model amendment could end the parent cohort early.

A player's yards and receptions are settled by the same game, and a team's lines move
together, so the effective sample is far smaller than the bet count. With per-bet
ROI standard deviation near 0.95, the difference interval will be roughly 10-15 ROI
points wide either side; only a large difference is detectable.

## Overlap with other studies

- Same parent bets as `nfl-prop-under-role-change-v1`, which splits them by a
  teammate's fresh absence. The two splits are independent questions on the same
  bets and are not pooled or combined. A fringe receiver whose top teammate was just
  ruled out stays fringe here; that interaction is the role-change study's subject.
- The parent's own verdict is unaffected: this study only reads it.

## Limits

- Rank comes from DraftKings' board, not a depth chart. A player with no line is
  not ranked at all, so the ranks run among the players DraftKings chose to price.
- One book, one capture per game, no closing-line value, as in the parent study.
- The idea came from one game's results. Those results are excluded, and the rule is
  frozen before any later game is seen.

## Commands

    python -m research.nfl_prop_under_fringe sizes --week 7    # tag counts, no results
    python -m research.nfl_prop_under_fringe grade [--write-db] [--out path]

With `--write-db` it appends one run row and, at the final look only, the verdict.
Tests: `tests/test_nfl_prop_under_fringe.py`.
