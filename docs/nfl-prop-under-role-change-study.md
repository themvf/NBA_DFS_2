# Flagged prop unders after a teammate's fresh absence (`nfl-prop-under-role-change-v1`)

**Registered 2026-10-20 11:21 UTC**, before any week-7 game. Constants are frozen in
`docs/nfl-prop-under-role-change-study.json` and pinned by
`tests/test_nfl_prop_under_role_change.py`; a change after this point is a new
version. Graded Monday and Tuesday by `refresh_nfl_dfs_postweek.yml` (job
`grade-prop-under`) into the append-only `nfl_prop_under_role_change_runs`; the one
verdict goes to `nfl_prop_under_role_change_verdicts`.

## Why

On the Monday night game of 2026 week 6 (WAS at SF) the game-leaders model flagged
14 DraftKings unders. Five were on players whose same-position teammate had just been
ruled out: Brandin Cooks with Deebo Samuel out, and Rachaad White with Jacory
Croskey-Merritt on IR. Those five were labelled "skip": the model reads a player's
last five games, so it cannot see a role that changes tonight. They went 3-2; the
nine that were kept went 3-6.

That advice was a football story, not a measurement. The repo's absence studies say
vacated volume spreads across the room and the player who benefits is not
predictable (`nfl-wr-breakout-blind-v1`, `nfl-rb-absence-reallocation-v2`,
`nfl-replacement-upside-v2`), which argues against it. This study measures the
question directly instead of answering it with either story.

## Question

Among the unders that `nfl-prop-under-v2` flags, do the ones on a player whose
same-position teammate has a **fresh absence** return a different flat-stake ROI
from the other flagged unders?

## Population (frozen)

- The graded bets of `nfl-prop-under-v2`, exactly as its own grader selects and
  settles them (`research/nfl_prop_under_study.py`, imported unchanged): DraftKings
  under on rushing yards, receiving yards or receptions, flagged when the model's
  P(over) is at least 10 points below DraftKings' de-vigged P(over), latest pregame
  price, $1 flat, voids excluded, pushes 0.
- Only games kicking off after this study's `registered_at`. Week-6 games, including
  the Monday game that prompted it, are discovery and never count.
- If the parent study is amended or superseded, rows that leave its cohort leave this
  study too. Following a new parent version needs a new version of this study.

## The tag (frozen)

Each flagged under is tagged from the **frozen availability request the forecast was
built from**: the committed `artifacts/nfl-game-leaders/*/requests-*.json` whose
`decision_at` equals the priced row's `forecast_decision_at`. That is what the model
knew when the line was priced; nothing later is used.

- **Fresh absence** (the tested arm): the player is an RB, WR or TE (FB counts as RB),
  and a teammate in the same position group
  1. has status `out` in that request,
  2. averaged at least 3.0 targets or 5.0 carries per game over his games in the
     team's last five completed regular-season games before this one, and
  3. has a stat row in the team's most recent completed game (he played last week).
- **Untagged** (the comparison arm): every other flagged under on an RB, WR or TE,
  including continuing absences (condition 3 fails).
- **Not tagged, reported only:** quarterbacks (`not_taggable`) and rows whose request
  cannot be found (`unknown`). Neither arm includes them.

Usage comes from the nflverse weekly player file. A player who was active but
recorded no stat has no row, so a teammate who played without touching the ball
counts as not having played: the fresh tag can be missed, never invented.

## Metric and verdict

- **Primary:** ROI(fresh absence) minus ROI(untagged), with a 95% interval from a
  bootstrap that resamples **game weeks**, drawing both arms from the same weeks
  (2,000 resamples, seed 20261020).
- **Floors:** at least 120 graded fresh-absence bets over at least 8 game weeks.
- **One look:** the verdict is computed once, on the first grade run after every
  regular-season game through week 18 is graded. Nothing earlier is a verdict.
  - **TAGGED_WORSE** (interval wholly below zero): licenses a display-only warning
    on such flags ("role changed tonight; the model may lag"). It does not change
    the parent study, the threshold or the model.
  - **TAGGED_BETTER** (wholly above zero) or **NO_DIFFERENCE** (includes zero): the
    skip-on-role-change advice is retired. Flags are read the same whether or not a
    teammate is out.
  - **INCONCLUSIVE** (floors not met): no warning and no advice; a 2027 version may
    re-ask.
- **Reported every week, gating nothing:** both arms' records and ROI, continuing
  absences, flags where only a different-position teammate was freshly ruled out
  (Deebo Samuel's targets going to the tight end, for example), and the count of
  unknown and not-taggable rows.

## Size and power, stated before any result

Tags only, no results read for this count:

| week | flagged | fresh absence | continuing | none | QB |
|---:|---:|---:|---:|---:|---:|
| 5 | 163 | 26 (16%) | 27 | 98 | 12 |
| 6 | 278 | 19 (7%) | 72 | 173 | 14 |

At about 22 a week, weeks 7-18 give roughly 260 fresh-absence bets; the floor is set
at about half of that, because the weekly count swings and a model amendment could
end the parent cohort early.

Several of a player's lines (yards and receptions) are settled by the same game, so
the effective sample is far smaller than the bet count. With per-bet ROI standard
deviation near 0.95, the interval will be roughly 20-30 ROI points wide either side.
Only a large difference can be detected. The expected answer, given the absence
studies above, is NO_DIFFERENCE, and that answer is still useful: it means skipping
these flags has no support.

## Limits

- The tag uses the model's own availability (`out` only). A teammate the model left
  `unresolved` who then sat is not tagged.
- One book, one capture per game, no closing-line value, as in the parent study.
- The rule was suggested by one game's five bets. Those bets are excluded, and the
  rule is frozen before any later game is seen.

## Commands

    python -m research.nfl_prop_under_role_change sizes --week 6   # tag counts, no results
    python -m research.nfl_prop_under_role_change grade [--write-db] [--out path]

With `--write-db` it appends one run row and, at the final look only, the verdict. Tests: `tests/test_nfl_prop_under_role_change.py`.
