# NFL ownership: news-driven chalk — `nfl-ownership-news-v1` (registered 2026-10-24)

Status: REGISTERED, NOT BUILT. Nothing in production changes. This document
fixes the trigger, the candidate terms, how one is chosen, which slates grade
it and what counts as a pass, before any code is written or any design number
is computed. Every frozen value is also in `docs/nfl-ownership-news-chalk-study.json`;
if the two disagree, the JSON wins and the disagreement is a defect to record.

Companions: `docs/nfl-ownership-prior-v5.md` (the prior this would extend; its
section 4 names news chalk as the largest miss it does not address) and
`docs/nfl-ownership-scorecard-v1.md` (scorecard v3: labels, frozen forecasts,
reference contests; unchanged here).

Reviewed before freezing by an independent agent; its eight findings (stale
vacancies, the WAS/WSH code split, the worst-case rule, undefined timestamps,
the interval, the design stop rules, implementer ambiguity, an incomplete
JSON) are all fixed below.

## 1. Why

When a team's main back, receiver, tight end or quarterback is ruled out, the
field piles onto the teammates who inherit the work, mostly the cheap ones.
The prior cannot see this:

- It drafts on `0.6 x our projection + 0.4 x DraftKings season average`. The
  season average describes the old role, so it drags a promoted backup down.
- v5's flattening (`score^0.75` Classic) moves share away from the top of each
  position, the wrong direction for a player the field is about to make the
  top of his position.
- No input says "this player's role changed this week."

The case that prompted this (2026-10-25 main slate, week 7): with Emeka
Egbuka out, one public source had Ted Hurst III ($3,000) at 77% and Chris
Godwin at 43%; v5 had them at 2.6% and 3.3%. The 77% is a forecast, not a
result, and is evidence for nothing below. Recorded misses that are results:
Wilson 11.35% predicted vs 45.59% actual, Lance 1.45% vs 23.87% (v3,
`docs/nfl-ownership-review-2026-10-19.md`). The author has seen those, which
is why section 4's first step is a mechanical rule rather than a judgement.

## 2. The trigger (frozen)

Inputs, both fixed before kickoff:

1. the canonical frozen forecast input scorecard v3 picks (pool capture,
   optimizer-run snapshot or ghost freeze): `ffPlayerId`, `team`, `position`,
   `salary`, `isOut`. (`ffPlayerId` is in every input but the scorecard's
   `_player_row` does not read it yet; section 5 adds it.)
2. completed regular-season rows in `ff_player_week_stats`
   (`source = 'nflverse'`, `season_type = 'REG'`) for weeks strictly before the
   slate's week, seasons 2025 and 2026. Rows are selected by week only, never
   by `fetched_at` (rows are refetched, which moves that column). A row that
   lacks the `targets`, `carries` or `attempts` key counts that stat as 0. A
   player with no row in a game is treated as not having appeared, so a
   baseline averages only games with a stat line; this follows the repo's
   absence studies and can run slightly high for a player who was active and
   recorded nothing.

**Team codes** are compared through the franchise key in
`web/src/lib/nfl-dfs/availability.ts` (LA/LAR, WAS/WSH, AZ/ARI, JAC/JAX), so a
DraftKings `WAS` matches an nflverse `WSH`.

**Baseline** for a player: the half-life-4 weighted mean of his per-game
targets and carries over his last 8 appearances for his current team, most
recent weighted most. Fewer than 2 such appearances: no baseline.

**Vacancy:** a player who is `isOut` in the frozen input, who appeared for his
current team in at least one of that team's last 3 completed games (so a
long-term absence is not news), and who clears his position's bar:

| vacated position | bar |
|---|---|
| RB | carries baseline >= 5 (the repo's "material donor", `nfl-rb-absence-reallocation-v2`) |
| WR | targets baseline >= 7 (the "star", `nfl-wr-breakout-blind-v1`) |
| TE | targets baseline >= 4 (the "starting TE", `nfl-te-backup-blind-v1`) |
| QB | his team's passing leader in its most recent completed game, by the rule in `readLastGamePassingLeaders` (`web/src/db/nfl-dfs-usage-window.ts`): `ff_players.position = 'QB'`, at least 15 attempts, most attempts wins; ties go to the smaller `ff_players.id` |

**Beneficiaries:** every player in the frozen input on the vacancy's team (by
franchise key) who is not `isOut`, has a salary, and plays the room:

| vacancy | room |
|---|---|
| RB | RB |
| WR | WR and TE |
| TE | TE |
| QB | QB |

A player who benefits from two vacancies is one beneficiary. Showdown uses the
same rules; captain and flex are the same player.

Recorded with every beneficiary, never used to choose him: whether the vacated
player appeared in the team's previous game (`first_game`), the vacated
player's baseline, whether our projection already moved for the absence
(`absenceCommit` or `marketAnchor` present), and a checksum of the box-score
extract and of the beneficiary list.

## 3. Candidate terms (enumerated now; no others)

Non-beneficiaries' scores are exactly v5's. Position budgets, caps, the
Showdown captain position shares and the punt tier are unchanged, so share a
beneficiary gains comes from his own allocation group.

- **C0** is v5 (the control).
- **C1**, no constant: a beneficiary's field points are our projection alone
  (the DraftKings average gets weight 0, because it describes the old role).
- **C2**, one constant `m`: C1, and every allocation weight of a beneficiary
  is multiplied by `m` after v5's flattening (Classic weight; Showdown flex
  main-pool or punt-tier weight; Showdown captain weight). Grid: 1.5, 2, 3, 4,
  6, 8. One `m` per format, covering captain and flex, matching v5's single
  flattening parameter per format.

Not candidates, and not to be added after the design numbers are seen: a
multiplier scaled by salary, vacated volume, `first_game`, position, or the
number of beneficiaries sharing a vacancy. Any of those is a new version.

## 4. Design (choosing the candidate)

**Design slates:** every 2026 slate whose first kickoff is before
2026-10-25 00:00 UTC and that has a canonical frozen forecast and a stored
reference contest. The set is fixed by that date, not by when the design run
happens. All are already v5 design slates.

Metrics here and in section 6: Classic is scored on the whole lineup's
ownership; Showdown on the FLEX slot (captain reported, never gating). A
format's MAE is the mean over slates of each slate's MAE, never pooled rows.
"Flagged group" = beneficiaries with an `observed` or `reconciled_zero`
label. "Observed population" = every player with such a label.

Steps, per format:

1. **Premise check.** Pool the design slates; take the 10 players with the
   largest `actual - v5` (biggest under-predictions). Proceed only if the
   beneficiary share among those 10 is more than twice the beneficiary share
   of the observed population. Otherwise stop: `NOT_BUILT`, recorded.
2. **Score** C0, C1 and every C2 grid point: flagged-group MAE and
   observed-population MAE.
3. **Choose.** A candidate is admissible if its observed-population MAE is at
   most v5's plus the tolerance (Classic 0.02, Showdown FLEX 0.10 percentage
   points). Among admissible candidates (C1 and the C2 grid), take the lowest
   flagged-group MAE; ties go to the simpler (C1, then the smaller `m`). If
   none is admissible, or the winner's flagged-group MAE is not below v5's,
   stop: `NOT_BUILT`.
4. **Freeze:** write the chosen candidate, `m`, the design numbers, the design
   slate keys and the commit SHA to `nfl_ownership_news_freezes` (append-only,
   one row per format, `frozen_at` set once) and an artifact under
   `artifacts/nfl-ownership-news-v1/`. The prior gets the name
   `nfl-ownership-prior-v6` only if it is later promoted.

Design numbers are in-sample and say nothing about the grade.

## 5. Implementation (follow-up work, in order)

1. `model/nfl_ownership_news.py`: team key, baselines, vacancies,
   beneficiaries. Pure, tested on hand-built frames: a traded player, a
   WAS/WSH team, a two-vacancy teammate, a player with one appearance, a
   long-term absence, a missing stat key, a QB tie, a Q-tagged beneficiary.
2. Add `ffPlayerId` to `_player_row` (`research/nfl_ownership_scorecard.py`).
3. A `v6` variant in the scorecard's Classic port (`model/nfl_ownership_scorecard.py`)
   and Showdown mirror (`model/nfl_showdown_ownership_eval.py`), taking the
   beneficiary set and the frozen `m`. v3, v4 and v5 outputs must not change
   (the existing fixtures are the check).
4. `research/nfl_ownership_news_design.py`: section 4, writes the freeze.
5. Grading: the post-week job (`refresh_nfl_dfs_postweek.yml`) recomputes v5 and
   the challenger from the same frozen input for every gradable slate, appends
   to `nfl_ownership_news_grades` (with the checksums from section 2), and
   writes the one-look verdict to `nfl_ownership_news_verdicts` (one row per
   format, append-only, cannot be written twice). Both tables go on `/health`.
6. No TypeScript change and no change to what the optimizer reads until a
   format passes. Switching production is the owner's call on that evidence.

## 6. Grading (frozen)

**Gradable slate:** first kickoff on or after 2026-10-25 00:00 UTC; a
canonical frozen forecast (scorecard v3) on its reference contest; that
contest's `nfl_dfs_field_contests.imported_at` later than the format's
`frozen_at`; and at least one labeled beneficiary. A slate imported before
the freeze is neither design nor graded.

**Floors, per format:** 8 gradable slates and 40 labeled beneficiary rows.
One look, at the first post-week run where both are met. If they are not met
by the end of the 2026 regular season, the verdict is `INCONCLUSIVE`.

**Primary:** per slate, flagged-group MAE of the challenger minus v5's. Pooled
as the mean over slates, with a 95% t-interval on the per-slate differences
(n - 1 degrees of freedom). A bootstrap over this few slates is too narrow.

**Unlabeled beneficiaries.** Since week 6 the flagship Classic contest opens
only its top 100 users, so a beneficiary nobody in those lineups used has no
label (`unresolved`) and dropping him could hide a miss. The primary is
computed twice:

- **excluded:** unresolved beneficiary rows left out;
- **worst case:** each unresolved beneficiary row adds `|challenger - v5|` to
  the challenger's error and nothing to v5's. That is the most the challenger
  can lose on the row, whatever the true value; scoring the row at 0% is only
  worst when the challenger predicts higher.

Both must pass. Showdown reference contests are usually complete DraftKings
exports, where a missing player is a `reconciled_zero`, so this mostly bites
in Classic. Each grade also reports the contest's uncovered ownership mass,
the most the unlabeled players can hold between them.

**Guard:** observed-population MAE, challenger minus v5, same t-interval; its
upper bound must be at most +0.05 points (Classic) or +0.20 points (Showdown
FLEX).

**Verdict per format:**

- `PASS`: primary upper bound below 0 under both treatments, and the guard holds.
- `FAIL`: primary point estimate at or above 0 under either treatment, or the
  guard's point estimate above its bound.
- `INCONCLUSIVE`: anything else, or floors unmet at season end. The challenger
  stays out of production; a second attempt is a new registration.

Reported with every verdict, gating nothing: flagged-group bias, the
`first_game` split, the split by whether our projection had already moved,
beneficiaries per slate, the ten largest misses, captain-slot results.

## 7. Honest prior

Expect a small effect at best. Our projection for a promoted backup often lags
the public sites the field reads (Hurst at 7.3 points), and C1 cannot fix
that. C2 can push a beneficiary up but cannot tell which of two cheap
teammates the field picks; it raises the whole room. A pass means the prior is
less wrong about news chalk in aggregate, not that it can call the 77% player.
It is also an ownership-accuracy result only: whether it improves built
lineups belongs to the ghost factorial.
