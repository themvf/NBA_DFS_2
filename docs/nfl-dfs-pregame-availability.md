# NFL DFS — Pre-Kickoff Availability and Replacement — Spec (2026-09-16)

Supersedes the team-opportunity-ceiling framing of the first two drafts. That
spec treated a symptom (two quarterbacks summing to 39.6 DK points) with a
proportional scale-down, which would have produced 14.2/9.6 when the correct
answer was 0/~20. The rule below fixes it at the source; the ceiling survives
only as a backstop for the case where no designation exists.

## The rule

**At projection time, which is before kickoff:**

1. A player with an OUT-class designation published before the slate locks
   projects **0**. Not shrunk, not discounted — zero. He cannot score.
2. His **opportunity** does not vanish. It transfers to the replacement, because
   the team still throws the passes and runs the plays.
3. The replacement converts that opportunity at **his own** efficiency.

Point 3 is the one refinement to "the replacement should get all of it". The
volume transfers; the conversion rate does not. Bennett inheriting Stafford's
35 attempts is not Bennett inheriting Stafford's 23.6 points — a backup
typically turns the same volume into fewer points. Transferring the points
directly would replace an over-projection with an over-projection.

This maps onto the decomposition already built: `nfl_dfs_workload` is
opportunity, `nfl_dfs_efficiency` is conversion. The transfer happens in
workload; efficiency is re-applied per player.

## What "injured" has to mean here

`ff_player_injuries.status` is constrained to QUESTIONABLE, DOUBTFUL, OUT, IR,
PUP, NFI, SUSPENDED, UNKNOWN. The rule applies to a subset, and the boundary is
load-bearing:

| Status | Treatment | Why |
|---|---|---|
| OUT, IR, PUP, NFI, SUSPENDED | project **0**, transfer opportunity | Unambiguous. He is not playing. |
| DOUBTFUL | near-zero, transfer most opportunity | Historically a small minority play, and usually in a reduced role. |
| QUESTIONABLE | **do not zero** — weight by probability | Most Questionable players play. Zeroing them would be a larger error than the bug being fixed, and would fire on far more players. |
| UNKNOWN | no change | Absence of information is not information. |

Zeroing a Questionable starter every week would be the classic over-correction:
a fix that is right about the mechanism and wrong about the threshold. The
probability for QUESTIONABLE must be **fitted from history, not asserted** —
see P2.

## Scope: quarterback first, and possibly only

| Position | Transfer shape | Verdict |
|---|---|---|
| **QB** | One slot, one replacement, near-total volume transfer | Clean. Start here. |
| RB | Committee — carries split across RB2/RB3 non-uniformly | Needs a distribution, not a handoff |
| WR/TE | Most diffuse — a team may simply target its existing receivers more rather than elevate WR4 | Weakest case |

"All of it to the replacement" is true at quarterback and false at receiver.
Shipping QB-only is not a partial delivery; it is the part that is correct.

## What is already in place

This is much closer to working than the previous drafts implied.

- **`ingest/nfl_dfs_projections.py::_players` already selects from
  `ff_players`** — the same table that carries `injury_status TEXT`. It selects
  six columns and simply does not read it. The status is one `SELECT` clause
  from the projection loop.
- **Depth order is already ingested.** `ingest/ff_independent.py:592` pulls
  Sleeper's `depth_chart_order`, and the whole Sleeper blob is stored in
  `ff_players.metadata`, so it is reachable at
  `metadata->'sleeper'->>'depth_chart_order'`. This answers "who is the
  replacement" without a new source.
- **A depth→role mapping already exists.** `ff_independent.py:996
  ::_depth_factor` converts a depth order into a role multiplier for the
  fantasy-football board. Precedent, and a starting shape to borrow or beat.
- **Two injury feeds run weekly**, Sleeper canonical and FantasyPros in shadow,
  with a rich observation schema (`practice_status`, `weeks_out_min/max`,
  `availability_probability`).

## The point-in-time question, correctly scoped

The previous draft said an unverified provider timezone blocks everything. That
was too broad, and the distinction matters:

- **Live use is not blocked.** `ff_source_snapshots.fetched_at` is
  `NOT NULL DEFAULT NOW()` — *our own* clock, on every capture, reachable from
  each observation via `source_snapshot_id`. If we fetched a status at a known
  UTC time and kickoff is later, it was knowable before kickoff. That is
  provable without trusting the provider's timestamp at all.
- **Backtesting is constrained.** Reconstructing what was knowable at a past
  moment needs `provider_updated_at` for statuses published between our
  captures, and that field's timezone is unverified — which is why capture is
  stamped `model_eligible=False`.

So the honest position: the rule can be **run forward** on our own capture
clock, and **validated on history** only as far as our own capture cadence
allows. Backfilling a richer history needs the provider timezone resolved.

Do not paper over this by assuming an offset. A guessed offset is
indistinguishable from a leak, and a leak is invisible because it improves the
backtest — the failure this repo has already found three times
(`mlb_matchups.our_prob_home`, `mlb_bets.event_commence`, the stats join that
read June into March).

## Phases

**P1 — Zero the OUT-class. No transfer yet.**
Read `injury_status` in `_players`, project 0 for OUT/IR/PUP/NFI/SUSPENDED
where the capture precedes kickoff. Do not transfer the opportunity yet.

This is deliberately half the rule, because the two halves have very different
risk. Zeroing a player who is definitionally not playing cannot make a
projection worse — it removes points that were certain to be wrong. Transferring
them somewhere can. Ship the safe half first and measure it alone.

Bumps `MODEL_VERSION` (projections change): `nfl-dfs-historical-v3`.

**P2 — Transfer opportunity to the replacement.**
Resolve the replacement by depth order within team+position, transfer the
absent player's projected *opportunity*, re-apply the replacement's own
efficiency. QB only. Fit the DOUBTFUL and QUESTIONABLE weights from history
here, never assert them.

**P3 — Extend beyond QB, or decline to.**
Only with a fitted distribution for committee positions. A proportional split
across RB2/RB3 is a guess; the correct weights are measurable from team-weeks
where a lead back was out.

**P4 — Team ceiling as backstop.**
The original spec, demoted. It catches two players projected as starters when
*no designation exists* — an unannounced change, or a model that simply rates
two players highly. Narrower than it looked, and worth building only if P1-P2
leave a measurable residue.

## Validation

- **P1 gate:** on weeks where an OUT-class QB was captured pre-kickoff, does
  projection MAE for that team's QB group improve? This is close to a
  tautology for the zeroed player himself, so the gate is the **team group**,
  not the individual.
- **P2 gate:** does the replacement's projection beat what the model would have
  produced without the transfer, walk-forward, date-clustered bootstrap CI
  excluding zero?
- **Reported always, gating nothing:** how many players per week are zeroed,
  how many QB transfers fire, and the OUT→did-not-play precision (an OUT player
  who plays is a provider error worth knowing about).
- Fit any status weight on seasons strictly before the week being projected.

## Non-negotiables

- QUESTIONABLE is never zeroed. It is a probability, fitted, or it is left
  alone.
- A status only applies if its capture — by *our* clock — precedes kickoff. No
  guessed provider offsets.
- Opportunity transfers; efficiency does not. The replacement is projected as
  himself.
- P1 ships without P2. Zeroing is safe alone; transferring is not.
- This spec consumes injury state; it does not promote injury fields. The
  governing programme is [`docs/NFL Injury.md`](NFL%20Injury.md), whose phase 6
  promotion gate still applies to anything beyond a plain status read, and
  whose Sleeper/FantasyPros conflict policy is still pending — so P1 must name
  which source it reads rather than blending two that may disagree.

## Honest expected value

Higher than the ceiling check it replaces, and for a specific reason: this is
not a modelling improvement, it is the removal of projections that were
guaranteed wrong. A player who is ruled out scores zero with certainty, and the
model currently projects him a full game. There is no variance argument on the
other side of that.

Size is still bounded by frequency — a handful of OUT-class starters per week
across 32 teams, concentrated at positions where one absence matters most. It
will not transform MAE. It will stop the specific, embarrassing class of error
where a lineup is built around a player who was known not to be playing.
