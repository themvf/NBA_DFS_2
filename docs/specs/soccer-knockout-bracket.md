# Spec — Soccer Knockout Bracket View

**Status:** V1 shipped 2026-06-28 · V2 (deep-run probabilities) shipped 2026-06-28
**Related:** `web/src/db/queries.ts` (`getSoccerKnockoutAdvance`, `getSoccerTitleOdds`),
`web/src/app/vegas/soccer-vegas-client.tsx` (`KnockoutPanel`), `model/soccer_futures.py`

## Goal
See, per knockout tie, **each team's % chance to advance vs the market**, plus a
title-odds board (our Monte-Carlo P(win tournament) vs the outright market) — so the
user can judge which teams are under/over-priced to advance and to win it all.

## Key constraint
The Odds API has **no "to advance / to qualify" market** for soccer (verified:
`to_qualify`, `advance_to_next_round` rejected as invalid markets). So the market
advance probability is **derived**, not a real line.

## V1 (shipped) — presentation layer over existing data, no new ingestion

**Advance probability (both ours and market):**
```
P(advance) = P(win in 90′) + 0.5 · P(draw)      # 50/50 extra-time/penalties prior
```
- **ours** from `soccer_matchups.our_prob_{home,draw,away}` (bivariate Poisson)
- **market** from `vegas_prob_{home,draw,away}` (vig-free 3-way)
- **edge** = ours − market (points)

**Queries (`web/src/db/queries.ts`):**
- `getSoccerKnockoutAdvance()` → `SoccerKnockoutTieRow[]`: upcoming non-group ties
  with both teams' our/market advance%, edge, and the real DNB 2-way price where DK
  posts it (`dk_dnb_*_ml`).
- `getSoccerTitleOdds(limit=16)` → `SoccerTitleOddsRow[]`: pending `outright_winner`
  bets from the latest futures model — our P(win) vs vig-free market + odds + stars.
  Eliminated teams fall toward 0 as the sim re-runs, so top-N surfaces contenders.

**UI:** new `🏆 Knockout` tab (`KnockoutPanel`):
- *Who advances* — one card per tie, an advance% bar per team with the market % and
  edge alongside; ties with a ≥4pt edge flagged ⚡.
- *Title odds* — table of contenders, our P(win) vs market + odds + star rating.

**Honesty guardrails (baked into the panel copy):**
- Market advance% is labeled **derived from the 3-way**, not a bettable line.
- DNB is shown as the real bettable 2-way (90′) price.
- The 50/50 ET/pens prior is stated as a simplification.

## V2 (shipped) — deep-run probabilities
Per-team **P(reach R16 / QF / SF / Final / Champion)** from a bracket Monte-Carlo:
- `model/soccer_futures.simulate_bracket_reach()` — **round 1 (R16) is exact**: it
  uses the real R32 ties + the match-model advance prob, so P(reach R16) == the V1
  advance%. Rounds 2+ re-pair survivors at random and resolve by Elo (R16+ pairings
  aren't published — `to_qualify`/R16 fixtures absent), so QF→Champ are
  strength-seeded approximations, **labeled as such** in the UI.
- Stored in each `outright_winner` bet's `inputs_json` (`reach_*`) — **no new table**;
  written by the futures job already in `refresh_soccer.yml`, so it refreshes per round.
- `getSoccerTitleOdds()` → `SoccerDeepRunRow[]` reads the reach_* inputs for the 32
  live teams (eliminated teams have no reach data), ordered by P(champion).
- UI: the title board became a **Deep Run** table (R16/QF/SF/Final/Champ + market +
  edge). Champion edge = our P(champion) − vig-free title market.

**Critical fix made during V2:** the knockout filter (`stage != 'group'` +
`game_date >= today`) leaked **completed** group games that have `stage=NULL` and
today's date, double-counting teams (Argentina showed 165% to reach R16). Added
`home_score IS NULL` (group stage is over, so all unplayed upcoming games are
knockouts) to both `getSoccerKnockoutAdvance` and the futures R32 query → clean 16
ties / 32 teams.

## V3 (still planned) — visual bracket tree
- A fixed bracket-pairing map (R32→R16→…) so winners visually connect like the
  screenshot, and exact deeper-round probs. Unblocks once R16 fixtures load (then
  pairings are real instead of strength-seeded).

## Known limitations
- 50/50 ET prior ignores that the stronger side also wins ET/pens more often.
- No bracket linkage in V1 (ties shown as a chronological list, not a tree).
- Title odds bracket pairing inside the sim is strength-seeded, not the exact FIFA
  bracket (documented in the soccer futures plan).
