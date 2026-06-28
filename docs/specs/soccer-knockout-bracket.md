# Spec — Soccer Knockout Bracket View

**Status:** V1 shipped 2026-06-28 · V2 planned
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

## V2 (planned) — full bracket tree
- Add a fixed bracket-pairing map (R32→R16→QF→SF→F) so winners visually connect.
- Emit **P(reach each round)** per team from the futures Monte-Carlo (the sim already
  advances teams — expose per-round reach counts) → deep-run comparison like a real
  bracket. Needs the pairing map + a sim change to record per-round survival.

## Known limitations
- 50/50 ET prior ignores that the stronger side also wins ET/pens more often.
- No bracket linkage in V1 (ties shown as a chronological list, not a tree).
- Title odds bracket pairing inside the sim is strength-seeded, not the exact FIFA
  bracket (documented in the soccer futures plan).
