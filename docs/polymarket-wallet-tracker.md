# Polymarket wallet tracking — status, method, and why it was closed

Status as of **2026-08-26**: **investigated, answered, and closed.** The
capability was built and works. It found no tradeable signal, and the reason
is understood rather than merely suspected. Nothing here is scheduled, and
nothing writes to the database.

Read this before proposing wallet tracking again. The question that killed it
is already answered, and re-running the pilots will not un-kill it.

---

## 1. What exists

| File | Role |
|---|---|
| `ingest/polymarket_wallet_pilot_common.py` | Shared engine — settlement, ranking, walk-forward split |
| `ingest/polymarket_tennis_wallet_pilot.py` | ATP/WTA singles discovery |
| `ingest/polymarket_mlb_wallet_pilot.py` | MLB game-market discovery |
| `ingest/polymarket_wallet_forensics.py` | Wallet behaviour classifier (the closer) |
| `tests/test_polymarket_wallet_pilot_common.py` | 11 tests, one per ranking bug below |
| `ingest/polymarket_tennis.py` | **Production** odds capture (not a pilot) |

Shipped in [PR #129](https://github.com/themvf/NBA_DFS_2/pull/129) and
[PR #143](https://github.com/themvf/NBA_DFS_2/pull/143).

All of it is **read-only research tooling**: prints a report, writes a JSON
file, touches no table and runs on no schedule.

**Polymarket's APIs are free, unauthenticated and unmetered** — Gamma
(`gamma-api.polymarket.com`) for markets/outcomes, Data
(`data-api.polymarket.com`) for the fill tape. Unlike the Odds API work in
[`the-odds-api.md`](the-odds-api.md), none of this competes for quota. Cost
was never the constraint here; signal was.

---

## 2. The method (and where it came from)

Ported from the sibling repo `themvf/Speeches`, whose SEC-25 pilot does this
for earnings and macro markets. The core move is **outcome-verified P&L**:

1. Pull each resolved market's true winning outcome from Gamma
   (`outcomePrices`, one side at ~$1).
2. Pull the full fill tape from the Data API (`takerOnly=false`, so maker
   fills are included).
3. Reconstruct per-wallet cash flow: `BUY` = `+size, -size×price`;
   `SELL` = `-size, +size×price`.
4. At resolution winners redeem at $1 and losers at $0, so
   `pnl = cash + max(net_winner, 0)`.

Negative net positions (possible via on-chain split/merge, invisible in the
fill tape) clamp to zero payout — a small documented approximation inherited
from the source pilot.

This matters because it is the piece the public tooling does not do. Most
open-source Polymarket trackers rank by the platform's own unverified
leaderboard, and the one serious published writeup
(`darrnhard/polymarket-smart-money`) explicitly **deferred** outcome
verification as unreliable. `Speeches` had already solved it; this is that
solution pointed at sports.

---

## 3. Five ranking bugs, each found by the previous fix

Every metric below looked correct until the one before it stopped hiding the
next. This sequence is the most reusable thing in this document.

| # | Bug | Symptom | Fix |
|---|---|---|---|
| 1 | Raw win% has small-sample bias | A 5-for-5 wallet outranked 200-for-280 | Wilson score lower bound |
| 2 | Win rate ignores price paid | 101 markets, 100% win, $7.5M cost, **$8,103 profit** — buying near-certainties at near-certain prices | `edge = Wilson floor − avg entry price` |
| 3 | Tiny stakes dominate | #1 wallet had 51 markets, 96% win, **$29.95 total cost** | $1,000 minimum-cost floor |
| 4 | Averaging per-market ratios | One $2,000 bet + 140 $1 longshots showed "avg entry 1.9%" | Dollar-weight: sum, then divide once |
| 5 | Market makers pollute entry price | Top wallet was a bot: 279 sells vs 221 buys, plus an unrelated Bitcoin market | `buy_dominance ≥ 0.6` filter |

**Standing lesson:** a plausible-looking leaderboard is not evidence the
metric is sound. Each fix here was prompted by an implausible *row*, not by
review of the formula.

---

## 4. What the pilots found

At 800 markets per sport, split chronologically into dev/holdout halves:

| | Tennis | MLB |
|---|---|---|
| Fills processed | ~4.4M | ~3.5M |
| Distinct wallets | 67,077 | 84,575 |
| Qualified (≥5 markets) | 21,244 | 21,580 |
| Top-30 dev wallets with holdout activity | 14 | 6 |
| **Persisted** | **6 (43%)** | **3 (50%)** |

**Walk-forward was a coin flip in both sports.** Top-of-leaderboard status
did not predict later performance.

The one striking result: **six wallet addresses appeared independently in
both sports' top-20 edge lists.** At ~21,000 qualified wallets per sport that
is far past chance, so it demanded an explanation.

---

## 5. The forensics verdict — why it was closed

`ingest/polymarket_wallet_forensics.py` pulled every reachable trade for all
six (~19,400 trades) and computed behavioural signals only — no settlement,
because the question does not need it.

**Not one entity.** Pairwise market overlap is near zero (highest Jaccard
0.139, most exactly 0.00). Six genuinely independent actors.

**Not sports specialists.** Each trades 7–11 unrelated categories. MLB and
tennis — the sports they were *ranked on* — are only **8–31%** of their
activity:

| Wallet | MLB+Tennis | All sport | Trades/day |
|---|---|---|---|
| Taiethc | 30.5% | 41.7% | 13 |
| `0x39820b9a` | 23.2% | 39.4% | 109 |
| `0x42c74ed2` | 8.0% | 9.3% | 250 |
| `0x82a59f9c` | 18.9% | 21.2% | 59 |
| `0x84d4f0a8` | 15.9% | 17.5% | 219 |
| `0x948b7f2f` | 29.4% | 32.9% | 59 |

The remainder is esports, crypto, politics and novelty markets ("Will Donald
Trump publicly insult someone on August 23, 2026?").

**Not humans placing considered bets.** 59–250 trades/day across 826–2,673
distinct markets. Two are outright automated (53% and 23% of trades under $1
notional; 28% and 27% sharing an exact second with another trade). Even the
four that pass the automation screen place **10–13%** of trades in the same
second as another — not achievable by hand.

### The actual failure

**The edge metric was measuring trading style, not skill.**

`edge = Wilson win-rate floor − average entry price` presumes a *considered
wager at a chosen price*. For a quoter earning the spread, entry price is
simply wherever the book sat. Feed high-frequency generalist flow into a
metric built for directional bettors and it returns a large, confident,
meaningless number.

That is also, precisely, why walk-forward found nothing to persist: the
leaderboard was never ranking predictive skill.

The sharpest illustration: the best-looking wallet by PnL (`0x84d4f0a`,
$8,142 tennis profit) is one of the two confirmed bots.

**Caveat, recorded not buried:** 5 of 6 wallets hit the Data API's
~3,500-offset ceiling, so these are recent slices, not lifetime records. That
*understates* volume and cannot flip any conclusion — only strengthen it.

---

## 6. A production bug this work uncovered

Separately valuable, and already fixed: `ingest/polymarket_tennis.py`
discovered matches via `ATP_TAG_ID=101232` / `WTA_TAG_ID=102123`. Live
verification found those tags are almost entirely tournament futures/props —
**1 real head-to-head match in 500 closed events**, and **zero** live matches
at capture time while real matches were actively trading.

Real match events carry the generic `tag_id=864` ("Tennis"), 99.6%
match-event density. Fixed in PR #129 with regression tests
(`tests/test_polymarket_tennis.py`); futures capture still uses the tour tags,
which is where futures genuinely live.

---

## 7. An earlier attempt — reconcile before trusting

Five design docs and nine scratch scripts from **2026-08-06/07** sit
**uncommitted** in the working directory (`POLYMARKET_SHARP_TRACKER_PLAN.md`,
`SHARP_BETTOR_IMPLEMENTATION.md`, `POLYMARKET_P0_RESULTS.md`,
`POLYMARKET_MLB_DISCOVERY.md`, `POLYMARKET_COST_ANALYSIS.md`, plus
`test_polymarket_mlb*.py`, `explore_polymarket_api.py` and similar).

That effort got real things right — it found the no-auth Data API, identified
`proxyWallet` as the key field, and designed sensible tables
(`polymarket_mlb_events`, `polymarket_mlb_markets`).

**It also recorded two conclusions that are wrong and were never revised:**

1. *"BLOCKED — cannot track individual wallets without authentication."*
   It hit `clob.polymarket.com/trades` (401). The free
   `data-api.polymarket.com` was found the next day, but the P0 verdict still
   reads BLOCKED.
2. *"No active MLB games in Polymarket API (August 2026 = off-season)."*
   MLB is mid-season in August. The plan used `series_id=3`, which surfaces
   only **"First 5 Innings"** markets. Using `tag_id=100381` the MLB pilot
   found 2,085 real full-game markets.

Left as-is, those docs read as a dead end and someone will redo the work.
Either reconcile them against this document or delete them.

---

## 8. What does not exist

- **No persistence.** Pilots print and write JSON. No table stores any of it.
- **No scheduling.** No workflow runs any of this.
- **No UI.** Nothing surfaces in the web app.
- **No live signal.** Open-position lookup is implemented but nothing runs it
  on a cadence.

---

## 9. If this is ever revisited

The negative result is about **this metric on these wallets**, not a proof
that Polymarket contains no information. Any revival should clear these bars
first, in order:

1. **Screen out automation before ranking, not after.** The
   `buy_dominance ≥ 0.6` filter was insufficient — it passed four wallets
   that still show 10–13% same-second trades. Fold the forensics signals
   (sub-$1 share, same-second share, category breadth) into eligibility.
2. **Rank on something a market maker cannot accidentally win.** Realized ROI
   on *directional, held-to-resolution* positions would be a start. Entry
   price relative to a quoted fair value is not.
3. **Pre-register a walk-forward test with a sample floor before looking**,
   per the standing discipline everywhere else in this project. The pilots
   ran the split honestly, but the metric was chosen first and tested after.
4. **Only then discuss persistence.** Building tables for a signal that has
   not survived out-of-sample is exactly the failure this project's specs
   exist to prevent.

Absent all four, the honest answer is the one already reached: following
these wallets means following market-making flow, not information.
