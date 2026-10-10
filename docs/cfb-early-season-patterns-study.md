# CFB Early-Season Pattern Watch — Pre-Registered Study

**Version:** `cfb-pattern-watch-v1`
**Registered:** 2026-10-09 22:00 UTC, before any week-7 game kicked off
**Implementation:** `model/cfb_pattern_watch.py` (classifier, grader, append-only ledger)
**Ledger:** `artifacts/cfb_pattern_watch/ledger.jsonl` (local, JSON lines, never rewritten)
**Verdict:** computed once, not before 2026-12-07 (after conference championship games)

## Why this exists

A descriptive sweep of the 2026 season through 2026-10-09 (329 completed games
with closing lines, 259 FBS-vs-FBS) found favorites winning straight up at
81.1% against a market-implied 75.3%, with the spread market calibrated
(favorites 51.8% ATS). Roughly sixty slices were examined across two passes.
Five patterns were large enough to state as triggers. **None is an edge.** This
document freezes them so the rest of the season is a clean test, in the same
form as the NFL `total_walking` fade study and the MLB underdog-value study.

The discovery sample is recorded below and can never confirm anything. It is
not pooled with the prospective sample, not even as a "combined" secondary.

## Triggers (frozen)

All triggers read the closing consensus: the verified close
(`event_closing_lines` quality A or B, joined to `game_odds_history`) when one
exists, otherwise the last pre-kickoff capture on `cfb_matchups`. The price
source is recorded on every ledger row. Moneyline bets grade at the consensus
American price. Spread and total bets grade at an assumed −110 because the
consensus row carries no spread or total price; this is a stated assumption,
and a real-price grade would need the per-book JSON.

| Trigger | Population | Bet | Floor (games) |
|---|---|---|---|
| T1 `g5_mid_fav_ml` | FBS vs FBS, both teams outside SEC/Big Ten/Big 12/ACC/Independents, closing spread 7 to 13.5, underdog from Conference USA, Sun Belt or MAC | favorite moneyline | 40 |
| T2 `fcs_fav_spread` | FBS favorite against a non-FBS opponent | favorite spread | 25 |
| T3 `small_fav_dog_spread` | FBS vs FBS, closing spread 3 to 6.5 | underdog spread | 80 |
| T4 `big_fav_over` | FBS vs FBS, closing spread 14 to 20.5, total posted | over | 50 |
| T5 `sat_evening_g5_fav_ml` | FBS vs FBS, at least one team outside the P4, Saturday kickoff 5:00 pm to 8:29 pm ET | favorite moneyline | 60 |

T1 and T5 overlap (11 of 24 discovery T1 games are also T5). They are graded
separately and the overlap count is reported every run. The test family is
five; the Bonferroni-adjusted bar is a 99% interval per trigger, reported
beside the nominal 95%.

## Metrics

- **Primary:** flat one-unit ROI at the frozen price, with a 95% bootstrap
  interval resampling **game dates** (same-day games are correlated).
- **Secondary:** realized win rate against the vig-free implied probability
  (moneyline triggers) or 50% (spread and total triggers).
- Reported, gating nothing: pushes, price source mix, distinct dates, T1/T5
  overlap, per-week accrual.

## Kill and pass criteria (frozen)

Evaluated per trigger, once, on or after the verdict date, only if the floor
is reached:

- **Dead:** the ROI 95% interval includes zero. No re-slicing by conference,
  week, home/away or spread size to rescue it. A variant is a new study.
- **Pass (licenses a live shadow period only, never a star rating or a
  recommendation):** ROI 95% interval lower bound above zero **and** realized
  win rate at or above implied **and** the result is not carried by a single
  date or a single team (no one team in more than 25% of the trigger's games).
- **Not decidable:** floor not reached by the verdict date. The trigger
  carries to 2027 with the same definition; the bar does not drop.

T2 will probably not reach its floor in 2026: FBS-vs-FCS games are concentrated
in September. That is stated now rather than discovered later.

## Discovery sample (frozen, cannot confirm)

Games with commence time between 2026-08-01 and 2026-10-09 22:00 UTC, graded
by the same code (`python -m model.cfb_pattern_watch --discovery`):

| Trigger | n | record | win rate | implied | units | ROI | date-clustered 95% CI |
|---|---:|---|---:|---:|---:|---:|---|
| T1 favorite ML | 24 | 22-2-0 | 91.7% | 75.9% | +3.90 | +16.2% | [+5.9%, +29.3%] (9 dates) |
| T2 FCS favorite spread | 68 | 42-25-1 | 62.7% | 50.0% | +13.18 | +19.4% | [−11.0%, +34.3%] (8 dates) |
| T3 underdog spread | 64 | 35-26-3 | 57.4% | 50.0% | +5.82 | +9.1% | [−22.4%, +45.8%] (13 dates) |
| T4 over | 40 | 25-15-0 | 62.5% | 50.0% | +7.73 | +19.3% | [−19.9%, +45.1%] (7 dates) |
| T5 favorite ML | 58 | 53-5-0 | 91.4% | 77.5% | +8.63 | +14.9% | [+7.4%, +25.2%] (5 dates) |

Two honest readings of that table:

1. **The moneyline triggers (T1, T5) have narrow intervals because they have
   five to nine dates, not because the effect is precise.** A date-clustered
   bootstrap on five Saturdays cannot express the real uncertainty. The
   favorites in those games pay roughly −300 to −500; two more upsets in the
   discovery window would have put T5 near zero. Expect the prospective ROI
   to be far below +15% even if the pattern is real.
2. **The spread and total triggers (T2, T3, T4) have intervals that already
   include zero on the discovery sample.** They are registered because the
   underlying game-shape numbers (half-time leads, backdoor cover rates,
   second-half scoring) moved in a coherent direction, not because the ROI
   is established.

Historical base rates for the same triggers on 2022–2025 weeks 1–5, where the
data allow: G5-vs-G5 favorites won at their implied rate (−0.3pp), 3–6.5 point
favorites covered 46.5%, 14–20.5 point games went over 44.3%. The Saturday
evening slot was the one window where underdogs historically beat their price.
No FBS-vs-FCS baseline exists in our data.

## Operations

- `refresh_cfb_pattern_watch.bat` runs the scan and appends to the log; a
  Windows scheduled task (`NBADFS CFB pattern watch`) runs it Tuesdays at
  09:00 local, after the weekend's scores and verified closes have landed.
- The scan is idempotent: a (trigger, game) pair is written once. Re-running
  after a score correction does not change the stored row; a corrected grade
  would be a new row with a new study version, per the repo's append-only
  rule.
- Nothing here is pushed, deployed or surfaced on `/health` yet. Promotion of
  the scan to a GitHub workflow is a separate decision.

## Non-negotiables

- Trigger definitions, floors, price basis and the verdict date do not move.
  Changing any of them is `cfb-pattern-watch-v2` with its own discovery note.
- No trigger becomes a recommendation, a star rating, or a line on `/vegas`
  from this study alone. A pass licenses shadow tracking at live prices.
- The discovery table above is the only place the discovery sample appears in
  a summary. It is never added to the prospective counts.
