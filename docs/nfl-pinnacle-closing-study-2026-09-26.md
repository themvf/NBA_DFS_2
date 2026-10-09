# NFL closing moneyline movement using Pinnacle alone — September 26, 2026

This reruns the [CFB/NFL retail-book study](football-closing-steam-study-2026-09-25.md) with **Pinnacle as the sole signal source**. A selection requires Pinnacle's two-sided, vig-free win probability for a team to rise at least 1.5 percentage points. Both Pinnacle quotes must have provider updates no more than 35 minutes old at their capture. The final capture must be within 15 minutes of kickoff. Each game contributes at most one team. This is a Pinnacle movement test; one sportsbook moving is not multi-book steam.

The strict rule compares the final two captures, no more than 40 minutes apart. The closing-hour rule compares the last capture with the most recent one 45–90 minutes before kickoff. Return assumes one unit staked at the final observed Pinnacle price; the DraftKings return is shown at its simultaneous observed quote for comparison. Historical quotes do not guarantee an executable fill.

| NFL sample and rule | Eligible games with fresh Pinnacle pair | Picks | Record | Pinnacle profit / ROI | DraftKings profit / ROI | Pinnacle no-vig expected wins |
|---|---:|---:|---:|---:|---:|---:|
| Preseason, strict final capture | 20 | 3 | 1–2 | −1.07 units / −35.5% | −0.95 / −31.7% | 1.69 |
| Preseason, closing hour | 23 | 15 | 9–6 | +5.08 units / +33.8% | +4.89 / +32.6% | 6.76 |
| Regular season, strict final capture | 27 | 0 | — | — | — | — |
| Regular season, closing hour | 27 | 0 | — | — | — | — |

The five additional preseason closing-hour picks beyond the three-retail-book rule went **3–2**. Ten of the 15 Pinnacle picks also had at least three retail books confirming the same 1.5-point move. At a looser 1.0-point Pinnacle threshold, preseason went 9–7 for +25.5% Pinnacle ROI; regular season went 2–5 for −16.1% Pinnacle ROI. The 15-pick closing-hour result has a descriptive game-level bootstrap 95% ROI interval of about **−23.4% to +90.8%**. This is too small and too concentrated in preseason to establish an edge.

The regular-season zero is a **threshold result**, not a missing-quote result. Of 32 completed non-tied regular-season games with book-level odds, 27 had a timely closing-hour pair and both Pinnacle moneylines passed the freshness check. Their absolute closing-hour moves had a median of **0.47 percentage points** and a maximum of **1.305 points**; seven reached 1.0 point, but none reached 1.5. In the final-two-capture window, the largest regular-season Pinnacle move was **1.267 points**. The 23 eligible preseason closing-hour games had a median move of **2.184 points**.

## Favorites at the final Pinnacle quote

Favorite means **Pinnacle's two-sided, no-vig win probability exceeds 50%** at the final capture. A negative American price alone is insufficient because both sides can have negative prices. Under the 1.5-point closing-hour rule, four of the 15 Pinnacle preseason picks were favorites: Raiders won at −131, Steelers lost at −142, Lions won at −109, and Bears lost at −191. They went **2–2, −0.319 units, −8.0% Pinnacle ROI**. The other 11 were underdogs and went **7–4, +5.395 units, +49.0% ROI**. The strict final-capture rule selected two favorites, Steelers and Bears; both lost. Regular season still had no 1.5-point selection of either kind. At the 1.0-point closing-hour threshold, the sole regular-season favorite was Minnesota, which won at −136. These splits are descriptive and very small.

The available ledger contains 32 completed non-tied preseason games and 32 completed non-tied regular-season games with pregame book-level odds. The full selections, move sizes, prices, scores, and retail confirmation counts are in [the JSON report](../artifacts/nfl_pinnacle_closing_study_2026-09-26.json). The read-only study is [the script](../research/nfl_pinnacle_closing_study.py).

```text
python -m research.nfl_pinnacle_closing_study --out artifacts/nfl_pinnacle_closing_study_2026-09-26.json
```
