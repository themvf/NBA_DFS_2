# Top 10 market-structure signals — CFB and Tennis

These are research observations, not recommendations. Every prospective alert
freezes its source capture, same-book support, available execution price, and
detector version. Retrospective replays are explicitly marked and must never be
mixed into prospective performance claims.

| # | Signal | Evidence rule | CFB application | Tennis application |
|---|---|---|---|---|
| 1 | Steam | 3+ books make a synchronized material move | Moneyline plus spread/total line moves | Moneyline (≥1.5 probability points/book) |
| 2 | Walking | Same-book consensus drifts at least 2 points from open | Moneyline plus spread/total | Moneyline |
| 3 | Reversal | Same-book first leg followed by a material opposite retrace | Spread/total market-specific thresholds | Moneyline: first leg ≥2pp, retrace ≥1.5pp and ≥50% |
| 4 | Reference led | Pinnacle moves first, retail is initially quiet, retail follows | Spread/total | Moneyline within six hours |
| 5 | Price pressure | 5+ same books move ≥0.5 points; average ≥1.0 | Same-line spread/total price | Moneyline |
| 6 | Pinnacle divergence | Pinnacle differs from retail by ≥2 points | Moneyline | Moneyline |
| 7 | Book disagreement | Retail probability range ≥6 points across 4+ books | Moneyline | Moneyline |
| 8 | Market convergence | Dispersion contracts from ≥4 to ≤2 points by at least 2 | Moneyline | Moneyline |
| 9 | Late move | Same-book consensus changes ≥1 point inside the final hour | Moneyline | Moneyline |
| 10 | Threshold cross | Material market boundary changes side | Spread key number (3/7/10/14) | Favorite flips through 50% |

Provider additions/removals cannot qualify a move: every multi-snapshot rule
uses the intersection of successfully parsed retail books. Pinnacle and
Polymarket are excluded from retail consensus and retain their reference lanes.

The first qualifying prospective observation is immutable because the alert
ledger deduplicates by sport, match, signal, and side. Detector health uses a
14-day grace period before a newly deployed signal can be classified as dead.
