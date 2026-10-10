# CFB and NFL closing moneyline movement — September 25, 2026

This repeats the [MLB closing movement study](mlb-closing-steam-study-2026-09-25.md) on the football odds ledger. A team qualifies when at least three matched retail books raise its two-sided, vig-free moneyline win probability by at least 1.5 percentage points. The last quote must be captured within 15 minutes of scheduled kickoff. The strict rule uses the final two captures, no more than 40 minutes apart. The closing-hour rule compares the last capture with a quote taken 45–90 minutes before kickoff. Each book's quote must be no more than 35 minutes old at each capture. Hypothetical returns use one unit at the last observed DraftKings moneyline.

| Sport and rule | Eligible games with three fresh matched books | Qualifying teams | Record | DraftKings profit | ROI | Expected wins from DraftKings no-vig price |
|---|---:|---:|---:|---:|---:|---:|
| CFB, strict final capture | 176 | 1 | 0–1 | −1.00 units | −100.0% | 0.21 |
| CFB, closing hour | 176 | 8 | 4–4 | −1.50 units | −18.7% | 3.88 |
| NFL preseason, strict final capture | 20 | 2 | 0–2 | −2.00 units | −100.0% | 1.19 |
| NFL preseason, closing hour | 23 | 10 | 6–4 | +2.96 units | +29.6% | 4.71 |
| NFL regular season, strict final capture | 27 | 0 | — | — | — | — |
| NFL regular season, closing hour | 27 | 0 | — | — | — | — |

The CFB sample covers September 3–25, 2026, with 251 completed games that have pregame book-level odds. The NFL sample covers August 6–September 24, 2026, with 32 preseason and 32 regular-season completed, non-tied games with pregame book-level odds. The NFL closing-hour return comes **entirely from preseason**. The regular season had no 1.5-point, three-book qualifying closing-hour move in 27 eligible games.

At a looser **1.0 percentage point** threshold, CFB closing-hour teams went 7–9 for −28.7% ROI; NFL preseason teams went 7–6 for +13.1%; NFL regular season had one qualifier, which lost. These small samples do not establish a betting edge. Game-level bootstrap 95% ROI intervals are approximately −78.7% to +41.6% for the eight CFB closing-hour bets and −36.5% to +95.6% for the ten NFL preseason bets. The odds ledger contains only recent 2026 football coverage; this is not a multi-season test. The CFB historical archive pilot stores its reconstructed snapshots separately and is outside this comparison.

The final capture is an observed quote, not a guaranteed fill. Capture cadence can miss movement between polls. The full game-level selections and prices are in [the JSON report](../artifacts/football_closing_steam_study_2026-09-25.json); the reusable read-only query is [the study script](../research/football_closing_steam_study.py).

```text
python -m research.football_closing_steam_study --out artifacts/football_closing_steam_study_2026-09-25.json
```
