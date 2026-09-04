# 2026 missing-game historical movement pilot

Completed September 4, 2026. Historical backtest only; 2025 untouched.

## Scope and cost

- Eight previously uncaptured Week 0 games (August 29–30); existing live captures and signals unchanged.
- 213 sport-wide archive requests produced 599 unique game snapshots (75 per game except NC State–Virginia: 74).
- Five-minute sampling in the final six hours, reference snapshots at 24/48 hours, and latest available pre-kickoff observation. Not the full trail from market open.
- 2,486 field transitions across individual books, including availability changes. This is not a count of independent market moves or betting signals.
- Confirmed successful-response cost: **6,390 credits**. Conservative budget accounting: **6,450**, including 60 reserved for failed/interrupted attempts whose charges are uncertain.
- Latest observed remaining account quota: **91,265** (shared with other workers).
- Replaying the fully cached archive required **zero additional API requests**.

## Reconstructed closing observations

Spread is the home-team consensus line. These are near-close proxies, not guaranteed exact final prices.

| Game | Snapshots | Home spread | Total | Observation before kickoff |
|---|---:|---:|---:|---:|
| North Carolina @ TCU | 75 | -7.5 | 46 | 4m 22s |
| San José State @ USC | 75 | -37.5 | 61 | 4m 22s |
| NC State @ Virginia | 74 | -4 | 50.5 | 9m 22s |
| Jacksonville State @ North Dakota State | 75 | -7 | 46.5 | 4m 22s |
| Sacramento State @ Eastern Michigan | 75 | -9.5 | 52.5 | 4m 22s |
| Hawai'i @ Stanford | 75 | -4.5 | 48.5 | 4m 22s |
| New Mexico State @ Florida State | 75 | -31.5 | 53.5 | 4m 22s |
| Memphis @ UNLV | 75 | -4.5 | 55.5 | 4m 22s |

## Signal results

Uses existing detector thresholds and first qualifying breach per game/type/side.
Prices are archived execution-book quotes, not assumed -110. Positive CLV means
a better entry line than that same book's closing line; this is line-point CLV,
not price-based expected value. Closing consensus above can differ from that book.

| Game / signal | Historical entry | Outcome | Same-book CLV | Hypothetical units |
|---|---|---|---:|---:|
| San José State @ USC — spread steam | San José State +37.5, -103 (Caesars) | Won | 0.0 | +0.9709 |
| NC State @ Virginia — total walk | Under 51, -108 (Caesars) | Won | +0.5 | +0.9259 |
| Jacksonville State @ North Dakota State — key cross | Jacksonville State +6.5, -102 (FanDuel) | Lost | 0.0 | -1.0000 |
| Jacksonville State @ North Dakota State — key cross | North Dakota State -7, -105 (BetMGM) | Won | -0.5 | +0.9524 |
| New Mexico State @ Florida State — spread walk | New Mexico State +31, -110 (BetMGM) | Won | 0.0 | +0.9091 |

Five overlapping signals across four games: **4 won / 1 lost**, **+2.7583 hypothetical
units**, **0.0-point average CLV**. One positive-CLV signal, one negative, three flat.
No qualifying reversal, total steam, price-pressure, or reference-led signal was
emitted in this sample. This does not imply no price changes or smaller reversals.

## Interpretation and limitations

The win record is encouraging but far too small and selected to validate an edge.
Two opposing key-cross signals belong to the same game. Signals are correlated,
and hypothetical best-available quotes do not prove real execution or availability
at betting limits. No thresholds were tuned to improve these results.

The NC State–Virginia close is older than the other seven; all closes retain
their actual age. Same-book CLV additionally requires a bookmaker update within
15 minutes of kickoff. A missing/stale eligible closing quote would yield null.

One archive timestamp returned duplicate rows for the same events with identical
normalized prices but slightly different bookmaker update times. The replay
collapses only identity- and price-equivalent rows, conservatively retaining the
older timestamp. Conflicting prices/identities remain fatal errors. Raw responses
are preserved unchanged.

Historical evidence lives in `cfb_historical_archive`; replay results and frozen
entry details live in `cfb_historical_replays`, profile `2026-missing-6h-v1`.
They are not inserted into prospective alerts or substituted for immutable live
closes. The dashboard's live badges are unchanged. This pilot does not repair
the previously identified stale closes on games already captured live.

Verification: 75 targeted tests passed, including away-side settlement signs,
same-book CLV, stale evidence, repeated movement, and duplicate archive handling.
No 2025 API requests were made. Review this pilot before expanding historical scope.
