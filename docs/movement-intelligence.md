# Movement intelligence strip

Tennis and CFB retain their terminal layout. A shared strip below the top bar
shows up to four game/market cards; View all expands additional qualifying
groups. Search and tour/movement filters scope the strip. Selecting a card
opens that match and market (and the CFB side) in the existing instrument pane.

This is a presentation layer over recorded detectors, not a new predictive
model. It includes steam, walking, reversal, reference-led, key-cross,
price-pressure, late-move and favorite-flip signals. Book price gaps and
best-price comparisons are excluded from the shortlist.

Eligibility is explicitly current: prospective, unsettled observations from
the last 30 minutes, an available market capture within 30 minutes, and a known
future scheduled start on an unfinished event. Bad/future timestamps are
excluded. Old and retrospective observations remain in the existing tape and
audits. The 30-minute limit is a display freshness rule, not an edge threshold.

One card represents one match and market. The latest observation supplies the
direction and description; simultaneous opposing observations are labeled
mixed direction. Other qualifying signal types appear as related context.
Sorting uses the latest signal's recorded book support, then observation time;
support labels distinguish moved books, comparable books and exact-line support.
There is no profit score, inferred sharp-money cause, or star promotion.

Sparklines are contextual trails using the lower median of a stable retail-book
cohort across the available preceding 24 hours. They exclude Pinnacle and
Polymarket, future/post-start captures, and require two shared books. They are
independently scaled, name the home-referenced market, and dash gaps over 30m.
An unavailable comparable trail is disclosed rather than synthesized. Card
movement magnitudes come from frozen detector details and can span a different
interval than the contextual sparkline. Tennis only has a moneyline history
adapter; unsupported total/handicap trails are not invented.

Validation: `cd web` then `node --import tsx scripts/test-movement-intelligence.ts`.
