# NBA percentile and joint P75 card trial

The existing cloud recording and morning review run this independent experiment.
The [frozen protocol](../research/artifacts/nba_percentile_protocol_v1.json)
starts eligibility on November 16, 2026. A slate enrolls only when its new indexed
v3 original and this attachment are completed and published before the frozen
cutoff. Earlier replays remain development evidence. A missed attachment cannot
be added to an existing original.

For every player in the original modeled pool, freeze P50, P60, P70, P75, P80
and P90 directly from the aligned v3 draws. Compare the experimental percentile
with the frozen market line, v2 blend and v3 mean on identical resolved rows
for each reference. P70 and P75 are the primary player targets. Report counts,
strict exceedances, ties, at-or-above rates and pinball loss for the full pool,
below-20 market lines and 20+ market lines. Full-game scoring includes overtime.
The forecast is conditional on playing.

For the card comparison, take the ten highest frozen v3 means (or all available
if fewer), enumerate distinct three-player entries, and record five entries
ranked by mean total and five by joint P75. Compute joint P75 after adding the
three scores within each shared draw. Resolve ties by the sorted event/player
identities. If fewer than five distinct trios are possible, retain all player
forecasts and mark the cards unavailable. The primary result is the highest
actual entry on each card. The secondary result is the sum of its five entries.
A paired date requires the exact full slate to be final and every selection on
both cards to resolve. DNP or missing selections remain unresolved.

The original archive retains inputs, quote and injury evidence, forecasts, raw
draws, the percentile record and its index. The review archive retains the
scoreboard, reconciled diagnostics with available minutes, attempts and scoring
components, and the per-player percentile grade. These are linked by the
original hashes. Grading never replaces an original, and repeated reviews count
only the latest valid grade for each indexed record.

Read `percentile-report.json` alongside the other morning reports. Report
coverage, P70/P75 loss and exceedances by cohort, paired best and combined card
points, and any incomplete grades. Review the first 30 paired dates and use the
next ten as confirmation. The experiment contributes evidence for any later
projection or selection change and has no automatic promotion.
