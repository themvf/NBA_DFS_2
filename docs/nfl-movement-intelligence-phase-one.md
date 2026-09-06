# Movement intelligence phase one — 2026-09-06

Scope: existing sportsbook data only. No additional subscription, prop requests,
weather/news feed, bet recommendations, predictive model, or star-rating change.

## NFL detector parity

NFL retains its existing moneyline/spread/total steam and walking definitions.
The shared football detector adds spread/total reversals, reference-led moves,
same-line price pressure, and spread crossings of 3 or 7. These observations use
`nfl-structure-v1`; CFB keeps its own version and key-number list. Shared
moneyline structure adds disagreement, convergence and late movement. Detector
health includes each newly enabled family. Sampling only establishes observed
ordering, not true first-mover identity or professional betting activity.

NFL now uses the same four-card black/amber movement strip as CFB and Tennis.
Selection highlights the game and describes the chosen market. All three strips
read current lifecycle observations; first-breach signal tapes and outcome audits
continue to read immutable `line_alerts` rows. NFL exposes the latest observation
states, including failed moves and missing comparable data, in a separate panel.

## Append-only lifecycle contract (frozen before production collection)

`market_signal_observations` stores one row per sport, event, market, detector,
side, detector version and source history id. Repeated scans of one capture cannot
create additional observations. Distinct markets cannot collide. First-breach
alerts are neither overwritten nor counted again in CLV/ROI audits.

New enrollment requires a capture no more than 30 minutes old, before kickoff.
No retrospective enrollment/backfill. Each observation retains trigger and
baseline history ids, provider capture time, recording time, rule version,
direction and original detector evidence. Exact quotes and available provider
timestamps remain in referenced immutable `game_odds_history.books` snapshots.

Follow-up comparisons require at least three identical retail books present at
baseline, trigger and current capture, excluding Pinnacle and Polymarket. Movement
is the median of per-book differences, in the signal's direction. Moneylines use
paired no-vig probabilities; spread/total price pressure additionally requires the
same line at all three times. Missing/nonfinite quotes produce `unavailable`.
The trigger baseline is opening for drift, pivot for reversal where recorded,
otherwise previous capture. Without a positive measurable initial leg, retention
is unavailable rather than inferred.

- `triggered`: first prospective observation of this detector/market/side/version.
- `strengthened`: movement exceeds its initial leg.
- `held`: initial movement remains, although the detector need not fire again.
- `confirmed`: initial movement remains and the detector fires on this capture.
- `weakened`: some positive movement remains but less than the initial leg.
- `faded`: back to baseline.
- `reversed`: past baseline in the opposite direction.
- `unavailable`: insufficient comparable evidence.

The 1e-9 comparison tolerance only handles floating-point precision. Retention is
a descriptive measurement, not a confidence score. Expiration is calculated from
capture age at display time: after 30 minutes an observation leaves the cards.
No synthetic database row pretends an expired signal was freshly observed.
Faded, reversed and unavailable old-direction observations do not headline cards.

## Capture pilot and quota

NFL adds twice-daily ET calendar targets on D-7 through D-4, retains D-3/D-2
six-hourly and D-1 three-hourly targets, and adds five-minute windows throughout
the final 120 minutes. Legacy hourly/closing checkpoints remain. Due checkpoints
are grouped by season type for the existing bulk NFL request; overlapping targets
can share an accepted capture. Late windows are marked missed, not reconstructed.
Early saved quotes are first observed, never asserted to be true market openers.

Before implementation, repository variables were checked: daily cap 2,000,
remaining reserve 5,000. Latest stored usage at 2026-09-06 15:02:41 UTC reported
88,382 credits remaining. These are dated observations, not a guaranteed future
balance. Both guards remain in force; no quota variable was increased. Scheduled
GitHub jobs can run late, so five-minute targets are not a latency guarantee.

## Deployment and verification

Run `python -m model.signal_observations` for targeted additive DDL before deploying
web readers/scanners. It deliberately avoids global schema initialization. The
shared capture workflow also ensures this table, and normal full schema setup
includes the same DDL. Existing fallback scan workflows continue to function.

Checks cover NFL key numbers/version, lifecycle signs and retention, missing
books, changed propositions, duplicate scans, freshness, ET/DST checkpoints,
existing football settlement, and UI eligibility/routing. Do not infer an edge
from lifecycle counts. Any profitability study must separately freeze its rules,
sample floor and evaluation periods before inspecting outcomes, with game/date
clustering and prospective closing-line evaluation.

Deferred: injury-event linking, weather revisions, props, alternate/period
markets, public splits and exchange liquidity.
