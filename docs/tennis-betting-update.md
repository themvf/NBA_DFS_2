# Tennis Betting Update

**Status:** Implemented prospectively on 2026-08-29
**Scope:** Tennis moneyline movement, sharp-book divergence, and the Vegas research UI

## Decision

There is no validated generic Tennis line-movement edge in the current ledger. Steam and walking remain research-only. The recommended path is a clean prospective test of one narrower hypothesis: **Pinnacle divergence toward a favorite**, recorded only when an executable retail price is frozen at trigger time.

This is a measurement program, not a betting recommendation. It cannot be promoted based on historical slicing or a small early sample.

## Evidence behind the update

The 2026-08-29 audit found two separate issues:

1. The live movement panel mixed sportsbook captures with Polymarket-only captures. A sportsbook consensus open could therefore be compared with a Polymarket close and presented as line movement.
2. The historical detector ledger did not establish a broad movement edge. Closing prices were only marginally more accurate than opening prices, and steam/walking results were weak. The most promising narrow cut was Pinnacle divergence toward favorites, but it remained below the validation floor, was correlated within slates, and lacked frozen execution prices.

Snapshot from that audit:

| Signal | Observation | Decision |
|---|---|---|
| Open to close | Brier improvement +0.00050; confidence interval crossed zero | No generic closing-line edge |
| Steam | Negative frozen-price return in the available sample | Research only |
| Walking | Near-flat negative frozen-price return | Research only |
| DK value | Positive headline return driven by two longshot wins | Do not promote |
| Pinnacle divergence, favorites | Positive CLV in a small, correlated sample | Start a new prospective cohort |

## Data integrity rules

### Separate market lanes

- **Sportsbook movement** uses captures that do not contain Polymarket.
- **Polymarket movement** uses Polymarket captures and is displayed in its own panel.
- A Polymarket capture must never become the open or close of a sportsbook trail.
- A movement row requires at least two pre-match captures.
- Sportsbook walking requires at least one retail book present in both the opening and current capture. Probabilities are computed from the overlapping retail books only.
- Steam continues to require synchronized movement at three or more comparable books.

### Prospective alert cohort

The new immutable alert type is `pinnacle_favorite_forward` with program version `tennis-pin-favorite-v1`.

An alert enters this cohort only when all of the following are true at trigger time:

- sport is Tennis;
- Pinnacle's vig-free probability is at least 2.0 percentage points above the retail consensus for the selected side;
- the selected side is a favorite by retail consensus (`P > 0.50`);
- at least three retail sportsbooks contribute to the consensus;
- a price is available at an approved executable sportsbook;
- the best available executable book, American odds, decimal odds, and number of quoted execution books are frozen in `details_json`.

Eligible Tennis favorites are written only to the prospective alert type, avoiding duplicate rows in the general Pinnacle-divergence cohort. Underdogs and candidates without an executable price remain in the general research ledger and do not enter the forward test.

Historical alerts are never relabeled or backfilled into the prospective cohort.

## Validation gate

The forward test targets **100 independently settled, frozen-price alerts**. Until then, the UI must display `forward test n/100` and must not label the cohort validated.

At the gate, review all of the following:

- average vig-free CLV and confidence interval;
- percentage of alerts that beat the close;
- return at the exact frozen execution price and confidence interval;
- result after removing the largest two wins and losses;
- ATP/WTA and favorite-probability buckets;
- concentration by tournament, slate, player, and calendar week;
- unique-match count rather than raw repeated observations.

Promotion requires positive CLV and economically positive frozen-price performance that are not explained by a handful of outliers or one concentrated segment. Failing that, retire or redesign the cohort.

## UI behavior

The Tennis research desk shows two explicit panels:

1. **Sportsbook movement** — comparable sportsbook captures only. Pinnacle gap, walking, and jump annotations remain research signals.
2. **Polymarket movement** — a standalone prediction-market trail. It never describes its movement as sportsbook money arriving.

Empty sportsbook state explains that two comparable captures with an overlapping book are required. The alert feed labels the new cohort as a forward test and shows its frozen execution book and price.

## Acceptance criteria

- A Polymarket-only latest capture cannot change sportsbook open, close, jump, or walking values.
- Tennis scanning selects the latest sportsbook capture even when a newer Polymarket capture exists.
- Tennis walking compares only overlapping retail books and records the overlap count.
- Every `pinnacle_favorite_forward` row has `program_version`, `forward_test_target`, `retail_books`, `exec_book`, `exec_odds`, and `exec_decimal`.
- No underdog can enter `pinnacle_favorite_forward`.
- The prospective alert type is included in health monitoring, settlement, CLV grading, ROI reporting, and the Tennis UI.
- Automated tests cover source separation, comparable-book walking, favorite eligibility, execution-price freezing, and the 100-alert disclosure gate.
