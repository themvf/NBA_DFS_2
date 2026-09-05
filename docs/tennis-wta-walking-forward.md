# WTA walking prospective study v1

Registered 2026-09-05, before implementation or collection for this version.
Historical findings motivated these rules; they are not validation data.

Historical audit caveats (2026-09-05): the 60-70% walking band was 43/57,
but only 20 had frozen prices (15 wins, +11.55% hypothetical ROI). ATP's 11
priced rows returned -3.11%; WTA's nine returned +29.47%. Two highest-paying
wins changed to losses would turn the combined ROI into -4.51%. Twelve priced
rows had unknown completion and eight lacked canonical surface metadata.
These are reasons to collect a new sample, not reasons to promote this slice.
The new freshness/paired-book requirements make this cohort different from
that historical sample; its results must never be pooled with the old sample.

## Frozen hypothesis and population

WTA match-moneyline walking alerts at 60% <= trigger probability < 70%
may outperform the trigger-time retail probability and earn positive returns
at the exact recorded execution price. This is a new research study, separate
from the closed Elo studies and the Pinnacle-favorite forward program.

- Keep the existing walking definition: >=2 percentage points from the first
  sportsbook capture, using the same retail books at both endpoints.
- At least three overlapping usable retail books; exclude Pinnacle and
  Polymarket from the probability mean. Normalize each book's paired implied
  probabilities by their sum before averaging (proportional margin removal).
- WTA only; no surface restriction. Missing surface is disclosed, not inferred.
- Only newly inserted walking alerts receive `walking_study_version` =
  `wta-walking-60-70-v1`. Never backfill, relabel, or update historical alerts.
- Use the first walking alert per match, irrespective of side or eligibility.
  An earlier ineligible or historical walking alert prevents later enrollment.
- Trigger capture and enrollment must precede the recorded scheduled start.
  Capture and execution-book update must be no older than 15 minutes at
  enrollment, and no later than enrollment. A scheduled boundary is not verified
  first serve. Reject mixed Polymarket/sportsbook captures from enrollment.
- Require a frozen approved execution-book moneyline price. Retain book, exact
  odds, rule snapshot, opening/trigger capture IDs, overlapping book keys,
  trigger probability, tour, tournament, surface (including null), and times.
- No added API calls, betting recommendations, star changes, or new messages.

## Outcomes and estimands

Primary: one-unit hypothetical ROI and win rate minus the frozen trigger
probability on confirmed `completed` matches with a recorded winner. Execution
is assumed, not demonstrated account profit. No CLV price substitutes for entry.

Report separately: unknown-status matches with a winner (sensitivity only),
pending/no-winner matches, and retirement/walkover/cancelled/other statuses.
Never force an unknown winner-bearing match into ordinary settlement. Report
completion coverage so selective missingness is visible.

Secondary: frozen same-book moneyline execution CLV and closing-fair ticket EV
(`entry_decimal * closing_fair_probability - 1`). Use only the primary verified
close view, paired moneyline quotes at the execution book, and valid timestamps.
Label tennis closes as scheduled-boundary observations. Missing stays missing.

## Review rule (no automatic promotion)

The first review is due at >=200 unique confirmed-completed matches, >=20 match
dates and >=5 tour/tournament labels. Dates and labels are diversity floors,
not a claim of independent samples. Freeze/export the dataset once at that
review; do not repeatedly test until a pass appears. A human reviews the frozen
dataset before any promotion or threshold change.

For consideration, require at least 90% completion among winner-bearing
non-retired rows, >=80% paired primary-close coverage in the completed sample,
and positive lower bounds for 95% date-clustered bootstrap intervals for ROI,
win-rate excess, and closing-fair ticket EV. These are necessary, not sufficient:
also inspect tournament/week/player concentration and removal of the largest
two wins/losses. Surface and fixed 60-62.5/62.5-65/65-67.5/67.5-70 slices are
descriptive only, never alternative routes to a pass. Bootstrap intervals are
unstable in small samples. Report progress and uncertainty without validation
labels. If inconclusive at review, retire or register a new independent test.

## Operations

`python -m model.tennis_walking_study` prints a read-only JSON report. The normal
Tennis refresh prints it after scanning. Deployment starts collection; local
implementation alone does not start the live program. Historical enrollment
must remain zero. Existing walking alerts and their settlement stay intact.

## Deployment verification and completion evidence

The 2026-09-05 source preview matched complete set-level TennisExplorer scores
for 202 of 203 recent unknown-status matches without a winner conflict. Parser
v4 retains source row HTML, provider match ID, set scores and match format in
the immutable result observation. It rejects exception markers, partial scores,
inconsistent aggregates and mismatched HTML row pairs. The importer revisits
unknown-status matches even when a winner is already present. Missing evidence
remains unknown; no existing result is reclassified solely from its winner.

Both Tennis workflows print the study report; the settlement workflow prints
it even when the broader health gate detects unrelated unresolved fixtures.
No fabricated qualifying alert is inserted during deployment verification.
