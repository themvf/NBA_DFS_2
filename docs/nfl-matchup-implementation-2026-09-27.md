# NFL matchup implementation and September 27 slate verification

Date: September 27, 2026. This is the implementation evidence for
[the matchup specification](./nfl-matchup-data-projection-spec.md).

The new matchup forecasts are executable, frozen in the database, and connected
to the local pick'em and DFS interfaces. They are research comparisons awaiting
the registered forward evaluation. Production player projections remain v5;
the protected opponent-points-allowed switch remains off. No production
deployment or model promotion has been made by this task.

## Today's comparison

The test uses the saved September 27 Classic salary slate, `nfl_9_27.csv`, with
671 salary entries across 13 games. It joins 643 player entries to the pinned
v5 projection run `3ef96f66-7a8c-5070-a41e-6ae59b5712b0`. The baseline was
captured at 14:14:31 UTC. Unmatched entries remain explicitly unmatched.

The final matchup comparison was frozen at 14:54:57 UTC (10:54 a.m. Eastern).
It applies eligible adjustments to 149 rows: 112 RBs and 37 QBs. The other
494 matched entries retain the baseline: 352 have no registered position
effect, 82 are unavailable, and 60 lack complete matching history.

| Player | Baseline DK points | Matchup candidate | Change |
|---|---:|---:|---:|
| Breece Hall | 14.23 | 15.27 | +1.05 |
| Javonte Williams | 14.19 | 15.05 | +0.86 |
| Tyrone Tracy Jr. | 9.06 | 9.61 | +0.55 |
| Bucky Irving | 13.79 | 13.23 | -0.56 |
| Derrick Henry | 26.90 | 26.42 | -0.48 |
| Tyler Shough | 21.30 | 20.97 | -0.33 |

Differences use unrounded simulation means. These are candidate projections,
not lineup recommendations or observed accuracy gains. Earlier development
comparisons are superseded by this final freeze; Kenneth Walker's earlier
downgrade remains withheld because the required evidence is incomplete.

- [Player comparison and changes](../artifacts/nfl-matchup-implementation/2026-09-27/slate-comparison.md)
- [Downloadable player changes CSV](../artifacts/nfl-matchup-implementation/2026-09-27/player-changes.csv)
- [Complete player forecasts and frozen source manifests](../artifacts/nfl-matchup-implementation/2026-09-27/slate-comparison.json)
- [Pick'em shadow forecasts](../artifacts/nfl-matchup-implementation/2026-09-27/pickem-shadow.json)
- [Forward evaluation and eligibility report](../artifacts/nfl-matchup-forward-report.json)

The player comparison holds the saved baseline fixed. It isolates the new
matchup adjustment from ordinary changes in odds, availability, or a newer
production run. A changed research forecast is not a measured improvement in
accuracy, and it does not silently replace a saved lineup's projection.

## What is connected

| Package | Implementation | Verification or retained evidence |
|---|---|---|
| WP0: Existing opponent experiment | `context_variants` grading, study-pin filtering, freeze health and report-card streams | Protected variant tests and the context study report; insufficient completed forward weeks returns no verdict |
| WP1: Source identity | PFR captures preserve source files, parser/schema, identity mappings and timestamps | Frozen manifests, HTML/nflverse unit checks, later-revision cutoff tests |
| WP2: Shared matchup features | Prior-game offensive and defensive pressure and RB contact features published through context observations and releases | Actual target-game manifests with participant completeness, exact contact denominators and missing-data reasons |
| WP3: Pick'em | Regularized market-logit residual challenger, common tie component, corrected rival pick-share marginals, weekly/season prize policies | Frozen three-way probability comparisons, crowd/tie/payout tests and local interface checks |
| WP4: DFS projections | Passing-yard efficiency and RB rushing-yard efficiency challengers; full component ledgers and exact DK rescoring | Real slate comparison; mean/quantile/stat replay checks; source and model hashes |
| WP5: Tournament decisions | Existing team simulator and optimizer adapters, independent diagnostic banks, separately named coherent game banks, contest evaluator | Saved-slate portfolio artifacts, event-ledger and scorer reconciliation; missing current contest fields retain construction-only status |
| WP6: Interfaces | Pick'em evidence and research probabilities; DFS player component/range explanation and tournament research review | Browser checks against the real saved salary slate and Tampa Bay–Minnesota pick'em evidence |
| WP7: Operations | Pre-lock captures, immutable registrations and result revisions, paired grading, health and rollback rules | Scheduled refresh definitions and retained forward reports |

## Forecast mechanics and limits

The pressure challenger estimates a residual passing-yard rate from the
offense's prior pressure faced and the opponent's prior pressure created.
The contact challenger estimates residual rushing-yard efficiency from
offensive and opponent-allowed yards before and after contact. Each changes
only its declared yardage component, with a maximum relative efficiency change
of 10%. Neither adds carries, pass attempts, touchdowns, a receiving bonus, or
a generic defense multiplier. Every simulated outcome is rescored under the
existing DraftKings rules, including threshold bonuses.

The initial models were fitted on 2023–2025 retrospective development data:
1,196 pressure training rows and 1,837 contact training rows. These are
development counts, not independent forward successes. Frozen coefficients
are not refitted by the scheduled refresh.

Before an adjustment can be displayed, the retained v5 mean, P10, median, P90,
boom rate, and every projected stat must reproduce within 0.00011, allowing
for the saved four-decimal rounding. An unavailable player or a distribution
that cannot be reproduced retains the exact saved forecast. Unverified draws
are not supplied to the scenario adapter.

PFR pressure percentages remain unweighted single-QB game observations; no
exact pressure denominator is invented. Multi-QB games are withheld from that
aggregate. RB contact uses carries and yards from the same charting rows.
Participant completeness is checked against frozen raw nflverse weekly player
statistics. The retained PBP facts do not contain participant IDs, so this
check is explicitly labeled weekly player-stat evidence, not PBP identity
reconstruction. Missing players, unresolved identities, conflicting carry
counts, or incomplete prior games trigger a fallback.

Receiving drops, bad throws, play-by-play production, usage, market context,
and availability remain explanation or baseline inputs unless a separately
registered numerical effect applies. A source being present does not authorize
an additional multiplier. PFR starters and snap counts remain unavailable from
the advanced-stat import.

## Tournament interpretation

The independent-player bank is a diagnostic for isolating projection changes.
It does not establish stack value or realistic lineup tails. The coherent
bank is a separate research model using paired historical game budgets and
one offense/receiver/defense scoring ledger. It changes player and DST
marginals and must qualify separately; it is not represented as a harmless
correlation overlay on the approved v5 forecasts.

Single-entry, three-entry and multi-entry comparisons use equal entry counts
within each comparison, legal complete rosters, portfolio constraints and
independent selection/evaluation streams. The multi-entry construction
objective measures the best score among the selected entries in each joint
draw, not the sum of player percentiles or independent lineup means.

Actual current contest ownership, a validated complete field, fees and payouts
are required for real contest-relative payout claims. Missing inputs do not
become zero ownership or an invented ROI. Archived contest outcomes are kept
separate from pregame inputs.

The isolated PFR sensitivity run retains the same baseline lineups and uses
409 players whose frozen distributions could be replayed. In that diagnostic,
the simulated mean best-lineup score changes from 139.69 to 139.33 for one
entry, 162.27 to 161.97 for three entries, and 182.08 to 181.88 for 20 entries.
These small changes are the direct effect of the new player adjustments on
those fixed lineups. Independent player draws omit football correlation.
Reselecting the lineup also changes the decision policy, so its larger
differences must not be attributed entirely to PFR or called measured gains.
See [the construction sensitivity comparison](../artifacts/nfl-matchup-implementation/2026-09-27/portfolio-comparison.md).

A separate attribution check uses the same 29-candidate union and the same
selection objective for both arms. Policy-only versus policy-plus-PFR average
best scores are 156.67 versus 156.75 for one entry, 172.17 versus 172.06 for
three, and 186.71 versus 186.76 for 20. The unrounded differences are +0.07,
-0.11 and +0.05 points. The different candidate pool explains why the
three-entry number differs from the earlier sensitivity table. This is a small
conditional simulation effect, not proof of better tournament returns. The
[fixed-pool attribution artifact](../artifacts/nfl-matchup-implementation/2026-09-27/portfolio-attribution.json)
retains its original comparison hash and all three arms.

The archived-result importer reconciled the two available contest exports and
graded 80 saved pre-lock lineups with the appropriate Classic or Captain/Flex
scoring. A later-created rerun was excluded. The best saved scores were 137.82
for the 20-lineup Classic portfolio, 100.77 for the 20-lineup Showdown portfolio,
and 110.77 for the 40-lineup Showdown portfolio. These are retrospective scores;
the files do not establish entry submission, fees or payouts. They cannot
qualify today's newly frozen model or establish realized ROI.

The ordinary optimizer also passed the required integration smoke on three
real saved salary slates: one Classic and two Showdown. Each returned 20
complete, legal and unique lineups. Missing optional research inputs preserved
the original production result. The retained evidence is in
[the saved-slate gate](../artifacts/nfl-matchup-implementation/2026-09-27/three-saved-slate-gate.json)
and [archived contest grades](../artifacts/nfl-matchup-implementation/2026-09-27/archived-contest-grades.json).

## Study controls

[The study index](./nfl-matchup-studies.json) points to immutable registration
files. Pressure, contact, their combined DFS configuration, and the pick'em
combination have separate forecast studies. Implementation and baseline
amendments are append-only and move eligibility forward; earlier captures
are not rewritten or retrospectively approved. Code hashes normalize line
endings so equivalent Windows and Linux checkouts reproduce the same hash.

The final forward report accepted the intended DFS and pick'em captures with
no current source, cutoff, model or code-pin violations. Its DFS combined
population is 235 QB/RB entries, including 149 covered adjustments; other
positions are outside that registered cohort. Pick'em has one covered
adjustment among 15 games: Detroit's unconditional win probability moves
from 72.49% to 73.58%; the shared tie probability stays fixed. The other 14
games retain the exact market forecast. All four registered studies return
no verdict with zero completed forward weeks.

The new forecast gates require at least eight scorable forward weeks and the
registered sample, uncertainty and harm thresholds. The existing allowed-carries
experiment and GPP Showdown promotion gates retain their original contracts.
No result from today's unplayed games can meet those gates.

The coherent scenario model also has an executable distribution grader. It
compares exact frozen central 50% and 80% intervals using weighted interval
score, checks mean error and boom calibration, and grades Classic and Showdown
as separate registered cohorts. It requires the precise baseline configuration,
source/code identities and pre-lock publication, and rejects retrospective
replays or missing baseline quantiles. Its first full forward week is week 4;
today's week 3 demonstration is not counted as qualification evidence. The
regular prospective grading command includes this report.

## Operations

The existing local morning automation retains its Monday, Tuesday and Friday
7 a.m. America/New_York schedule. It refreshes PBP/PFR, markets and availability,
freezes the registered research forecasts, and grades eligible completed
games. Missing salary slates or unpublished source files remain coverage
states. The local computer and app must be running for this automation.

PFR ingestion now also refreshes exact PFR/GSIS identifiers from the official
current-season weekly roster file. The September 27 refresh added 2,048
source-backed claims and found 2,496 resolved identifiers with no conflicts.
This recovered rookie coverage without guessing from names. The 33 completed
games retain all four advanced sections and 1,812 player/section rows. Their
overall supplement status remains partial because starter and snap sections
are absent.

The repository's existing twice-daily NFL workflow now includes PFR before
projection generation and the research freezes, grades and artifacts afterward.
Those workflow edits take effect on the remote scheduler only after deployment.

The local morning automation also runs the coherent scenario and portfolio
cycle after the comparison attempt, with a fresh upcoming Showdown baseline
fallback when no supported Classic comparison exists. The cycle checks its capture time
and kickoff before dependent stages, retains raw banks and event ledgers, and
publishes a compact immutable database report. The research page reads that
report by salary upload rather than depending on files on a scheduled worker.
Failed or missing refreshes cannot substitute an older slate as a fresh result.
Each cycle writes to a unique snapshot directory with a SHA256 archive manifest;
later runs preserve previous banks, ledgers and reports. Showdown captures
require one complete future game and exact salary/player identities. The actual
week 3 upcoming-Showdown check found no unlocked saved slate and returned
pending coverage, without writing an invented comparison.

Run a fresh comparison against the same pinned baseline from the repository root:

```powershell
python -m research.nfl_matchup_implementation --season 2026 --week 3 --baseline-run-id 3ef96f66-7a8c-5070-a41e-6ae59b5712b0 --persist
python -m research.nfl_pickem_matchup --season 2026 --week 3 --persist
python -m research.nfl_matchup_study prospective --season 2026 --capture-results --output artifacts/nfl-matchup-forward-report.json
python -m research.nfl_matchup_study context --season 2026
python -m research.nfl_matchup_scenario_refresh --season 2026 --week 3 --persist
```

Run captures before the target game's kickoff. Do not use `--fit` during a
registered study refresh. A future run will have a new capture identity;
the retained September 27 JSON and database rows remain the evidence for
this comparison.

## Verification

The focused Python implementation suite passed 122 tests. An additional
publication, refresh and archive suite passed nine tests. TypeScript checking
and targeted contest, pick'em and player-explanation tests passed. The live
non-persisting weekly pipeline smoke built 1,087 player forecasts, attached
matchup context to 1,021, and produced 221 numerical research challengers with
zero active projection changes. This weekly universe is larger than the saved
13-game salary slate, so its count differs from the 149 slate adjustments.

Browser checks confirmed Breece Hall's component change, Jaxson Dart's exact
zero fallback, Tampa Bay-Minnesota's evidence and missing-feature explanation,
and the database-backed research page. Details are retained in
[the verification record](../artifacts/nfl-matchup-implementation/2026-09-27/verification.json).
