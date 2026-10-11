# NFL model completion: local integration handoff

Prepared October 5, 2026 EDT from `origin/main` at
`fcdb196038462327c7ca1f01a6df360ab6aed96b`. Branch:
`codex/nfl-model-completion`. No merge, push, deployment or database writes.
The main checkout's unrelated work is preserved.

## Changes ready for integration

| Area | Implemented behavior | Authority |
|---|---|---|
| Late availability | Built entries recheck once per minute in the last 90 minutes before kickoff. A newer frozen projection run can tighten eligibility using an exact-player/team/game, pregame, sourced OUT/Q/D decision. OUT cannot be cleared by a later ACTIVE row. | Ordinary build and export checks after integration |
| Results and build explanation | Newly unavailable rostered players show a rebuild notice. One readonly card explains player-level floor/ceiling search, ownership penalties and retained teammate workloads. No new experimental selectors. | Ordinary UI after integration |
| Ownership | Complete coverage alone cannot qualify ownership. Classic requires four independent chronological holdout slates and the existing accuracy thresholds. Captain/Flex requires separate qualification. Marginal accuracy cannot authorize a joint duplication model. | Guard available; fitted model still withheld |
| Absence workloads | Same-team absence-conditioned carries/targets, shrinkage, current role/evidence checks, multiple-absence matching, fixed team budgets and explicit unallocated work. QB rushing is not transferred to RBs. Recipient efficiency is unchanged. | New shadow candidate |
| Uncertain workload | Learned inactive/limited/normal states with frozen designation/baseline provenance, settled labels and chronological fitting. No data means unknown. Learned limited usage can feed conditional workload banks; unqualified state probabilities do not weight production projections. | New shadow candidate |
| Joint upside | Conditional offensive events, opponent DST scoring and kicker scoring reuse the existing coherent event engine. A new joint selector starts with a complete feasible portfolio and preserves every accepted swap's exposure, Captain/Flex, leader coverage, signal, stack, salary, overlap, eligibility and concrete archetype rules. | New shadow candidate; no export authority |
| Research refresh | Joint comparison runs after the existing coherent portfolio comparison. Missing/incompatible banks or incomplete construction are recorded as unavailable with a reason. | Background comparison after integration |

The ordinary optimizer still searches individual-player scores. No new model
has been silently promoted. Tonight's NO–ATL game had already started when this
work began; its saved forecasts and entries were not rebuilt or backdated.

## Evidence and frozen gates

The supplied ownership archive has one contest per format and **zero trained
chronological holdout slates**. Its reproducible qualification report is
`artifacts/nfl-model-completion/qualification-574b952fbfd031b421a6f05c7e2b5a4d15828112cbcdf023c78ee27c7c898def.json`.
This describes that archive, not the current live database. Both formats remain
withheld. Existing heuristic leverage remains default-off.

These new candidates cannot inherit a pass from another model or an already
inspected season. `nfl-model-completion-candidates.json` pins this implementation
before real-outcome evaluation of these candidates. Tests use artificial data
to check mechanics, not to claim forecast accuracy.

Existing gates are unchanged:

- Classic ownership: at least four distinct trained holdout slates; pooled
  Spearman >= .70, MAE <= 2 percentage points, absolute bias <= .5 points.
- Participation: `participation_probability_v1` in the existing availability
  registry; 500 independent Q/D player-games, two held-out seasons and eight
  prospective weeks, Brier improvement >= 5%, log-loss improvement >= 3%, ECE
  <= .05. The risk report separates three-state workload metrics from binary
  participation metrics and uses earlier position-by-designation baselines.
- Skill redistribution: `skill_position_redistribution_v1`; 100 qualified
  absence games **per position family**, two held-out seasons, team and recipient
  opportunity MAE improvement >= 3%, WIS improvement >= 2%, zero accounting
  failures. Passing carries alone cannot qualify fantasy-point ranges.
- Coherent distributions and portfolio selection must pass their existing
  forward forecast and portfolio release requirements independently. New
  constraint-preserving selection is recorded separately; original v5 scores,
  registrations, event generator and selection/evaluation streams are unchanged.

Limited workload has no separately passed probability/range gate. The new
candidate remains shadow-only even if the binary participation metric improves.
An official ACTIVE listing is not evidence of normal usage.

## Reproducible local commands

```powershell
# Grade frozen imported-contest history; output names are content-addressed.
python -m research.nfl_ownership qualify --history artifacts/nfl-ownership-v1/history-3a8e5f3b98e7fc02ff2b78bc456044ac00efbf66bbfe4146de60d2e46bd37e85.json --output-dir artifacts/nfl-model-completion

# Learn/grade prepared frozen Q/D cases. Output must be a new file.
python -m research.nfl_workload_risk --input risk-input.json --output risk-report.json

# Freeze conditional workload/coherent banks; no database or entry writes.
python -m research.nfl_availability_scenarios --input availability-input.json --output availability-report.json

# Compare an existing frozen coherent portfolio using joint draws.
cd web
node -r ./scripts/server-only-stub.cjs --import tsx scripts/compare-nfl-joint-portfolios.ts bank.json portfolio.json joint-report.json
```

Use the archive filename actually present in the checkout for the first command.
Prepared workload input requires `forecasts`, `evidence`, earlier opportunity
`history`, and `decision_at`; optional `coherent_input` supplies the existing
event engine's complete input contract. Optional `workload_risk_input` contains
`cases`, `as_of`, and `forecast_cases`. Every risk forecast must match a current
uncertain player/game and the same decision cutoff. Without a learned limited
factor, 50% is an explicitly unweighted sensitivity, not an estimated injury
probability. Unknown/missing work stays unallocated.

Risk cases require frozen `snapshot_id`, `observation_ids`, a SHA256
`baseline_source_digest`, player/game, position, Q/D designation, positive
baseline opportunities, features-available/decision/kickoff timestamps and
optional depth/recency features. Training cases additionally require played,
actual opportunities, settled-at and labels-available-at timestamps. Current
outcomes are never forecast features. Prepared cases must come from real frozen
pregame captures; today's roster cannot reconstruct old injury decisions.
Historical opportunity rows must assert complete absence coverage and name
their source snapshot. A partial inactive list cannot qualify a transfer.

## Remaining evidence and operational dependencies

1. Integration and deployment are intentionally pending user review. No new
   production behavior can be claimed live from these local tests.
2. Collect qualifying frozen injury/role, absence-history and contest cohorts;
   run these new candidates through the existing promotion process. The new
   availability/risk CLIs accept prepared captures; they do not invent a
   historical designation archive or add an unverified live writer/schedule.
3. Verify the late-status check against the next pregame upload in production,
   including a new OUT after entries are built and an export rejection. Unit
   tests cover cutoff/provenance and the results notice; no live UI check was
   performed during this local-only work.
4. The joint comparison uses the frozen baseline plus supplied shadow entries,
   not an exhaustive candidate space or a contest payout model. Independent
   evaluation draws cannot choose the portfolio. Search completion proves
   constraint preservation, not optimality, ROI or forecast qualification.

No textual integration conflicts were encountered. `actions.ts`,
`nfl-dfs-client.tsx`, `nfl-optimizer.ts`, ownership capability and the research
refresh are shared integration surfaces; reconcile later agent edits there.

Verification: 136 web test scripts passed, one live-database script excluded as
in CI. The full Python run passed 1,705 tests with the two expected opt-in skips;
the later registration test also passed. TypeScript is clean. ESLint reports
zero errors and the existing 63 warnings. Local commit identities are supplied
with the final handoff.
