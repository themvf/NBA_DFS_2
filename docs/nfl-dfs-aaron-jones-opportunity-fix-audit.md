# Aaron Jones opportunity fix — local staged implementation

2026-09-20. Implemented and verified locally against the real saved slate. **Not deployed.** This is the handoff's baseline-retention fallback, not a validated replacement injury allocator or a completed re-simulation release.

## Verified reproduction and root cause

- Refreshed upload: `f067c8b9-f920-4b26-b77c-36f43a2c9df0`, `DKSalaries (3).csv`, 670 players, 2026 week 2, MIN–CHI kickoff `2026-09-20T17:00:00Z`.
- Projection run: `73cd196d-cc66-51f4-be0b-f19f7a98a015`, `nfl-dfs-historical-v3`, as of `2026-09-20T15:16:09.871Z`. Jones DK ID `44132714`, local ID `190`, GSIS `00-0033293`.
- Original upload: `9183e089-2dca-40d1-841f-f19eb300699b`, run `3df228c6-c5f1-5cde-a668-1b6e17419202`. It has no carry inputs and does not reproduce the reported carry increase.
- GitHub deployment `6553861557` reports successful production deployment of `5a8623b348fd7ab341f271d80a5b773451bb793a` at `14:30:24Z`. That source includes QB in rush donors. Running its allocator against the refreshed saved slate reproduces **19.764 points, +13.7581 carries, and +0.6288 receptions**, including the exact four named carry donors.
- Murray is QB/MIN in the salary, model and roster evidence; this was not a QB-to-RB identity-mapping error. His stored availability decision explicitly has `slate_transfer_allowed: false`. The older allocator ignored that decision and included QB rushing in RB inheritance.
- Current production deployment `6554864718` corresponds to `22e08a77fc23dcd9654da76de2d34c35081eb354` (successful status `16:14:02Z`). A fresh browser visit to `https://nbadfs.vercel.app/dfs/nfl` showed Jones at **17.4**, +9.3 carries without Murray, and the incorrect simulated-mean/confidence copy. Thus the reported 19.8 is reproduced by the earlier deployed code; the particular user's browser response/cache history is not available.

All four donors have more than two recorded games. Their roster capture is `2026-09-14T06:19:11.079Z`, older than the 72-hour role gate. The saved DK statuses are Mason IR, Murray OUT, Jennings OUT, Yurosek IR. Current role evidence cannot authorize new promotions here. Raw artifacts contain the stable identities, original statuses, source evidence, roster timestamps, baseline stats and resolved rows.

### Donor-specific reconstruction

These are independent historical averages, **not proven incremental vacated workload**. Legacy Jones gains used his 13.1365 / 13.7165 share of eligible RB carries and 2.2848 / 16.7073 share of eligible receptions.

| Donor | Historical carries | Legacy carries to Jones | Historical receptions | Legacy receptions to Jones |
|---|---:|---:|---:|---:|
| Jordan Mason | 9.7010 | 9.2908 | 0.6822 | 0.0933 |
| Kyler Murray | 4.6190 | 4.4237 | 0.0020 | 0 |
| Jauan Jennings | 0.0435 | 0.0417 | 3.5017 | 0.4789 |
| Ben Yurosek | 0.0020 | 0.0019 | 0.4141 | 0.0566 |

### Before and after

| Metric | Earlier deployed v1 | Current production v2 | Local v3 fallback |
|---|---:|---:|---:|
| Baseline DK points | 11.2802 | 11.2802 | 11.2802 |
| Final DK points | 19.7640 | 17.4083 | 11.2802 |
| Own carries | 13.1365 | 13.1365 | 13.1365 |
| Inherited carries | 13.7581 | 9.3344 | 0 |
| Final carries | 26.8946 | 22.4709 | 13.1365 |
| Own receptions | 2.2848 | 2.2848 | 2.2848 |
| Inherited receptions | 0.6288 | 0.6288 | 0 |
| Final receptions | 2.9136 | 2.9136 | 2.2848 |
| Floor / median / ceiling | mixed/scaled | 8.2893 / 10.9689 / 24.4772, mixed | 5.3713 / 10.9689 / 15.8607, baseline |
| Boom rate | stale baseline | stale baseline | 0.0135, baseline |

The historical salary-row sum is 53.3605 carries, 33.3451 receptions and 95.8695 attempts; after removing DK-absent rows it is still 38.9290 carries, 28.5373 receptions and 65.6335 attempts. Those sums include independent priors and multiple QB histories. They are not a simultaneous team scenario. The fallback prevents **additional** unsupported inflation; it does not claim the baseline itself is a coherent team allocation.

## Workload shares: verified independent scenario

The Model Lab workload surface reads `nfl_dfs_workload_runs`, not the salary-pool historical stat means. Retrieved report `nfl-dfs-workload-v1`, digest `6dbb697d58ee4b31f183acb9b3660b49a23c139c926bfc75b42bbc9f025d4123`, has a 34-player MIN research roster and week-2 forecast as of `2026-09-20T15:18:09.402131+00:00`.

| Unit per team game | Research budget | Assigned | Reserve |
|---|---:|---:|---:|
| Carries | 26.701290 | 26.701290 | 0 |
| Targets | 26.911590 | 26.911590 | 0 |
| Pass attempts | 27.939496 | 27.939496 | 0 |

Reconciliation error is at most `3.6e-15`. Jones has 40.42898% carry share (10.79506 carries) and 12.73635% target share (3.42755 targets). These are research allocations, not the baseline's 13.1365 carries / 2.2848 receptions. The denominator is the full modeled roster's team/game budget; selection and salary-table filters do not change it. The UI now states scenario, unit, denominator and reserve, and explicitly says this report does not describe historical optimizer projections. No research model was promoted.

## Implementation

- `opportunity-redistribution.ts`: version `nfl-dfs-redistribution-v3-budget-required`. Non-QB donor history is withheld with per-donor stable keys and units, never represented as an established offered pool or unassigned budget. Baselines remain unchanged. Existing QB promotion, upstream rejection, history guard and cap accounting remain; conflicting QB1 donors cannot combine workloads.
- `resolved-projection.ts` and `actions.ts`: one shared pool/drawer/optimizer scenario, including resolved stats. Web and upstream availability estimates suppress median, floor, ceiling and boom metrics because no new draws exist. OUT rows also zero the resolved stat line. Optimizer snapshots record scenario and redistribution version; original model rows and lineup audits remain untouched.
- `player-explanation-panel.tsx`: baseline versus adjusted-estimate copy, history-support labeling, explicit unresolved-absence notice, and corrected stat attribution. Upstream injury point deltas get their own waterfall step.
- `nfl-dfs-client.tsx`: separate unresolved-history panel with donor amounts; no fabricated workload conservation claim.
- `nfl-optimizer.ts`: explicitly discloses existing 0.74×/1.28× search heuristics when scenario tails are unavailable. Missing boom rates provide no boom bonus.
- `research-stages.tsx`: explicit research scenario and denominator/units/reserve labeling.

## Verification

- Focused deterministic captured-Jones fixture: unsupported overlap, QB exclusion and promotion, already-handled/rejected donors, no-history recipients, cap accounting, conflicting depth evidence, non-reduction of QB1, order/filter independence, shared stats/metrics, immutability, and threshold-bonus expected scoring.
- 16 TypeScript suites passed: opportunity redistribution, scoring, OUT projections, availability, game availability, projection audit, refresh/settings contract, stale run, slate persistence, DFS workspace, workload, workload optimizer, cash optimizer, position workload, calibrated source, saved workspace.
- 84 Python tests passed across availability, projection availability, workload, workload ranges, team context and shadow suites. Python model code and the research study pin were not changed.
- `npx tsc --noEmit --pretty false` and `npm run build` passed. Initial fixture typing issue was corrected before the successful build.
- Explicit refresh of the original upload reused `f067c8b9-...`, matched all 670 players, preserved salaries/comparison data, and left the original snapshot unchanged. Before/after captures also compare equal for upload/run metadata, salary rows and immutable model projections.
- Read-only audit executes a local one-lineup cash solve with Jones locked: optimizer, pool and drawer all use **11.2802**. No optimizer run or lineup was persisted by this verification.
- Local production-build browser at port 3012: pool and drawer show **11.3**, 13.1 carries, 2.3 receptions, baseline distribution, history-support clarification and unresolved-absence notice. The spurious +6.2 bonus/distribution attribution is now +0.1. Filtering to Jones preserves his slate-wide value/rank assessment.

Reproduce the read-only capture (from `web/`):

```powershell
node --env-file=.env.local -r ./scripts/server-only-stub.cjs --import tsx ./scripts/audit-nfl-opportunity.ts f067c8b9-f920-4b26-b77c-36f43a2c9df0 ../artifacts/nfl-jones-after.json
node --import tsx ./scripts/test-nfl-opportunity-redistribution.ts
```

Evidence: `artifacts/nfl-jones-before.json`, `artifacts/nfl-jones-after.json`, `artifacts/nfl-jones-original-slate.json`, `artifacts/nfl-jones-legacy-before.json`, `artifacts/nfl-jones-legacy-original-slate.json`, and `web/scripts/fixtures/nfl-jones-opportunity.json`. No credentials are in these artifacts.

## Remaining release gates

This completes the safe baseline-retention stage, **not all original acceptance criteria**. The historical baseline still has no certified team allocation. A common injury-adjusted budget/share/points scenario, per-draw re-simulation and held-out injury calibration remain unimplemented. The saved volume/share study reports **zero historical pregame availability observations** and explicitly excludes target-week injury information from its retrospective replay. It cannot validate an absence allocator. New point-in-time role/availability evidence and the existing research release gates are required before activating one.

No deployment was performed. Production still has the observed v2 non-QB uplift and old copy until this local patch is released. Existing saved lineup results intentionally keep their original scores.
