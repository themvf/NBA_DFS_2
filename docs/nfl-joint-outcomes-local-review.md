# Joint NFL outcome candidate: local completion review

## Implemented functionality

The candidate builds shared player production draws for receptions, receiving
yards, rushing yards and total yards. It computes individual leader credit and
exact unordered top-three sets directly from those draws. Opportunity shares
conserve team targets and carries. Gain sampling uses reconciled signed gains,
target-depth buckets, catch context, sparse-player pooling and opponent
adjustments. Field position is historical sampled context, not a drive simulator.

Adjudicated exit labels can fit pooled segment hazards, absence duration and
replacement-role weights. Those fits exclude labeled injury games from role
dispersion and role-center estimation. Redistribution applies to remaining
opportunities and can feed both the partial production candidate and the
complete DFS candidate. Both consumers verify fit content, implementation and
decision boundaries. The first candidate supports one injury absence and return
per player, rather than recurrent or interval-censored injury processes.

Matched timestamped alternate prices produce explicit constraints. Soft/hard
relative-entropy reweighting has support, feasibility, residual and effective
sample-size checks. Failed conditioning retains the parent weights and records
the reason. One-sided prices require an explicit margin assumption. Paid capture
is disabled by default; raw responses, ladder rungs and quota records are
preserved, including provider, network and normalization failures.

Complete DFS production views retain identical event-scenario IDs and order.
The canonical scorer computes legal supplied-field lineup outcomes and splits
prizes across tied finishing places, including duplicate entries. Ownership
fitting predicts marginal participation only; it does not manufacture a realistic
contest field. Unallocated production never competes as a fictional player.

The local CLI supports source audit, fitting, forecast, constraints, conditioning,
grading, study registration, baseline comparison, ownership and immutable bundles.
The historical comparison currently executes the registered baseline/role-gains
variant; exit, market and ownership incremental studies need sourced inputs and
separately registered comparisons. It does not automatically qualify a model.

## Verification

- Python: 1,767 passed, two skipped, no failures or errors.
- TypeScript type check, new joint-contest regression, existing shared-game
  regression and lint for changed TypeScript passed.
- Local game-model route returned HTTP 200. Its rendered content showed 5,000
  scenarios, unresolved availability, disabled exits, and a separate earlier
  baseline example.
- A frozen TB/DAL request was replayed with the fitted role/gain candidate.
  Replay results and the current source audit are preserved locally.
- Immutable bundle verification checked file and content digests.

These checks establish code mechanics and reproducibility. They do not establish
tail calibration, reliable leader probabilities or profitable betting/DFS edges.

## Stage status and evidence still required

| Plan stage | Local result | Remaining acceptance work |
| --- | --- | --- |
| A: audit and registration | Comparison tool, artifact digests and development study template implemented | Recover original ladder script/prices/output; audit prior exposure; populate and freeze eligible study games |
| B: capture and historical audit | Budgeted dry-run/capture and current-source audit implemented; zero paid credits used | Verify provider coverage under an authorized pilot; capture/reconcile 2020-22; audit historical availability coverage |
| C: opportunity and gains | Fitted candidate, aligned exports and meaningful mechanical tests pass | Execute registered chronological development comparisons and role/tail diagnostics |
| D: exits | Label-driven fit and two-engine redistribution implemented and tested | Supply adjudicated historical exposure/exit labels; audit coverage; run registered incremental ablation |
| E: market conditioning | Quote constraints, guarded solver and fallback tests pass | Capture matched real ladders; evaluate held-out rungs and joint decision scores |
| F: locked and forward evaluation | Registration guards, grades and paired-game comparison tools implemented | Audit exposure, freeze a genuine untouched/forward population and precision plan, execute gates and forward settlement |
| G: DFS and presentation | Complete-event bridge, weight transfer, supplied-field payouts, marginal ownership fitter and local summary page implemented | Fit ownership on sourced labels, evaluate realistic supplied fields and economic returns; no optimizer promotion |

The supplied study remains a development template with empty populations. There
is no locked/forward predictive result or claimed economic edge. Availability is
unverified in the frozen replay, and exits and market conditioning are disabled
there because their required datasets have not been supplied.

## Recovery and operating instructions

Use `docs/nfl-joint-outcomes-implementation.md` for runnable commands and input
contracts. `docs/nfl-joint-outcomes-verification-final.json` records exact digests,
artifact paths and test counts. The large source, fit and draw files remain in
this local worktree; they have not been remotely archived. Copy and verify those
artifacts before removing or archiving the checkout.

Use new output filenames for every run. The local page publishes a frozen
summary and requires an explicit new publication to change it. Existing protected
baselines and optimizer projections were not replaced. No push, production
deployment, database write or paid provider request was performed.

For this Windows worktree's linked dependencies, preview with:

```powershell
cd web
npm run dev -- --webpack --hostname 127.0.0.1 --port 3022
```

Then open `/nfl/game-model`. The default Turbopack preview cannot resolve the
external dependency junction in this worktree; Webpack was verified successfully.
