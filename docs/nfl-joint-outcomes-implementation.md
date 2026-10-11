# Local joint NFL outcome implementation

## Current scope

The local candidate implements aligned game production draws, weighted individual
ranges and leader shares, exact unordered top-three probabilities with fractional
boundary ties, game-market volume fitting, pooled injury-exit fitting, remaining
opportunity redistribution, role/depth-conditioned empirical gains, paired-quote
normalization, guarded entropy reweighting, historical scoring, and registered
evaluation. The complete DFS candidate can reuse exit allocation and empirical
gain profiles while retaining its canonical event ledger. Its completed catches
follow the QB/receiver ledger, so its marginals differ from the partial engine.

Local file bundles are immutable and verified with both file and content digests.
The page `/nfl/game-model` displays a frozen summary; it does not change optimizer
points. The existing fixed-dispersion baseline and original registered engines
remain intact. `pytest.ini` limits discovery to tests instead of artifact caches.

This is executable research code, not a passed predictive qualification. Unknown
roles, retrospective evidence, non-sequential field position, and unallocated
complete-DFS contributors remain explicit. An accounting OTHER pool never competes
as a single player. Complete-DFS leader views are scoped to named modeled players,
not advertised as full-field sportsbook probabilities.

## Modules

- `model/nfl_joint_contracts.py`: canonical banks, evidence boundaries, weights,
  signed official credit, and weighted quantiles.
- `model/nfl_joint_outcomes.py`: baseline adapter, conservative play-feature joins,
  fitted gain/role candidate, common catch context and source/fit digests.
- `model/nfl_gain_distribution.py`: pooled role/depth gain sampling and cached
  empirical per-touch gains for complete DFS.
- `model/nfl_midgame_exits.py`: sourced adjudicated injury exposure, segment hazard,
  temporary absence and return, and role-conditioned redistribution.
- `model/nfl_full_exit_allocation.py`: remaining-work allocation for attempts,
  carries, and targets inside complete event simulations.
- `model/nfl_opportunity_process.py`: fitted game-market volume conditioning and
  opportunity-conserving segment allocation.
- `model/nfl_market_reweighting.py`: price conversion, paired power/normalization
  methods, integer-push conditions, soft/hard KL reweighting and ESS fallback.
- `model/nfl_joint_decisions.py`: ranges, individual leader credit, exact-set
  distributions, and portfolio return scenarios under explicit dead-heat rules.
- `model/nfl_joint_evaluation.py`: CRPS, intervals, leader and exact-set proper
  scores, paired game bootstrap, and study validation.
- `model/nfl_joint_full_dfs.py`: complete-bank generation, identical-scenario
  production views and complete-bank weight transfer.
- `model/nfl_ownership_model.py`: regularized, sourced marginal ownership fits;
  not a legal contest-field generator.
- `research/nfl_alt_capture.py`: explicit-budget dry-run/capture, raw-first local
  evidence, all ladder rungs, bookmaker/timestamp pairing, and quota reporting.
- `research/nfl_joint_store.py`: exclusive immutable bundles and tamper checks.
- `research/nfl_joint_outcomes.py`: executable CLI for these workflows.
- `web/src/lib/nfl-dfs/joint-contest.ts`: canonical legal-lineup scoring and
  payout sharing for supplied fields, including duplicate entries.

## Reproduce a local forecast

From the worktree root, use new output filenames; existing outputs cannot be
overwritten. First inspect source coverage and freeze a decision-time request:

```powershell
python -m research.nfl_joint_outcomes audit-sources --input CAPTURE.json.gz --output COVERAGE.json
python -m research.nfl_joint_outcomes fit --input CAPTURE.json.gz --decision-at CUTOFF --output FIT.json.gz
python -m research.nfl_joint_outcomes forecast --input CAPTURE.json.gz --request REQUEST.json --fit FIT.json.gz --draws 5000 --output FORECAST.json.gz
python -m research.nfl_joint_outcomes publish --input FORECAST.json.gz --output NEW_PAGE_SUMMARY.json
```

The request must match the canonical schedule. The CLI attaches canonical expected
recent game IDs and preserves the existing coverage gate. Use `--retrospective`
explicitly for later source corrections; it never backdates captures. A baseline
forecast is the same command without `--fit`. Provider-unverified availability
remains unverified in the report.

The fit fingerprint covers fitted content and implementation dependencies.
Changing an implementation requires a new fit artifact. Source enrichment is
conservative because existing prepared events omit play IDs: matched role/credit
event sequences retain source play IDs; ambiguous matches have unknown depth.
No name-only or arbitrary feature join is performed.

The example is the frozen 2026-10-08 TB/DAL request, replayed locally. It does not
refresh game-day availability. Original scratchpad `ladder.py` and its exact inputs
are still required to reproduce the reported original 1.1% estimate.

## Exits and game markets

```powershell
python -m research.nfl_joint_outcomes fit-exits --input ADJUDICATED_EXITS.json --decision-at CUTOFF --output EXITS.json
python -m research.nfl_joint_outcomes fit --input CAPTURE.json.gz --decision-at CUTOFF --exit-fit EXITS.json --output CONDITIONAL_FIT.json.gz
python -m research.nfl_joint_outcomes fit-volume --input HISTORICAL_GAME_MARKETS.json --decision-at CUTOFF --output VOLUME.json
python -m research.nfl_joint_outcomes forecast --input CAPTURE.json.gz --request REQUEST.json --fit CONDITIONAL_FIT.json.gz --market GAME_MARKET.json --volume-fit VOLUME.json --output CONDITIONAL_FORECAST.json.gz
```

Exit observations need canonical game/player, role, adjudicated confidence,
source reference, game end, at-risk segments, reason, exit segment and optional
return segment, and replacement counts by role/action. Future or unsourced labels
are rejected. Non-exit role fitting excludes documented injury games to avoid
simply adding injury variance twice. Missing replacement evidence allocates to an
unresolved recipient; it does not automatically reward another named receiver.
Unknown newcomers use a disclosed pooled exit hazard. The current hazard fits
one injury/return episode per player-game; recurrent or interval-censored episodes
need additional adjudicated data and an expanded label contract.

Game-market fitting needs at least 20 prior paired game records with eligible
market evidence and both teams' counts. It fits ridge log-volume coefficients;
there is no invented spread-to-passing multiplier. Paired historical fluctuations
remain shared. Segment opportunities are uniform allocations of aggregate volume,
not a sequential drive/clock simulator. Detailed QB, line, weather and coverage
coefficients are not inferred from missing fields.

## Player market conditioning

```powershell
python -m research.nfl_joint_outcomes constraints --input NORMALIZED_QUOTES.json --metric-map MARKET_MAP.json --decision-at CUTOFF --output CONSTRAINTS.json
python -m research.nfl_joint_outcomes reweight --input FORECAST.json.gz --constraints CONSTRAINTS.json --output WEIGHTED.json.gz
```

`MARKET_MAP.json` explicitly maps actually supported provider keys to receptions,
receiving_yards, or rushing_yards. Quotes preserve every book/line/side/timestamp.
One-sided lines are retained but not assigned invented fair probabilities. Integer
line over/under pairs condition on no push, and the solver checks the achieved
conditional probability. Explicit one-sided research constraints require their
margin assumption. No quotes for a different game or after the cutoff are allowed.

Soft KL is default. Hard mode additionally checks feasibility. Failed convergence,
constraint tolerances, undefined conditional events or ESS below 10% retain original
weights and a rejection reason. ESS below 25% warns. These safeguards are numerical
design choices, not predictive calibration. Matching the constraints does not
identify unquoted tails or prove an edge against the same prices.

## Immutable odds capture

```powershell
python -m research.nfl_alt_capture --events EVENTS.json --markets SUPPORTED_KEYS --books BOOK_KEYS --decision-at CUTOFF --output PLAN.json
```

This performs no network calls. `EVENTS.json` contains provider events and optional
verified canonical event/player mappings. To capture, supply `--apply`, an explicit
`--credit-budget`, and a new output directory. Market/book keys and quota must be
verified before spending. The pilot accepts at most ten books, retains raw payloads
before normalization, stops on provider/quota failures, and skips passed kickoffs.
No scheduled production workflow or paid historical purchase is enabled by default.

## Evaluation and bundles

```powershell
python -m research.nfl_joint_outcomes register --input STUDY.json --output REGISTERED.json
python -m research.nfl_joint_outcomes evaluate --input CAPTURE.json.gz --requests REQUESTS_BY_GAME.json --study REGISTERED.json --population development --output REVIEW.json
python -m research.nfl_joint_outcomes grade --input FORECAST.json.gz --actuals COMPLETE_PLAYER_BOXES.json --output GRADE.json
python -m research.nfl_joint_outcomes audit-thursday --input FORECAST.json.gz --trio GSIS1,GSIS2,GSIS3 --metric total_yards --output THURSDAY_COMPARISON.json
```

The evaluation CLI executes the registered baseline/role-gains comparison. Other
components have standalone fitting and forecast paths; additional registered
ablation grids must be added deliberately, not selected by whichever replay wins.
The supplied study is a development template with empty game populations, not a
pretend locked test. An audited exposure registry, populated sample and registered
precision minimum are required for locked execution. Opening a locked CLI run
writes an exclusive marker beside its registration. Reports do not auto-promote.

Labels with an unresolved zero-stat boundary field cannot receive a definitive
exact-set score. Full-field boxes remain required; unmodeled winners receive zero
represented probability, with support failure disclosed. Numerical log floors are
reporting devices. Historical evaluation uses an explicit retrospective mode and
conservative eight-hour completion eligibility; source reconstruction limitations
remain in its output. Player-level calibration alone is not a joint decision gate.

To write a bundle, input is a mapping of artifact filenames to their JSON payloads:

```powershell
python -m research.nfl_joint_outcomes bundle --input PAYLOADS.json --study-id STUDY --run-id RUN --output artifacts/nfl-joint-outcomes/BUNDLE_RECEIPT.json
```

The store creates an exclusive study/run directory, data files, and manifest with
digests. `verify_bundle()` rejects modified content or unsafe artifact references.

## Complete DFS and contest returns

```powershell
python -m research.nfl_joint_outcomes complete-dfs --input FROZEN_FULL_ENGINE_INPUTS.json --games CANONICAL_GAMES.json --fit FIT.json.gz --output COMPLETE.json.gz
python -m research.nfl_joint_outcomes fit-ownership --input SOURCED_OWNERSHIP_ROWS.json --decision-at CUTOFF --output OWNERSHIP_FIT.json
python -m research.nfl_joint_outcomes ownership --input PLAYER_FEATURES.json --fit OWNERSHIP_FIT.json --decision-at CUTOFF --output OWNERSHIP.json
```

Full engine inputs are the frozen `build_coherent_banks` keyword inputs documented
in the existing shared-simulation guide. The companion production bank derives
from exactly those event scenarios, not separately sampled partial draws. Complete
and production banks must have identical scenario IDs/order to transfer weights.
Complete DFS count/catch, TD, turnover and scoring conservation remain enforced.
Signed empirical rushing/receiving gains are retained in the candidate.

The TypeScript contest consumer scores supplied legal lineups and field entries,
then divides prizes for tied occupied places. Marginal ownership predictions do
not create a realistic field by themselves. Full DK data, site eligibility,
contest payouts, and an independently evaluated field model remain prerequisites
for claiming economic value. Exact top-three contests are not conventional DFS.

## Verification and remaining evidence

Meaningful checks cover source/identity/time boundaries, ties, signed credit,
weighted ranges, reweight feasibility/ESS, integer pushes, roster support, exits,
complete-event conservation, immutable bundles, and contest duplication. Existing
leader and canonical DFS tests run alongside the new tests. See the local artifact
verification report for the exact final counts and generated-file digests.

Missing original ladder inputs, adjudicated historical exit datasets, historical
matched NFL ladders, actual ownership labels, and unexamined/forward outcomes
cannot be replaced with synthetic training data. The captured history currently
covers 2023-26; the 2020-22 expansion must pass the existing source audit before
fitting it. Existing quarantined primary-source discrepancies remain excluded;
there is no fabricated stat-credit repair or relaxed recent-history gate.
