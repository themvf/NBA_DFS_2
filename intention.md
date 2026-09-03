# Intention: Build Historical, Team, and Roster Signals for the CFB Line Terminal

**Status:** Implemented 2026-09-03; historical hydration remains gated by the generated coverage audit
**Companion specification:** [`docs/cfb-historical-team-roster-signal-spec.md`](docs/cfb-historical-team-roster-signal-spec.md)
**Existing terminal specification:** [`docs/cfb-live-data-terminal-spec.md`](docs/cfb-live-data-terminal-spec.md)

## Intent

Build a point-in-time research layer for the College Football Line Terminal that can:

1. Answer historical questions such as “How often do non-neutral home teams win or cover when favored by exactly 14.5 points?”
2. Show team- and coaching-regime-specific results without overstating small samples.
3. Accumulate current-season team and roster evidence as games are played.
4. Test preregistered hypotheses with walk-forward, holdout, and prospective evidence.
5. Keep descriptive historical context visibly separate from validated signals and live line movement.

The first useful deliverable is not a predictive model. It is a trustworthy, reproducible historical cohort card with clear provenance. Team, roster, and predictive layers are added only after that foundation is correct.

## Non-Negotiable Invariants

- Do not modify or reinterpret the append-only `game_odds_history` ledger to manufacture historical movement.
- Do not label a historical reference line as an opener or close unless the source explicitly provides that designation.
- Do not use information that became available after a game’s signal snapshot.
- Do not join a current roster or end-of-season rating to an earlier game.
- Do not present raw team ATS records without sample size, interval, and a shrunk estimate.
- Do not allow a backtest-only result to affect alert ranking or a composite score.
- Do not promote a signal by inspecting and tuning against the same holdout.
- Do not let history, roster, or research failures interrupt live odds capture or the existing CFB board.
- Preserve provider-level lines and source payload provenance.
- Make ingestion and feature generation idempotent and resumable.

## Existing Seams to Reuse

The current project already provides the necessary live-data foundation:

- `ingest/cfb_schedule.py` owns CFBD schedule identity and current exact-book ingestion.
- `db/schema.py` contains the PostgreSQL bootstrap/migration statements.
- `db/queries.py` contains Python persistence helpers.
- `web/src/db/schema.ts` mirrors database tables for Drizzle.
- `web/src/db/queries.ts` supplies server-side dashboard queries.
- `web/src/app/cfb/page.tsx` loads the CFB board and degrades gracefully on query failure.
- `web/src/app/cfb/cfb-terminal-client.tsx` renders the interactive terminal.
- `web/src/app/cfb/cfb-terminal.module.css` owns terminal presentation.
- `tests/test_cfb_schedule.py` demonstrates the existing CFB unit-test style.

Do not turn `ingest/cfb_schedule.py` into a monolith. Historical backfill, feature construction, and hypothesis evaluation should be separate modules with narrow contracts.

## Delivery Strategy

Build this as six independently verifiable vertical slices:

```text
0. Source audit
   ↓
1. Historical games and reference lines
   ↓
2. Cohort engine and HISTORY dashboard card
   ↓
3. Team/staff regime context with partial pooling
   ↓
4. Point-in-time season and roster snapshots
   ↓
5. Hypothesis registry and prospective shadow validation
```

Each slice must have a database contract, deterministic tests, a data-quality report, and a UI fallback before the next slice begins.

## Slice 0 — Source and Coverage Audit

### Purpose

Determine what historical and roster data is actually available before committing the schema or making analytical claims.

### Build

Create:

```text
ingest/cfb_history.py
tests/test_cfb_history.py
artifacts/cfb/history-audits/
```

The first version of `ingest/cfb_history.py` should support an audit-only mode:

```powershell
python -m ingest.cfb_history --start-season 2016 --end-season 2025 --audit-only
```

The audit fetcher should:

- Use `CFBD_API_KEY` through the same bearer-token convention as `cfb_schedule.py`.
- Fetch one season at a time and cache raw responses locally so reruns do not spend quota unnecessarily.
- Apply retry/backoff to transient network and rate-limit responses.
- Never log the API key or authenticated headers.
- Enumerate games, scores, classifications, neutral-site flags, season types, providers, spreads, totals, moneylines, and any source line designation.
- Record payload hashes so identical reruns can be recognized.
- Produce JSON and human-readable Markdown audit artifacts by season.

The report must contain:

- All games and FBS-versus-FBS games.
- Completed-game count.
- Spread/total/moneyline/price coverage.
- Provider coverage and continuity.
- Missing team IDs and failed canonical mappings.
- Duplicate source game IDs.
- Neutral-site and postseason completeness.
- Whether the provider exposes verified opener/close semantics.

### Stop condition

Do not start a backtest if:

- Result completeness is below 98% for the selected population.
- Team mapping failures exceed 0.5%.
- The source’s line timing/designation cannot be described accurately.
- Licensing or retention terms do not permit the planned storage.

Roster, injury, depth-chart, transfer, returning-production, and staff sources require a separate audit. It is acceptable for Slice 4 to remain blocked while Slices 1–3 proceed.

## Slice 1 — Historical Storage and Backfill

### Schema work

Add `cfb_historical_game_lines` in both:

- `db/schema.py`
- `web/src/db/schema.ts`

Follow the project’s existing idempotent `CREATE TABLE IF NOT EXISTS`, `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, and index conventions. Do not create a second representation of teams or games: historical rows must reference the existing `cfb_matchups` and `cfb_teams` identities.

Required historical-line fields are defined in the companion specification. At minimum preserve:

- `game_id`
- `provider`
- `market_type`
- home/away values and prices
- `line_designation`
- source event/update timestamps
- `available_at` when known
- `captured_at`
- `raw_payload_hash`
- `is_canonical_reference`

Add uniqueness that describes source identity, not a guessed consensus. A safe initial key is based on game, provider, market, designation, source update time, and value fields. Verify the exact rule against the audit before migration.

### Persistence work

Add focused helpers to `db/queries.py`:

```text
upsert_cfb_historical_game(...)
insert_cfb_historical_line(...)
mark_cfb_canonical_reference(...)
get_cfb_history_coverage(...)
```

`upsert_cfb_historical_game` should reuse CFBD game identity and the existing team alias/canonicalization path. It must not create a duplicate matchup merely because its date, venue, or status changed.

### Backfill work

Extend `ingest/cfb_history.py` with resumable ingestion:

```powershell
python -m ingest.cfb_history --season 2016
python -m ingest.cfb_history --start-season 2016 --end-season 2025
```

Process one season in a transaction-sized batch, log counts, and make a second identical run produce no new logical records.

Canonical reference selection occurs after provider rows are stored:

1. Explicit verified close, when truly supplied.
2. Configured continuous provider priority.
3. Median provider reference.

The canonical reference is for cohort classification only. Preserve all source rows.

### Tests

Add fixtures for:

- Multiple books for the same game.
- Even and odd provider counts.
- Missing prices.
- Neutral-site and FBS-versus-FCS games.
- Rescheduled games.
- Duplicate payloads.
- Historical reference lines without timestamps.
- A source-labeled close.

### Exit criteria

- 2016–2025 backfill completes or has an approved reduced range based on the coverage audit.
- Rerunning the backfill is idempotent.
- A database coverage query reconciles to the stored audit.
- No historical row is written into `game_odds_history`.

## Slice 2 — Cohort Engine and First Dashboard Release

### Computation module

Create:

```text
model/cfb_historical_signals.py
tests/test_cfb_historical_signals.py
```

Keep outcome math in pure functions so it can be tested without a database:

```text
home_margin = home_score - away_score
home_win    = home_margin > 0
home_cover  = home_margin + home_spread > 0
push        = home_margin + home_spread == 0
```

Implement:

- Favorite orientation for home and away favorites.
- Exact-line filtering.
- Versioned spread buckets.
- SU and ATS W/L/P counts.
- Cover percentage excluding pushes.
- Wilson 95% confidence intervals.
- Date/season, venue, classification, conference, and postseason filters.
- ROI only from stored price; optional assumed-price scenarios must be labeled.

Store bucket definitions as versioned code or configuration. Do not accept arbitrary UI-created buckets in the first release because that encourages unregistered slicing.

### Server query

Add typed result contracts and queries to `web/src/db/queries.ts`:

```text
CfbHistoricalCohort
getCfbHistoricalCohort(gameId, market, definitionVersion)
getCfbComparableGames(gameId, definitionVersion, limit)
```

The result must include definition version, as-of time, population description, seasons, source/line designation, `n`, SU, ATS, intervals, price coverage, and provenance.

Precompute common national spread buckets only after profiling the direct query. If necessary, add a materialized view or aggregate table, but preserve a query path that can reproduce the aggregate from source rows.

### UI

Modify:

- `web/src/app/cfb/page.tsx`
- `web/src/app/cfb/cfb-terminal-client.tsx`
- `web/src/app/cfb/cfb-terminal.module.css`

Add a `HISTORY` market tab or selected-game subpanel. The first release shows:

- Exact spread cohort.
- Registered spread bucket cohort.
- SU and ATS records.
- Sample size and Wilson interval.
- Seasons covered.
- Historical reference/verified-close label.
- A small comparable-games list.
- `HISTORICAL CONTEXT — NOT A VALIDATED EDGE` banner.

Load history independently from the main market board. A history query failure should render a local unavailable state, not replace the live board.

### Exit criteria

- The selected game can answer the exact home `-14.5` example and its `-14.0 through -16.5` cohort.
- Every displayed percentage has `n`, period, and provenance.
- No placeholder or fabricated statistics appear.
- Existing CFB live-board tests and production build still pass.

## Slice 3 — Team and Staff-Regime Context

### Schema

Add `cfb_staff_regimes` to the Python and Drizzle schemas with head coach, offensive coordinator, and defensive coordinator roles. Store start/end season and week, source, and `available_at`.

Do not infer historical coordinator assignments from a current staff page. Backfill only from a source that can establish the applicable dates.

### Model

Extend `model/cfb_historical_signals.py` or create `model/cfb_team_context.py` if the module becomes large.

Implement the fixed fallback hierarchy:

```text
team × exact line × venue × current regime
team × spread bucket × venue × current regime
team × spread bucket × venue across recent regimes
conference/archetype × spread bucket × venue
national FBS cohort × spread bucket × venue
```

Return every level used, not only the first result. Apply partial pooling:

```text
shrunk_rate = (team_n * team_rate + prior_strength * cohort_rate)
              / (team_n + prior_strength)
```

Start with `prior_strength = 20`, version it, and expose it in result metadata. Show raw and shrunk values together.

### Query and UI

Add:

```text
CfbTeamHistoricalContext
getCfbTeamHistoricalContext(gameId, teamId, definitionVersion)
```

The team card must show:

- Exact team/regime result when available.
- Bucket result.
- Raw rate, shrunk rate, cohort prior, and sample.
- Staff regime and fallback level.
- Reliability label that describes sample size, not predictive validity.

### Exit criteria

- A two-game 2–0 team record is visibly shrunk toward the cohort prior.
- Regime-boundary tests prevent an old coach’s games from appearing in the current-regime count.
- Neutral-site and away-team contexts orient correctly.

## Slice 4 — Point-in-Time Team and Roster Features

### Schema

Add:

- `cfb_team_game_features`
- `cfb_roster_snapshots`
- `cfb_roster_players`

Add the tables to `db/schema.py` and `web/src/db/schema.ts`. Enforce one feature row per game/team/feature version. Roster membership must reference a snapshot, never a mutable season-only row.

### Ingestion modules

Create only after the source audit approves the provider contracts:

```text
ingest/cfb_rosters.py
ingest/cfb_staff.py
model/cfb_team_features.py
tests/test_cfb_team_features.py
tests/test_cfb_rosters.py
```

Every source record must retain:

- Source/provider.
- Source entity ID.
- `source_updated_at` when supplied.
- `available_at` — when the application could legitimately know it.
- `captured_at` — when this system stored it.
- Payload hash.
- Completeness/confidence.

### Feature generation

For each scheduled game, generate both teams’ features using only records with:

```text
available_at <= feature_snapshot_at < kickoff_at
```

Initial preseason features:

- Regressed prior-year opponent-adjusted rating.
- Multi-year program rating.
- Head-coach and coordinator continuity.
- Returning production.
- Returning quarterback and experience.
- Offensive-line returning starts/snaps.
- Position-group continuity.
- Transfer additions/losses and weighted prior production.
- Talent composite.

Initial current-season features:

- Effective games and data completeness.
- Opponent-adjusted offense, defense, and overall performance.
- Success/explosiveness and play/drive efficiency when licensed.
- Pace.
- Havoc/pressure/sack rates.
- Red-zone rates.
- Turnover regression indicators.
- Rest/travel flags.
- Two-deep availability and weighted starter absences when reliable.

Use the versioned early-season blend:

```text
current_weight = effective_games / (effective_games + 4)
blended = current_weight * current_feature
          + (1 - current_weight) * preseason_prior
```

Missing values remain missing and reduce confidence. They do not become zero.

### UI

Add two independent HISTORY sections:

- `SEASON TO DATE`: effective games, current/prior weighting, opponent-adjusted indicators.
- `ROSTER CONTINUITY`: QB, OL, returning production, transfers, injuries/availability, snapshot time, confidence.

The panel should be useful in Week 1 by explicitly explaining that the prior owns nearly all weight. As games accumulate, display the changing weights rather than switching abruptly to current-season data.

### Exit criteria

- A post-kickoff roster report cannot change that game’s stored feature snapshot.
- Rebuilding a feature version reproduces the same row from the same source snapshots.
- A final-season retrospective rating is rejected as a contemporaneous feature.
- The dashboard remains functional when all roster features are missing.

## Slice 5 — Hypothesis Registry and Prospective Validation

### Schema

Add:

- `cfb_hypotheses`
- `cfb_hypothesis_results`
- `cfb_game_signal_snapshots`

Implement immutable hypothesis versions. Editing a frozen definition creates a new version; it does not mutate the evaluated one.

### Research module

Create:

```text
research/__init__.py
research/cfb_hypotheses.py
tests/test_cfb_hypotheses.py
```

Support:

```powershell
python -m research.cfb_hypotheses evaluate CFB-H001 --walk-forward
python -m research.cfb_hypotheses snapshot-qualified --date 2026-09-05
python -m research.cfb_hypotheses settle --through-date 2026-09-05
```

The evaluator must:

- Load only a frozen hypothesis version.
- Enforce chronological expanding-window splits.
- Keep the final holdout untouched until the definition is frozen.
- Record W/L/P, effect, interval, ROI when priced, CLV when meaningful, p-value/q-value when used, data version, and code version.
- Track related slices as one declared multiple-testing family.
- Save result payload hashes for reproducibility.

The snapshot job writes candidate signals before kickoff. Settlement joins them later to final results and qualified closing lines. Never recreate a missing pregame snapshot after the result is known.

### Status enforcement

Implement the state machine:

```text
PROPOSED
→ PREREGISTERED
→ BACKTESTED
→ HOLDOUT PASSED
→ PROSPECTIVE SHADOW
→ VALIDATED SIGNAL
→ RETIRED
```

The service layer, not client-side code, decides whether a signal is validated. Only validated signals may be passed into alert ranking, and that integration should be a separate, explicitly reviewed change after prospective requirements are met.

### Exit criteria

- A changed hypothesis definition cannot reuse old results under the same version.
- Walk-forward tests prove no future game enters training.
- Prospective snapshots exist before kickoff.
- Backtest-only cards cannot affect alert order.
- Promotion requirements are machine-checked and auditable.

## Operational Cadence

### Preseason

- Refresh teams, conferences, schedules, staff regimes, roster snapshots, transfers, and preseason priors.
- Freeze feature and bucket versions intended for the season.
- Register candidate hypotheses before looking at new-season outcomes.

### Weekly

- Refresh completed games and scores.
- Capture roster/depth/injury snapshots as sources update.
- Recompute upcoming-game point-in-time features under the same feature version.
- Write qualified hypothesis snapshots before kickoff.
- Settle prior games after final scores and qualified closes are available.
- Publish data-quality and missingness metrics.

### In-season model changes

- Never overwrite the model/feature version that produced an existing signal.
- Introduce a new version and run it in parallel.
- Do not apply retrospective corrections to the displayed historical record without preserving the original and labeling the correction.

## Testing and Verification Commands

Run the narrow tests after each slice, then the broader checks:

```powershell
python -m pytest tests/test_cfb_schedule.py
python -m pytest tests/test_cfb_history.py
python -m pytest tests/test_cfb_historical_signals.py
python -m pytest tests/test_cfb_team_features.py tests/test_cfb_rosters.py
python -m pytest tests/test_cfb_hypotheses.py
```

From `web/`:

```powershell
npm run lint
npm run build
```

Add a purpose-built TypeScript fixture test for the history response adapter if calculation or fallback logic exists in TypeScript. Statistical calculations should preferably stay in one tested layer rather than being reimplemented differently in Python and the browser.

Before deploying a schema slice:

1. Run `git diff --check`.
2. Review generated SQL or idempotent statements.
3. Apply against a non-production database or rollback-capable branch when available.
4. Run the slice’s reconciliation query.
5. Verify the existing `/cfb` board still loads with historical tables empty.
6. Backfill one season and verify counts before starting the full range.

## Observability

Add structured logs and a small health query for:

- Last successful historical season ingested.
- Rows inserted, updated, skipped, and quarantined.
- API request and quota use.
- Team mapping failures.
- Historical line coverage by season/provider.
- Feature rows missing required inputs.
- Roster snapshot age and completeness.
- Upcoming games missing pregame feature/signal snapshots.
- Hypotheses by lifecycle state.
- Prospective qualified sample count.

Historical and roster health should appear as separate status from `DATA PULSE` for live odds. `PARTIAL` history must not make the live feed appear stale.

## Suggested Implementation Checkpoints

### Checkpoint A — Trustworthy answer to the 14.5 question

- Audit completed.
- Historical games/lines stored.
- Exact and bucket cohort functions tested.
- HISTORY card renders counts, intervals, period, and provenance.

This is the first shippable milestone.

### Checkpoint B — Team-specific context

- Staff regimes stored.
- Raw and shrunk team results shown.
- Fallback hierarchy and reliability labels tested.

### Checkpoint C — Season/roster accumulation

- Point-in-time snapshots work.
- Week-dependent prior/current blending works.
- Missing and late-arriving roster data are handled safely.

### Checkpoint D — Research system

- Hypotheses are preregistered and versioned.
- Walk-forward and holdout evaluation is reproducible.
- Prospective snapshots and settlement operate without hindsight.

### Checkpoint E — Optional operational promotion

- A signal has met the frozen prospective threshold.
- CLV/outcome evidence and failure modes have been reviewed.
- Only then consider a separate change that lets it influence alert priority.

## Definition of Done

This initiative is done when:

- Historical source coverage and limitations are documented from actual audits.
- The terminal answers exact-line and registered-bucket questions reproducibly.
- Team cards show raw and shrunk regime-aware context.
- Season and roster signals are point-in-time correct and confidence-aware.
- Week 1 relies mainly on priors and later weeks accumulate current evidence smoothly.
- Every displayed result states sample, period, source, line classification, and validation state.
- Historical reference data never masquerades as intraday movement or a verified close.
- Hypothesis definitions, evaluations, and prospective snapshots are immutable and auditable.
- No unvalidated result affects operational alerts.
- Live CFB collection and display continue when the new research layer is unavailable.

## First Action

Implement only Slice 0 first. Produce the 2016–2025 historical source/coverage audit and the roster-source feasibility report. Review those artifacts before finalizing table constraints or writing any historical result to the dashboard.
