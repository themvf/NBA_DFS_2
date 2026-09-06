# NFL game-week availability evidence

The NFL DFS slate now combines its Sleeper role evidence with explicit week-scoped FantasyPros injury observations. Observation IDs, hashes, source/provider timestamps, practice status, conflicts and canonical kickoff are retained inside every saved optimizer input snapshot. No projected workload changes automatically. DraftKings exclusions and existing unavailable/backup-QB exclusions remain effective.

The daily projection workflow captures FantasyPros injuries using the inferred next eligible week, or its explicit week input. The dedicated `Capture NFL game-week availability` manual workflow refreshes only this feed. Both retain sanitized audit artifacts. An empty/missing feed never clears a player. Week zero, wrong-team observations, future observations and expired evidence cannot drive new exclusions. The collector validates any returned season/week fields; absence of a provider period is disclosed rather than treated as confirmed coverage. Provider probabilities are retained upstream but are not used as calibrated participation probabilities.

Freshness is 24 hours for injury evidence, tightened to two hours within six hours of canonical kickoff. A timezone-resolved provider update time is required and must also pass. Missing timestamps or dates with an unverified timezone remain display-only. Roster freshness remains the existing 72-hour research criterion with a separate game-day refresh warning. Generic roster retrieval time is not proof of provider recency. Late/unknown kickoff snapshots have explicit warnings and cannot be presented as verified pregame evidence.

## Official game-day confirmation

There is no automated official inactive feed. The optional CLI imports a manually reviewed NFL.com report:

`python -m ingest.nfl_dfs_official_availability path/to/reviewed-report.json`

The JSON contains `season`, `week`, `report_type: "inactive_list"`, official `url`, timezone-aware `published_at`, exact canonical `kickoff`, and `players`: explicit `gsis_id`, `team`, `position`, `status` (ACTIVE or INACTIVE) rows. Never infer ACTIVE from absence in an article. Capture must be before kickoff and publication within three hours of kickoff. Ambiguous identities, duplicate players, mismatched games and unsupported URLs fail transactionally. Import preserves observation evidence without rewriting canonical injuries. Official active status does not establish starter workload or automatically clear a conflicting existing exclusion; review remains required.

The app shows coverage counts and an expandable player evidence table. Official confirmations are zero until qualifying reviewed reports have been imported. Scheme remains in Team Context; no news-to-scheme multiplier or replacement-target allocation is inferred here.

## Verification

- `python -m pytest tests/test_nfl_dfs_availability.py -q`
- From web: `npx tsx scripts/test-nfl-game-availability.ts` and `npm run test:nfl-availability`
- Live capture: `python -m ingest.nfl_dfs_availability --season 2026 --week 1`

The live collector requires DATABASE_URL and FANTASYPROS_API_KEY. Missing credentials fail with a sanitized audit, while the daily projection pipeline treats this feed as optional. A successful API call alone does not establish complete player coverage or provider freshness. The manual capture workflow does not provide near-kickoff monitoring or late swap.


## Live verification, September 6, 2026

The first GitHub Week 1 capture succeeded: 224 source injury records, 70 matched canonical players, 154 unmatched, zero ambiguous matches. It did not echo a provider week. All 70 persisted observations lacked a resolved provider update timestamp; the raw feed instead supplies `injury_update_date` without a timezone and separate practice_1/2/3 fields. The UI now preserves those practice values and shows raw dates as timezone-unverified. These observations cannot newly change eligibility. Reconcile the provider date contract and unmatched position coverage before claiming a complete feed.

The 719-player saved slate rendered the new panel successfully. Python report validation, TypeScript availability/workspace tests, lint, typecheck and production build passed. No official inactive observations were fabricated or imported during verification.
