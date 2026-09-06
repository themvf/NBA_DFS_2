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

Final live run: https://github.com/themvf/NBA_DFS_2/actions/runs/34055007139 succeeded on commit e77b6d0, source snapshot 3332. The 224 records include 87 offensive players (36 WR, 17 RB, 25 TE, 9 QB) and 137 individual defenders. Seventy records matched; the remaining 17 offensive identities require investigation, while individual-defender coverage is outside the current skill-player directory. Four records contain practice report fields. All 224 lack one of the recognized timezone-resolved update fields. No timestamp timezone was guessed.

## Identity and endpoint reconciliation, September 6, 2026

A read-only probe (GitHub run 34055408478) compared documented requests for 2026 W1, 2026 W0 and 2025 W1. Responses contained 224, 21 and 444 injury records respectively. That is consistent with year/week selection, not a feed that simply returns identical current data for every request. It does not establish historical publication timestamps or an archived pregame state. Each response reports `public_api_limited=true` and `tier=premium`; completeness remains unverified.

The current public reference is https://api.fantasypros.com/public/v2/docs, backed by https://api.fantasypros.com/public/v2/docs/fantasypros_v2_public.yml. It documents year/week and the three practice fields, but `injury_update_date` is merely a string with a date-only example. It specifies no injury-date timezone. Therefore timezone verification remains an external contract gap, and the source remains display-only. We do not infer UTC from a timestamp that happens to resemble retrieval time.

The six repaired identities are David Sills V (David Sills), Michael Woods II (Mike Woods), Irv Charles (Irvin Charles), Quentin Moore, Carter Bradley and Jacoby Jones. Matching uses unique FantasyPros IDs, then independent Yahoo IDs, then exact normalized name/team/position. Team aliases WAS/WSH and JAC/JAX are normalized. All ID paths still enforce team and position agreement; conflicting IDs and ambiguous matches fail closed. No player name, current team, depth chart or projection was rewritten.

The frozen 17-record regression fixture accounts for six matched, nine provider_nonteam, one position_conflict (Robbie Ouzts: provider TE versus canonical RB), and one missing_identity (Seth Williams). Provider nonteam is a source claim, not independently verified retirement. A separate Sleeper read lists Ouzts as RB/IR and Williams with no team, so neither discrepancy justifies forcing a current-team match. Full-feed reconciliation improves from 70 to 76 matches out of 78 provider team-assigned offensive records; 137 individual defenders remain outside this directory.

Every capture now saves all identity decisions and unresolved source records in its immutable source snapshot's audit metadata. The DFS page displays the whole-feed categories and unresolved names, separately from salary-slate counts. Saved optimizer player inputs retain the coverage snapshot ID. The raw injury source remains excluded from model features while its timezone contract is unresolved.

Provider clarification needed before enabling freshness-based eligibility: “What timezone applies to injury_update_date, including date-only values? Is it an injury-event time or a refresh time? What records does public_api_limited=true omit on a premium injury response? Do historical year/week requests return archived statuses or retrospectively revised statuses?” No support message has been sent.

Verified updated live capture: https://github.com/themvf/NBA_DFS_2/actions/runs/34056024324 succeeded on implementation d3fd75c, saving snapshot 3334 with 76 matched / 9 provider_nonteam / 1 position_conflict / 1 missing_identity / 137 outside_skill_pool. Browser verification resumed the 719-player salary slate and displayed this audit, its unresolved-name list, and 52 observed salary-slate injury records with zero verified-fresh FantasyPros records. Existing 99 exclusions were unchanged. Eight Python tests, both TypeScript availability suites, workspace regression checks, lint, typecheck and production build passed. Support clarification remains necessary to establish injury-date timezone; no automatic injury workload adjustments were enabled.
