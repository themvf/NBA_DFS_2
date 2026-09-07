# NFL player identity and result reconciliation

Implemented September 7, 2026. The registry stores immutable identity claims in
Neon and exposes a consistent provider-ID → GSIS crosswalk. It does not merge or
delete canonical player records and does not change current rosters.

## What is connected

- 29,221 supporting claims resolve 24,090 namespace/ID pairs. No conflicts were
  observed in this import. This means the available assertions agree; it is not
  independent verification against every provider.
- Namespaces include GSIS, ESPN, Yahoo, Sleeper, FantasyPros, PFR, Sportradar,
  PFF, RotoWire, FantasyData, local `ff_players` IDs, and the archive's two
  distinct DraftKings player-ID fields.
- DraftKings CSV roster-entry IDs are **not** treated as permanent player IDs.
  Captain/FLEX entry IDs remain roster options for one physical player.
- Source-attested aliases include `Drew Ogletree` / `Andrew Ogletree`. No fuzzy
  matching or invented nickname dictionary is used.
- Duplicate local Trevor Lawrence records (30/364) and Puka Nacua records
  (34/560) link to the same permanent identities. Their records/FKs are preserved.

For live NFL salary imports, a name/alias identifies a candidate in the current
season's roster. Its resolved GSIS ID selects the projection, which must still
match the salary team's identity and compatible position. Historical aliases
never establish a current team, role or starting assignment. Conflicts do not
fall back to name. Two salary players resolving to one identity are rejected
before the upload is written.

The NFL player table displays matching details. Saved salary rows and optimizer
snapshots retain the selected GSIS, projection run/row, roster capture time,
source claim digests and salary identity. Injury observations can be shared
between duplicate local records only when GSIS, team and position agree; the
original/target local IDs are retained in the injury evidence.

The database migration adds `identity_evidence` and expands the existing matching
status check constraint. New conflict labels can therefore be stored on databases
created by either the Python or web schema path.

## Upcoming-season exception

Of 1,086 local 2026 rows, 1,053 non-defense rows have a resolved registry link
(including the two duplicate rows). Team defenses are matched by team rather
than player GSIS. **Al-Jay Henderson** is the remaining non-defense record without
a permanent GSIS in these sources. His Sleeper ID is retained in the canonical
database, but is not mislabeled as a GSIS ID. He stays unresolved for the permanent
projection match until source evidence supplies the link.

The current [nflverse player directory](https://github.com/nflverse/nflverse-data/releases/download/players/players.parquet)
was also checked. Henderson's `gsis_id` is `HEN032810`, identical to its ESB field;
the row has PFR and Smart IDs but no numeric NFL/GSIS link. Those IDs are not
silently substituted into a different namespace. The checked source SHA-256 is
`b38d690910364b9d7e0df46d7bb1dbbe44ec6b7142b3f7ce1cbcab142395a208`;
the local review is `artifacts/nfl-identity-registry/al-jay-id-review.json`.
This review does not overwrite his roster or availability.

## Complete historical pilot results

| Slate | Reconciled pool | Hindsight DK points | Salary |
|---|---:|---:|---:|
| 134423 — SF at LA Showdown | 53/53 | 166.91 | $49,100 |
| 134675 — Sunday main Classic | 504/504 | 265.74 | $48,900 |

These are optimal over the **complete archived pools using reconstructed DK
scores**. They are not independently verified official contest results. DST scores
still use the existing component-backed realized-results ledger.

For players without a stat row, a zero requires all of:

1. A resolved permanent player identity, with no disagreement between DK namespaces.
2. A play-by-play end-of-game marker whose score matches the archived final score.
3. Every relevant nondeleted football play linked to participation records,
   including special teams, with participant lists matching declared side counts.
4. No counted event attributed to the player. An attributed event without a stat
   row requires investigation rather than automatically becoming zero.

Results distinguish recorded nonparticipants, recorded participants with no
attributed events, and scored stat lines. Inactive/reserve roster status alone
does not establish zero. An active-play/roster-status contradiction is flagged.
Recorded participation is not an official snap count or pregame availability.

Konata Mumpfield's only attributed target was explicitly erased by a defensive
holding penalty. Its IDs remain in `voided_event_player_ids`; that uncounted target
does not prevent a zero. Counted scoring flags on a `no_play` row still block this
shortcut.

The same historical-only mean replays now have complete realized totals: 29.56
for Showdown and 156.48 for Classic. Those intentionally context-free diagnostics
include backups/injured players; they are **not** results for the production
optimizer. Postgame participation settles results only and never filters the
pregame candidate pool or enters projection features.

## Refresh and reproduce

Run after refreshing canonical NFL player/roster data, before loading new salary
files. This is an explicit refresh command, not a newly scheduled automation.
The source-root checkout must contain the existing verified 2025 caches and
`artifacts/ff_v2_historical_context_2020_2025.json`.

```powershell
python -m ingest.nfl_identity_registry --source-root 'C:\Docs\_AI Python Projects\NBADFS_v2' --persist
python -m ingest.nfl_archive_case_study --identity-run d5219cbaa4755f6d48ad724f7cc44ff6b1abee4e70d1076bc50721283dd38592 --output artifacts/nfl-archive-pilot-v3 --persist
```

`--persist` is opt-in; otherwise the registry command only reads the database and
writes local reports. The registry checks local source SHA-256 hashes before
reading them. Current canonical claims preserve their source-row values and
capture times. Reimports deduplicate by content digest. Conflicting permanent-ID
assertions are quarantined by the view rather than overwriting earlier evidence.

Verified registry run:
`d5219cbaa4755f6d48ad724f7cc44ff6b1abee4e70d1076bc50721283dd38592`.

Complete pilot report:
`cfbb7039645ea3b2a5e322fe0cf1f90bd50fd1faf2b6d689effc2be68b4f690f`.

Local outputs are `artifacts/nfl-identity-registry/report.json`, `results.json`,
and `artifacts/nfl-archive-pilot-v3/index.html` / `report.json`.

## Query examples

```sql
-- All permanent provider identities for a player.
SELECT namespace, external_id, gsis_id, status, evidence_count
FROM nfl_player_identity_crosswalk WHERE gsis_id = '00-0039075';

-- Conflicts requiring review; never choose the first candidate.
SELECT * FROM nfl_player_identity_crosswalk WHERE status = 'conflict';

-- Reconstructed results and their complete evidence for the saved pilot.
SELECT draft_group_id, player_id, gsis_id, actual_dk_fpts, status, evidence
FROM nfl_dk_archive_result_reconciliation
WHERE run_digest = 'd5219cbaa4755f6d48ad724f7cc44ff6b1abee4e70d1076bc50721283dd38592'
ORDER BY draft_group_id, player_id;

-- Resolve an audit's source digest to its immutable claim.
SELECT * FROM nfl_player_identity_claims WHERE claim_digest = '<digest from player audit>';
```

## Verification

Thirty-two Python tests and twenty-one focused TypeScript identity assertions
passed, along with the existing NFL availability, salary CSV and workspace
suites. The production build passed. A repeated registry import retained 29,221
claims and one copy of the same run; all 535 non-DST pilot results have scores,
with the 22 DST results supplied by the existing ledger. Desktop and mobile
report rendering showed both complete pools and no horizontal overflow.

Python tests cover ID namespaces/conflicts, source coverage gaps, duplicate plays,
voided targets, missing IDs/stat rows, contradictory roster evidence, scoring and
lineup legality. TypeScript tests cover permanent-ID resolution, aliases, current
team/position guards, duplicate-local injury bridges and duplicate salary entries.

Read-only live verification is available from `web/`:

```powershell
node --env-file=.env.local -r ./scripts/server-only-stub.cjs --import tsx scripts/verify-nfl-identity-registry.ts
```

It checks the two duplicate-local bridges, actual GSIS projection matches and
the deployed SQL constraint. No current Week 1 Puka injury observations were
present during verification, so injury propagation is covered by fixture tests;
the live read itself was verified not to fail.
