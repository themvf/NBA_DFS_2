# NFL Model Lab — source coverage slice

Implemented and verified 2026-09-04; prepared for production release through the existing GitHub/Vercel integration. This is Slice A, not the component projection model.

## Visible result

NFL board and NFL DFS workspace → **Model Lab**, `/dfs/nfl/model`.

- Field-by-season heatmap with dataset, position and field-group filters.
- Click-through denominators, present/valid/missing/invalid values, valid zeros and capture timestamps.
- Core-field completeness bars by position, excluded-row counts, data-age indicators and legacy-normalization warning.
- Audit JSON export and immutable permalink. Later model stages are visibly marked not implemented.
- Missing table/report and unsupported contract states never show fabricated coverage.

## Evidence and limitations

First audit inspected two overlapping populations separately:

| Population | Stored rows | In-scope rows | Seasons |
|---|---:|---:|---|
| Pinned full research history | 36,551 | 36,551 | 2020–2025 |
| Working source rows | 17,232 | 16,016 | 2023–2025 |

The 1,216 excluded working rows are outside QB/RB/WR/TE/DST scope (kickers). The audit is of stored records, not all original provider rows or complete weekly active rosters. Upstream unmatched players and no-stat/DNP weeks are not counted as observed zeros.

Frozen research QB rows lack completions. Frozen DST history carries transformed scoring evidence, not raw team workload inputs. Working rows contain completions and nested raw team stats. More importantly, the previous research importer defaults missing values to zero: 100% numeric presence in that transformed ledger does not establish original-source completeness. The new audit retains null/missing values and warns prominently about this legacy behavior.

All values are classified conservatively as retrospective-only. A latest `fetched_at` proves storage at that time, not historical source publication or pregame availability. Deferred routes/snaps/red-zone/injury/role keys do not have approved aliases or a usable source contract. The page does not claim that missing named keys prove no provider has that data.

The next component-history version and workload slice are now implemented locally. They preserve missingness and do not modify or combine the old frozen study. Historical weekly roster membership remains unavailable, so only team budgets receive retrospective grading; player allocation begins as forward research.

Corrected team-volume diagnostic (2024–2025, previously inspected; 1,088 team-games per field): attempts MAE 6.179 versus recency 6.331, carries 5.884 versus 6.088, and targets 5.892 versus 6.053. These small retrospective differences are descriptive, not a promotion gate or profitability evidence. See `docs/nfl-dfs-workload-delivery.md`.

## Storage and operations

`model/nfl_dfs_feature_audit.py` defines pure validation, units and the field contract. `ingest/nfl_dfs_feature_audit.py` reads both populations in one repeatable-read transaction and writes an append-only `nfl_dfs_feature_audits` record. Its normalized row evidence retains audited values, original-row hashes, identities and capture metadata. Summary/input digests and implementation hashes allow replay without relying on mutable working tables.

Python and Drizzle table definitions agree. Identical report persistence is idempotent; a later observation gets a new audit record. The first normalized evidence payload uses about 6.5 MB of PostgreSQL compressed storage. Daily observations intentionally retain evidence; monitor storage growth before expanding cadence. No retention deletion is implemented.

The web query reads only compact summaries, not row evidence. No page request fits a model, creates tables or writes data. The existing daily workflow now includes the audit step; scheduling changes take effect only after release to main. No paid provider calls or production forecast changes occur.

```powershell
python -m ingest.nfl_dfs_feature_audit
python -m pytest tests/test_nfl_dfs_feature_audit.py -q
```

## Verification

- Python regression suite: 97 passed, including 21 feature-audit cases.
- TypeScript and targeted lint/coverage helper checks run for the new UI.
- A stored audit was replayed from persisted row evidence and matched its full digest.
- Browser checked real frozen/working data, missing versus valid-zero cells, filter changes, DST data and a narrow viewport.
- Remaining component model phases are not implemented; production model and optimizer defaults are unchanged.
