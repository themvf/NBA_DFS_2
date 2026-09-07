# 2025 archive pilot: one Showdown and one Classic

Run the first two case studies with:

```powershell
python -m ingest.nfl_archive_case_study --output artifacts/nfl-archive-pilot --persist
python -m pytest tests/test_nfl_archive_case_study.py -q
```

Open `artifacts/nfl-archive-pilot/index.html` for lineup tables and projection-miss bars.
`report.json` retains each player identity decision, source checksum, scoring inputs,
historical projection and solver certificate. Archive source tables are read-only;
`--persist` appends the derived report to `nfl_dk_archive_case_studies`, deduplicated
by its content digest. This is a research artifact, not yet an app route.

## Selection and first results

Fixed Week 5 examples, chosen before calculating outcomes:

| Format | Draft group | Slate | Reconstructed optimum over reconciled players | Salary | Coverage |
|---|---:|---|---:|---:|---:|
| Showdown | 134423 | SF at LA, October 2, 2025 | 166.91 | $49,100 | 27/53 players |
| Classic | 134675 | Sunday main, October 5, 2025, 10 games | 265.74 | $48,900 | 245/504 players |

The Showdown lineup is Kyren Williams CPT; Matthew Stafford, Kendrick Bourne,
Christian McCaffrey, Eddy Pineiro and Jake Tonges FLEX.

The Classic lineup is Sam Darnold QB; Rico Dowdle and Jacory Croskey-Merritt RB;
Ja'Marr Chase, Jaxon Smith-Njigba and Emeka Egbuka WR; AJ Barner TE;
Javonte Williams FLEX; Colts DST.

These are **not certified full-slate or contest-winning lineups**. Missing game-log
rows remain unknown, not zero. Actual points are reconstructed from component
statistics and the versioned DST ledger, not independently verified official DK
standings. The mixed-integer solver certifies the best score only within the
reconciled subset, using the actual roster-slot salary and scoring multiplier.
No contest entries, ownership or payouts are included.

## What the baseline replay teaches us

The diagnostic runs historical-v2 with only 2025 Weeks 1–4, neutral environment,
and seed 202505. It compares maximizing historical mean with summing player P90s.
The latter is deliberately a flawed comparator: its sum is not a lineup P90.
Neither run reproduces the complete production pipeline or establishes its quality.

Without a reliable as-of availability and role layer, the baseline selects backup
quarterbacks in Showdown and Malik Nabers in Classic. Several selected players have
unknown realized scores in this reconciliation, so lineup realized totals remain
incomplete. This must not become an apparently scored production backtest through
postgame filtering or by assigning missing scores zero.

Among reconciled players, Bourne scored 27.2 versus a 5.01 historical projection;
Dowdle scored 35.4 versus 6.57. Those misses identify concrete cases for examining
pregame opportunity redistribution. They do not prove what caused the extra work
or that it was predictable. Retrieve dated availability/role evidence before
claiming an injury, scheme or roster explanation.

## Next acceptance gate

1. Resolve missing identities and document nonparticipation; independently reconcile
   official DK scoring, particularly DST and return touchdowns.
2. Attach source-dated 2025 availability, starters and workload context. Historical
   statistics reconstructed later are usable for diagnostics but not evidence that
   the exact data version was available at lock. Never use 2026 context for 2025.
3. Replay the production pipeline with frozen inputs and joint scenarios, then
   compare actual legal lineup results on a larger chronological sample. Treat
   these two inspected cases as development examples, not untouched test data.

## Query a saved report

```sql
SELECT created_at, report_digest,
       c->>'draft_group_id' AS draft_group_id,
       c->>'optimum_scope' AS scope,
       c->'hindsight_optimal'->>'actual_points' AS actual_points,
       c->'hindsight_optimal'->>'salary' AS salary,
       c->>'reconciled_players' AS reconciled,
       c->>'pool_players' AS pool
FROM nfl_dk_archive_case_studies
CROSS JOIN LATERAL jsonb_array_elements(report->'cases') c
ORDER BY created_at DESC;
```

Determinism is checked with the same inputs and installed solver/model versions.
The report uses a repeatable-read database snapshot. Updating source rows or model
code creates a new digest rather than silently rewriting an old report.

Verified initial report digest:
`19c2a2741c8a4c3ac8860187e24b65f792e8427327406abcb7e87bd65d2847be`.
Two consecutive runs produced the same digest, and two persistence attempts left
one database report containing both cases. Nineteen focused scoring, solver and
historical-model tests passed. The generated report was rendered and checked at
1500px and 390px widths with no horizontal overflow.
