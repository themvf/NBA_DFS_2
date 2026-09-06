# NFL player context — increment 1

Available at `/dfs/nfl/history`, linked from NFL DFS and Model Lab. QB/WR/TE
rows in the salary pool open an exact-name historical lookup in a new tab so
the unsaved slate remains intact. Ambiguous or absent matches show an explicit
unavailable page. Internal historical links use GSIS player IDs.

This increment exposes 2025 regular-season results, opponent filters, historical
scored-game P10/P50/P90, weekly scoring bars, actual recorded QB participation,
and weekly roster status. The roster can expand beyond offensive skill players.
It does not update optimizer projections or generate predictive lineup ranges.

## Rebuild from existing source bytes

From the repository root, with Python/pandas and a parquet engine installed:

```powershell
python ingest/nfl_dfs_player_context.py --source-root 'C:/Docs/_AI Python Projects/NBADFS_v2' --output web/src/data/nfl-player-context-2025.json
python -m unittest tests.test_nfl_dfs_player_context
cd web
npm run test:nfl-player-context
```

The exporter validates every file's SHA-256 against the existing context
manifest before reading it. It makes no network calls or database changes.
The generated JSON is versioned with the app and is imported only by the
server page; each response sends only the selected player's games/rosters,
plus the player selector and aggregate audit. Original parquet files remain local.

## Interpretation

- Weekly stats provide complete input fields for the app's shared DK scorer.
  Missing rows stay null, including players with no box-score contribution;
  they are excluded from historical sample percentiles. This can bias the
  scored-game sample and is explicitly not a projected floor or ceiling.
- Personnel counts join by game ID and play ID and count pass/run plays,
  including kneels and spikes. Kickoffs and other special-teams plays are
  excluded. A matched personnel row does not prove official snap completeness.
- No observed participation means just that; it is not a confirmed injury,
  absence from the stadium, or proof of being inactive. Incomplete coverage
  has a separate label. Weekly roster statuses are retrospective source values.
- All QB appearances are shown, rather than labeling the largest count as
  the starter. There is no inferred injury time or inferred route count.
- Personnel IDs missing from roster/stats are preserved as unknown identities
  in the full-roster table. Offensive line and defensive names are retained;
  the current view counts offensive participation only.
- Sources were fetched after the season. They explain historical outcomes;
  pre-lock injury availability must be verified separately for backtesting.

## Next increments

2. Roster-context scenario comparisons with sample-size adjustments and a
   fixed-lineup comparison of P10/P50/P90 and target probability.
3. Chronological calibration/selection evaluation before enabling the new
   estimates in optimizer ranking. Kelly remains outside this increment.
