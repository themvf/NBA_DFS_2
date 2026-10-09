# CFB pilot operations evidence — 2026-09-25 20:34:28 UTC

This is a human-readable index to the pinned [pilot operations JSON](../artifacts/cfb_pilot_operations.json). The database evidence cutoff is `2026-09-25T20:34:28.772218Z`; the reporting interval begins at the frozen pilot boundary `2026-09-25T00:00:00Z`. GitHub run records were read at report generation. The execution environment is GitHub Actions with the configured PostgreSQL database. The frozen moneyline study remains version 4, digest `364db068d16ef1e272489e0523bb0cc8643f938c29caafdf270aa9e087b8152c`, primary metric `decimal_price_ratio_pct`.

## Run evidence

| Workflow | Expected cron slots | Observed scheduled | Observed dispatch | First successful run after boundary | Latest completed success inspected |
|---|---:|---:|---:|---|---|
| Closing-lines capture | 247 | 5 | 116 | [Run 36075506771](https://github.com/themvf/NBA_DFS_2/actions/runs/36075506771) | [Run 36185195936](https://github.com/themvf/NBA_DFS_2/actions/runs/36185195936), commit `52373b82` |
| CFB terminal refresh | 82 | 12 | 0 | [Run 36078876551](https://github.com/themvf/NBA_DFS_2/actions/runs/36078876551) | [Run 36179864861](https://github.com/themvf/NBA_DFS_2/actions/runs/36179864861), commit `ee52e1f1` |

These are counts of observed runs, not a one-to-one reconciliation of cron slots: GitHub can delay or skip scheduled events, and the capture worker also receives dispatches. [Run 36185496921](https://github.com/themvf/NBA_DFS_2/actions/runs/36185496921) was still in progress and [run 36186445346](https://github.com/themvf/NBA_DFS_2/actions/runs/36186445346) was pending at the report cutoff. The latest completed capture run executed detection, but its deployed commit did **not** include the new normalization and movement-publication steps. The JSON contains every observed run ID, outcome, link, and first success by deployed commit within the queried interval.

## Database reconciliation

| Stage | Pilot-period records | Evidence |
|---|---:|---|
| Stored provider history | 136 | `game_odds_history` |
| Normalized prospective captures | 136 | `cfb_engine_captures` |
| Normalized quote observations | 3,724 | `cfb_engine_quote_observations` |
| Persisted detector runs / funnels | 0 / 0 | Missing operational evidence |
| Persisted prospective signals | 8 | `line_alerts` |
| Canonical economic heads | 8 | All currently pending settlement |
| Frozen study v4 pilot observations | 8 | Collecting; no final evaluation |
| Detector publication receipts / shared policy-reader decisions | 0 / 0 | Missing integration evidence |

The counts have different grains and need not match. A reproducible real-data path starts with provider history `59386`, normalized capture `fc8ba3f5-e618-5636-8035-a94388c59415` (30 quotes), movement snapshot `61bfd708-f897-5a3f-be38-bd255dd431d8`, input manifest `b8e0fef8-05b4-5cfa-97c9-3496c5d640f6`, signal `92774`, and pending economic resolution `e24cbe4a-1217-5344-b96e-4d7c6c43e529`. The snapshot uses definition version 1 and the capture's `2026-09-25T15:46:28Z` as-of boundary.

**Conclusion:** The stored capture-to-pending-economics path is now evidenced locally. Collection is **not operationally verified** because live detector funnels and publication receipts are absent, and the new normalization/publication steps have not run on the deployed workflow. The [operations contract](cfb-pilot-operations-contract-v1.md) defines monitoring thresholds; funnel-dependent checks remain not evaluable until the live path records those funnels. The pilot cannot qualify a consumer, and frozen study rules are unchanged.
