# CFB pilot operations evidence — September 26, 2026

This report indexes the pinned [operations snapshot](../artifacts/cfb_pilot_operations_2026-09-26.json). Its database cutoff is **2026-09-26 20:09:41 UTC** and its collection interval begins at the frozen pilot boundary, **2026-09-25 00:00 UTC**. GitHub run history was read independently. The frozen moneyline study remains version 4 with configuration digest `364db068d16ef1e272489e0523bb0cc8643f938c29caafdf270aa9e087b8152c`.

## What ran

The latest successful [capture run](https://github.com/themvf/NBA_DFS_2/actions/runs/36268130516) executed normalization, movement publication, signal grading, and economic reconciliation. At the cutoff, the capture worker's last success was 2.1 minutes old and the terminal refresher's was 9.4 minutes old; both passed the [operations contract](cfb-pilot-operations-contract-v1.md) run-gap checks. The detector's recent zero-eligible, temporal-rejection, stale-book, and missing-frozen-funnel checks passed.

Across the full pilot interval, 134 of 283 successful capture workflow runs lacked a complete frozen detector/version funnel for that exact run. The linked JSON lists their run IDs and missing versions; many preceded the deployed funnel path, but the historical gap remains part of the pilot evidence. Expected cron slots and observed runs are reported separately because GitHub can delay or omit scheduled triggers.

## Stored evidence at the cutoff

| Stage | Count |
|---|---:|
| Stored provider history / normalized prospective captures | 1,794 / 1,794 |
| Normalized quote observations | 48,674 |
| Detector runs / funnels | 965 / 51,719 |
| Prospective signals / current canonical economic heads | 43 / 43 |
| Prospective context snapshots | 1,755 |
| Detector publication receipts / shared policy-reader decisions | 0 / 0 |

These stages have different grains; the counts are not expected to match. A reproducible trace connects provider history `59386`, normalized capture `fc8ba3f5-e618-5636-8035-a94388c59415`, 30 normalized quotes, movement snapshot `61bfd708-f897-5a3f-be38-bd255dd431d8`, input manifest `b8e0fef8-05b4-5cfa-97c9-3496c5d640f6`, signal `92774`, and settled economic resolution `7d1459ec-4ea9-566c-9a89-189fd1df0a5f`.

The frozen moneyline candidate set has 36 pilot signals. Twelve had settled across two independent game dates; all 12 had the primary `decimal_price_ratio_pct`, a verified close ID, and a canonical settled head, with no economic conflicts. **Settlement-rule provenance is unresolved:** zero rule versions are registered and none of 15,016 prospective moneyline quotes carries a rule ID. The 12 ratios are therefore explicitly marked as comparisons of legacy quotes with unverified settlement rules. Their presence does not satisfy the rule-comparability gate.

## Open delivery gates

1. **Data/platform:** persist publication receipts for actual consumer publication and reconcile each deployed run's detector/version funnels. Do not backfill a receipt for a consumer that never read the observation.
2. **Application/data:** connect the terminal, Python shadow study, and postgame export through the shared policy-aware reader, then save real read decisions and cross-consumer replay evidence.
3. **Data/platform with product/legal/account owner:** register reviewed, book-specific moneyline settlement-rule artifacts and attach versioned IDs to eligible quotes. Leave unverifiable historical quotes marked unknown.
4. **Platform operations:** investigate the irregular pilot-monitor schedule and retry transient database schema-lock deadlocks in fresh read transactions. [Monitor run 36268425903](https://github.com/themvf/NBA_DFS_2/actions/runs/36268425903) failed on that deadlock; the retry change accompanies this report.

Collection is **incomplete**, not operationally verified. The pilot is diagnostic only and cannot qualify a consumer. The earliest pilot review is **2026-10-17 00:00 UTC**; the two later frozen confirmation windows and separate activation gates remain unchanged.
