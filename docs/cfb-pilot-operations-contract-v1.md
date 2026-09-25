# CFB pilot operations contract v1

Effective for the frozen moneyline study version 4 pilot from 2026-09-25 00:00 UTC. This contract governs collection monitoring only. It does not change study definitions, cohort enrollment, evaluation gates, or provider spending limits.

## Evidence and ownership

Data/platform engineering owns the read-only [`cfb_pilot_operations`](../research/cfb_pilot_operations.py) report. The [pilot monitor workflow](../.github/workflows/monitor_cfb_pilot.yml) runs every 15 minutes, saves the pinned JSON as a run artifact, and fails when an actionable run-gap or funnel alert is observed. Incomplete engineering gates remain visible in the report without failing every monitor run. The report records the database cutoff, deployed workflow commits and links, run outcomes, stage counts, one exact real-data trace, and the frozen study status. A workflow success establishes that its steps exited successfully; it does not establish a complete capture-to-study chain.

The shared closing-lines worker owns paid captures. The CFB terminal refresher does not add paid odds captures except an explicitly requested manual capture. Normalization, movement publication, reporting, and monitoring use stored data and require no added provider requests.

The closing-lines worker writes a run and per-event moneyline funnel for each frozen detector version after its CFB scan. A zero-match event still has a funnel; a run with no upcoming games has detector runs with an empty input manifest and no event funnels. Historical workflow runs before this instrumentation cannot acquire authentic run-level funnels retroactively. The report keeps those gaps visible and checks the latest instrumented capture run for an actionable missing-funnel alert.

## Version 1 operational thresholds

| Check | Alert condition | Response |
|---|---|---|
| Capture worker gap | No successful capture-worker completion in 20 minutes during the active pilot | Check GitHub run history, the due-checkpoint ledger, and data freshness. Dispatch the existing worker only through its established capture path if necessary. |
| Terminal refresher gap | No successful terminal-refresher completion in 120 minutes | Check schedule, provider mapping, and health output. |
| Missing detector funnel | Any scheduled detector/version run lacks its persisted funnel, including a zero-match run | Treat as an operational failure. Do not interpret zero triggers as zero opportunities. |
| Zero eligible opportunities | The funnel reports zero eligible opportunities across an expected slate with stored pregame captures | Inspect identity, freshness, market coverage, and temporal rejection reasons before interpreting outcomes. |
| Temporal rejection spike | Temporal-origin violations exceed 5% of candidates in the latest 24 hours, with at least 20 candidates | Inspect timestamp provenance and schedule revisions. |
| Stale-book spike | Stale-quote rejections exceed 20% of candidates in the latest 24 hours, with at least 20 candidates | Inspect bookmaker update times and provider coverage. |
| Persistence or publication failure | Any failed signal persistence or failed publication receipt | Reconcile the failed record and retry idempotently after fixing the path. |

The funnel-based checks are **not evaluable** until the live detector path persists every scheduled detector/version funnel. An absent funnel is never a passing zero. A zero final trigger count alone is not an alert. These operational thresholds are versioned here; changing them does not alter the frozen study gates.

## Current gate interpretation

Successful runs, reconciled stage counts, persisted zero-match funnels, publication receipts, and a real-data trace are required before collection is called operationally verified. A stage may legitimately have fewer rows than the previous stage because of eligibility, deduplication, and pending settlement; each exclusion needs recorded evidence. Provider permission review, shared-consumer integration, and the separate football-context experiment remain separate completion gates.
