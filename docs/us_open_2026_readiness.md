# US Open 2026 — Tennis Operations Runbook

**Tournament window:** 24 August–13 September 2026. `refresh_tennis.yml` increases its automated odds capture from six to three hours only during this window. It remains an accountability and data-quality workflow: live moneyline ratings are capped at **2★** and are not an edge claim.

## Pre-tournament

Run before qualifying or the main-draw fixture release:

```powershell
python -m ingest.tennis_us_open_preflight
```

The report must show active ATP and WTA US Open provider keys before the schedule can be considered covered. It also reports persisted US Open fixtures and priced fixtures by tour. A missing key means **provider_not_covered**, not that the tournament has no matches.

## Every session

1. Review upcoming draws and capture health on `/vegas?sport=tennis`.
2. Verify any reported withdrawal from an official tournament or player source before first serve.
3. If verified, void the stored match explicitly; this command refuses a started or already settled match:

```powershell
python -m ingest.tennis_withdrawals --match-id <id> --reason "Official US Open withdrawal notice"
```

4. Do not treat total-games or handicap quotes as rated recommendations. Only the existing total-games alert ledger is active research instrumentation.

## Settlement policy

- Completed results settle the advancing player as the moneyline winner.
- `retired`, `walkover`, and `awarded` are explicit result states.
- Total-games derivative alerts void for all three states. A regular total-game push also voids.
- Result-source and original comment are stored with the match for audit.

## Post-tournament report

Run the existing ledger, CLV, and line-alert reports. Report settled count, void count, Brier score, realized-versus-market probability gap, frozen-price ROI, and best-price ROI separately. Do not convert a small US Open sample into a new star cap or model claim; any promotion requires a separately pre-registered, out-of-sample study.
