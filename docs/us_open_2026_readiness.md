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
python -m ingest.tennis_withdrawals --match-id <id> `
  --reason "Official US Open withdrawal notice" `
  --evidence-url "https://official-source.example/notice" `
  --actor "$env:USERNAME"
```

4. Do not treat total-games or handicap quotes as rated recommendations. Only the existing total-games alert ledger is active research instrumentation.
5. Review the settlement gate when any provider fails or a result remains stale:

```powershell
python -m ingest.tennis_reconciliation --fail-on-unhealthy
```

The gate reports provider freshness, stranded provider runs, unresolved disputes,
stale matches, match/bet/alert outcome consistency, and current alert-grade history.
Never clear it by guessing an outcome or loosening identity matching; ambiguous
cases require verified evidence and an audited resolution. Use the manual
resolution command only with an official evidence URL and an identified actor:

```powershell
python -m ingest.tennis_resolve_result --match-id <id> `
  --winner home --completion-status completed `
  --reason "Official result" --evidence-url "https://official-source.example/result" `
  --actor "$env:USERNAME"
```

A verified no-contest walkover/cancellation may omit `--winner`. Reversing any
published result additionally requires `--correct-published-result`; the prior
observation, resolution, ledger state, and alert grade remain in the audit trail.

## Settlement policy

- Completed results settle the advancing player as the moneyline winner.
- `retired`, `walkover`, and `awarded` are explicit result states.
- Total-games derivative alerts void for all three states. A regular total-game push also voids.
- Result-source and original comment are stored with the match for audit.

## Post-tournament report

Run the existing ledger, CLV, and line-alert reports. Report settled count, void count, Brier score, realized-versus-market probability gap, frozen-price ROI, and best-price ROI separately. Do not convert a small US Open sample into a new star cap or model claim; any promotion requires a separately pre-registered, out-of-sample study.
