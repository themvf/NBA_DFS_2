# NFL Model Lab — workload slice

Implemented locally 2026-09-04. This is Slice B of the opportunity/efficiency roadmap. It is research-only and does not alter `ourProj`, optimizer inputs, lineup generation, or production defaults.

## Visible result

NFL DFS → **Model Lab** → **02 / Workload**.

- Team pass-attempt, carry, and target budgets with recent estimate, league prior, known allocation, and explicit unallocated work.
- Player opportunity allocations and eight-game recorded-history bars. Missing weeks remain absent rather than becoming zeros.
- Team and player selectors, source/run hashes, sample sizes, bias, and the saved retrospective comparison.
- A prominent role warning because verified depth charts, injuries, and historical weekly roster membership are not yet integrated.

## Model contract

The candidate uses only rows before the forecasted game week. Team volume is an exponentially weighted mean (six-game half-life, maximum 17 recorded games), shrunk toward the historical league population with four prior games of weight. Player opportunity shares use the same chronological weighting within the player's current team.

Targets cannot exceed forecast pass attempts. If player shares sum above one, they are normalized to the team budget and labeled. If they sum below one, the difference remains `unallocated`; the model does not silently assign unknown work. A player without valid history receives no component forecast—not a zero.

The source dataset is content-addressed from preserved raw nflverse payloads and missingness. Its digest excludes observation time, so identical stored evidence reuses one dataset. Each forecast run separately records observation time, model/config version, dataset digest, and model/ingest implementation hashes. Player-share history is explicitly filtered to `(season, week) < target`, preventing an earlier game in the same NFL week from leaking into later-game forecasts.

## Retrospective evidence

Expanding-window team-volume evaluation covers 2024–2025, which has already been inspected. Each field has 1,088 team-games.

| Team workload | Candidate MAE | Recency MAE | Bias, actual − projected |
|---|---:|---:|---:|
| Pass attempts | 6.179 | 6.331 | -0.390 |
| Carries | 5.884 | 6.088 | +0.044 |
| Targets | 5.892 | 6.053 | -0.416 |

This is a descriptive paired comparison, not a fresh gate, player-allocation validation, DFS-return claim, or authorization to promote the candidate. Player allocations require forward results and a historically valid roster/role cohort.

## Operations and limits

`ingest/nfl_dfs_workload.py` reads stored data only and writes append-only component datasets and workload runs. The existing daily NFL workflow freezes the next eligible week after release. Page reads are typed and read-only; query failure leaves the coverage audit usable and marks workload unavailable.

The displayed roster is the canonical active season roster, not a DraftKings salary slate. Multiple quarterbacks and stale roster candidates can appear. No injury status, depth-chart role, routes, snaps, or role-change prior is used. The team-coupled offense and separate DST process in Slice C now consume this workload.

Latest saved Week 1 evidence: workload run `b1323a61dee55fefb688ad3b0931f27d5b9faad4a27d47bbba9b48aaa78cf6e1` on component dataset `e61b77837d8b4a9ddb4f14c188908f5a30930ed21751f3d3a1035fc12f8a00cf`.

## Verification

Run:

```powershell
python -m pytest tests/test_nfl_dfs_workload.py -q
python -m ingest.nfl_dfs_workload --season 2026 --week 1
```

Tests cover missing-versus-zero behavior, chronological weighting, capped sample disclosure, shrinkage, target constraints, allocation reconciliation, explicit target-week leakage resistance, and bias direction. Browser verification covers saved-data rendering, team/player selection, role warning, history order, and the retrospective comparison.
