# NFL DFS — environment, opponent and interval studies (registered 2026-09-22)

WP6 and WP7 of [the 2026-09-22 implementation handoff](nfl-dfs-implementation-handoff-2026-09-22.md).
Registered before the first context-bearing shadow row is frozen. Production
`nfl-dfs-historical-v3` is unchanged; every variant below is frozen into the
shadow ledger (`nfl_dfs_shadow_predictions.payload.context_variants`,
`nfl-dfs-context-variants-v1`) and graded later against the same outcomes.

## Why the shadow ledger needed a context-bearing variant first

The shadow freeze (`ingest/nfl_dfs_shadow.py`) projected every baseline with
`ProjectionContext()` — no team implied total at all — while production
applies a team factor of `implied / 22.5` clamped to [0.8, 1.2]. Any
environment change was therefore ungradeable: there was no like-for-like
production reference in the ledger. `env_baseline` (v3 with the frozen
team implied total) is that reference. Every study below is paired against
it, never against the context-free baseline.

The opponent factor in v3 is dead code: `opponent_exponent = 0.35` exists in
`MODEL_CONFIG` but `build_week` never sets `ProjectionContext.opponent_factor`
(`feature_snapshot.opponent_factor` is NULL on every row). It stays as-is
until study (ii) reports; it is then either populated by a promoted variant
or removed.

## Variants (frozen; `model/nfl_dfs_environment_variants.py`)

| variant | differs from `env_baseline` in exactly | study |
|---|---|---|
| `env_trailing` | team factor = implied / the team's own shrunk trailing points per game (half-life 6, 17 games, 4 prior games toward league, completed games before the cutoff), clamp [0.7, 1.3] | (i) |
| `opp_carries` | rushing yards and rushing TDs scaled by `v1 / candidate` carries from the opponent workload study (`own + 0.5 × (opp allowed − league)`), bounded [0.5, 1.5] | (ii) |
| `interval_rq` | same mean; p10/p90 = mean + residual quantiles of the recency-weighted own-mean estimator by position × history bucket (2–5 / 6–16 / 17+), computed walk-forward inside the frozen history, ≥ 30 residuals per bucket | (iii) |
| `prior8` | `prior_equivalent_games` 4 → 8 | (iv) |

No constant above is fitted here. The 0.5 is the opponent study's stated
prior; the shrinkage constants are the workload study's; 8 is a stated
alternative, not a search.

## Studies — one variant each, four families total

All four: population = shadow rows with `context_variants` present, weeks
from the first frozen week through **eight forward weeks**, one accepted
pregame forecast per player-week (the ledger's existing dedup), scored
outcomes only (the ledger's existing policy: no stat row is not zero).
Metric = weeks-clustered bootstrap (2,000 draws, seed 20260922) of the paired
per-row difference vs `env_baseline`. Floors: ≥ 6 distinct weeks and ≥ 100
scored player-weeks per position. Positions graded independently; DST is
reported but not gated for (i)–(ii).

| study | hypothesis | PASS | kill |
|---|---|---|---|
| (i) `env_trailing` | a team-relative environment factor beats the 22.5 constant | paired MAE CI upper bound < 0 at QB, RB, WR, TE | CI includes zero at any of the four |
| (ii) `opp_carries` | opponent allowed-rush volume improves RB rushing lines | paired MAE CI upper bound < 0 at **RB** (QB/WR/TE must not have a lower bound > 0) | CI includes zero at RB |
| (iii) `interval_rq` | residual-quantile intervals fix the depth-dependent miscoverage (hist 17–34 p10–p90 coverage 0.73 vs nominal 0.80) | p10–p90 coverage closer to 0.80 than `env_baseline` in the hist_17_plus bucket AND boom Brier not worse; means are identical by construction | coverage not closer, or boom Brier worse |
| (iv) `prior8` | heavier shrinkage helps shallow-history players | paired MAE CI upper bound < 0 in the hist_2_5 bucket AND no bucket with a lower bound > 0 | otherwise |

No verdict before the eighth forward week is scorable. A PASS licenses a
production candidate with its own version bump and shadow re-pin, never a
direct config edit. A FAIL closes the variant; a different weight, clamp,
bucket or prior is a new registration.

## First freeze (2026-09-22, week 3)

676 shadow rows frozen with `context_variants`: `env_trailing` and
`interval_rq` on all 676, `opp_carries` on 644 (DST excluded by design).
Two things observed on the frozen rows, recorded before any grading:

- `interval_rq` p10 is **not floored at zero** (e.g. a 2.7-point RB carries
  p10 −3.7). A negative floor always "covers", which flatters coverage for
  low-mean players. Study (iii) is graded in the `hist_17_plus` bucket,
  where means are larger, and must also report coverage by projected-mean
  tercile so this artifact is visible rather than averaged away. Flooring
  is a construction change and would be a new variant.
- `env_baseline` differs from the ledger's context-free `baseline` field
  (2.74 vs 2.85 on the same row), which is the whole reason it exists.

## Honest prior

(i) is the one with a mechanism: the clamp binds on 7 of 32 teams today and
a 22.5 constant ignores that a 27-point team and a 17-point team get the
same factor from the same implied total. (ii) is worth ~0.07 DK team points
of MAE by the opponent study's own numbers, so a PASS at RB would still be
small. (iii) is expected to pass on coverage — the shadow candidate's
residual-quantile p10 is already calibrated — and is a calibration fix, not
an accuracy one. (iv) is expected to fail: v3's 1–5 game players are mostly
rookies with real usage, and the v4 study already found that pulling them
toward a prior hurt.

## Commands

```bash
python -m ingest.nfl_dfs_shadow                  # freezes context_variants with every shadow row
python -m pytest tests/test_nfl_dfs_environment_variants.py -q
```
