# NFL DFS field-ownership model

Registered 2026-09-27, before any fitted model existed and with one Classic
slate of actual ownership in hand.

## Why

Every leverage feature in the NFL optimizer — the ownership penalty, the
chalk-fade archetypes, the field-duplication estimate — is gated on ownership
being present and validated. With no ownership the server resolves capability
`unavailable` and the optimizer maximises raw projection. Ownership is the
missing input, not a new feature.

## Phase 1 — stated prior (`nfl-ownership-prior-v1`, shipped 2026-09-27)

`web/src/lib/nfl-dfs/ownership-prior.ts`. Deterministic and pure. The field
drafts points and value, anchored on DraftKings' printed season average, and
concentrates on the top few at each position:

```
blend = 0.6 × our projection + 0.4 × DK season average
score = blend² × (blend per $1K)^1.5 × status   (Q 0.6, D 0.25; OUT 0)
share = score / Σscore within position × position budget, capped 60%
budgets: QB 100 · RB 255 · WR 340 · TE 105 · DST 100  (= 900, FLEX split 55/40/5)
showdown: flex 500 (cap 90), captain 100 on score^1.5 (cap 50)
```

Every constant is a judgement. The prior declares itself heuristic, so
`assessOwnership` caps it at `heuristic_uncalibrated`: leverage runs only
through the user's explicit opt-in and is labelled "Uncalibrated estimate";
the duplication model stays off. LineStar ownership, when present, takes
precedence (`ownSource`).

## Phase 2 — fitted model and promotion gate (frozen now)

Port `model/mlb_ownership_model.py` (`mlb_ownership_v1`: Ridge on log
ownership, slate-relative rank features, one model per role, artifact read by
Python and the web app; scored corr 0.75 / MAE 0.96 pts on 13,404 MLB
player-slates) to NFL positions QB, RB, WR, TE, DST.

Training data: `nfl_dfs_field_ownership` joined to the slate each contest was
imported against. The largest-field Classic GPP each week is the reference
contest; Showdowns are a separate model with Captain and Flex targets.

**Gate to `validated` (unlocks leverage in production without opt-in):**

| requirement | threshold |
|---|---|
| held-out Classic slates | ≥ 4, holdout by slate, never by row |
| Spearman rank correlation, pooled over held-out slates | ≥ 0.70 |
| mean absolute error, pooled, percentage points | ≤ 2.0 |
| bias, pooled | within ± 0.5 |
| coverage / mass / sum checks in `assessOwnership` | unchanged |

The thresholds do not move. NFL ownership is more concentrated than MLB's, so
a higher point error than MLB's 0.96 is expected; 2.0 was set on that basis
before any NFL number was computed. If the fitted model fails, the prior stays
heuristic and the gate stays closed — no re-slicing to the weeks it fits best.

**Governance fix required with Phase 2:** `assessOwnership`'s `validated`
state is a coverage check plus the source not declaring itself heuristic. It
has no accuracy test. Before any source can be promoted, the calibration
record above must be wired in as a precondition, so promotion is by evidence
rather than by declaration.

## Grading

`npm run calibrate:nfl-ownership` grades whatever is deployed against every
imported contest. Descriptive for the prior (it was designed against those
slates); the holdout discipline above applies only to the fitted model.

Import the largest GPP entered each week on the Results step; that is the
training set.

## First grading — 2026-09-27, prior v1 against every imported contest

Constants were set from judgement before this was run; nothing was tuned on
these slates.

| contest | n | Spearman | MAE (pts) | bias |
|---|---:|---:|---:|---:|
| wk2 Classic, 317,082 entries | 670 | **0.80** | **1.03** | 0.00 |
| wk2 Showdown NYG@LAR, 47,562 | 53 | 0.83 | 7.93 | 0.07 |
| wk3 Showdown ATL@GB, 83,234 | 53 | 0.75 | 8.22 | 0.06 |

By position on the Classic slate: TE 0.85, WR 0.81, RB 0.79, QB 0.74,
**DST 0.33 (MAE 4.07)**. For reference `mlb_ownership_v1`, a fitted model,
scores 0.75 / 0.96 on 13,404 rows.

The Classic number clears the Phase 2 gate on one slate; the gate needs four
held out, and this one is descriptive. What the misses say, recorded as
**hypotheses for the fitted model**, not as edits to the prior:

1. **The field follows the market's projection, not ours.** Every large
   over-projection of ownership is a player our model likes more than the
   consensus (Swift 31.6% vs 7.1%, Coker 38.9% vs 17.1%, Watson 34.1% vs
   14.0%, Smith-Njigba 27.8% vs 10.7%); every large under-projection is a
   value or news play the field jumped on that our model did not (Schultz
   $3,200 18.4% vs 3.2%, Aaron Jones 20.0% vs 2.3%). The 0.6 weight on our
   projection is too high for predicting the field; the fitted model should
   carry the DK average, a consensus projection, and our-vs-consensus gap as
   separate features.
2. **DST ownership is a Vegas decision.** The field drafts defenses on spread
   and opponent implied total (Buccaneers 13.9%, 49ers 10.9%); the prior
   has no line features. Add spread and opponent implied total.
3. **Showdown punts.** Fields punt cheap Flex plays far more than points²
   allows (Ferguson $1,800 TE 21.7% vs 1.9%; Corum 28.8% vs 6.9%). A
   salary-tier feature, and a separate kicker/DST treatment, are needed.
4. **Availability at grading time must be the slate's own.** Puka Nacua drew
   0.8% because he was out; the historical upload still carried him active.
   The fitted model must grade against the availability decision pinned on
   that slate, never today's.

Live check on the 2026-09-27 Classic slate: 671 of 671 players carry a prior,
sum 900.0%, source `nfl-ownership-prior-v1`; top chalk Smith-Njigba 53.5%,
St. Brown 51.0%, Henry 38.9%, Allen 18.5% — the same WR-heavy,
QB-light skew as lesson 1, visible before results and left unchanged on
purpose.

## Field structure from the standings lineups — 2026-09-30

`model/nfl_dfs_field_structure.py` (`nfl-dfs-field-structure-v1`) summarizes each
imported contest's full lineups into `nfl_dfs_field_structure`: duplication, who
fills the field, and pair co-ownership. Backfill an already-imported contest with
`python -m ingest.nfl_dfs_field_audit --contest FILE --structure-only` (refuses a
file whose digest differs from the one imported); a fresh `--contest` import now
stores it automatically. The lineups themselves are not stored; the file is the
source. Stored entries run 0.06-0.65% under `entry_count`: those are unfilled
entries (blank lineup, score 0), not parse losses.

| contest | entries | unique lineups | entries in a duplicated lineup | users with 20+ entries -> share of entries |
|---|---:|---:|---:|---|
| wk2 Classic | 316,828 | 299,470 | 8.3% (max 156 copies) | 4,892 -> 31% |
| wk3 Classic | 7,130 | 6,710 | 8.6% (max 25) | 159 -> 45% |
| wk2 Showdown | 47,255 | 9,382 | **89.7%** (max 556) | 1,440 -> 61% |
| wk3 Showdown | 82,798 | 12,242 | **93.3%** (max 394) | 1,989 -> 48% |

Read these as description, not a model. Two things worth carrying forward:

- **Showdown is a duplication contest.** Roughly nine in ten entries share a
  lineup, and 98% of the top 1% do. Classic is the opposite (8-9%), so any
  duplication model must be fitted per format, never pooled.
- **Stacks are visible only at pair level.** Classic QB pairings sit at 3.4-4.3x
  independence (Mayfield-Egbuka 4.3x, Wentz-Jefferson 4.1x, Purdy-Evans 3.7x)
  while the most-owned pairs sit near 1.0x. Pairs are limited to the top 40
  players by ownership, so tail stacks are not measured.

Sample: 2 Classic and 2 Showdown contests. Still short of the 4 held-out Classic
slates the Phase 2 gate needs; this adds labels per slate, not slates.

FantasyCruncher (the 2025 archive's `fc_catalogue` points at it) was probed: its
public pages are a link index with no ownership numbers, so 2025 ownership
history is not recoverable from public pages. Not pursued further.

## Showdown value exponent: research candidate (2026-10-04)

`model/nfl_showdown_ownership_eval.py` and its read-only runner compare the
current v2 value exponent (1.5) with a candidate exponent (0.5). The runner
builds equal-size Showdown portfolios and measures projected points retained
against actual contest-lineup duplication. It also reports ownership error and
chalk ranking. The production prior remains v2.

The candidate improved flex and captain ownership error and chalk rank on two
fit contests (weeks 2-3) and two subsequently imported week-4 contests. On the
week-4 contests, flex MAE fell 4.35 to 3.82 and 5.23 to 4.61; captain MAE fell
1.63 to 1.49 and 1.38 to 1.04. These are four contests in one format. The
portfolio trade-off was roughly neutral: the candidate kept more projected
points but produced slightly more duplication. It also missed true punts and
overestimated kickers. Treat it as an evaluation candidate, not a promoted
lineup input. Reassess on additional saved contests before changing the live
Showdown prior.
