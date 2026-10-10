# Local longest-touchdown research model

Status: implemented locally, exploratory, no production/optimizer integration.
Owner: `model/nfl_longest_touchdown.py`; capture/forecast/evaluation CLI:
`research/nfl_longest_touchdown.py`. Source joins are documented in
`docs/nfl-team-identity-source-map.md`.

## What it predicts

For a supplied two-team game and eligible roster, simulate regulation possessions
and retain each player's longest **rushing or receiving** touchdown. Output each
player's probability of any touchdown, a 20+ yard touchdown, a 40+ yard touchdown,
sole longest touchdown, longest including ties, and fractional longest-TD win
share. A no-scrimmage-touchdown outcome is separate; non-scorers never tie for
first at zero. Fractional shares plus the no-TD probability sum to one.

This is a scrimmage-only scope, not a forecast for markets including returns or
defensive touchdowns. It does not publish betting edges or calibrated fair odds.

## How it works

1. Validate canonical game/season/week/home/away joins and unique play identities.
   Reject target/future games and, in strict mode, labels after the decision.
2. Verify offensive scoring from descriptions plus field position/yardage agreement
   and an unambiguous GSIS receiver/rusher. Exclude no-play, overturned, lateral,
   fumble/recovery, return and unverified scoring rows. Report exclusions.
3. Estimate touchdown probability per target or carry within starting-field bins:
   1–5, 6–10, 11–20, 21–39, 40–59 and 60–99 yards from the goal. Include non-scoring
   opportunities in the denominator. Blend player rates with position-peer and
   league-role rates so a player's zero recent long TDs need not imply zero risk.
4. Apply a shrunk opponent TD-risk factor for the same opportunity type/field bin.
   Compare each current-season offense against the defense with its opportunities
   against other defenses. This is an outcome association, not a causal effect.
5. Simulate snaps using empirical down, distance, field, score-state and late-game
   action cells, with team/league smoothing. Player role shares use current team
   history when available, conditioned on field and score state with overall-role
   fallback. Trailing does not automatically mean deeper throws: non-scoring gain
   templates are empirical player/peer plays conditioned on field and score state.
6. Update down, yards to gain, field, remaining clock and score. A touchdown's
   simulated distance is exactly its starting distance to the end zone. Ordinary
   gains cannot silently turn into touchdowns. Keep sample snap ledgers for review.
7. Grade chronologically fitted forecasts against actual scoring outcomes and a
   simpler independent-player TD-count/distance baseline. Report top-choice hits,
   multiclass Brier score, log loss and calibration bins. Lower Brier/log loss is
   better; a larger simulation is not empirical validation.

## Local commands

Run from the repository root. Existing dependencies suffice; no schema migration
or database write is required. Database capture needs configured read access.

```powershell
python -m research.nfl_longest_touchdown capture --minimum-season 2023 --maximum-season 2026 --output artifacts/nfl-longest-td/input.json.gz
python -m research.nfl_longest_touchdown forecast --input artifacts/nfl-longest-td/input.json.gz --request artifacts/nfl-longest-td/request.json --draws 2000 --sensitivity --output artifacts/nfl-longest-td/forecast.json
python -m research.nfl_longest_touchdown backtest --input artifacts/nfl-longest-td/input.json.gz --season 2025 --start-week 5 --end-week 8 --draws 2000 --output artifacts/nfl-longest-td/evaluation.json
# Before kickoff: freeze the challenger baseline from the forecast's own input and request.
python -m research.nfl_longest_touchdown baseline --input artifacts/nfl-longest-td/input.json.gz --request artifacts/nfl-longest-td/request.json --output artifacts/nfl-longest-td/baseline.json
# After the game: capture fresh PBP, then grade the frozen forecast (and baseline).
python -m research.nfl_longest_touchdown grade --input artifacts/nfl-longest-td/postgame.json.gz --forecast artifacts/nfl-longest-td/forecast.json --baseline artifacts/nfl-longest-td/baseline.json --output artifacts/nfl-longest-td/grade.json
```

`baseline` refuses to run at or after kickoff and records the same source and
request digests as the forecast. `grade` returns one of three statuses:
`graded`; `outcome_unknown` when the target game has no PBP (never a no-TD
result); or `needs_review` when any run/pass play in the target game mentions a
touchdown but failed scoring verification (reversal text, lateral, fumble,
yardage mismatch). That play could be the real longest TD, so it is never scored
silently. A winner outside the modeled roster is graded as `OTHER:TEAM`, which
every forecast prices at zero, so its log loss hits the 1e-12 floor (about 27.6)
and `winner_was_unmodeled` is set.

Outputs are exclusive-create; choose a new filename for every new run. Saved
source, request and implementation digests identify the inputs/logic used.
Sensitivity changes peer/defense smoothing; its ranges are not confidence intervals.

Request format (GSIS IDs must match participants; target must match the captured
canonical schedule):

```json
{
  "decision_at": "2026-10-05T22:25:08+00:00",
  "game": {
    "game_id": "2026_04_ATL_NO", "season": 2026, "week": 4,
    "kickoff": "2026-10-06T00:15:00+00:00", "away": "ATL", "home": "NO"
  },
  "players": [
    {"identity": "GSIS_ID", "name": "Player name", "team": "ATL", "status": "active"},
    {"identity": "OTHER_GSIS_ID", "name": "Player name", "team": "NO", "status": "out"}
  ],
  "roster_evidence": {"description": "Record source IDs, timestamps and unresolved roles here"},
  "retrospective": false
}
```

Supply the full eligible field on both teams, including supported rushing QBs.
`active` is a caller assumption, not something this model verifies. Use the
repository's roster/injury evidence workflow before making a current-game claim.
Unsupported new players appear in `unresolved_players`, without invented named
probabilities. Missing team-role support uses visible `OTHER:TEAM` residual
scorers. When inactive players are removed, historical supported-role weights
are renormalized among the supplied eligible players; no injury-specific
replacement workload forecast is learned.

## Historical and modeling limits

- Historical PBP labels were captured later. Walk-forward fitting excludes future
  games, but deliberately allows later corrections and reconstructs eligible
  players from prior three-game usage. It is **retrospective research**, not an
  archived pregame forecast with verified inactives. Missing actual PBP is unknown,
  never a no-TD result.
- Positions come from season/week/GSIS historical roster rows or unambiguous
  same-season `ff_players` mappings. Missing/conflicting positions remain UNKNOWN.
- A score-state model is implemented, but field bins and all smoothing amounts are
  assumptions. It has not passed a calibration/promotion gate.
- Kicks, possession starting fields, clock, approximate timeout use and extra
  points are approximate. There is no overtime, timeout inventory, two-point,
  safety, return-TD or penalty-resolution simulation. Defensive/return scores
  excluded from scoring can also affect real game script; this is a material limit.
- No Vegas inputs, direct weather effects or quantified defender-absence effects.
- `simple_baseline` draws independent player TD counts and empirical TD distances.
  It is a comparison model, not a production probability benchmark. Calibration
  rows are correlated within games; bin counts are not independent sample sizes.
- Zero wins in a finite Monte Carlo run is not proof of zero underlying probability.
  Log-loss scoring uses a disclosed numerical floor of 1e-12.

## Acceptance checks and next gate

Focused tests cover scoring vs long gains, nullified/return/ambiguous scores,
canonical joins, label/future-game leakage, rare-event priors, score-state play
choice, distance geometry, tie/no-TD treatment, missing actuals, inactive and
unsupported players, repeatability and immutable artifact files.

The local real-data capture contains 879 regular-season games in 2023–2026. The
position-enriched pre-Oct-5 sample has 4,129 verified offensive touchdowns, 217
excluded return/complex scores, and 86 attributed plays lacking a mapped position.
These are capture-specific coverage facts, not constants in the model.

Before using these outputs for decisions: review scoring exclusions and roster
coverage, run larger untouched-season evaluations, assess parameter sensitivity
and calibration against the baseline, then retain frozen forward forecasts with
verified availability. Existing example/smoke artifacts are development evidence;
the initial smoke input without historical positions is superseded for interpretation.

## First frozen Thursday forward test

The October 8, 2026 TB–DAL uploaded showdown slate is frozen locally under
`artifacts/nfl-longest-td/thursday-20261008/`. Start with `report.md` and
`test-manifest.json`; `forecast.json` is the primary prediction for later grading.
The request decision time is October 6 at 13:49:36 UTC. The refreshed source
contains 880 games, including all 64 current-season games through Week 4.

Five runs use 2,000 simulations each: standard settings, two smoothing variations,
TB offense restricted to Week 4, and disputed Ko Kieft availability included.
The Week-4 restriction changes player scoring evidence and team tendencies
together; it does not isolate a causal quarterback effect. Kieft inclusion leaves
the known-player results unchanged because his current role is unmodeled.
Neither result resolves replacement workloads or future Tucker usage.

Both published depth sources and week-matched injury observations are retained,
including source timestamps and disagreements. No Thursday inactive list yet
exists at this decision time. Ten eligible players lack historical team-role
support; the probability field is conditional on modeled roles and does not
reserve quantified probability for every unsupported player's possible work.
Do not interpret unresolved names or unchanged Kieft results as proven zeros.

The challenger baseline was frozen on October 6 (before kickoff) from the same
input and request (`baseline.json`; digests match `forecast.json`). It also puts
CeeDee Lamb first, at 20.0% versus the model's 14.3%, so a Lamb result cannot
separate the two models on top choice; Brier score and log loss can, slightly.
The 7 MB PBP inputs stay local and are identified by `source_sha256`.

For the postgame audit, capture PBP after the game and run `grade` on
`forecast.json` with `baseline.json`; do not select the best sensitivity after
seeing the result. The outcome is pending and no calibration gate has passed.

## Improvement round 1 — newcomer reserve (registered 2026-10-06, before results)

**Defect.** v1 gives every team opportunity to a named, supported player. A
scorer outside that list (a depth player, an in-season newcomer, one of the ten
unsupported Thursday names) therefore has probability exactly zero, and when one
wins, log loss hits the 1e-12 floor (~27.6). The first two-game 2024 check hit
this on TB@ATL week 5 (KhaDarel Hodge's 45-yard overtime TD; he was outside the
inferred roster). Overtime is not simulated but is graded, a known mismatch.

**Fix (`Settings.newcomer_reserve`; default since 2026-10-09 after the 2025 confirmation, `--no-newcomer-reserve` reproduces v1; frozen v1 artifacts are preserved).**
`OTHER:TEAM` receives, per action, the share of team carries/targets that went
to players absent from that team's prior three same-season games. It is computed
from the pre-decision training rows only. Measured on the Thursday input it is
4.8% of carries and 4.0% of targets; pooling across seasons would have inflated
it with offseason churn, so the estimate is within team-season.

**Evaluation (frozen before any result is read).**
- Development: 2024 weeks 4–18, current v1 vs v1 + reserve, same games, same
  seed, 2,000 draws. Adopt the reserve only if its mean log loss is lower.
- Confirmation: 2025 weeks 4–18, untouched by any tuning. Report the paired
  model-minus-v1 log-loss difference with a game-level bootstrap 95% CI, and the
  same comparison against the independent-TD baseline. Promotion of the
  reserve as default requires the 2025 CI to exclude zero in its favour.
- Always reported, gating nothing: Brier, top-choice hit rate, how often the
  winner was priced at zero or was `OTHER`, and per-player any-TD and 40+ TD
  calibration (many more outcomes than one winner per game).
- Retrospective runs reconstruct rosters from prior usage and use corrected
  PBP; they are research evidence, not archived pregame forecasts.

### Development result (2024 weeks 4–18, 224 games, read 2026-10-06)

| | v1 | v1 + reserve |
|---|---:|---:|
| mean log loss | 4.566 | **3.043** |
| median log loss | **2.769** | 2.810 |
| mean log loss, floor at 1/2000 instead of 1e-12 | 3.268 | **3.043** |
| winner priced at exactly zero | 14 | **0** |
| mean Brier | 0.928 | 0.925 |
| top-choice hit rate | 9.8% | 10.3% |
| any-TD Brier (5,562 player-games) | 0.1203 | 0.1202 |

Paired reserve-minus-v1 log loss: −1.52, 95% CI [−2.32, −0.80]. Per the rule,
the reserve is adopted for the 2025 confirmation. Read it honestly: the whole gain
comes from removing 14 zero-priced catastrophes. The reserve is slightly worse in
a typical game (median difference +0.04; better in 83 of 224), because it moves
about 4–5% of probability away from named players. It is insurance against
blow-ups, not a sharper forecast.

Two findings outside the registered question, recorded as observations only:
- **v1 was not clearly better than the simple baseline**: model minus baseline
  −0.07, CI [−0.31, +0.07]. The reserve's large lead over the baseline (−1.59)
  is mostly the same zero-pricing defect, which the baseline also has, so it is
  not evidence that the simulation engine beats a simple TD-count model.
- Per-player any-TD probabilities are well calibrated from 0% to 50%, but the
  ≥50% bin is overconfident (predicted 0.53–0.54, observed 0.37–0.40, n=73–100).
  The heaviest-usage scorers are overpriced. Not acted on; a separate study.

### Why v1 does not beat the simple baseline (diagnosed 2026-10-06, 2024 dev games)

1. **On a fair scale it is a tie, not a loss.** Both models price some actual
   winners at exactly zero (model 16 games, baseline 17), and the 1e-12 floor
   lets those few games dominate the mean. With both floored at 1/2000 instead:
   model 3.240, baseline 3.219. Probability on the actual winner: 6.8% vs 7.1%.
2. **The two models mostly agree.** Player win shares correlate 0.875. Both are
   built from the same ingredients (workload, TD rate per opportunity, empirical TD
   distances); field position, score state and defense change the ranking little.
   Margin is no better by winning distance (1–9, 10–19, 20–39, 40+ all within ±0.06).
3. **The model has better workload information, then gives it back through
   bias.** Per-player opportunity MAE is 2.59 vs 2.71 for the baseline's
   three-game average, and its calibration slope is better (0.82 vs 0.79). But it
   hands named players 4.82 opportunities a game against 4.40 actual (+9.5%),
   which inflates TD chances where it matters: the top usage player's any-TD is
   48% predicted vs 40% observed, its 15–25% win-share bin wins 8.5% of the time
   (n=80, baseline 13.3%), and no-TD games are priced at 0.4% vs 0.9% actual.
4. **The +9.5% has three measured sources:**
   - ~4.5%: newcomers and depth players (the missing `OTHER` share). The reserve
     brings named workload to 4.61.
   - ~2%: every non-sack pass gets a receiver. 3.8% of 2024 pass plays have no
     targeted player (throwaways and similar); sacks are handled, these are not.
   - ~2%: slightly too many plays (62.3 run+pass per team vs 61.1 actual).
5. **Ceiling.** Even a good forecast puts only ~7% on the realised winner; most
   of the outcome is noise, so a real edge over the baseline will be small and
   needs many games to show.

Not yet fixed or tested: dropping the receiver on no-target pass templates, and
the play-count excess. Each needs its own development run before use.


## Correctness review and v2 fixes � October 7, 2026

The original Thursday prediction and baseline are retained unchanged. Version 2
is a revised experimental implementation, not a replacement of the saved forecast
or a validated probability model.

- Final grading requires a completed canonical schedule with final scores,
  matching captured play count, quarter coverage and regulation-end evidence.
  A Q4 zero-clock row or an overtime END GAME marker provides end evidence.
  Missing coverage stays `outcome_unknown`; ambiguous scoring stays `needs_review`.
  This checks internal coverage and a terminal boundary, not an independent
  guarantee that the upstream provider supplied every middle-of-game event.
- Actual ties are determined per individual scorer before aggregating unknown
  identities into OTHER. Multiple unknown winners keep their correct total credit.
  The simulation still pools residual scorers; their internal tie structure is
  approximate and needs separate research if materially relevant.
- No-target pass templates do not create receiver or OTHER target opportunities.
  Missing attribution and genuine throwaways both remain visibly unattributed;
  missing attribution is not proof that no real target existed.
- Both training and grading now exclude OT explicitly using quarter. This is a
  regulation-only model. Markets including OT, defensive or return scores require
  a different scope and must not use these results as matching probabilities.
- Late leads no longer end automatically at two minutes. A conservative kneel
  budget uses downs and remaining opponent timeouts. The simulated defense starts
  each half with three timeouts and uses them to stop late leading possessions.
  Earlier tactical timeout use is not modeled. Halftime receipt is fixed from the
  initial receiver instead of whichever side held possession at the interval.
- The optional newcomer allowance is applied to the independent baseline too;
  named work is reduced and OTHER receives corresponding opportunities. The
  allowance remains a league pooled research assumption, not a named-player
  replacement-workload forecast.
- Baseline grading verifies matching game, decision time, source and request.

The other agent's documented 224-game development numbers have no saved game-level
report or study script in this checkout. They are not independently verified by
this review and must not be used as release evidence. The revised runner below
freezes its registration before evaluating and saves both complete game-level
reports, skipped games, implementation/source digests, paired differences and a
game-level bootstrap. Small samples and bootstraps are development checks, not
calibration evidence. The earlier 2025 confirmation is not claimed complete.

```powershell
python -m research.nfl_longest_touchdown capture --minimum-season 2023 --maximum-season 2026 --output INPUT-V2.json.gz
python -m research.nfl_longest_touchdown_comparison --input INPUT-V2.json.gz --season 2024 --start-week 5 --end-week 5 --limit 8 --draws 300 --output COMPARISON.json
```

Acceptance for this correction batch: regressions reproduce and reject partial
actuals, retain individual tie shares, eliminate invented no-target opportunities,
exclude OT consistently, respect kneel budgets, fix halftime receipt, and preserve
probability accounting in both reserve variants and the matched baseline. Source
capture and historical/forward grading must agree on quarter and final coverage.
Larger evaluations, workload/QB changes, air-yard/YAC features and uncertainty in
fitted scoring rates remain separate research; no optimizer/export authority is
introduced by this batch.


## Production research page

The first web release is `/nfl/longest-touchdown`, linked from NFL navigation.
It displays a dated, experimental, regulation-only forecast, separate from DFS
point projections. `web/src/data/longest-touchdown.json` is a reviewed publication,
not a live source or an automatically refreshed projection. Later injuries and
role changes are not silently included. After kickoff the page labels it as a
saved pregame forecast; it does not pretend the game was graded.

`research/nfl_longest_touchdown_publish.py` validates the outcome scope,
probability accounting, matching support-audit decision time and no-market-input
policy before producing the public snapshot. Unsupported current roles are
shown as unresolved rather than reliable zero-probability players. For a refresh,
freeze new PBP and both roster/injury providers, create a new request/support
audit, forecast with the documented CLI, then publish to a new filename for
review and update the page's imported snapshot. Exclusive-create prevents
accidental replacement of frozen evidence:

```powershell
python -m research.nfl_longest_touchdown_publish --forecast FORECAST.json --support SUPPORT.json --output PUBLICATION.json
```

The initial page uses v2 with the newcomer reserve enabled, frozen October 6
roster evidence and strict label cutoffs. Its revised source capture excludes
1,145 later-labeled plays, leaving 873 training games. The publication does not
supersede the original Thursday v1 prediction. Source/request/implementation
hashes are in the saved v2 forecast. This is an explicitly experimental release
at the owner's request; no empirical calibration or profitable-edge gate is
claimed passed and no optimizer weighting changes are introduced.
