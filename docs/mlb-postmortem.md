# MLB Postmortem Framework

## Purpose

The MLB Postmortem is no longer only a projection accuracy report. Its job is to validate whether the full DFS decision stack is trustworthy:

- Projection layer: are we independently accurate, or are we leaning on fallback sources?
- Ownership layer: are we modeling field behavior well enough to identify leverage?
- Signal layer: are badges and heuristics calibrated to DFS outcomes?
- Exploit layer: are actionable pitcher flags supported by projection trust, ownership, and context?
- Decision layer: are our pools capturing slate-winning outcomes before lineup EV is fully modeled?

The report should make failure visible. If a section can look good accidentally because of fallback contamination, small samples, or misaligned outcomes, the UI should expose that risk directly.

## Projection Independence

Projection improvement is only meaningful if it is independent. The postmortem must show source attribution for every effective projection.

Tracked sources:

- `live`: a live/post-processed projection was available.
- `our`: the raw internal projection was used as the effective projection.
- `linestar`: LineStar was used as fallback.
- `unknown`: no projection source was available.

Required metrics:

- Raw MAE: `our_proj` only.
- Final MAE: effective projection after live/internal/LineStar fallback.
- LineStar MAE: external benchmark.
- Source coverage percent by window.
- Fallback dependency percent.
- Our-source MAE: rows where the effective source is `our`.
- Non-LineStar MAE: rows where the effective source is not LineStar.
- Fallback MAE vs non-fallback MAE.
- Blend uplift: raw internal MAE minus final MAE.

Warning rules:

- Show `Model independence compromised` when LineStar fallback dependency is greater than 30%.
- Show an independence warning when non-LineStar MAE is more than 1.0 DK point worse than final MAE.

These warnings are intentionally strict. If fallback rows are carrying the result, the model has not proven independent edge.

## Ownership Behavior

Ownership is treated as a market-modeling problem, not just a regression problem. Projection edge is not useful if ownership errors misclassify chalk and leverage.

Required metrics:

- Ownership MAE, bias, and correlation.
- Chalk capture at 20%, 30%, and 40% actual ownership.
- False-low misses: players projected below a chalk threshold who became chalk.
- False-chalk misses: players projected chalk who were actually low-owned.
- Top-5 and top-10 ownership overlap.
- Rank correlation over the top ownership pool.
- Leverage error rate for high-impact players.

Current high-impact definition:

```text
high_impact =
  projection quartile = top 25%
  OR salary >= 7000
  OR pitcher
```

Current leverage error definition:

```text
abs(projected_ownership - actual_ownership) >= 10 percentage points
```

This is intentionally decision-focused. A 10-point ownership miss on a low-impact punt is less damaging than the same miss on a top projection, chalk pitcher, or expensive hitter.

## Signal Calibration

Badges should be presentation labels, not the model. The underlying model should move toward calibrated probabilities.

Current available DFS utility tracking:

- `P(15+)`, `P(20+)`, and `P(25+)` proxies by observed outcome rate.
- Lift versus the relevant baseline group.
- Average actual ownership.
- Average projection miss.

Current signal groups:

- HR Badge 25%+
- Strong HR Badge 35%+
- Blowup Top 12
- Pitcher 18+ Projection
- Pitcher 2.5x+ Value

Important limitation:

The database currently stores `hr_prob_1plus`, but it does not store an actual MLB home run result column. That means the postmortem can evaluate HR badges as DFS utility signals, but it cannot yet grade pure HR skill calibration with Brier score or HR-rate calibration buckets. Add an actual HR outcome field before claiming HR probability calibration.

Future skill-calibration metrics:

- `P(HR)` calibration buckets.
- Actual HR rate by probability bucket.
- Brier score.
- Lift versus baseline HR rate.

## Pitcher Exploit Watch

Pitcher exploit candidates are surfaced before full signal calibration because they can be made actionable with existing data.

Current criteria:

- Pitcher only.
- Projection at least 14 DK points.
- Projected/field ownership no higher than 18%.
- Salary available.
- Favorable context when available: opponent implied total no higher than 4.5 or moneyline favorite at -120 or better.
- Extra trust when the effective source is not LineStar fallback.

The exploit watch is not a recommendation engine yet. It is a postmortem audit that shows whether low-owned, context-supported pitchers are converting.

## Decision Capture

Decision-level metrics should be logged early, even before lineup EV is fully modeled. The first version tracks whether our player pools are capturing slate-winning tails.

Tracked outcome buckets:

- Top 1% actual scorers.
- Top 5% actual scorers.
- Top 10% actual scorers.

Tracked capture pools:

- High-projection pool: top 10% by effective projection.
- Ceiling pool: top 10% by ceiling proxy or HR badge hitter.
- Leverage pool: field ownership no higher than 10% and top projection quartile.

These metrics create a baseline for future lineup EV and ownership-adjusted ROI work.

## Implementation Order

1. Projection independence and warning states.
2. Ownership behavior diagnostics.
3. Pitcher exploit watch.
4. Decision capture baseline.
5. Signal probability calibration.
6. Lineup-level EV and ownership-adjusted ROI.

## Interpretation Rules

- Do not celebrate final MAE if fallback dependency is high.
- Do not trust leverage decisions if chalk capture and top ownership overlap are weak.
- Do not use badges as decision-grade signals until they show lift versus baseline and calibration.
- Do not compare hitter and pitcher results with the same standard: pitchers are closer to an accuracy problem, hitters are a ceiling-distribution problem.
