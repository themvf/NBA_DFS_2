# MLB O/U Vegas Page Changes

Date: 2026-05-17

## Summary

This note documents the Phase 1 improvements to the MLB over/under recommendation score used on the Vegas page.

The goal was to improve the page-level `O/U` recommendation score without changing schema or introducing a larger model rewrite. The work focused on using MLB context already present in the database but previously unused by the page.

## What Changed

The MLB `O/U` score now blends in:

- starter xFIP context
- starter strikeout profile via average `K/9`
- park run factor
- temperature
- directional wind when the feed clearly indicates wind blowing `in` or `out`
- existing total-tier calibration
- existing home/away team over-history with shrinkage

The MLB matchup query now exposes:

- `parkRunsFactor`
- `weatherTemp`
- `windSpeed`
- `windDirection`

These fields were already supported by the database layer and did not require schema changes.

## Why This Was Done

The prior page score was still too dependent on broad historical rates:

- total-tier history
- team over-history
- a single starter-quality signal

That was a reasonable baseline, but it was missing obvious MLB-specific context that materially affects totals:

- park environment
- weather
- strikeout suppression / run prevention traits

Phase 1 closes that gap while keeping the implementation simple and auditable.

## Backtest Readout

Walk-forward MLB `O/U` results after the Phase 1 update:

### Full sample

- legacy accuracy: `51.56%`
- current accuracy: `52.37%`
- legacy Brier: `0.2500567`
- current Brier: `0.2495344`

### Actionable only

- legacy accuracy: `52.07%` on `169` picks
- current accuracy: `53.17%` on `126` picks
- legacy Brier: `0.2501065`
- current Brier: `0.2480377`

## Interpretation

This is a real improvement, but it is not the final MLB `O/U` architecture.

What improved:

- overall calibration
- actionable pick quality
- use of real MLB game-environment context

What remains limited:

- no bullpen context
- no lineup/offense-strength context
- simplified wind interpretation
- still a hand-weighted score, not a residual-over-Vegas model

## Next Steps

Recommended order for the next iteration:

1. Add lineup/offense context
2. Add bullpen fatigue/depth context
3. Improve wind handling beyond simple `in/out`
4. Replace the hand-weighted blend with a residual model over the Vegas total

