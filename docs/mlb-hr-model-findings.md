# MLB HR Model — Improvement Findings

_2026-06-18. Tested whether the HR model (`mlb_homerun_v2`) can be improved, after
the model assessment flagged it as "heuristic-equal, tapped out at ~2× lift" and
recommended adding Statcast batted-ball features._

## Hypothesis tested

The assessment recommended new features — specifically **Statcast batted-ball
quality (barrel%, hard-hit%, exit velo, xISO/xSLG)** — as the path to beat the
~AUC 0.62 / 2× lift ceiling, since the model only uses outcome rates (HR/g, ISO,
SLG) and no contact-quality data.

## Experiment (clean, no look-ahead)

- Pulled Baseball Savant batter leaderboards (barrels/PA, barrel%, hard-hit%,
  avg/max EV, avg HR distance, sweet-spot%, xSLG, xwOBA) for 2023 + 2024.
  FanGraphs is 403-blocked from here; **Baseball Savant CSV works**.
- Joined **prior-season** values to each training game by `hitter_mlb_id`
  (2023→2024 games, 2024→2025 games) — power is a stable skill, so prior-season
  is an honest pregame signal with zero look-ahead. **Coverage: 87.5%** of rows.
- Logistic, 80/20 chronological split (n=19,522 holdout, 11.3% HR base rate).

## Result — batted-ball does **not** help

| Feature set | AUC | AP | Top-15/day hit | Lift |
|---|---|---|---|---|
| Base (HR/g, ISO, SLG, wRC+, pitcher HR/9, park…) | 0.672 | 0.203 | 27.1% | 2.40× |
| Base **+ batted-ball** | 0.671 | 0.203 | 26.8% | 2.37× |
| Batted-ball **only** | 0.600 | 0.160 | 20.5% | 1.81× |

Adding barrel%/EV/xSLG changed nothing (marginally worse). **Why:** the base
already includes current-season rolling **HR/g, ISO, SLG** — which measure the
same underlying power skill barrel% does. Prior-season barrel% is redundant; the
rolling power rates already capture it. Batted-ball *alone* is worse than the
rolling rates, confirming it carries no orthogonal single-game signal.

## Conclusion

**The HR model is at its practical ceiling.** Single-game HR is a rare,
high-variance event; ~AUC 0.67 and ~2.4× lift on top daily picks is near the
achievable limit for public-data HR models, and contact-quality data does not
move it because the power signal is already priced in via rolling ISO/HR-rate.

**Do not** build a Statcast batted-ball ingestion for the HR model — tested,
no payoff.

## Where the remaining upside actually is (not AUC)

1. **Usage, not ranking.** The 2.4× lift is already useful — the leverage is in
   how it feeds GPP leverage/stacking and ceiling, not in chasing AUC.
2. **Calibration.** Verify predicted P(HR) is well-calibrated (reliability curve /
   Brier by bucket) — a calibration fix helps EV even with AUC fixed. The
   postmortem doc already notes an actual-HR outcome column is needed for this.
3. **Small-sample early season** is the *only* spot batted-ball might help —
   barrel% stabilizes faster than HR-rate, so in March/April a blend that regresses
   a thin HR-rate toward a barrel-implied rate could help for ~3 weeks/year. Low ROI.
4. **Orthogonal context** (weather temperature, wind-out parks) is physically real
   but only ~31% populated in our data and `NULL` in training rows — fix the data
   coverage before modeling it. Pitcher batted-ball-allowed is weaker than batter
   power (which was already redundant), so it's unlikely to help.

### Note: possible over-engineering of v2

The 12-feature base above (AUC 0.672) edged the deployed 42-feature `mlb_homerun_v2`
(reported AUC 0.617) — though on a different split/row set, so not apples-to-apples.
Worth a confirmatory check: a leaner, better-regularized feature set may match or
beat v2 with less overfitting. This is the one concrete model-side experiment left
that could yield a (small) real win.
