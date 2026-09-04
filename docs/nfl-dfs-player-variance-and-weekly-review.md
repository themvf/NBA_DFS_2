# NFL DFS: player variance and weekly review

Date: 2026-09-04. Review format: simulated junior-developer discussion, not a human consultation.

## What this release does

Ships the pinned research study and daily shadow automation without promoting a model or changing optimizer defaults. At 13:35 UTC daily (09:35 EDT / 08:35 EST; GitHub scheduling can be delayed):

1. Refresh the canonical NFL schedule/final scores from free nflverse data.
2. Once completed games exist, fetch current-season player-week and team-week components. Accept valid small early-season feeds; missing or malformed published data fails visibly rather than becoming zero points. No completed games is explicitly pending.
3. Upsert the source working tables, then append versioned DK-scored results. DST revision identity includes opponent components and final score, not only the defense's own stats.
4. Build the existing production projection snapshot. Its formula is unchanged.
5. Settle shadow forecasts against the latest available completed-game result and freeze upcoming-game baseline/candidate forecasts. This step still runs if a preceding phase fails, while the overall workflow retains that failure.

The season defaults to the preceding year in January-March. There are no paid Odds API calls. The scheduled runner applies only DFS table creation, not unrelated global migrations; the existing core schema is a prerequisite. The pinned study and full historical cohort must already exist in the target database. A missing study or changed model implementation blocks shadow forecasts. Git LF/CRLF conversion alone is allowed and the observed code digest is recorded.

Historical results are in `nfl_dfs_player_week_results`; predictions and corrected outcomes are separate in `nfl_dfs_shadow_predictions` and `nfl_dfs_shadow_outcomes`. Saved evaluation reports include season/week/position metrics. Every stored forecast remains inspectable, but the primary shadow evaluation selects one last accepted pregame forecast per player-week, not every daily snapshot as a new sample.

The current collection population is the known canonical weekly roster, not a complete DK salary slate. Unmatched/no-history players are skipped and counted. Missing result rows remain unscored, not presumed DNP or zero. Upstream publication delays mean grading occurs after the data arrives, not necessarily immediately after the game.

## Junior-dev discussion: what should improve next?

**Junior dev: "Isn't one projection enough?"**

No. Two hypothetical players can each project for 15 points while one usually lands near 12-18 and another alternates between very low scores and big games. We need an expected score AND an outcome distribution. Neither example is a claim about a specific real player.

**Junior dev: "Don't we already have floor and ceiling?"**

Yes. The baseline resamples complete historical game stat lines, weighting recent games and using a comparable-player prior. This preserves observed relationships among a player's yards, catches and touchdowns and produces individual mean, median, P10, P90 and boom probability. P10/P90 are quantiles, not guaranteed bounds.

The opportunity candidate changes the mean using prior opportunity, but currently shifts the same training-residual distribution for every player at that position. Thus two WRs can have different projected points but identical interval widths. That is the first granularity gap to test, not a reason to replace the production model immediately.

**Junior dev: "How do we estimate each player's variance with so few games?"**

Start with a transparent, shrinkage-based candidate. Use errors from forecasts made without seeing that game's outcome, not only raw fantasy-score volatility. Combine player/role residual variance with a position prior:

`variance = weight * player_residual_variance + (1 - weight) * role_or_position_variance`

`weight = effective_prior_sample_size / (effective_prior_sample_size + shrinkage_strength)`

The effective sample size accounts for recency weights; it is not simply the row count. Estimate shrinkage strength on training/validation data only. A rookie or changed-role player gets more prior support, not a falsely narrow interval. Report sample size, role assumption and reliability next to the range.

For an asymmetric distribution, scale a standardized empirical residual distribution around the predicted mean rather than blindly drawing a Gaussian. Test separate lower/upper tails if the data supports it. Keep position-appropriate score support (DST can be negative), touchdown discreteness and DK bonuses. This design is proposed, not implemented by this release.

**Junior dev: "What football inputs make the mean more granular?"**

Model opportunity before efficiency, keeping pregame timestamps and source lineage:

| Position | Opportunity | Conversion/efficiency |
|---|---|---|
| QB | Team plays, pass attempts, designed runs/scrambles | Completion rate, yards/attempt, passing/rushing TDs, interceptions |
| RB | Snap share, carries, routes, targets, goal-line work | Yards/carry, catches/target, yards/catch, TD conversion |
| WR/TE | Routes, target share, air yards, red-zone targets | Catch rate, yards/catch, TD conversion |
| DST | Opponent dropbacks, expected drives, game script | Sacks, turnovers, return scores, points-allowed distribution |

Some inputs already exist in raw feeds; routes, reliable role history and time-stamped injury availability require a coverage audit before they can be used. Team totals/spreads are context, not proof of an edge. Props remain a separately versioned future overlay, never a rewrite of the historical baseline.

**Junior dev: "What about injuries and changing roles?"**

Separate game randomness from uncertainty about the player's role. Preserve scenario forecasts (normal workload / limited / inactive) with timestamped evidence. Do not invent scenario probabilities. Evaluate injury-shortened games as realized outcomes; don't retrospectively remove inconvenient misses. An inactive zero needs verified participation/contest eligibility evidence.

**Junior dev: "How do we know the ranges improved?"**

Compare frozen candidates with the same baseline on the same player-weeks. Check MAE/bias for the mean, pinball loss or CRPS for distribution quality, P10/P90 coverage, interval width, and boom calibration. Group results by week, role, position, opportunity band and history size. Wider intervals alone must not qualify as improvement. Preserve game-clustered uncertainty and a genuinely forward evaluation period. No automatic promotion or profitability claim follows from lower projection error.

## Next build: weekly player report card

This is the next UI/data-contract proposal, not a shipped UI feature:

- One row per model/player/game/checkpoint with expected score, median, P10/P90, boom probability, actual, signed error (`actual - projected`), absolute error and interval result.
- Show component forecasts versus actuals (attempts/carries/targets/receptions/yards/TDs), so a workload miss is distinguishable from an efficiency or scoring miss. Component forecasts must actually be modeled and frozen before displaying component errors.
- Include captured time, kickoff, horizon, model/config/seed, input lineage, sample size, raw-input availability, result/scoring revision and stable game/player IDs.
- Explicit statuses: pending game, awaiting source, identity unresolved, excluded scoring, verified inactive, scored, corrected. Never silently drop missing players from the denominator.
- Show scheduled, forecasted, completed, matched and unscored counts per week; alert on completed games lacking results beyond a defined publication grace period.
- Player detail: weekly forecast intervals with actual points overlaid, recent bias/error and workload trajectory. Do not present a handful of games as a reliable personalized calibration estimate.
- Grade production snapshots separately from the market-free research baseline and opportunity candidate. Current shadow metrics are NOT a full evaluation of production `ourProj`, exported lineups or ROI.
- Primary checkpoint: last valid pregame forecast. Optional earlier horizons (T-48h/T-24h/T-6h) are separate comparisons, never mixed or counted as independent games.

## Acceptance tests for the next iteration

1. Future outcomes and late-arriving source corrections cannot rewrite a frozen projection or change its original input state.
2. Sparse-history variance shrinks toward the prior; estimated effective sample size and weight are reproducible.
3. Players with different observed residual variability can receive different interval widths at equal projected means.
4. Repeated daily freezes count once per player-week in primary evaluation; checkpoint comparisons remain separate.
5. Partial Week 1 feeds work; missing/malformed feeds and identity failures are visible; no-stat-row never automatically equals zero.
6. Own-stat, opponent-stat and final-score corrections append outcome revisions. An invalidated latest result cannot fall back to an older convenient exact score.
7. Weekly player/position totals reconcile with stored forecast/outcome rows, including explicitly unscored observations.
8. Candidates are calibrated and assessed chronologically before controlled promotion; optimizer defaults remain unchanged throughout research.

Recommended order: finish the per-player weekly report and coverage controls, then test player/role variance shrinkage, then expand workload/efficiency features. Better visibility makes subsequent modeling changes auditable.
