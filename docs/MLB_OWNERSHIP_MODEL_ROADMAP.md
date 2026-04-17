# MLB Ownership Model Roadmap

Last updated: April 17, 2026

## Purpose

This note documents the current MLB ownership-model stack, what was added in `v1`, how we will track progress, and what results we should expect over the next 6 weeks.

The goal is not just to produce a better ownership number. The goal is to build a repeatable loop:

1. Capture raw ownership inputs at slate load time.
2. Save our modeled ownership outputs by version.
3. Backfill actual ownership after contests settle.
4. Compare vendor ownership vs our model vs actual.
5. Use those results to improve the next version intentionally.

## What Exists Now

### Data semantics

- `linestar_own_pct`: raw vendor ownership
- `proj_own_pct`: our modeled field ownership
- `our_own_pct`: ownership implied by our internal projection input

### Model and runtime

- Python trainer and artifact:
  - `model/mlb_ownership_model.py`
  - `model/mlb_ownership_v1.json`
- Web/runtime predictor:
  - `web/src/app/dfs/mlb-ownership-model.ts`

### Tracking infrastructure

- Ownership run tables:
  - `ownership_runs`
  - `ownership_player_snapshots`
- Snapshot creation is wired into MLB slate load and LineStar refresh flows.
- Actual ownership is synced back into snapshots after results import.

### Analytics

The MLB Analytics page now shows:

- source-level ownership accuracy
- hitter and pitcher segment splits
- ownership bucket calibration
- recent tracked slates
- version summary
- latest-slate miss examples
- per-slate detail table with sorting

Relevant app files:

- `web/src/app/analytics/mlb-ownership-model-panel.tsx`
- `web/src/db/queries.ts`
- `web/src/db/analytics-cache.ts`

## Why This Matters

Before this work, we could inspect the latest ownership values in `dk_players`, but we could not reliably answer:

- what the model predicted at lock
- which model version produced that prediction
- whether our model beat raw LineStar on that slate
- where the model helped or failed by segment

That is now fixed for MLB.

## Primary Success Metrics

The first metrics that matter are:

- ownership MAE: lower is better
- ownership bias: closer to `0` is better
- ownership correlation: higher is better

We should track them at three levels:

- overall
- hitters vs pitchers
- key sub-buckets:
  - `SP`
  - hitters `1-4`
  - hitters `5-9`
  - ownership buckets
  - salary buckets in the next version

## Expected Results

These windows assume regular MLB slate ingestion and post-slate actual ownership imports from April 17, 2026 forward.

### April 17 to April 30, 2026

What we expect:

- enough tracked slates to confirm the tracking loop is stable
- early evidence on whether `Field Own%` is beating raw LineStar overall
- early evidence on where `v1` helps most:
  - pitchers
  - top-of-order hitters
  - low-owned vs mid-owned lanes

What "good" looks like:

- every loaded MLB slate creates an ownership run
- actual ownership is backfilled consistently
- analytics page starts showing repeat patterns instead of one-off anecdotes

Main output from this window:

- diagnosis, not a major model rewrite

### May 1 to May 14, 2026

What we expect:

- enough volume to compare segments with more confidence
- clearer patterns on where `v1` still misses
- enough evidence to define `v2` without guessing

What "good" looks like:

- stable read on whether `v1` beats LineStar by segment
- a short list of repeat miss archetypes, such as:
  - cheap top-4 hitters
  - low-owned SPs
  - specific chalk lanes

Main output from this window:

- final `v2` feature specification

### May 15 to May 28, 2026

What we expect:

- enough tracked slates to treat this as a real benchmark system
- version-to-version comparisons that are meaningful
- ownership model evaluation tied back to DFS decisions

What "good" looks like:

- stable model comparison by version
- evidence that better ownership estimates are improving:
  - pivot detection
  - SP leverage decisions
  - under-owned stack identification

Main output from this window:

- promote `v2` if it clearly beats `v1`

## Immediate Next Steps

### 1. Attribution layer

Add more explanatory fields to the slate detail view so we can group misses by:

- lineup slot
- salary tier
- position
- team implied total
- projected ownership lane

This answers "why did we miss them?" instead of only "who did we miss?"

### 2. Ownership model `v2`

Likely `v2` feature additions:

- better lineup-slot interactions
- salary x lineup interactions
- team-total and favorite-status interactions
- better SP-specific handling
- better cheap-chalk handling for hitters

### 3. Decision linkage

Once ownership tracking is stable, connect it more directly to DFS decisions:

- under-owned stack identification
- SP leverage evaluation
- post-slate review of ownership misses vs winning lineup construction

## Guardrails

We should not add model complexity faster than the data supports.

That means:

- keep `v1` conservative
- let tracked slates accumulate
- promote new versions only after they beat the prior version on held-out or newly observed slates

The mistake to avoid is building a more complex ownership model before we have enough tracked evidence to know what is actually broken.

## Current Recommendation

The next highest-ROI step is not a full model rewrite. It is better attribution on the ownership miss table, followed by a targeted `v2` informed by the tracked results from late April and early May 2026.
