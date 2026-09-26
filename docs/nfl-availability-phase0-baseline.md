# NFL availability Phase 0 baseline and point-in-time coverage

Frozen on 2026-09-25 before the shared resolver changed production outputs.
These rows preserve existing artifacts; they do not certify that every input
was historically reconstructable.

## Selected pre-first-kickoff runs

| Week | Run | Model | As of (UTC) | First kickoff (UTC) | Players | Artifact digest |
|---|---|---|---|---|---:|---|
| 1 | `46b94b68-8441-5670-a116-50071056060f` | `nfl-dfs-historical-v2` | 2026-09-09 17:15:21 | 2026-09-10 00:20 | 1,086 | `208fe368f32c787ea79263203259e05a812d3a20ebfa3ae074ab550c2c83590c` |
| 2 | `3df228c6-c5f1-5cde-a668-1b6e17419202` | `nfl-dfs-historical-v2` | 2026-09-16 17:43:51 | 2026-09-18 00:15 | 1,086 | `8190594fa15df6d3a7f842d72d5fdc26ae72395608eb9b07486e1a6ed7a7efcd` |
| 3 | `a77d1494-5fe0-565a-bbbe-fc0308297f95` | `nfl-dfs-historical-v5` | 2026-09-24 18:02:05 | 2026-09-25 00:15 | 1,087 | `6025a78ea591b704e3bafa1ca3516b91755f6770124694ddafb2ae8b33dd5582` |

## Coverage matrix

| Input family | Frozen output reproducible? | Point-in-time input reconstructable? | Phase 0 classification | Consequence |
|---|---|---|---|---|
| Historical player-week production | Yes; nflverse snapshot IDs are pinned on the run | Yes for the pinned source releases and week cutoff | Valid for replay, subject to existing stat-quality contracts | May support baseline replay. |
| Schedule and kickoff | Output retained, but schedule row revision is not pinned in these runs | Partially | Review required | Verify canonical kickoff revision before using a row in evaluation. |
| Market implied totals | Values affected projections, but exact odds observation IDs are not pinned per player projection | No | Not historically certified | Existing output is a baseline artifact, not a clean market-feature evaluation row. |
| Roster membership | Builder read mutable `ff_players` | No immutable roster snapshot is pinned | Current-state contamination risk | Do not use these runs to validate roster-sensitive candidates. |
| Sleeper depth order | Builder read mutable `ff_players.metadata` | No immutable depth snapshot is pinned | Not reconstructable | Do not use these runs to validate replacement selection. |
| FantasyPros injury status | Older builder selected week rows, but the run source ID list contains only nflverse snapshots | Decision observation/snapshot not pinned | Not reconstructable as a model decision | Preserve output only; exclude from availability-effect evaluation. |
| Official inactive | No observations existed | No | Missing | Prospective collection required. |
| DraftKings platform eligibility | Stored on uploaded slate, not on the projection run | Reconstructable only for a specific saved upload | Cohort-specific | Evaluate only through a pinned upload/slate manifest. |
| Web-side redistribution | Calculated during page reads from mutable roster/injury evidence | No single saved adjustment identity | Not reconstructable | Cannot be treated as a historical model prediction. |

## Phase 0 conclusion

The three runs are valid regression fixtures for preserving prior serialized
outputs. They are not valid historical evidence for promoting injury/depth
effects. Availability and replacement candidates therefore begin in
prospective shadow unless a narrower cohort can prove all inputs were captured
and pinned before its decision time.

Numerical promotion requirements are frozen in
`docs/nfl-availability-promotion-registry.json`.
