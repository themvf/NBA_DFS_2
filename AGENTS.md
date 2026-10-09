# Repository agent guide

For NFL play-by-play archetypes, team-season comparisons, or the proposed Team Identity page, read `docs/nfl-team-identity-source-map.md` before changing queries, metrics, or UI. It records the canonical game and team joins, market timing rules, source coverage, and drilldown contract. Keep that map current when adding a source or changing a join.

Existing work in this checkout may be in progress. Inspect the working tree before editing and preserve unrelated changes.

## NFL roster and availability evidence

For NFL player-specific roster, depth, replacement-role, injury, availability, or projection research, inspect **both** Sleeper and FantasyPros evidence for the same team and decision time. Use `python -m research.nfl_dual_depth_audit --season YEAR --team ABBR --output PATH` for a timestamped Sleeper/FantasyPros depth comparison. Also inspect the week-matched FantasyPros injury observation and an official inactive list when one exists. A FantasyPros depth page is a separate published source; its injury API is not a depth feed. When one provider lacks the field in question, record that missing coverage rather than substituting an unrelated field.

Keep source ranks, alignment, capture times, identity matches, and disagreements visible. If either source is unavailable or stale, say so and leave the role unresolved. Do not silently choose one provider, convert a depth rank into projected snaps or targets, or treat an active roster listing as game-day confirmation. After kickoff, new captures are retrospective and must not be described as pre-lock evidence.

## Parallel agents: commit locally, one session pushes (2026-10-04)

Every push to GitHub starts a Vercel build (a branch push builds Preview, a
merge to main builds Production): at least 100 in the week to 2026-10-04,
largely from one-fix-per-PR pushes by parallel sessions.

- **An agent working on an assigned task in its own worktree** makes focused
  local commits and runs the relevant tests. It does **not** push, open a PR,
  deploy, or merge. When finished it reports its commit SHA(s), changed files,
  test results, and any conflicts or dependencies, and leaves the work in its
  worktree.
- **The integrating session** (the one the owner is talking to) collects
  finished commits onto one integration branch, runs the full suite, and pushes
  once per batch: one branch, one PR. It sweeps agent worktrees at least daily,
  because unpushed commits exist on one machine only.
- **Exception:** a fix blocking a live slate, a production outage, or an
  imminent lock ships on its own immediately, flagged as a separate release.
  Batching never delays a pre-lock fix.
- `web/vercel.json` `ignoreCommand` skips a Vercel build when nothing under
  `web/` changed since the last deployed commit (`VERCEL_GIT_PREVIOUS_SHA`,
  else the parent commit; if git cannot compare, it builds). Most changes
  touch `web/` anyway (a workflow edit regenerates
  `web/src/data/workflow-manifest.json`), so this saves roughly 1 build in 4;
  the batching rule above is the main saving.

## Prediction evidence and decision quality

- Before recommending a player, strategy, or bet, verify that the calculation models the exact requested outcome. Yardage, explosive gains, touchdown probability, longest touchdown, and game-leader probability are distinct outcomes.
- Inspect source coverage, canonical identities and joins, decision-time boundaries, player availability, expected opportunities, opponent context, and material missing factors before drawing a conclusion. Preserve source provenance and frozen pregame inputs for later audits.
- Distinguish observed statistics, manually chosen assumptions, descriptive rankings, and validated predictions. If an important calculation is missing, investigate it within the authorized scope and disclose the gap before recommending an action.
- For probabilities and edges, verify relevant historical performance and calibration using information available before each historical decision. Simulation count and mechanical consistency checks do not establish predictive accuracy. Label estimates without this evidence as exploratory; do not imply reliable profitability.
- Define acceptance criteria before building analytical features and verify the path from source data to displayed assessments. Check rare outcomes using eligible opportunities and the distribution of outcomes, not just averages or recent maxima.
- For postgame reviews, preserve original predictions and compare them with verified results. Separate outcome variance from demonstrated data, modeling, and communication failures. Do not tune a model only to explain the latest missed result.
