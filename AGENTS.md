# Repository agent guide

For NFL play-by-play archetypes, team-season comparisons, or the proposed Team Identity page, read `docs/nfl-team-identity-source-map.md` before changing queries, metrics, or UI. It records the canonical game and team joins, market timing rules, source coverage, and drilldown contract. Keep that map current when adding a source or changing a join.

Existing work in this checkout may be in progress. Inspect the working tree before editing and preserve unrelated changes.

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
