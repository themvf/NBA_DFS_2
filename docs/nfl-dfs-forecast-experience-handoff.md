# NFL DFS forecast experience fixes

## Simulated review panel

Three independent AI roles reviewed the same live Classic and Showdown evidence:
UX/interaction, systems design, and UI/content/accessibility. The shared finding
was that the workspace mixed current build readiness with saved historical
records and research eligibility. A normal build should show the forecast that
actually applies, a material exception, and its available recovery action.
A closed slate should lead to saved lineups and results.

The live verification found zero saved matchup forecasts for 24 Classic DSTs,
and zero for both Showdown DSTs and both kickers, alongside an automatic-use
promise, an Experimental model badge, and post-kickoff refresh instructions.
The saved runs predated the new special-teams payload. The pipeline's existing
full feature-snapshot digest already detected changed player payloads; that was
not a missing-payload reuse bug.

### Scorecard

Scores are qualitative review targets, not measured user-study results.

| Dimension | Observed → target | Evidence |
|---|---|---|
| Next-action clarity | 2 → 5 | Closed slates offered pregame recovery chores |
| Cognitive load | 2 → 4 | Long research eligibility reasons crowded Slate Check |
| Simulation honesty | 2 → 5 | Zero forecast coverage appeared next to an automatic-use promise |
| Consistency | 2 → 5 | Player pool described a removed defensive profile selector |
| Depth accessibility | 3 → 5 | Provenance needed accessible disclosure |
| Accessibility | 3 → 4 | Small recovery targets and mobile table overflow |

## Implemented priorities and acceptance

| Priority / user problem | Behavior and preservation | Dependency | Verified acceptance |
|---|---|---|---|
| P1: archive asks users to refresh | Neutral archive state, saved settings/lineups labels, no update/export actions; read-only player rules and build settings; original projection snapshots retained | Verified kickoff for refresh | No archive repair buttons or autosave; server rejects refresh at/after first kickoff and unknown/invalid times |
| P1: missing forecast looks ready | Shared status uses actual non-OUT DST/K payloads; complete, partial, historical fallback, unavailable and invalid states; one action starts the existing update pipeline or adopts a compatible newer run | Existing projection/update workflow | Classic and Showdown coverage, fallback, successful/pending/failed recovery tested; no automatic-use claim at zero coverage |
| P1: research looks like a build requirement | Remove global Experimental model badge and obsolete profile copy; collapse research-only diagnostics while keeping production failures visible | Existing diagnostic evidence | Study IDs and inactive source explanations are hidden by default but remain accessible in native disclosures |
| P1: forecast explanation disagrees with pool | DST/K drawer shows saved matchup mean/floor/ceiling and inputs; historical baseline is separate when a matchup forecast applies | Saved special-teams payload | Drawer and displayed pool read the same payload; no numerical forecast is relabeled as validated |
| P2: model readiness is hard to audit | Pin special-teams version/coverage in run identity and persisted availability manifest | Next projection publication | Version/readiness/payload changes invalidate reuse; identical runs still reuse; persisted metadata tested |
| P2: narrow layout spills sideways | Contain the player table's paint within its own scrolling card; let long legend prose wrap; 44px recovery/disclosure targets; polite status and error announcements | Existing CSS and React | 320px layout has no horizontal page pan; table retains its own scroll; keyboard opens forecast details |

Build identity remains available. Unknown build time no longer appears as
"built unknown" in the normal page footer; implementation details are disclosed.

## Verification

- `node scripts/run-test-scripts.mjs --jobs 6`: **132 passed, 0 failed,
  1 explicitly excluded** (the existing live database pick'em writer).
- `python -m pytest tests/test_nfl_dfs_projection_reuse.py tests/test_nfl_special_teams_projection.py tests/test_nfl_dfs_projections_availability.py -q`:
  **57 passed**.
- `tsc --noEmit`: clean.
- Repository ESLint: **0 errors, 63 existing warnings**; optional browser runner
  also linted cleanly.
- Actual client rendered with controlled server-action fixtures and actual
  workspace CSS in Chromium: Classic, Showdown, pending update, failed update,
  closed Classic, closed Showdown; desktop and 320px mobile; keyboard disclosure,
  archive no-write state, recovery completion, no browser errors.

The browser fixtures verify UI behavior, not live GitHub dispatch or production
database writes. No existing Sunday projection rows or saved lineups were
recomputed.

### Replay the browser check

`web/scripts/verify-nfl-forecast-ui.mjs` is optional and outside CI's offline
test discovery. It uses an existing Playwright/Chromium installation; no
production dependency was added. From `web/`, set `NFL_DFS_PLAYWRIGHT` to the
Playwright module path if it is not normally resolvable, and
`NFL_DFS_CHROMIUM` to a Chromium executable if necessary, then run:

```text
node scripts/verify-nfl-forecast-ui.mjs
```

Local screenshots, fixture files and verification logs are left under
`artifacts/nfl-dfs-ui-finish/` in this worktree. They are generated review
evidence, not production forecasts.

## Integration files

### Projection metadata

- `ingest/nfl_dfs_projections.py`
- `tests/test_nfl_dfs_projection_reuse.py`

### Workspace, shared state and verification

- `web/src/app/dfs/nfl/actions.ts`
- `web/src/app/dfs/nfl/data-update-panel.tsx`
- `web/src/app/dfs/nfl/nfl-dfs-client.tsx`
- `web/src/app/dfs/nfl/nfl-workspace.css`
- `web/src/app/dfs/nfl/page.tsx`
- `web/src/app/dfs/nfl/player-explanation-panel.tsx`
- `web/src/app/dfs/nfl/slate-check-card.tsx`
- `web/src/app/dfs/nfl/special-teams-status-card.tsx`
- `web/src/app/dfs/nfl/value-chip.tsx`
- `web/src/app/dfs/nfl/workspace-stepper.tsx`
- `web/src/lib/nfl-dfs/slate-check.ts`
- `web/src/lib/nfl-dfs/special-teams-projection.ts`
- `web/src/lib/nfl-dfs/special-teams-status.ts`
- `web/src/lib/nfl-dfs/workspace-stage.ts`
- `web/scripts/test-nfl-forecast-experience.tsx`
- `web/scripts/test-nfl-special-teams-projection.ts`
- `web/scripts/test-nfl-workspace-stage.ts`
- `web/scripts/verify-nfl-forecast-ui.mjs`
- `web/package.json`
- This handoff document.

## Integration boundaries

Based on `origin/main` merge `e61762aeca8916c988c99314682f4b66beb926d1`,
in isolated branch `codex/nfl-dfs-ui-finish`. No textual conflicts were
encountered. The primary checkout's unrelated edits were preserved.

No schema migration or new production package is required. Existing projection
and data-update workflows produce current payloads; the next publication also
stores explicit special-teams readiness metadata. Automatic adoption remains
pregame-only and never hides saved lineup runs; a manual pregame refresh clones
the salary snapshot and retains comparisons.

The panel had no unresolved design disagreement. Historical reconstruction
with today's mutable market data would be misleading: archived Sunday slates
keep their original forecasts. Any separate retrospective reconstruction would
need market observations with reliable capture times. Tournament advantage and
ownership model promotion remain matters for their existing grading programs.

No push, PR, deployment or merge was performed for this follow-up.
