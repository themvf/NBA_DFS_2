# NFL DFS reliability program (started 2026-09-28)

**Why this exists.** The PHI@CHI Showdown on 2026-09-28 and the Sunday
2026-09-27 contest review exposed a pattern: features failed quietly. They
produced nothing, fell back to a default, and the page looked normal. This
program fixes each failure and then makes silent failure structurally
impossible, so that every feature reports on every slate whether it worked.

Jira (`SCRUM`) is the normal source of truth for scope and progress. It was
not reachable from the session that started this program, so this file
tracks status until the items are mirrored there. Each item lists what "done"
means. Nothing is marked done without verification on a real saved slate.

Status key: `todo` · `in progress` · `done (PR)` · `blocked (reason)`

## Already fixed on 2026-09-28

| Item | PR |
|---|---|
| Generate showed a masked production error; it now returns the real reason | #289 |
| A slate load reset defensive adjustments to experimental while the source was Position workload | #289 |
| Starting-QB selector (override); backups blocked; promotion from the ruled-out starter | #290 |

**Correction recorded after the game:** Case Keenum started, not Tyson
Bagent. The Sleeper depth chart (Keenum QB1) was right, and the manual
override was wrong. Phase A2 makes the automatic path handle this case, and
turns the selector into an evidence-backed override.

## Phase A: correctness (things that produced wrong lineups)

| # | Item | Done when | Status |
|---|---|---|---|
| A1 | **Roles from the pregame archive.** Read Sleeper depth/role evidence from `ff_player_injury_observations` (source `sleeper`) as of the projection cutoff, using the latest row per player that HAS a `depth_chart_order` key (~3.5% of rows come from a partial writer without it). Today the page reads mutable `ff_players` and discards any capture newer than the cutoff, which blanks every role. | Replaying PHI@CHI after its 21:08 roster refresh shows Hurts QB1, Keenum QB1, and PHI/CHI backups blocked | todo |
| A2 | **Automatic QB promotion.** When a team's workload QB is ruled out, promote the current depth-chart QB1, even when the injured starter has already been moved down the chart. The selector becomes an override: it shows the source evidence and warns when a pick contradicts it. Fix the #290 regression where the confirmed role string failed the workload source's exact role check. | PHI@CHI promotes Keenum with no user input; choosing Bagent shows a contradiction warning | todo |
| A3 | **Showdown ownership prior.** Captain and flex ownership handled separately (never summed into one "probability"); a salary floor on the value term; leverage switches off, with a plain message, when ownership is invalid; the run snapshot records the ownership actually used; the leverage label stops saying "LineStar". | PHI@CHI: no player over 100%, Ahmed ($200) not ~93%, a leverage-on build keeps Hurts/Swift and passes export QA | todo |
| A4 | **Caps mean what you set.** A typed per-player cap is exact (captain + flex combined). The global default may stretch for chalk captains, but the page says so. If a plan can't fit the caps, stop and name the cap to raise. Add a cap-only control (today the % box is an exact target). | Swift capped at 70% gives ≤14/20 under chalk-captain; an infeasible cap produces a named, plain error | todo |
| A5 | **Export guard.** Export QA blocks runs whose build is not a committed deployment (`local-uncommitted`). | A local-built run cannot be exported; a deployed run can | todo |
| A6 | **No masked errors.** Every server action the NFL DFS page calls returns its reason instead of throwing, so production shows the real message. | A forced validation error on each action shows its message in production | todo |
| A7 | **Build form persists per slate** (locks, exclusions, exposure, captain ranges, plan, starting QBs), server-side, and survives reloads and devices. Fix the false "Settings changed" banner. | Reload after setting captain ranges keeps them; no banner right after Generate | todo |

## Phase B: data freshness and pipeline honesty

| # | Item | Done when | Status |
|---|---|---|---|
| B1 | **Split production from research** in `refresh_nfl_dfs_projections.yml`. 6 of the last 8 runs concluded `failure` because research steps failed, even though projections were written. Red must mean production broke. | A research-step failure no longer marks the production refresh failed | todo |
| B2 | **Schedule + "Update data" button.** Roster/injury/DK status hourly, every 15 min near kickoff. The button dispatches the same jobs through the existing GitHub dispatch bridge, shows progress and "data as of", moves the slate to the newest projections before building, and is disabled at lock. | Button refreshes a live slate end to end; schedule observed firing | todo |
| B3 | **Opponent adjustments for every slate.** The capture job selects only the newest Classic upload of the week, so Showdowns never get them. Capture every eligible upload; the page shows "N of M players adjusted" or "not available for this slate". | A Showdown upload receives captures; the count is visible | todo |
| B4 | **Position workload honesty.** The WR snapshot is a committed JSON file frozen at week 1. QB eligibility requires availability evaluated within 60s, which a saved run never meets. Week-3 QB candidates were never produced. Make the data refresh automatically, fix the window, and disable the source with a reason when it can't be used. | The source either produces forecasts for the current week or is disabled with a stated reason | todo |
| B5 | **Replacement upside for starters known to be OUT.** Their stored slate rows are zeroed, so upside skips them (Goedert). Use the pre-injury projection range. | PHI@CHI flags Goedert's replacements (or states a precise reason) | todo |
| B6 | **DST scoring reconciliation.** Five week-3 defenses were 2 points under DK: fumble recoveries are credited by the team aggregate and DK but not by the play-by-play rule. Choose a resolution rule against DK actuals, and correct the report cards. | Report-card DST equals DK for the week 2-3 contests | todo |

## Phase C: make silent failure impossible

| # | Item | Done when | Status |
|---|---|---|---|
| C1 | **Slate Check card.** On every slate load, before Generate, one card proves each feature worked: roles, injured starters, opponent adjustments, ownership validity, workload freshness, upside skips, projection freshness, code version, salary match. Plain language, with a fix button per line. | PHI@CHI as it stood at build time would have listed all of that night's problems | todo |
| C2 | **Scheduled Slate Check** on upcoming slates, recorded and visible before the page is opened. | The check runs on schedule and records results | todo |
| C3 | **Honest estimates.** A promoted QB shows "estimate, no range". Teammates of a changed QB (receivers, kicker) get a shadow-only adjustment note; this does not change projections, because non-QB reallocation failed this project's absence studies. | Notes visible; projections unchanged | todo |

## Phase D: redesign ("the Apple of design")

Clickable mockup first, then build. Flow: slate, a few plain questions,
lineups shown as rosters, one-click export. Results in one line, technical
detail behind "Details", experimental features off unless chosen. The Slate
Check (C1) is the centerpiece.

## Phase E: model evaluation

After A-C: evaluate projections and exposure strategy across several slates,
never one (for example, established QBs ran ~4 pts over projection in the
week-3 report card).

## Operational note for the user

The main checkout (`C:\Docs\_AI Python Projects\NBADFS_v2`) is on branch
`nfl-gpp-portfolio` from 2026-09-21, 132 commits behind main, with many
uncommitted files. The Sunday 2026-09-27 lineups were built from it. Don't use
it for real contests until it's brought up to date; it holds uncommitted work,
so that needs the user's go-ahead.
