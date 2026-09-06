# NFL competitor benchmark and target redistribution

## Release scope
The NFL workspace (`/dfs/nfl`) now contains **Our model vs competitors**, below the availability panel. It freezes authentic user-imported competitor projections alongside our existing historical model and grades the same players after completed games. It does not yet establish an advantage or enable injury-based point uplifts.

Import a same-week, DraftKings-scored FantasyPros or LineStar CSV through Source comparison, resume/refresh forecasts, choose the competitor, and freeze before kickoff. Publication time, week, and scoring format are not independently verified from a generic CSV. Import time proves when the file was captured, not when its publisher produced it.

## Capture and scoring
- Our model must be at most 72 hours old; competitor imports at most 24 hours old. Future timestamps are rejected.
- Paired players require valid positive forecasts from both sources, canonical identities, valid salaries, exact salary/game kickoff agreement, and future games. Offensive players require fresh resolved roster evidence. Exclusions are retained.
- Immutable SHA-256-addressed JSON snapshots retain player inputs, source import digests, roster/injury evidence, model run, salaries, settings, and generated lineups. The database refuses a new freeze after the earliest included kickoff.
- Both arms use the same deterministic Classic optimizer: five lineups, $45,000 minimum salary, one-player minimum uniqueness, one QB pass catcher, no bring-back requirement, no ownership, no randomness, no fallback. Both means enter the custom projection path, including its common multiplier. Source-specific ceiling and boom bonuses are excluded. Unequal portfolio counts are not compared.
- Grades use latest result revisions for completed games with exact player/team/position/kickoff matching. Missing, ambiguous, or excluded results remain pending; they are never invented as zero.
- Mean absolute error and bias use the same scored cohort. Top-ten value picks are fixed by projected points per salary before outcomes are known. Missing outcomes do not cause backfilling. Inclusive 3x/4x hits use salary per $1,000.
- Mean/best lineup scores are withheld until both entire equal-size portfolios are scored. These are fantasy-score comparisons, not payout, ROI, or ownership-aware tournament evaluations.
- Refresh report card recomputes grades without modifying frozen inputs. Download includes result revision IDs/digests. Repeated snapshots are not independent season samples.

## Full-roster research preview
The adjacent target preview uses saved full team context, including RBs and players outside the salary CSV. It requires exactly one officially confirmed inactive WR/TE with a supported same-team prior role and the same resolved starting QB as the last-four-game historical reference. Freshness, game identity, and target-share budgets are checked.

The fixed hypothesis reallocates half the absent player's prior target share proportionally among eligible returning pass catchers. The remainder and unsupported roles stay reserved. Rookies, arrivals, unavailable players, and unresolved roles do not receive guessed allocations. The team target budget derives from historical plays, run rate, sacks, scrambles, and target rate. Coaching continuity is displayed; historical play mix is not claimed as a confirmed current scheme.

This preview does not change optimizer points, floor, or ceiling. Next work must convert supported workload changes into joint scoring distributions and validate them on chronological held-out games before enabling a candidate projection model. Different-QB scenarios need a separate passing-budget model.

## Installation and verification
Schema definitions exist in Python and Drizzle. `web/scripts/install-nfl-benchmark.ts` installs the table idempotently; explicit freeze also ensures the table exists. Read-only report loading returns an empty list if not installed.

Verified with `test:nfl-competitor-benchmark`, `test:nfl-dfs-workspace`, `test:nfl-absence-preview`, TypeScript, targeted ESLint, and a production build. Browser checks exercise the authentic missing-comparator and empty-report paths. The live install audit found 719 saved players and zero FantasyPros/LineStar projections; no fabricated benchmark was inserted. Actual superiority remains unmeasured until authentic pregame inputs and completed-game outcomes are available.
