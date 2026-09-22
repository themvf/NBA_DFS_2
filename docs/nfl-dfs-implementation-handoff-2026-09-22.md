# NFL DFS + line movement — engineer handoff (revised 2026-09-22, end of day)

For the engineer who picks this up next. On the morning of 2026-09-22 this
file was a work plan derived from [the review handoff](nfl-dfs-redzone-review-handoff-2026-09-21.md).
By the end of the day every package in it except one optional study had
shipped, so it has been rewritten as a state-of-play document: what exists
now, what is waiting on a clock, what you should do next, and how to verify
each piece without re-deriving it. Read the review handoff only if you want
the evidence behind a decision; nothing there is still open.

## 0. What today bought, in one table

| Area | Before today | Now | Proof |
|---|---|---|---|
| Backups projected as starters | +14 QB / +5 RB / +5 WR / +4.5 TE over-projection on zero-history rows, invisible because the roster report card never scored them | A slate-scoped report card that scores exactly what DK paid; a registered v4 prior study with its grader built; verdict after week 10 | [#232](https://github.com/themvf/NBA_DFS_2/pull/232), [#233](https://github.com/themvf/NBA_DFS_2/pull/233) |
| OUT players | Stored at their model number, zeroed only on the web read | Stored at zero at write time; 354 historical rows backfilled | [#239](https://github.com/themvf/NBA_DFS_2/pull/239) |
| NFL closing-line CLV | Graded against a mean-consensus line no book posts | Graded at the frozen executable line (`nfl-lines-v2`); 243 rows regraded append-only, 0 outcomes changed | [#230](https://github.com/themvf/NBA_DFS_2/pull/230) |
| Sealed fade study | Its record leaked through `report()` and the `/nfl` audit | Withheld on both surfaces until floors (n≥100, 40 games; accrued 26/25) | [#230](https://github.com/themvf/NBA_DFS_2/pull/230) |
| Survivor refresh | Dead 4 of 5 runs on the schema lock; market columns frozen two weeks | Skips per-invocation DDL, verifies its tables; a dispatched run succeeded and the columns advanced | [#231](https://github.com/themvf/NBA_DFS_2/pull/231) |
| Shadow candidates | No numeric promotion rule existed | Frozen gate in config, enforced in every evaluation; verdict after week 12 | [#234](https://github.com/themvf/NBA_DFS_2/pull/234) |
| Environment / intervals | Ungradeable: the shadow ledger froze no team context | Five context variants frozen on every shadow row (676 in week 3); four registered studies | [#236](https://github.com/themvf/NBA_DFS_2/pull/236) |
| Dead detectors | "Dead" and "cadence-starved" indistinguishable | Eligible-capture denominator: NFL's three structure detectors had 361 chances and 0 alerts | [#237](https://github.com/themvf/NBA_DFS_2/pull/237) |
| Hygiene | Studies un-digested; `--tonight` applied a dead term; duplicate identities | Provenance stamps; fixed; 2 duplicates deactivated and a build-time guard | [#235](https://github.com/themvf/NBA_DFS_2/pull/235) |
| Record | Share study misattributed; west-coast result unrecorded | Corrected in `CLAUDE.md` and the study doc | [#229](https://github.com/themvf/NBA_DFS_2/pull/229) |

Nothing here changes a production projection. `nfl-dfs-historical-v3` is
still live. Nothing here claims an edge against closing lines; the repo's
record there remains zero across four sports.

## 1. Where things live

| Concern | Read | Write / run |
|---|---|---|
| Production projections (v3) | `model/nfl_dfs_historical.py` | `ingest/nfl_dfs_projections.py` via `refresh_nfl_dfs_projections.yml` |
| Slate rows (DK upload) | `nfl_dfs_slate_players` | `web/src/app/dfs/nfl/actions.ts` (`storedSlateProjection` zeroes OUT at write) |
| Roster report card (missing row = unknown) | `nfl_dfs_weekly_report_cards` | `ingest/nfl_dfs_reportcard.py` |
| Slate report card (missing row = 0) | `nfl_dfs_slate_report_cards` | `ingest/nfl_dfs_slate_reportcard.py`, `model/nfl_dfs_slate_reportcard.py` |
| v4 prior study | `docs/nfl-dfs-v4-zero-history-prior-study.md` | `model/nfl_dfs_zero_history_prior.py`, `model/nfl_dfs_zero_history_prior_study.py` |
| Shadow ledger + gate | `nfl_dfs_shadow_predictions/outcomes/evaluations`, `artifacts/nfl_dfs_shadow_config.json` | `ingest/nfl_dfs_shadow.py`, `model/nfl_dfs_shadow_gate.py` |
| Context variants + studies | `docs/nfl-dfs-environment-and-interval-studies.md` | `model/nfl_dfs_environment_variants.py` (frozen inside the shadow payload) |
| Line alerts, CLV, health | `line_alerts`, `alert_grades` | `model/line_alerts.py` (`--report`, `--regrade-football-lines`) |
| Fade study (sealed) | `model/nfl_walking_fade_study.py` | do not touch |
| Survivor refresh | `nfl_season_games.market_*` | `ingest/refresh_nfl_survivor.py` (`refresh_nfl_survivor.yml`) |

## 2. The five clocks

Each has a script that refuses a verdict early. Do not shorten a window or
lower a floor; a change is a new registration.

| Clock | Rings | Command | PASS means | FAIL means |
|---|---|---|---|---|
| v4 zero-history prior | after 2026 week 10 is scorable | `python -m model.nfl_dfs_zero_history_prior_study --season 2026` | hist_0 paired MAE CI < 0 at QB, RB, WR, TE and no regression elsewhere → bump `MODEL_VERSION` to v4, re-pin the shadow study (2026-09-20 handoff, "Replacement research pin"), flip production | starter prior stays; any other prior is a new study |
| Shadow promotion gate | after week 12 | `python -m ingest.nfl_dfs_shadow --settle-only` and read `forward_gate` in the newest `nfl_dfs_shadow_evaluations` payload | per-position PASS → a deliberate config commit flips `production_promotion` for that position's candidate | candidate closed for that position |
| Environment studies (i)–(iv) | eight forward weeks from week 3 (≈ week 10) | grader not yet written — see §3 item 1 | per study, in the doc | per study |
| Fade study | n ≥ 100 and ≥ 40 games | `python -m model.nfl_walking_fade_study` | its own sealed rule | its own sealed rule; then flip `NFL_FADE_STUDY_SEALED` in `queries.ts` and remove the `report()` exclusion |
| Dead NFL detectors | now | `python -m model.line_alerts --report --existing-schema` (top section) | — | decide fix or retire; they had 361 eligible capture pairs |

## 3. What to do next, in order

1. **Write the grader for the environment/interval studies.** The variants
   are frozen on every shadow row since week 3, but nothing reads them yet.
   Model it on `model/nfl_dfs_shadow_gate.py`: dedupe to one accepted
   forecast per player-week (the `evaluation()` query already does this),
   pair each variant against `env_baseline` from the same payload, weeks-
   clustered bootstrap, floors and PASS/kill exactly as the registration
   states. Study (iii) must also report coverage by projected-mean tercile
   (its p10 is unfloored). Check: a synthetic-ledger test refuses a verdict
   before eight weeks and grades each study independently.
2. **Watch three workflows for one week.** `refresh_nfl_dfs_projections.yml`
   gained two steps (dedupe, slate report card); `refresh_nfl_survivor.yml`
   changed how it constructs the DB; the shadow freeze now computes five
   extra projections per player and took roughly 25 minutes locally. If a
   run reddens, the step name tells you which change; none of them can
   silently pass while doing nothing, by design.
3. **Decide the three dead NFL structure detectors** (reversal,
   reference_led, price_pressure). They are not cadence-starved. Either
   their thresholds never fit NFL capture data or the fields they read are
   absent; `check_detector_health` now prints the eligible-pair count so the
   first thing to test is whether the pre-conditions are ever met on a real
   NFL capture pair.
4. **Optional: share study v2.** Registration is drafted in the review
   handoff §5 item 9. Expect a null; only run it if someone still wants the
   answer.
5. **Deferred from WP9:** a per-game cadence view (final-2h captures,
   closing-candidate hit rate). The live probe found 11–17 final-2h
   captures and a verified close on every sampled game, better than the
   review assumed, so measure again before building UI for it.

## 4. Gotchas that will cost you a day

- **Editing v3 breaks the shadow job.** Any change to
  `model/nfl_dfs_historical.py` changes the hash `ingest/nfl_dfs_shadow.py`
  pins; the job fails with "Baseline implementation drifted". Bump the
  version and re-pin, or build a separate candidate. v4 and the context
  variants both import v3 rather than editing it, on purpose.
- **Two report cards, never pooled.** Roster stream: missing stat row is
  unknown. Slate stream: missing row in a completed, results-bearing game
  is 0. Different versions, different tables. The slate stream also refuses
  to score a game until at least one exact result exists for it, so a
  lagging feed cannot masquerade as a slate of zeros.
- **Weeks 1–2 of 2026 are inspected.** They are discovery data for the v4
  study, the gate and the environment studies. Never let them into a
  confirmation window.
- **Append-only ledgers.** `alert_grades` grows on regrade; prediction and
  shadow rows are never rewritten. Corrections are new rows with a new
  version. `--regrade-football-lines` is version-gated and self-heals a row
  whose grade-history append was interrupted (a deadlock against
  `_ensure_schema` did exactly that on the first live run).
- **`_ensure_schema` still runs on every process start** in most jobs and
  still contends with other writers. Only the survivor refresh was moved off
  it. The one-time-migration refactor is out of scope and unchanged.
- **Tooling.** Long Bash heredocs fail on this Windows setup (write a script
  to the scratchpad and run it). A fresh worktree has no `web/node_modules`
  or `web/.env.local`; junction the first from the main checkout and copy
  the second before `tsc`/`build`.
- **Sealed things.** `model/nfl_walking_fade_study.py` and the three
  2026-09-21 studies are closed. Do not re-slice them; a variant is a new
  registration.

## 5. Verify without trusting this file

```bash
# Python
python -m pytest tests/test_nfl_dfs_slate_reportcard.py tests/test_nfl_dfs_zero_history_prior.py \
  tests/test_nfl_dfs_shadow.py tests/test_nfl_dfs_shadow_gate.py tests/test_nfl_dfs_environment_variants.py \
  tests/test_nfl_line_clv_instrument.py tests/test_cfb_market_signals.py tests/test_nfl_walking_fade_study.py \
  tests/test_refresh_nfl_survivor_schema.py tests/test_nfl_dfs_hygiene.py -q
python -m model.line_alerts --report --existing-schema          # lineCLV columns; sealed note; dead detectors with eligible pairs
python -m ingest.nfl_dfs_slate_reportcard --season 2026 --pooled # hist_0 cells exclude zero; all:out MAE 0.0
python -m model.nfl_dfs_zero_history_prior_study --season 2026   # INSPECTED weeks only; no verdict
python -m ingest.ff_dedupe_identities --dry-run                  # expect 0
python -m ingest.nfl_dfs_slate_zero_out --dry-run                # expect 0

# Web (from web/, after the node_modules junction)
node node_modules/typescript/bin/tsc --noEmit -p .
npm run test:nfl-out-projection
```

Live facts you can re-check in SQL: every settled NFL line alert has
`grading_version = 'nfl-lines-v2'`; `nfl_matchups.week` is non-null on all
272 linked 2026 rows; no `nfl_dfs_slate_players` row has `is_out` and a
nonzero `our_proj`; week-3 `nfl_dfs_shadow_predictions.payload` carries
`context_variants`.

## 6. Delivery contract reminder

Every further change reports state as one of Built / Tested / Backtested /
Prospectively validated / Production actionable, with the requirement-to-
evidence table:

| Requirement | Canonical implementation | Automated test | Command run | Result | Live state mutated? | Limitation |
|---|---|---|---|---|---|---|

A study that is registered but not graded is Built and Tested, never
Backtested. A package with any check FAIL or BLOCKED is not complete.

## 7. Do not

- Edit `nfl-dfs-historical-v3` in place or tune any constant against the
  weeks it will be graded on.
- Re-run or re-slice the 2026-09-21 studies, the west-coast study, or the
  fade study.
- Lower the 30-minute steam bound, or relabel NFL steam (it would orphan 198
  audit rows); the interval is stamped on new rows instead.
- Promote the carries term without a new workload version and a shadow
  cycle.
- Add any public-stat model against closing lines.
- Unseal the fade study on an alert count; the study's own n is smaller.
