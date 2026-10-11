# CFB Early-Season Pattern Watch — Pre-Registered Study

**Version:** `cfb-pattern-watch-v2` (v1 registered 2026-10-09 22:00 UTC with five triggers; T6 added 2026-10-10 13:30 UTC, before the first Saturday kickoff at 16:00 UTC, while the ledger held zero rows)
**Registered:** 2026-10-09 22:00 UTC for T1-T5; 2026-10-10 13:30 UTC for T6
**Implementation:** `model/cfb_pattern_watch.py` (classifier, grader, append-only ledger)
**Ledger:** `artifacts/cfb_pattern_watch/ledger.jsonl` (local, JSON lines, never rewritten)
**Verdict:** computed once, not before 2026-12-07 (after conference championship games)

## Why this exists

A descriptive sweep of the 2026 season through 2026-10-09 (329 completed games
with closing lines, 259 FBS-vs-FBS) found favorites winning straight up at
81.1% against a market-implied 75.3%, with the spread market calibrated
(favorites 51.8% ATS). Roughly sixty slices were examined across two passes.
Six patterns were large enough to state as triggers (five at v1; the sixth, the −110 spread form of the T1 story, was added the next morning at the owner's request after the moneyline prices were judged unplayable). **None is an edge.** This
document freezes them so the rest of the season is a clean test, in the same
form as the NFL `total_walking` fade study and the MLB underdog-value study.

The discovery sample is recorded below and can never confirm anything. It is
not pooled with the prospective sample, not even as a "combined" secondary.

## Triggers (frozen)

All triggers read the closing consensus: the verified close
(`event_closing_lines` quality A or B, joined to `game_odds_history`) when one
exists, otherwise the last pre-kickoff capture on `cfb_matchups`. The price
source is recorded on every ledger row. Moneyline bets grade at the consensus
American price. Spread and total bets grade at an assumed −110 because the
consensus row carries no spread or total price; this is a stated assumption,
and a real-price grade would need the per-book JSON.

| Trigger | Population | Bet | Floor (games) |
|---|---|---|---|
| T1 `g5_mid_fav_ml` | FBS vs FBS, both teams outside SEC/Big Ten/Big 12/ACC/Independents, closing spread 7 to 13.5, underdog from Conference USA, Sun Belt or MAC | favorite moneyline | 40 |
| T2 `fcs_fav_spread` | FBS favorite against a non-FBS opponent | favorite spread | 25 |
| T3 `small_fav_dog_spread` | FBS vs FBS, closing spread 3 to 6.5 | underdog spread | 80 |
| T4 `big_fav_over` | FBS vs FBS, closing spread 14 to 20.5, total posted | over | 50 |
| T5 `sat_evening_g5_fav_ml` | FBS vs FBS, at least one team outside the P4, Saturday kickoff 5:00 pm to 8:29 pm ET | favorite moneyline | 60 |
| T6 `mid_fav_spread` | FBS vs FBS, closing spread 7 to 13.5 (any conference) | favorite spread | 80 |

T1 and T5 overlap (11 of 24 discovery T1 games are also T5), and every T1
game is by construction also a T6 game (T1 is T6 restricted to G5 teams and
three dog conferences, bet on the moneyline instead of the spread). They are
graded separately and both overlap counts are reported every run. The test
family is six; the Bonferroni-adjusted bar is a 99.2% interval per trigger,
reported beside the nominal 95%. Each trigger's prospective population starts
at its OWN registration time, so T6 never counts a game that kicked off
before it was frozen (Friday 2026-10-09 games are T1-T5 prospective and T6
discovery).

## Metrics

- **Primary:** flat one-unit ROI at the frozen price, with a 95% bootstrap
  interval resampling **game dates** (same-day games are correlated).
- **Secondary:** realized win rate against the vig-free implied probability
  (moneyline triggers) or 50% (spread and total triggers).
- Reported, gating nothing: pushes, price source mix, distinct dates, T1/T5
  overlap, per-week accrual.

## Kill and pass criteria (frozen)

Evaluated per trigger, once, on or after the verdict date, only if the floor
is reached:

- **Dead:** the ROI 95% interval includes zero. No re-slicing by conference,
  week, home/away or spread size to rescue it. A variant is a new study.
- **Pass (licenses a live shadow period only, never a star rating or a
  recommendation):** ROI 95% interval lower bound above zero **and** realized
  win rate at or above implied **and** the result is not carried by a single
  date or a single team (no one team in more than 25% of the trigger's games).
- **Not decidable:** floor not reached by the verdict date. The trigger
  carries to 2027 with the same definition; the bar does not drop.

T2 will probably not reach its floor in 2026: FBS-vs-FCS games are concentrated
in September. That is stated now rather than discovered later.

## Discovery sample (frozen, cannot confirm)

Games with commence time between 2026-08-01 and 2026-10-09 22:00 UTC, graded
by the same code (`python -m model.cfb_pattern_watch --discovery`):

| Trigger | n | record | win rate | implied | units | ROI | date-clustered 95% CI |
|---|---:|---|---:|---:|---:|---:|---|
| T1 favorite ML | 24 | 22-2-0 | 91.7% | 75.9% | +3.90 | +16.2% | [+5.9%, +29.3%] (9 dates) |
| T2 FCS favorite spread | 68 | 42-25-1 | 62.7% | 50.0% | +13.18 | +19.4% | [−11.0%, +34.3%] (8 dates) |
| T3 underdog spread | 64 | 35-26-3 | 57.4% | 50.0% | +5.82 | +9.1% | [−22.4%, +45.8%] (13 dates) |
| T4 over | 40 | 25-15-0 | 62.5% | 50.0% | +7.73 | +19.3% | [−19.9%, +45.1%] (7 dates) |
| T5 favorite ML | 58 | 53-5-0 | 91.4% | 77.5% | +8.63 | +14.9% | [+7.4%, +25.2%] (5 dates) |
| T6 favorite spread | 68 | 43-23-2 | 65.2% | 50.0% | +16.09 | +23.7% | [+4.0%, +45.7%] (13 dates) |

Two honest readings of that table:

1. **The moneyline triggers (T1, T5) have narrow intervals because they have
   five to nine dates, not because the effect is precise.** A date-clustered
   bootstrap on five Saturdays cannot express the real uncertainty. The
   favorites in those games pay roughly −300 to −500; two more upsets in the
   discovery window would have put T5 near zero. Expect the prospective ROI
   to be far below +15% even if the pattern is real.
2. **The spread and total triggers (T2, T3, T4) have intervals that already
   include zero on the discovery sample.** They are registered because the
   underlying game-shape numbers (half-time leads, backdoor cover rates,
   second-half scoring) moved in a coherent direction, not because the ROI
   is established.

Historical base rates for the same triggers on 2022–2025 weeks 1–5, where the
data allow: G5-vs-G5 favorites won at their implied rate (−0.3pp), 3–6.5 point
favorites covered 46.5%, 14–20.5 point games went over 44.3%, and **7–13.5
point favorites covered 51.8%** (49.2% over the full 2022–2025 seasons). T6 is
the trigger with the largest gap between its discovery rate and its historical
base rate, which is the strongest reason to expect it to regress. The Saturday
evening slot was the one window where underdogs historically beat their price.
No FBS-vs-FCS baseline exists in our data.

## Operations

- `refresh_cfb_pattern_watch.bat` runs the scan and appends to the log; a
  Windows scheduled task (`NBADFS CFB pattern watch`) runs it Tuesdays at
  09:00 local, after the weekend's scores and verified closes have landed.
- The scan is idempotent: a (trigger, game) pair is written once. Re-running
  after a score correction does not change the stored row; a corrected grade
  would be a new row with a new study version, per the repo's append-only
  rule.
- Nothing here is pushed, deployed or surfaced on `/health` yet. Promotion of
  the scan to a GitHub workflow is a separate decision.

## Non-negotiables

- Trigger definitions, floors, price basis and the verdict date do not move.
  Changing any of them is `cfb-pattern-watch-v2` with its own discovery note.
- No trigger becomes a recommendation, a star rating, or a line on `/vegas`
  from this study alone. A pass licenses shadow tracking at live prices.
- The discovery table above is the only place the discovery sample appears in
  a summary. It is never added to the prospective counts.

## Live board — `cfb-pattern-board-v1` (added 2026-10-10)

The scan grades after the fact at the verified close. The board
(`model/cfb_pattern_board.py`, output `artifacts/cfb_pattern_watch/board.md`)
is what a reader sees before kickoff, and it follows rules frozen here so a
stale or moved line is never shown as a current selection. These rules change
the display only; the graded population and price basis above are untouched.

| Rule | Constant | Meaning |
|---|---|---|
| Stale quote | 60 min inside 6 h of kickoff, 360 min outside | A stale quote cannot create or refresh a selection. The card reads "line unavailable, last seen N min ago". |
| Frozen selection | first fresh sighting | Line, consensus price, best retail book and price, capture time, written once to `selections.jsonl` (append-only). Everything later is drift displayed beside it. |
| Final | 90 min before kickoff | The selection is the publication record. Drift is still displayed. |
| Watch (late move) | spread 1.5 pts, total 2 pts, or 4 pp of win probability, measured from the capture nearest 2 h before kickoff | No college injury feed exists in this repo (CFBD has none; FantasyPros returns NFL data). A late move is treated as news of unknown content and the card is downgraded to watch. |
| Dropped | trigger no longer fires at the current line | The frozen selection stays in the file and on the card with the reason. |
| Started | kickoff passed | Hidden from selection, still listed. |

Why the first run needed it: on 2026-10-10 the Hawai'i @ Arizona State line
was 45 hours old and fired T4, and James Madison moved from −7.5 to −7 and from
−105 to −110 between the morning list and kickoff. Neither was wrong in the
ledger. Both would have been wrong on a screen.

Operations: `refresh_cfb_pattern_board.bat` runs every 30 minutes on Saturdays
(Windows task "NBADFS CFB pattern board"), local only. Capture cadence is
unchanged: the board can only be as fresh as the half-hour CFB odds tick, so
inside the final hour the stale rule and the capture cadence are the same
constraint. Tightening capture toward kickoff (the NFL dispatcher's
quarter-hour pattern) and moving both jobs to a dispatched workflow with a
`/health` row are the next two steps and are NOT done; the first is an Odds
API budget decision, the second requires a push.

## Instrument measurements — `cfb-pattern-measures-v1` (added 2026-10-11)

Added after the first full Saturday. All four are descriptive: none gates a
verdict, changes a trigger, or alters how a ledger row is graded.
Implemented in `model/cfb_pattern_measures.py`, tested in
`tests/test_cfb_pattern_measures.py`, printed by the grader and shown on the
reader page.

| Measurement | What it answers | Where it lives |
|---|---|---|
| Weekly base rate | Is the effect T1, T5 and T6 rest on still present? Favorites' straight-up rate minus the vig-free implied rate, across every FBS game with a closing line, per CFBD week; plus favorite ATS, overs, and games decided by 7 or fewer. | `base_rates.json`, grader output, page |
| Independent games | How many games are behind the rows. One game can feed three triggers. | grader summary (`games=`), page tile and column |
| Published pick vs close | For each frozen selection: points gained or given up relative to the close from the pick's side, price change, result at the frozen line vs at the close, and P&L at the best retail price recorded at freeze. | page section; computed from `ledger.jsonl` + `selections.jsonl` |
| Board events | Stale refusals, late-move flags and dropped picks, written once per (trigger, game, status), plus one line per board run so a run that flagged nothing is a recorded zero. | `events.jsonl`, `board_runs.jsonl` |

First readings (through 2026-10-10):

- The favorite excess was +9.6 and +9.3 points in CFBD weeks 3 and 4, +1.6
  in week 5 and −3.7 in week 6, while games decided by 7 or fewer rose to
  44%. Two weeks do not decide anything, but this is the first evidence that
  the motivating effect was early-season scheduling.
- 51 graded rows came from 40 games; 10 games fed more than one trigger.
- All 10 frozen selections graded identically at the frozen line and at the
  close. That is uninformative: the board started at 7 pm, minutes before
  kickoff. The first real reading comes from a full Saturday starting 8 am.
- Saturday's 11 board runs and the Hawai'i @ Arizona State stale refusal
  existed only in the board log. They were reconstructed into the new files
  and each reconstructed row carries `reconstructed_from`.

**Week-label correction.** CollegeFootballData numbers 2026-10-10 as week 6.
Earlier versions of this document, `CLAUDE.md` and several commit messages
called it week 7. The discovery sample is CFBD weeks 1-5 plus the first seven
games of week 6 (through 2026-10-09 22:00 UTC). Registration is pinned to
timestamps, so no graded row or trigger window is affected; only labels were
wrong.

## Operations moved to GitHub Actions (2026-10-11)

Both jobs left the laptop. `.github/workflows/cfb_pattern_watch.yml` runs them,
selected by its `job` input, and Vercel's `/api/cron/dispatch` is the clock
(`web/src/lib/cron-dispatch.ts`):

| Job | Dispatch | What it does |
|---|---|---|
| `cfb-pattern-board` | every half hour (:07, :37), August-January | freezes selections on fresh lines, records stale/watch/dropped events, writes a heartbeat, renders the page |
| `cfb-pattern-grade` | 10:07 UTC daily, August-January | grades finished games at the verified close, prints the base-rate table, writes a heartbeat, renders the page |

The workflow keeps a thin GitHub fallback schedule (Saturday 16:41/22:41 UTC
board, daily 10:45 UTC grade). It reads stored odds only and spends no Odds
API credits.

**State is in the database.** A runner starts empty, so the files are gone as
a source of truth. `model/cfb_pattern_store.py` reads and writes five tables:
`cfb_pattern_ledger`, `cfb_pattern_selections`, `cfb_pattern_board_events`,
`cfb_pattern_runs` (one heartbeat per run, `job` = board or grade) and
`cfb_pattern_reports` (the rendered page, stored only when its content changes,
ignoring the clock and the board-run counter). The first four reject UPDATE and
DELETE with a trigger. The pre-migration files were imported once
(51 ledger rows, 10 selections, 1 event, 12 runs) and a second import added
nothing; the `artifacts/cfb_pattern_watch/*.jsonl` files in the repository are
that frozen export, not live state.

**Health.** Both heartbeats are registered in `model/pipeline_health.py`
(`cfb_pattern_board`, 2 h budget; `cfb_pattern_grade`, 30 h) and the workflow
appears on `/health` through the manifest, so a missed run is a FAIL row and
reaches the daily failure sweep.

**Not automated.** The claude.ai reader page cannot be published from a
runner. Each run stores the page in `cfb_pattern_reports` and attaches it to
the run as an artifact; republishing the link is a step taken from a Claude
session. The local humanizer rewrite (`scripts/humanize_copy.py`) also stays
local: its accepted copy is committed in `copy.json`, which CI reads.
