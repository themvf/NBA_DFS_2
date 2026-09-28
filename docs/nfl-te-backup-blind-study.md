# Starting TE out: does the backup TE take over? (pre-registered 2026-09-28)

Study id `nfl-te-backup-blind-v1`. Registered and pushed **before any
2014–2018 outcome in its population was computed**. Implementation:
`model/nfl_te_backup_blind.py`.

## Why

The [RB blind test](nfl-rb-first-game-blind-study.md) found that in the first
game a starting back misses, giving the leftover work to the back who takes
over beats his baseline. The [WR breakout test](nfl-wr-breakout-blind-study.md)
found that when a star receiver sits, the rest of the room has big games more
often. This asks the tight end version: when the starting TE sits, does the TE
who takes over beat his history? (Prompt: Terrance Ferguson, Rams 2026, 1
target in week 1 with the starters active, 9 targets and a touchdown in week 2
with Nacua out.)

## Definitions (frozen before any outcome was computed)

- **Starting TE:** a TE with at least **4 targets a game** over the team's last
  8 games (v1 window, half-life 4, ≥ 2 active games), inactive or on reserve.
- **First game:** the starter played the team's previous game. Primary
  population.
- **Lead remaining TE:** the active TE with the highest target baseline. Every
  active established TE is a row; only the lead one is graded.
- **TE-room pie:** the team budget counts only tight ends' targets and only
  tight ends inherit (`budget_only = {TE}`), otherwise v1's machinery
  (full-strength reserve, 4× cap). `leftover = min(donors, budget − active −
  reserve)`, split by target baseline, gain × the player's points per target.
- **BASE:** everyone keeps his own baseline.
- **Big game:** **12+ DraftKings points** (the frozen fantasy-board TE spike
  line, p85). Expected chance = recency-weighted share of the player's own
  active games in the window that reached 12+.
- **Control:** team games with no TE of any size missing; the comparison row is
  the **second TE** (the same kind of player, not promoted).
- Uncertainty: 10,000 bootstrap draws over events, seed 20260928.

## Discovery (2020–2025, true inactive status; 2019 warm-up)

113 first-game events (16–23 a season), 212 already under way, 1,951 control
games.

**H1, mean projection** (lead remaining TE; φ chosen from {0.5, 1.0} on points
squared error, ties to the smaller):

| | BASE | φ 0.5 | φ 1.0 |
|---|---:|---:|---:|
| points squared error | 42.46 | **40.46** | 56.17 |
| points MAE | 4.65 | 4.89 | 5.84 |

φ = 0.5 selected. Against BASE:

| measure | delta | 95% CI |
|---|---:|---|
| points squared error | −2.00 | [−9.98, +6.13] |
| points MAE | +0.24 | [−0.35, +0.83] |
| targets squared error | −2.87 | [−5.09, −0.65] |
| bias (actual − projected), points | BASE +2.94 → φ 0.5 −0.11 | |

The backup TE beats his baseline by about 3 points on average, and the pie
removes that bias and improves the targets projection, but it does not reliably
improve the points projection: which backups gain is not predictable from their
history. **H1 is expected to fail on the blind seasons.**

**H2, big game** (added after this discovery run; see disclosed changes):

| | 12+ rate | expected from history | gap |
|---|---:|---:|---:|
| lead remaining TE, first game (113) | 12.4% | 4.2% | +8.2 pp |
| TE2, control games (1,846) | 3.7% | 3.7% | +0.0 pp |
| **effect** | | | **+8.14 pp [+2.29, +14.63]** |

Described, gating nothing: absence already under way +6.68 pp [+1.34, +12.19]
(unlike the RB study, the TE effect does not fade after the first game); other
remaining TEs' points squared error −1.69 [−4.30, +0.45]; star WR also out:
17 events, nothing resolvable.

**Frozen:** `FROZEN_PHI = 0.5`, `MAE_MARGIN = 0.50` (the RB blind test's
margin, not tuned), `BIG_GAME_DISCOVERY_EFFECT = 0.0814`.

## Proxy validation (2019–2025; passed)

The 2014–2018 activity proxy (stat or snap-count row = active; see the RB blind
study) against true `INA` status:

| check | threshold | result |
|---|---|---|
| V1 recall of true first-game events (same lead TE) | ≥ 90% | 113 / 113 = 100% ✓ |
| V2 precision of proxy first-game events | ≥ 90% | 113 / 123 = 91.9% ✓ |
| V3 H1 proxy delta inside the true-status CI | [−9.98, +6.13] | −2.53 ✓ |
| V4 H2 proxy effect inside the true-status CI | [+2.29, +14.63] | +8.01 pp ✓ |

## Blind structure (counts only, computed before registration)

2014–2018: **75 first-game events** (2014: 12, 2015: 15, 2016: 19, 2017: 13,
2018: 16); 24 with a single remaining TE; 109 already under way; 1,722 control
games. **10 first-game events are excluded** because a star WR was also out,
so the WR breakout unmask already computed those TE rows (at 20+). No other TE
outcome from 2014–2018 has been computed for a TE-specific question.

## Gates (2014–2018, first-game events; each hypothesis graded alone)

**H1, mean projection (φ 0.5 vs BASE, lead remaining TE):**
- G1: points squared error delta, CI upper < 0.
- G2: points MAE delta, CI upper < +0.50.
- G3: targets squared error delta < 0.
- G4: ≥ 70 events.

**H2, big game (lead remaining TE, 12+ DK, vs control TE2):**
- B1: effect CI lower bound > 0.
- B2: effect ≥ half the discovery effect (≥ +4.07 pp).
- B3: ≥ 70 events.

Verdicts: `PROMOTE`, `NOT_PROMOTED` (sample passes, another fails),
`INSUFFICIENT` (sample fails). `unmask` re-runs the proxy validation and
refuses to grade if it fails (`VOID`).

## Disclosed changes and limits

- **H2 was added after the discovery run.** The code froze H1 (mean projection)
  as the only primary before any TE outcome was computed. Discovery showed H1
  null on points and a large big-game effect, so H2 was registered then. The
  blind seasons are untouched by that choice, but two hypotheses are graded.
  Each pass needs one side of a 95% CI to clear a bound (a one-sided 2.5%
  test), so the chance that at least one passes by luck is about 5%.
- **The sample floor was written as 80 events** before any count was seen. The
  blind seasons contain 75, so it was lowered to **70 before any blind outcome
  was computed.** At 75 events the expected H2 CI half-width is about ±7.6 pp:
  a true effect the size of discovery's would clear B1 only about half the time,
  and a smaller true effect (discovery estimates usually shrink) less often. A
  `NOT_PROMOTED` on H2 is therefore weak evidence against the effect, not proof
  it is absent.
- **Control TE2 rows are not perfectly unseen.** The WR breakout unmask pooled
  every WR and TE row in full-strength games into one 20+ control gap. No
  TE-only or 12+ control figure was computed, and the control game set here is
  defined differently (no TE missing).
- The history-based expected rate is what a model that resamples a player's
  past games would say. H2 tests whether that undersells a promoted backup; it
  does not say which backup will hit.

## What a verdict licenses

- **H2 PROMOTE:** a live **shadow** flag: when the slate rules out a TE with 4+
  targets a game who played his team's last game, the TE who takes over is
  flagged with the measured 12+ uplift beside his normal ceiling, and graded
  weekly against DraftKings results. No projection or optimizer change until
  that forward record exists.
- **H1 PROMOTE:** the same shadow status for the TE-room pie's adjusted
  projection (φ 0.5), graded weekly beside the baseline.
- **NOT_PROMOTED / INSUFFICIENT:** nothing changes. No retuning of the
  starter threshold, the 12-point line, φ, the window or the proxy on
  2014–2018.

## Protocol

1. Registration, code, tests and the pre-registration artifacts
   (`artifacts/nfl_te_backup_blind_v1_{discovery,validate,blind}.json`) are
   committed and pushed before `unmask` runs.
2. `python -m model.nfl_te_backup_blind unmask` runs once; its output is
   recorded below verbatim.
