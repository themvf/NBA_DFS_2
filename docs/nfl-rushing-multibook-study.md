# Multi-book NFL rushing-yards lines (`nfl-rushing-multibook`, discovery opened 2026-11-14)

`research/nfl_rushing_multibook.py`. The archive pull behind
`nfl-rushing-under-history-v1` stored six books' rushing-yards quotes for every
2023-2025 game (DraftKings, FanDuel, BetMGM, Caesars, Fanatics, Pinnacle); that
study read DraftKings only. This asks whether the other books turn the measured
bias (overs hit 47.3% against 49.9% implied at DraftKings, every under -1.2%)
into something that pays.

## Plan, written before any multi-book result was read (2026-11-14 19:30 UTC)

- **Discovery: 2023 and 2024 only.** Every rule in `RULES` is scored there:
  the under at DraftKings' line at the best price; the under at the highest
  line; any US-book side priced 2/3/5% or more above Pinnacle's de-vigged
  number at the same line; a US-book line 1/2/3/5 yards or more from
  Pinnacle's, or from the median of the other books (under when above, over
  when below). That is a family of about 40 cells, so a discovery number is a
  lead, never a result.
- **At most two rules are frozen** below, with their thresholds, before 2025
  or 2026 is scored with them.
- **Holdout 1: 2025. Holdout 2: 2026 weeks 1-9** (live captures, the same six
  books). A frozen rule holds only if its ROI interval (game-week bootstrap,
  2,000 resamples, seed 20261114) is above zero in 2025 and its 2026 ROI is
  positive. Anything less is recorded as not confirmed.
- Limits known in advance: one snapshot per game about 90 minutes before
  kickoff, so no closing-line value; a quote in the snapshot may be stale or
  limited at the book (stale is not available); the 2023-2025 aggregate
  DraftKings result has been seen, the multi-book rows have not.

## Discovery read, 2023-2024 (2026-11-14 19:45 UTC)

| Rule | Bets | ROI | 95% interval | 2023 | 2024 |
| --- | ---: | ---: | --- | ---: | ---: |
| Under at DraftKings | 2,618 | -0.7% | -4.3 to +2.5 | -1.2% | -0.3% |
| Under at DraftKings' line, best price of five US books | 2,618 | +0.7% | -2.9 to +4.1 | +0.5% | +0.9% |
| Under at the US book with the highest line | 2,792 | +2.2% | -1.1 to +5.1 | +2.4% | +2.0% |
| Over when a US line is 1+ yard below the others' median | 1,299 | -7.5% | -12.2 to -2.7 | -7.6% | -7.4% |
| Under when a US line is 1+ yard above the others' median | 1,113 | +0.6% | -4.1 to +5.2 | +2.8% | -0.7% |
| Any US side 3%+ above Pinnacle's fair number, same line | 123 | -0.4% | -20.6 to +19.0 | +1.5% | -10.3% |

Pinnacle quotes the same line as a US book too rarely for the value rule to say
anything (123 bets in two seasons). Every rule that takes an over loses. The one
lead is the highest-line under; its interval includes zero.

## Frozen before 2025 or 2026 was scored (2026-11-14 19:45 UTC)

**One rule: `highest_line_under`.** For each player, bet the under at the US book
(DraftKings, FanDuel, BetMGM, Caesars, Fanatics) quoting the highest rushing-yards
line; ties go to the best under price; players quoted by one US book are skipped.
$1 flat, settled as in the history study. No other rule is tested on the holdouts.
