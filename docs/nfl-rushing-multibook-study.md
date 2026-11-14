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
