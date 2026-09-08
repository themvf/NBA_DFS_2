# NFL Line Movement — the west-coast 1pm narrative study

**Pre-registered 2026-09-08, before any snapshot was purchased.** Snapshot
times, metrics, control group, sample and kill criteria are fixed below and are
not to be changed after the data is examined.

---

## The hypothesis, as stated by the user

> A sportsbook sets a line for a game where a west-coast team plays an
> east-coast team at 1pm ET. Then the articles, podcasts and YouTubers talk
> about the time difference, and the west-coast team's spread or moneyline
> becomes worse because of the notion that west-coast teams can't adjust. But
> at the end of the day, the final score ends up near where the line was
> originally set.

Three separately testable claims:

- **H1 — DRIFT.** From opener to close, the market moves against the
  west-coast visitor by more than it moves against a comparable road team in
  the same kickoff slot.
- **H2 — TIMING.** That drift accumulates during the narrative window
  (Tue→Fri→Sunday morning) rather than in the final sharp window.
- **H3 — THE OPENER WAS RIGHT.** The result lands closer to the OPENING number
  than to the closing number, i.e. the drift carried no information.

H3 is the load-bearing one and the rarest. The standard finding across sports
is that closing lines beat opening lines, because they absorb more information.
A confirmed H3 in this slice would be a real exception, which is exactly why
the bar has to be set before the data is seen.

---

## Population

- **Treatment**: road teams from the Pacific timezone (`SF`, `SEA`, `LAR`/`LA`,
  `LAC`, `LV`) playing a Sunday 1:00pm ET regular-season game, 2022–2025.
  **n = 73** (11 / 21 / 17 / 24 by season), counted from nflverse schedules
  before any odds were purchased.
- **Control**: every other road team in a Sunday 1:00pm ET game in the same
  seasons. **n = 456.** Same slot, same kickoff time, no timezone story. The
  control is what separates "west-coast teams drift" from "all road teams
  drift", and no conclusion may be drawn from the treatment arm alone.

Mountain-timezone visitors (`DEN`, `ARI`, n = 30) are recorded but are NOT part
of either arm. They are a weaker version of the same story and folding them in
either way would be a post-hoc choice.

---

## Snapshots — four per week, DST-aware

US clocks change on the first Sunday of November, so a fixed UTC time cannot
hold both halves of a season. The Sunday captures shift by an hour; the
midweek ones do not, because they are not anchored to kickoff.

| Label | Intent | EDT weeks | EST weeks |
|---|---|---|---|
| `open` | Tue 15:00Z — first look at the coming week | 15:00Z | 15:00Z |
| `friday` | Fri 15:00Z — after the article/podcast cycle | 15:00Z | 15:00Z |
| `sun_am` | Sunday 11:00am ET — casual research window | 15:00Z | 16:00Z |
| `sun_close` | Sunday 12:45pm ET — last look before the 1pm window | 16:45Z | 17:45Z |

**11:00am ET was chosen by the user over 09:30am**, on the reasoning that it
catches bettors on both coasts awake. The cost is a narrower 1h45m gap to the
close snapshot, which makes the casual/sharp separation harder to see. Recorded
because it is a judgement call that shapes H2's power.

Three intervals, each meaning something different:

| Interval | Reads as |
|---|---|
| `open → friday` | the article and podcast cycle |
| `friday → sun_am` | weekend casual research |
| `sun_am → sun_close` | late and sharp money |

**Measured, not assumed:** the provider's snapshot grid is ~10 minutes and it
serves the snapshot at or before the requested time, so `sun_close` lands
around **T-19min**, not T-15. Lines are confirmed live that close in (20 books
in 2022, 10–11 in 2024–25). Every row stores the actual lead time; no analysis
may assume the nominal one.

---

## Metrics

All spreads are expressed **from the road team's perspective**, so a negative
move means the market turned against the visitor.

- **H1**: `close_spread − open_spread`, treatment mean minus control mean,
  game-clustered bootstrap 95% CI. Same for the de-vigged moneyline in
  probability points.
- **H2**: the same difference computed per interval. The pattern across the
  three intervals is the result; no single interval is the headline.
- **H3**: paired per game, `|actual_margin − open_spread|` versus
  `|actual_margin − close_spread|`. Positive means the closing number was
  better. Reported for treatment and control separately — the control tells us
  whether closers beat openers generally in this sample, which is the
  benchmark H3 has to break.

---

## Kill criteria, fixed in advance

- **H1 dies** if the treatment-minus-control drift CI includes zero.
- **H2 dies** if the drift is concentrated in `sun_am → sun_close` (the sharp
  window) rather than the two narrative windows.
- **H3 dies** if the closing number is at least as accurate as the opener in
  the treatment arm, or if treatment and control show the same pattern.

No re-slicing by team, by season, by spread size, or by home opponent after the
fact. A failed hypothesis is not rescued by finding a subset where it holds —
that is the mechanism that produced this project's earlier false positives.

---

## Known limits, recorded before the result

- **2022 has half the observable window.** Games list ~6 days ahead in 2022
  versus ~12 in 2023–25, measured across three probe dates. A 2022 game
  therefore has mechanically less room to move. **2023–25 (n = 62) is the
  primary sample; 2022 (n = 11) is a short-window footnote and is not pooled.**
- **n = 73 is well powered for movement, marginal for calibration.** Movement
  has low variance (SD ~1–1.5 points), so H1 resolves effects of about ±0.35
  points. H3 compares continuous absolute errors and resolves roughly a
  half-point. If the true calibration effect is a tenth of a point, this study
  cannot see it and will say so rather than claim a null.
- **This cannot prove the PUBLIC moved the line.** Bet-percentage data is not
  available at any price we are paying. Sharp money and real news also move
  markets. A positive result supports "the drift in these games carried no
  information", never "the podcasts did it".
- **Prior evidence is mixed and is not being ignored.** West-coast teams at 1pm
  beat their closing price by +7.0pp over 2020–25 (n = 115, CI includes zero),
  which supports the user. But the closing line already gives them +0.80 points
  more than Elo alone (t = 2.2), which points against the drift mechanism. The
  movement data is what adjudicates.

---

## Cost

2 markets (`h2h`, `spreads`) × 1 region (`us`) × 10× historical multiplier =
**20 credits per call**. 18 weeks × 4 snapshots × 4 seasons = **288 calls =
5,760 credits**, about 6% of the 100,000/month quota.

Capture is **resumable and idempotent**: `nfl_line_snapshots` is keyed on
`(snapshot_at, event_id)` and the capturer skips any label/date already stored.
Credits are real money and a re-run must never re-buy a snapshot it already
holds.
