# NFL Pick'em Archetype Candidates — analysis (2026-09-09)

Companion to `web/src/lib/nfl/pickem-archetypes.ts`. This is a design
document, not an implementation. Nothing here is a prediction claim: the
selection criterion throughout is **narrative salience to a non-modelling pool
entrant**, and the ideal candidate is one the closing line prices correctly
(gap CI including zero) while the room reacts to it anyway.

Conventions used below:

- **Feasibility** — `ready` (computable from today's `TeamGameContext`),
  `needs-field` (derivable from `nfl_season_games`, derivation named),
  `blocked` (missing source named).
- **Symmetric** — tag fires on both teams in the game, so implied win prob sums
  to 1 across the pair and the tagged-team gap metric is 0.0pp with a
  zero-width CI by construction. Must collapse to one row reporting the
  favourite's gap, exactly as DIVISIONAL / IN_PRIMETIME / AT_INTERNATIONAL do.

A recurring theme worth stating once: **the loudest input a casual entrant uses
is win-loss record**, and not one of the seventeen existing archetypes reads
record or standings at all. The taxonomy currently models the schedule
(rest, travel, kickoff slot) and last week's margin, which is the part of the
room's thinking a sharp person would guess at, and omits the part the room
actually starts from. Most of the highest-priority candidates below are in
that family.

---

## Priority list

Implement in this order. Rationale after each block.

| # | Code | Family | Feasibility | Why first |
|---|---|---|---|---|
| 1 | `MARQUEE_BRAND` | brand bias | ready | Pure room model, zero schedule correlation, nothing like it exists |
| 2 | `RECORD_GAP` | standings | needs-field | The single loudest input a casual uses; taxonomy is blind to it |
| 3 | `HEAVY_FAVORITE` | line salience | ready | Drives confidence-point assignment directly; currently untagged |
| 4 | `UNDEFEATED` / `WINLESS` | standings | needs-field | Extreme records are the loudest story of any given week |
| 5 | `REMATCH_REVENGE` | rematch | needs-field | Broadcast-narrated every time; asymmetric, so metric-clean |
| 6 | `STAKELESS_WEEK18` | motivation | needs-field | The one week where the market moves for a reason the room half-knows |
| 7 | `DOME_TEAM_OUTDOOR_COLD` | venue | needs-field | Classic, repeated on every broadcast, distinct from COLD_OUTDOOR_LATE |
| 8 | `PAPER_RECORD` | standings | needs-field | The mechanism by which #2 and #4 mislead; makes the record family honest |

---

## 1. Record, standings and streaks

### `RECORD_GAP` — "much better record"

**Definition.** Compute season-to-date W-L for both teams from completed
`nfl_season_games` rows strictly before this game's `gameday`. Tag the team
whose win differential `(W_team - L_team) - (W_opp - L_opp) >= 4`, from week 5
onward.

*Threshold.* Win differential rather than win percentage, because it is what
appears on a broadcast graphic and in a standings table. `>= 4` is roughly a
two-game separation in records (e.g. 5-2 vs 3-4); `>= 2` fires on well over
half the board by midseason and stops being a story, `>= 6` waits until week 9
in most seasons and misses the early-season overreaction the tag exists to
capture. Week 5 floor because a 3-0 vs 1-2 split is noise the room already
treats as noise.

**visibility** loud · **lean** toward.
The record is the first and often only number a casual entrant looks at, and
they read it as a strength rating rather than as a schedule-and-variance
artefact the line has already digested.

**Feasibility** `needs-field` — season-to-date record for both teams as of the
game date. One pass over completed rows per season; also unlocks #4, #6, #8.

**Overlap** None with the existing seventeen. Will correlate with
`impliedWin`, necessarily — but that is the point: the tag marks games where
the room's reason for its pick is record rather than price, which is where the
two diverge most when the line disagrees with the standings.

**Symmetric?** No. Only the better-record side is tagged.

### `UNDEFEATED` and `WINLESS`

**Definition.** From week 4: `losses == 0 && wins >= 3` (`UNDEFEATED`);
`wins == 0 && losses >= 3` (`WINLESS`).

*Threshold.* Three is where national coverage starts calling it a story; at
2-0 nobody has written the column yet. Ties are rare enough to ignore, but if a
team has one, treat it as not-undefeated (the streak framing is broken by it in
the room's eyes too).

**visibility** loud · **lean** toward / against respectively.
An unbeaten team gets picked on reputation for weeks after the price has
caught up; a winless team gets faded well past the point where it is priced as
a live dog. Both are the room reading a season-long label rather than a game.

**Feasibility** `needs-field` (same standings derivation as #1).

**Overlap** Partial with `RECORD_GAP` (an undefeated team usually has one) and
with the streak tags below. Recommend implementing `UNDEFEATED`/`WINLESS` and
`RECORD_GAP` but treating them as one heat contribution when both fire, or
suppressing `RECORD_GAP` on games where the extreme tag already fires — three
tags for one story would inflate the score exactly the way the file warns about.

**Symmetric?** No.

### `WIN_STREAK` / `LOSS_STREAK`

**Definition.** Four or more consecutive wins (or losses) entering the game.

*Threshold.* Four, not three: three-game runs are common enough that roughly a
third of the board carries one by November, and a tag that fires on a third of
teams describes the schedule rather than a story. Four is where "hottest team
in football" language appears.

**visibility** moderate · **lean** toward / against.

**Feasibility** `needs-field` (game-by-game results in date order).

**Overlap** Meaningful with `OFF_BLOWOUT_WIN`/`OFF_BLOWOUT_LOSS` — a team on a
four-game losing run frequently just lost by 17+. Weak candidate on its own for
that reason; it earns its place only if the streak tags replace the single-game
blowout tags rather than joining them. Worth testing that substitution.

**Symmetric?** No.

### `PAPER_RECORD` — record flatters them

**Definition.** Season-to-date point differential per game below the league
median while win percentage is `>= 0.600`, from week 6 onward. Equivalently, a
Pythagorean-vs-actual win gap of about `>= 1.5` wins.

*Threshold.* Week 6 because differential is too noisy earlier to be worth
computing; 0.600 because that is the record level at which the room begins
treating a team as good.

**visibility** loud *by proxy* · **lean** toward.
The entrant never computes point differential — but they do read the record,
so the room's over-backing here is loud even though the underlying signal is
quiet. This is the tag that explains *why* `RECORD_GAP` and `UNDEFEATED` are
leverage rather than merely descriptive, and it is the one candidate in this
document where I would expect the room's error to be largest.

**Feasibility** `needs-field` (cumulative scores, already in the table).

**Overlap** Deliberately co-fires with `RECORD_GAP` / `UNDEFEATED`; it should
*modify* rather than add — a heat contribution conditional on one of those
firing, not an independent tag. Flag this to whoever implements the scorer.

**Symmetric?** No.

---

## 2. Playoff stakes and late-season motivation

### `STAKELESS_WEEK18`

**Definition.** Week 18, and the team is either (a) locked into its seed such
that the result cannot change it, or (b) eliminated. Exact clinch logic
requires the full NFL tiebreaker cascade and is genuinely hard; a defensible
approximation is: week 18 and the team is either top-2 in its conference by
record with a `>= 2` game cushion over the 3-seed, or `>= 3` games out of the
7-seed.

**visibility** moderate · **lean** against.
Half the room knows starters may rest and half does not, which makes this the
highest-variance perception on the board and therefore genuinely exploitable in
both directions. The market moves for this openly, so the price is honest; the
room's read is split.

**Feasibility** `needs-field` for the approximation (conference standings from
completed rows). A *correct* clinch determination is effectively `blocked`
without a standings/tiebreaker source, and I would rather ship the
approximation clearly labelled than pretend to the exact version. Note that
resting decisions themselves are `blocked` (no injury report or inactives feed).

**Overlap** None.

**Symmetric?** No, though both teams can independently qualify.

### `ELIMINATED` and `MUST_WIN`

**Definition.** From week 14. `ELIMINATED`: mathematically or near-certainly
out (approximation: `>= 4` games out of the 7-seed with fewer weeks
remaining). `MUST_WIN`: within one game of the cut line, where a loss makes
elimination near-certain.

**visibility** moderate (`ELIMINATED`, lean against) / loud (`MUST_WIN`, lean
toward).
"Playing for nothing" and "must-win" are both broadcast staples. The room reads
motivation as a large effect; the line prices it as a small one.

**Feasibility** `needs-field`, same standings derivation.

**Overlap** `ELIMINATED` will correlate strongly with `WINLESS` and the losing
end of `RECORD_GAP`. Lower priority than `STAKELESS_WEEK18` for that reason.

**Symmetric?** No.

---

## 3. Revenge and rematch structure

### `REMATCH_REVENGE`

**Definition.** A completed prior meeting between the same two teams exists in
the same season, and the tagged team lost it. Optionally split
`REMATCH_REVENGE_BLOWOUT` when that loss was by 17+.

**visibility** moderate · **lean** toward.
Revenge framing is narrated in every rematch broadcast and pregame graphic.
The entrant's read is "they owe them one"; there is no reason to think the
line misses a result it can see as plainly as we can.

**Feasibility** `needs-field` — look up the earlier same-season meeting of the
unordered team pair. Cheap.

**Overlap** Substantially inside `DIVISIONAL` by construction (most second
meetings are divisional), but `DIVISIONAL` is symmetric and neutral-lean while
this is asymmetric and directional, so it adds real information rather than
restating it. This is the cleanest of the rematch ideas.

**Symmetric?** No — only the team that lost the first meeting is tagged. That
asymmetry is what makes it metric-clean, unlike a plain `REMATCH` tag.

### `SEASON_SWEEP_ON_THE_LINE`

**Definition.** Rematch where the tagged team won the first meeting.

**visibility** quiet · **lean** neutral. Weak candidate — "going for the sweep"
is a light narrative and I am not confident the room reacts to it at all.
Listed for completeness; I would not implement it.

### Former-team / coach revenge

**blocked** — needs a roster and coaching-history source (player transactions,
head-coach tenure). This is one of the loudest narratives in the sport
("facing his former team") and is entirely unavailable today. If a roster feed
is ever added for another reason, revisit this first.

---

## 4. Trap games and schedule structure

### `LOOKAHEAD_TRAP`

**Definition.** This week's opponent is weak and next week's is strong, by
record: opponent win differential `<= -2` this week, next opponent's win
differential `>= +2`, from week 6. Restrict to weeks where the next game is a
divisional or primetime game if you want it tighter.

*Threshold.* Defined on **record rather than on next week's line**, deliberately:
next week's line is often not posted when this week's picks lock, so a
line-based definition would backtest cleanly and be unavailable live — the
exact asymmetry that produces a feature which cannot be used.

**visibility** moderate · **lean** against.
"Trap game" is common vocabulary and the room applies it enthusiastically and
inconsistently. My honest uncertainty is high here: the phrase is loud but the
room's actual *behaviour* may be to keep picking the good team anyway.

**Feasibility** `needs-field` (standings plus forward schedule).

**Overlap** None.

**Symmetric?** No.

### `SANDWICH_GAME`

**Definition.** Immediately preceded and followed by primetime and/or
divisional games, where this game is neither.

**visibility** quiet · **lean** neutral. Weak — this is a sharp-community
construct, not something the room articulates. Do not implement.

---

## 5. Line-based salience

### `HEAVY_FAVORITE`

**Definition.** `impliedWin >= 0.80`. Tag the favourite.

*Threshold.* 0.80 is roughly a 9.5-to-10-point spread — the point where the
game stops reading as a contest and starts reading as a formality, and where
confidence-pool entrants begin stacking their top points. 0.75 dilutes it to
a large share of the board; 0.85 misses the whole one-touchdown-plus-a-field-goal
band that produces most of the room's high-confidence assignments.

**visibility** loud · **lean** toward.

**Feasibility** `ready`.

**Overlap** None (the inverse of `HOME_DOG`, but distinct games).

**Symmetric?** No, but note the tag is defined on price, so it necessarily
correlates with the metric being measured — a gap measurement on this tag is
close to meaningless. It earns its place as a **strategy input** (which games
carry the top confidence points, which flips are cheap) rather than as a
market-gap observation. Say so wherever it surfaces.

### `COIN_FLIP`

**Definition.** `0.45 <= impliedWin <= 0.55`.

**visibility** quiet · **lean** neutral.
The room does not have a story about these; that is precisely their value.
This is already implicitly the strategy layer's "cheapest flip" concept
(`analyze:how-many-dogs`), so tagging it makes the existing recommendation
legible to the user rather than adding a new claim.

**Feasibility** `ready`. **Symmetric?** Yes — collapse to one row.

### `SHOOTOUT` (high total) and `ROCK_FIGHT` (low total)

**Definition.** `quoted_total_line >= 49.5` and `<= 39.5` respectively.

*Threshold.* Roughly the 85th/15th percentile of posted totals over
2020-2025; tighter bands stop firing often enough to matter.

**visibility** moderate · **lean** neutral (both teams).
A high total reads as "anything can happen", which nudges the room toward
dogs; a low total reads as "grind", nudging toward the favourite. That is a
real and testable perception effect even though the price contains the total.

**Feasibility** `needs-field` — `quoted_total_line` exists in
`nfl_season_games` but is not on the context object.

**Symmetric?** Yes — collapse.

### `TRAPPED_DOG` (spread-total interaction)

**Definition.** Large spread *and* low total together: `impliedWin >= 0.75`
and `quoted_total_line <= 41.5`. Tag the underdog.

**visibility** quiet · **lean** neutral.
A genuinely under-appreciated structure — few possessions and a big deficit is
the worst case for a dog — and one the room has no vocabulary for. Because it
is quiet on *both* sides it is a weaker fit for this taxonomy than the loud
tags, but it is cheap and it is the only candidate here that models a
structural fact rather than a story.

**Feasibility** `needs-field` (total line). **Symmetric?** No.

---

## 6. Venue, surface and travel beyond the existing tags

### `DOME_TEAM_OUTDOOR_COLD`

**Definition.** Tagged team's own home venue is a dome or closed roof; this
game is outdoors; week `>= 12`; venue is northern (reuse the existing
`NORTHERN` set).

*Threshold.* Week 12 rather than the existing `COLD_OUTDOOR_LATE` week-14
floor, because the narrative attaches as soon as the forecast turns, and the
tag is about the room's reaction rather than about actual temperature.

**visibility** loud · **lean** against.
"Dome team going to Buffalo in December" is one of the most repeated lines in
NFL broadcasting. Strong candidate.

**Feasibility** `needs-field` — the tagged team's home roof type, derivable by
looking up that team's home games' `roof` values in the same season.

**Overlap** With `COLD_OUTDOOR_LATE`, which fires on both teams in the same
game. This one is asymmetric and directional, so it should probably *replace*
`COLD_OUTDOOR_LATE` rather than sit beside it — the cold-weather home team
being tagged for cold weather is the degenerate half of the existing tag.

**Symmetric?** No.

### `SURFACE_CHANGE` (grass team on turf, or vice versa)

**visibility** quiet · **lean** neutral. `needs-field` (`surface`, plus the
team's home surface). Honest assessment: **do not implement.** Surface is an
injury-discourse topic, not a pick-driver; I cannot construct a plausible
story about a casual entrant changing a pick over it.

### `THIRD_STRAIGHT_ROAD` / `LONG_ROAD_TRIP`

**Definition.** Third or later consecutive road game (excluding byes).

**visibility** moderate · **lean** against. `needs-field` (venue lookback two
or more games; the context object currently exposes only `prevWasAway`).
Reasonable candidate, clearly distinct from `CROSS_COUNTRY` (which is a
single-game distance tag). The room does notice a "brutal road stretch".

### `EARLY_KICK_LONDON` — the 9:30am ET slot specifically

Substantially inside `AT_INTERNATIONAL` already. The distinct part is that
the room *forgets these games exist* and picks them carelessly, which is a
different failure mode from over-reaction and arguably deserves its own low-
attention flag. Marginal; `ready` (hourEt). Low priority.

### `SATURDAY_LATE_SEASON`

**Definition.** `weekday == 6` and `week >= 15`.

**visibility** moderate · **lean** neutral. `ready`. Symmetric — collapse.
Weak but nearly free; the December Saturday slate is watched differently from
a Sunday one.

---

## 7. Brand bias

### `MARQUEE_BRAND`

**Definition.** A static set of franchises the room over-backs irrespective of
quality. Proposed initial set: `DAL, KC, SF, GB, PIT, PHI, BUF, BAL, NE, NYG`
— chosen for national television share and fanbase size rather than current
strength, which is the whole point. Freeze the set and version it; do not tune
it against outcomes.

### `FLYOVER_FADE`

**Definition.** The complementary set the room under-backs:
`JAX, CAR, ARI, TEN, WSH, LV, HOU, IND`.

**visibility** loud (`MARQUEE_BRAND`) / moderate (`FLYOVER_FADE`) ·
**lean** toward / against.
A brand tag is the only archetype in this document with **no schedule or
market correlation whatsoever** — it is a pure statement about the room, which
makes it the cleanest possible test of whether the visibility priors in this
file describe anything real. If brand does not shift picks, the whole
visibility model is suspect, and that is worth knowing.

**Feasibility** `ready` (static sets).

**Overlap** None.

**Symmetric?** Possible — two marquee teams can meet. When both are tagged the
game is a marquee matchup and the tag carries no directional information;
collapse those to a separate `MARQUEE_MATCHUP` and report the favourite, per
the existing symmetry rule.

**Caveat, stated plainly.** These sets are my judgement, not a measurement, and
they will drift — a franchise's national profile changes over a decade. The
same warning that covers `visibility` covers this doubly.

---

## 8. Blocked candidates, with the missing source named

Listed so they are not rediscovered as ideas:

| Candidate | Missing source |
|---|---|
| `BACKUP_QB` / QB change | QB depth chart, inactives |
| `KEY_INJURY` | injury report feed |
| `NEW_COACH` / interim coach | coaching-tenure data |
| `REVENGE_VS_FORMER_TEAM` | player and coach transaction history |
| `BAD_WEATHER` (wind, snow, rain) | weather forecast at kickoff |
| `PUBLIC_HEAVY` (bet %) | betting handle / ticket-count feed — and note this is the closest thing to a direct measurement of the room, so it would upgrade `visibility` from prior to observation |
| `OFF_OVERTIME` | no OT flag in `nfl_season_games`; scores alone cannot distinguish it |
| `FLEXED_INTO_PRIMETIME` | schedule-change history (only the final kickoff time is stored) |
| `SHORT_WEEK_AFTER_TRAVEL` variants requiring exact travel distance | venue coordinates |

The betting-percentage feed is the highest-value of these by a wide margin: it
would convert the entire `visibility` column from stated prior to measured
quantity and would let the archetype set be validated rather than asserted.

---

## 9. Reconsidering the existing seventeen

Offered as observations, not as recommendations to delete anything measured.

- **`ALTITUDE_OFF` and `OFF_INTERNATIONAL`** are sharp-community memes rather
  than room stories. A casual entrant does not carry a "Denver hangover"
  model; they carry "Denver is thin air", which `ALTITUDE` already covers.
  Under this taxonomy's own criterion — how loudly it announces itself to a
  non-modeller — both look mis-classified as anything above `quiet`.
- **`REST_EDGE` and `OFF_BYE`** overlap heavily: a bye is the usual way a team
  acquires a 3+ day rest edge. Two tags, one story, and the file's own warning
  about summed tags applies.
- **`OFF_PRIMETIME_WIN` and `OFF_BLOWOUT_WIN`** likewise co-fire often. Worth
  measuring their joint firing rate before adding more recency tags.
- **`COLD_OUTDOOR_LATE`** is symmetric and therefore already degenerate under
  the gap metric; `DOME_TEAM_OUTDOOR_COLD` above is the asymmetric version of
  the same idea and is probably strictly better.
- **`CROSS_COUNTRY`** is the one survivor with a CI excluding zero, and the
  file already flags it as unconfirmed at roughly 54% false-positive odds on
  the sixth pass over overlapping games. Adding this many candidates makes the
  multiple-comparisons position materially worse: if these are measured, the
  family grows from 17 to ~35 tests over the same 3,220 team-games, and any
  new survivor should be trusted **less** than `CROSS_COUNTRY` currently is.
  Pre-register the measurement before running it, and expect zero confirmed
  gaps — which is the desired outcome here, since a calibrated market is the
  precondition for the leverage these tags are meant to capture.

---

## 10. Honest limitations

1. Every `visibility` and `lean` in this document is a prior. There is no
   pick-share feed, and the survivor popularity feed is a different
   distribution that must not be substituted.
2. The standings derivation (#1, #4, #6, #8) is one shared piece of work and
   should be built once, as an as-of-date snapshot keyed on `gameday`, not
   recomputed per tag. Getting the as-of cutoff wrong — including the current
   game's own result — would be a leak of exactly the kind this repo has been
   bitten by before.
3. Clinch and elimination logic is approximated, not correct. Ship it labelled.
4. Tag correlation is the main risk to the heat score. Several candidates here
   are explicitly modifiers (`PAPER_RECORD`) or substitutes
   (`DOME_TEAM_OUTDOOR_COLD` for `COLD_OUTDOOR_LATE`, streaks for single-game
   blowouts) rather than additions, and the scorer needs to know which is which
   before any of them ship.
