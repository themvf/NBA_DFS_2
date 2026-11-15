# First TD: DraftKings' prices read as a forecast (`nfl-first-td-dk-v1`)

**Registered 2026-11-16 UTC (Sunday night, November 15 Eastern), before the first game it grades
(PIT @ CIN).** `model/nfl_first_td_dk.py`; constants pinned by `tests/test_nfl_first_td_dk.py`.
A change to the reading is a new version.

## Why

Week 9 (14 games, the only graded week of `nfl-first-td-lines-v2`): our top pick and DraftKings'
favourite were the same three winners, but over the whole list DraftKings' prices with the margin
taken out scored a log loss of 2.86 against our 3.05 on the 13 games with both. Four first scorers
sat outside our top ten, two of them at 0.2% or not on our list at all. Betting every player we
priced above DraftKings went 2 of 62. That is one week and it was read after the fact, so it is
discovery, not a result.

## What is saved

Each publish run (`python -m model.nfl_first_td publish`) now saves a third forecast per game beside
the model alone and the availability version:

- DraftKings' latest `player_1st_td` prices before the save. "No Touchdown" is dropped, since our
  forecasts are conditional on a touchdown.
- Each implied chance divided by the sum over the remaining list, players and both D/ST entries.
  The D/ST entries are the return-or-defense share.
- 1% of the listed players' share held back for a player DraftKings does not list.
- A listed player is matched to our candidates by first initial and surname, then to the season's
  roster. One we cannot identify goes to "everyone else" on offense, split by each team's share.
- A list with fewer than 12 players is not read; that game has no DraftKings forecast.

Our own forecasts are unchanged. The page shows the number in a "From DraftKings' prices" column.

## How it is graded

`python -m model.nfl_first_td grade` grades each version's last forecast saved before kickoff, as it
does today. `stats` adds `dk_compare`: on games graded for both the availability version and this
one, each forecast's log loss, how often its top pick scored, and the mean gap (ours minus
DraftKings').

- **One read at 60 games graded for both** (about five weeks). From then the page shows a 95% range
  on the gap from 2,000 resamples of games (seed 20261116).
- Range wholly above zero: DraftKings' number is closer. Wholly below: ours is. Otherwise no call.
- Before 60 games the numbers are shown as descriptive.

## What it cannot show

- An edge. The margin is taken out of DraftKings' own prices, so no player beats his price by this
  number. A result in its favour means "trust the book's number over ours for who scores first".
- Whether a better reading of the prices exists. Dividing by the total leaves long shots a little
  high, because books put more margin on them. A power or Shin reading is a separate version.
- The saved prices are a capture about an hour before kickoff, the same one the page already shows.

## If DraftKings' number is closer at the review

The owner decides. The likely change is to lead the page with it and keep ours as the comparison,
and to stop work on our own first-TD model. Nothing changes automatically.
