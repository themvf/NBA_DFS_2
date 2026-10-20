# NFL betting-scenario charts (`nfl-market-scenarios-v1`, 2026-10-19)

The NFL counterpart of the college scenario charts. Twelve three-panel charts
(moneyline, spread at -110, total at -110; favorites vs underdogs, overs vs unders)
and five heatmaps, season to date with 2023-25 pooled as black ticks.
Descriptive, for publishing. No edge is claimed.

- **Source:** nflverse `games.csv`, which has closing moneylines, spread
  (`spread_line` is the home team's margin) and total for every game, plus rest
  days, division flag, roof and kickoff time. Regular season only. The
  favorite is the shorter moneyline. Level prices and tied finals are left out.
- **Splits:** venue, division game, kickoff window, point in the season,
  indoors/outdoors, spread (3 to 3.5 is its own band), total, price, the
  favorite's last result, rest, the home-team ladder and home-underdog price.
  Grids cover venue x spread, venue x total, spread x total, division x spread
  and roof x total.
- **Not available from the file:** line movement (the file has closing lines
  only) and halftime score.
- **Where it runs:** the `market-scenario-charts` job in
  `refresh_nfl_dfs_postweek.yml`, Monday and Tuesday, saved as the
  `nfl-market-scenario-charts` artifact. To run locally:
  `python -m model.nfl_market_scenarios --out-dir artifacts/nfl_market_scenarios`.
- **Chart code:** shared with `model/cfb_scenario_charts.py`, which takes the
  NFL titles as arguments.

## Reader charts (`model/story_charts.py`, 2026-10-20)

The same games, shown ten ways, for both sports. The NFL images are written
as `nfl_story_*.png` in the same job; the college ones as `story_*.png` in
the CFB daily export (`model/cfb_story_charts.py`). Charts:
every game against the spread and against the total (one dot per game), how
far off the line came plus final margins, a weekly bankroll race ($100 a game
on each side), the groups that changed most since 2023-25 (slope chart),
favorites' actual against price-implied win rate (with 95% intervals), every
team's cover result by week, a team quadrant (cover rate against over rate),
favorites won-and-covered / won-not-covered / lost by spread band, and games
whose spread result changed after 5:00 left in the fourth quarter. The
late-flip chart reads the score with 5:00 left from nflverse play-by-play for
the NFL, and from CFBD scoring plays for college. A college game is kept only
when its scoring plays add up to its box score.
