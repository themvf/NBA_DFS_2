# PBP-conditioned Showdown script bank

`research/nfl_pbp_script_export.py` builds a **shadow-only** challenger to the
registered coherent scenario bank. It reads previously labeled regular-season
plays through the canonical `nfl_season_games.nflverse_game_id` join, validates
season, week, both teams, and kickoff, and counts each `(game_id, posteam,
drive)` once. The source and join rules are in
`docs/nfl-team-identity-source-map.md`.

For each scenario, `model/nfl_pbp_script_bank.py` samples a sequence of drives
conditional on the offense's current score state: leading by more than seven,
trailing by more than seven, or close. A team and state cell needs at least
eight drives in the team's last eight games; otherwise the matching league
state supplies the drive. The sampled path updates score and clock and records
field goals, giveaways, sacks, plays, and dropbacks. It then finds a nearby
**whole** draw in the existing coherent game bank. All player, kicker, and DST
stats come from that one selected draw. Separate selection and evaluation
streams retain distinct seeds and IDs. The existing TypeScript scenario scorer
and portfolio selector can consume the output banks directly.

This transport is an approximation: its sampled drive ledger is used to choose
a coherent full-game draw; it does not directly generate every player's stats
on each possession. Touchdowns and scores against use seven-point proxies,
and drive duration includes a fixed 25-second terminal residual. Opponent,
weather, market, kicker distance, and two-point choices do not condition the
drive sampler. The mean matching-distance gate rejects a poor match, but its
threshold is an engineering guardrail, not evidence of forecast calibration.
The original coherent model's forward gate remains authoritative; this bank
has no production projection or lineup authority.

Run a pregame bank with:

```powershell
python -m research.nfl_pbp_script_export --coherent <coherent-showdown.json> --output <pbp-script-bank.json>
```

The exporter requires all source PBP labels to have been available by the
bank's decision time. `--retrospective` permits later labels only for a
mechanics replay and labels the result accordingly. An aligned
`<coherent-stem>-selection-ledger.json` and evaluation ledger must accompany
the coherent export. The output retains source hashes, label versions, cutoff,
coverage, score-state fallback counts, and scenario-match diagnostics.

Verification for the saved NYG@LAR Showdown research export:

```powershell
python -m unittest tests.test_nfl_pbp_script_bank -v
node web/node_modules/tsx/dist/cli.mjs web/scripts/verify-nfl-pbp-script-bank.ts artifacts/nfl-pbp-script-shadow/showdown-retrospective-input.json
```

The saved replay used 300 scenarios per stream and matched 133/136 distinct
base draws; mean signature distances were 4.09/3.99. This demonstrates that
the data and scorer paths work. It is not a pregame backtest or a measured
portfolio improvement. Promotion requires frozen pregame banks, comparison
with equal-entry-count saved portfolios, forecast calibration, and the existing
Showdown portfolio gate.
