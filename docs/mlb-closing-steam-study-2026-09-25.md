# MLB closing moneyline movement study — September 25, 2026

## Question and definition

Did the team whose moneyline strengthened just before first pitch win, and would a one-unit bet at the observed DraftKings price have profited?

"Strengthened" means its **vig-free implied win probability rose**. This is the direction in which its moneyline payout usually shortens. It does not mean that the positive American odds number increased. A qualifying move requires at least three of five retail sportsbooks present at both endpoints, each moving at least 1.5 percentage points toward the same team. Each quoted two-sided moneyline must have a provider update timestamp no more than 35 minutes old at its capture. Pinnacle and Polymarket are excluded from the retail vote.

The **strict final-capture** definition compares the last two pregame captures, at most 40 minutes apart, with the final capture no more than 15 minutes before scheduled first pitch. The **closing-hour** definition compares the most recent capture 45–90 minutes before first pitch against the last pregame capture, which must be within 15 minutes. Each game contributes at most one team. A game needs a final score and a fresh DraftKings moneyline at the last capture for the bet result.

## Results

Source: the project's `game_odds_history` and `mlb_matchups` tables, game dates July 2–September 25, 2026. There were 954 final games with pregame book-level odds. The table gives the win rate and hypothetical return from staking one unit on each qualifying team at its last observed DraftKings price. The market expectation is the sum of DraftKings' no-vig closing win probabilities for those same teams.

| Rule | Eligible games | Bets | Record | Win rate | Market-expected wins | Profit | ROI |
|---|---:|---:|---:|---:|---:|---:|---:|
| Strict final capture, 1.5 pp | 609 | 2 | 0–2 | 0.0% | 1.05 | −2.00 units | −100.0% |
| Closing hour, 1.5 pp | 618 | 13 | 4–9 | 30.8% | 7.30 | −5.86 units | −45.1% |
| Closing hour, 1.0 pp | 618 | 56 | 29–27 | 51.8% | 30.38 | −3.61 units | −6.5% |
| Closing hour, 2.0 pp | 618 | 5 | 2–3 | 40.0% | 2.93 | −1.77 units | −35.3% |

The 1.5 pp threshold matches the existing moneyline steam detector's per-book threshold. The closing-hour rules measure net movement over a longer period, so they should be called **closing movement**, not a single-burst steam event. For the 13 closing-hour bets, a game-level bootstrap gives a descriptive 95% ROI interval of about **−87.5% to +1.8%**. The observed four or fewer wins have probability about **5.8%** if the DraftKings no-vig probabilities were the true game probabilities; this is a post-hoc descriptive check, not a preregistered test.

## Reading the result

The stored data show **no profitable backing result** for these particular late-move definitions. The strict steam sample has only two games. The closing-hour sample has 13, and its result is sensitive to the chosen move threshold. These samples cannot establish that late steam predicts losses or that fading it would be profitable. The final pregame capture is also an observed snapshot, not a guarantee that a bettor could fill at that price. Captures may miss moves between polling times.

The script and game-level observations are in [the study script](../research/mlb_closing_steam_study.py) and [the JSON report](../artifacts/mlb_closing_steam_study_2026-09-25.json). Reproduce with:

```text
python -m research.mlb_closing_steam_study --out artifacts/mlb_closing_steam_study_2026-09-25.json
```
