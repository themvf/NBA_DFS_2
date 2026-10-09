"""Count-only reception analysis and comparison with supplied game-leader prices.

python -m research.nfl_receptions_analysis --forecast FORECAST.json --output REPORT.json
The comparison prices below are the user's 2026-10-08 screenshots, not a live feed.
"""
import argparse
from pathlib import Path
from hashlib import sha256

from research.nfl_longest_touchdown import read, write

SCREENSHOT_PRICES = {'CeeDee Lamb': -108, 'George Pickens': 371,
                     'Chris Godwin Jr.': 840, 'Cade Otton': 1240, 'Jake Ferguson': 1480}


def analyze(forecast):
    family = forecast['metrics']['receptions']
    rows = []
    for player in family['players']:
        if player['residual']:
            continue
        pmf = player['count_probabilities']
        if abs(sum(pmf.values()) - 1) > 1e-8:
            raise ValueError('Reception count probabilities do not sum to one')
        mean = sum(int(k) * v for k, v in pmf.items())
        if abs(mean - player['mean']) > 1e-8:
            raise ValueError('Reception distribution and mean disagree')
        price = SCREENSHOT_PRICES.get(player['name'])
        decimal = (1 + price / 100 if price > 0 else 1 + 100 / abs(price)) if price is not None else None
        threshold = 1 / decimal if decimal else None
        rows.append({**player,
            'at_least': {str(n): sum(v for k, v in pmf.items() if int(k) >= n) for n in range(1, 21)},
            'bins': {label: sum(v for k, v in pmf.items() if low <= int(k) <= high)
                     for label, low, high in [('0-2', 0, 2), ('3-4', 3, 4), ('5-6', 5, 6), ('7-8', 7, 8), ('9+', 9, 10000)]},
            'screenshot_american_odds': price, 'break_even_credit': threshold,
            'difference_percentage_points': (player['win_share'] - threshold) * 100 if threshold else None,
            'model_expected_net_per_unit': player['win_share'] * decimal - 1 if decimal else None})
    return {'game': forecast['game'], 'decision_at': forecast['decision_at'],
            'draws': forecast['settings']['draws'], 'rows': rows, 'tie_probability': family['tie_probability'],
            'market_provenance': 'User screenshots supplied 2026-10-08; original quote time unknown; not a live refresh.',
            'comparison': 'Raw offered-price break-even credit, not a vig-free market probability. Leader credit splits ties.',
            'settlement_assumption': 'Full-game most receptions with decimal returns divided by tied winner count; verify market terms.',
            'source_sha256': forecast['source_sha256'], 'forecast_implementation_sha256': forecast['implementation_sha256'],
            'reconciliation_fields': forecast['reconciliation_fields'], 'availability_verified': forecast['availability_verified'],
            'role_note': 'Both depth sources and week injuries reviewed. TB QB change is observed; no fitted QB-change coefficient.',
            'scope': 'Reception counts only; yardage accounting fixes are separate.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--forecast', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = analyze(read(args.forecast))
    result['forecast_sha256'] = sha256(args.forecast.read_bytes()).hexdigest()
    write(args.output, result)
    for r in result['rows']:
        if r['mean'] >= 1:
            print(r['name'], round(r['mean'], 1), r['p10'], r['p90'], round(r['win_share'] * 100, 1),
                  r['screenshot_american_odds'], round(r['difference_percentage_points'], 1) if r['difference_percentage_points'] is not None else None)


if __name__ == '__main__':
    main()
