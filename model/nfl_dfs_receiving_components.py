"""Prior-only receiving component experiment; no optimizer activation.

Hold the role model's target budget and uncertainty method fixed while replacing
its points-per-target shortcut. Fit all population rates before the target week.
"""
from collections import defaultdict
from math import isfinite

import numpy as np

from model.nfl_dfs_role_context import forecast as role_forecast
from model.nfl_dfs_target_share import average

CONFIG = {
    'catch_prior_targets': 30,
    'yardage_prior_receptions': 20,
    'td_prior_targets': 50,
    'minimum_population_targets': 300,
    'minimum_residuals': 100,
    'maximum_residuals': 2000,
    'team_games': 8,
}
FIELDS = ('targets', 'receptions', 'receiving_yards', 'receiving_tds', 'fpts')


def validate_history(history):
    seen = set()
    for row in history:
        key = (row['season'], row['week'], row['game_id'], row['identity'])
        if key in seen:
            raise ValueError('Duplicate player game')
        seen.add(key)
        if any(not isfinite(row[field]) for field in FIELDS):
            raise ValueError('Missing or non-finite receiving evidence')
        # Receiving TDs can be recorded without a reception (e.g. a lateral).
        # Preserve those source records instead of silently dropping the game.
        if not 0 <= row['receptions'] <= row['targets'] or row['receiving_tds'] < 0:
            raise ValueError('Invalid receiving counts')


def population_prior(history, cutoff):
    rows = sorted([r for r in history if r['position'] == 'WR' and (r['season'], r['week']) < cutoff],
                  key=lambda r: (r['season'], r['week'], r['game_id'], r['identity']))
    sums = {field: sum(r[field] for r in rows) for field in FIELDS[:4]}
    if sums['targets'] < CONFIG['minimum_population_targets'] or sums['receptions'] <= 0:
        return None
    return {
        'catch_rate': sums['receptions'] / sums['targets'],
        'yards_per_reception': sums['receiving_yards'] / sums['receptions'],
        'td_per_target': sums['receiving_tds'] / sums['targets'],
        'targets': sums['targets'],
        'cutoff_exclusive': list(cutoff),
    }


def component_forecast(own_team_games, targets, prior):
    if not isfinite(targets) or targets < 0:
        raise ValueError('Invalid target projection')
    recorded = [r for r in own_team_games if r is not None]
    if not recorded:
        raise ValueError('No player component evidence')
    observed = {field: sum(r[field] for r in recorded) for field in FIELDS[:4]}
    catch_rate = (observed['receptions'] + CONFIG['catch_prior_targets'] * prior['catch_rate']) / (observed['targets'] + CONFIG['catch_prior_targets'])
    ypr = (observed['receiving_yards'] + CONFIG['yardage_prior_receptions'] * prior['yards_per_reception']) / (observed['receptions'] + CONFIG['yardage_prior_receptions'])
    td_rate = (observed['receiving_tds'] + CONFIG['td_prior_targets'] * prior['td_per_target']) / (observed['targets'] + CONFIG['td_prior_targets'])
    # Separate smoothing can otherwise make TD rate exceed catch rate for tiny samples.
    td_rate = min(catch_rate, td_rate)
    catches, yards, touchdowns = targets * catch_rate, targets * catch_rate * ypr, targets * td_rate
    # Preserve historical bonuses/other scoring instead of awarding a bonus at
    # projected mean yardage. This term is not an independently modeled bonus probability.
    other = average([r['fpts'] - r['receptions'] - .1*r['receiving_yards'] - 6*r['receiving_tds'] if r else 0. for r in own_team_games])
    points = {'receptions': catches, 'receiving_yards': .1*yards, 'receiving_tds': 6*touchdowns, 'bonuses_and_other': other}
    return {
        'targets': targets, 'receptions': catches, 'receiving_yards': yards, 'receiving_tds': touchdowns,
        'catch_rate': catch_rate, 'yards_per_reception': ypr, 'td_per_target': td_rate,
        'points': points, 'mean': sum(points.values()), 'observed_targets': observed['targets'],
        'observed_receptions': observed['receptions'], 'population_targets': prior['targets'],
    }


def replay(history, teams):
    validate_history(history)
    weeks = defaultdict(list)
    for row in history:
        weeks[(row['season'], row['week'])].append(row)
    residuals, pooled = defaultdict(list), []
    output, excluded = [], defaultdict(int)
    for cutoff, current in sorted(weeks.items()):
        prior = population_prior(history, cutoff)
        if prior is None:
            excluded['population_warmup'] += sum(r['position'] == 'WR' for r in current)
            continue
        forecasts = {t: role_forecast(history, teams, t, cutoff) for t in sorted({r['team'] for r in current})}
        team_windows = {
            t: [(r['season'], r['week']) for r in sorted(
                [r for r in teams if r['team'] == t and (r['season'], r['week']) < cutoff],
                key=lambda r: (r['season'], r['week']))[-CONFIG['team_games']:]]
            for t in forecasts
        }
        lookup = {(r['team'], r['identity'], r['season'], r['week']): r for r in history if (r['season'], r['week']) < cutoff}
        pending = []
        for row in sorted(current, key=lambda r: (r['game_id'], r['identity'])):
            if row['position'] != 'WR':
                continue
            team = forecasts[row['team']]
            player = team['players'].get(row['identity']) if team else None
            if not player:
                excluded['insufficient_prior_role'] += 1
                continue
            own = [lookup.get((row['team'], row['identity'], *key)) for key in team_windows[row['team']]]
            components = component_forecast(own, player['targets'], prior)
            group = (0 if player['targets'] < 4 else 1 if player['targets'] < 7 else 2, team['qb_state'])
            errors = residuals[group] if len(residuals[group]) >= CONFIG['minimum_residuals'] else pooled
            if len(errors) >= CONFIG['minimum_residuals']:
                draws = components['mean'] + np.asarray(errors[-CONFIG['maximum_residuals']:])
                output.append({
                    'season': row['season'], 'week': row['week'], 'game_id': row['game_id'],
                    'identity': row['identity'], 'name': row['name'], 'team': row['team'], 'actual': row['fpts'],
                    'targets_baseline': player['targets'], 'targets_candidate': player['targets'], 'targets_actual': row['targets'],
                    'components': components, 'qb_state': team['qb_state'],
                    'component_actuals': {f: row[f] for f in FIELDS[1:4]},
                    'cutoff_exclusive': list(cutoff), 'residual_rows': min(len(errors), CONFIG['maximum_residuals']),
                    'calibration_offset': float(draws.mean() - components['mean']),
                    'candidate': {'mean': float(draws.mean()), 'p10': float(np.quantile(draws, .1)), 'p50': float(np.quantile(draws, .5)), 'p90': float(np.quantile(draws, .9))},
                    'boom_probability': float(np.mean(draws >= 25)),
                })
            else:
                excluded['residual_warmup'] += 1
            pending.append((group, row['fpts'] - components['mean']))
        # Do not let another player's result in this week enter the forecast.
        for group, error in pending:
            residuals[group].append(error)
            pooled.append(error)
    return output, dict(excluded)
