"""Study 1 research CLI: build profile games from the database, fit, and run the development grid.

    python -m research.nfl_joint_score_state build-games --seasons 2016-2025 --output artifacts/.../games.json
    python -m research.nfl_joint_score_state fit --games games.json --cutoff 2025-09-01T00:00:00Z --method state_profile --k 40 --ridge 10 --output fit.json
    python -m research.nfl_joint_score_state dev-grid --games games.json --train-through 2023 --develop 2024 --output dev.json

The development grid grades ONLY the volume layer (team targets and carries) on
the development season, for each (k, ridge) on the registered grid, against a
no-line control that uses the same machinery with every training game as the
neighbourhood. It chooses nothing for the 2025 evaluation beyond k and ridge,
which the registration allows. The primary metric (exact-set log loss) is graded
later through research.nfl_joint_outcomes evaluate with the chosen fit.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from model.nfl_joint_contracts import digest
from model.nfl_joint_score_state import (ACTIONS, FLIP, VERSION, _design, fit_market_weighted, fit_state_profile,
                                         neighbours, team_game_profiles)
from model.nfl_longest_touchdown import timestamp

K_GRID = (20, 40, 80)
RIDGE_GRID = (1., 10., 100.)
PLAY_COLUMNS = ('game_id', 'home_team', 'away_team', 'posteam', 'score_differential', 'game_seconds_remaining',
                'play_type', 'had_sack', 'scramble', 'spread_line', 'total_line')


def _connect():
    import psycopg2
    url = os.environ.get('DATABASE_URL')
    if not url:
        from dotenv import load_dotenv
        for candidate in (Path('.env'), Path(__file__).resolve().parents[1] / '.env'):
            if candidate.exists():
                load_dotenv(candidate)
        url = os.environ.get('DATABASE_URL')
    if not url:
        raise SystemExit('DATABASE_URL is required')
    connection = psycopg2.connect(url)
    connection.set_session(readonly=True)
    return connection


NFLVERSE_GAMES_URL = "https://github.com/nflverse/nflverse-data/releases/download/schedules/games.csv"


def _schedule_kickoffs(cache_dir):
    """Kickoff per nflverse game_id from the public schedule file (gameday + gametime, Eastern)."""
    import io
    import pandas as pd
    import requests
    from zoneinfo import ZoneInfo
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / 'nflverse-games.csv'
    if not path.exists():
        response = requests.get(NFLVERSE_GAMES_URL, timeout=60)
        response.raise_for_status()
        path.write_bytes(response.content)
    frame = pd.read_csv(io.BytesIO(path.read_bytes()), low_memory=False)
    eastern = ZoneInfo('America/New_York')
    out = {}
    for row in frame.itertuples(index=False):
        if pd.isna(row.gameday):
            continue
        stamp = f"{row.gameday} {row.gametime if isinstance(row.gametime, str) else '13:00'}"
        out[row.game_id] = datetime.strptime(stamp, '%Y-%m-%d %H:%M').replace(tzinfo=eastern).astimezone(timezone.utc).isoformat()
    return out, path


def build_games(seasons, cache_dir=Path('artifacts/nfl-joint-score-state/cache')):
    """Read-only. Regular season only. Kickoff from nfl_season_games when present (2023+),
    else from the nflverse schedule file, and the source is recorded per game."""
    connection = _connect()
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT a.{', a.'.join(PLAY_COLUMNS)}, g.kickoff
        FROM nfl_pbp_archetypes a
        LEFT JOIN nfl_season_games g ON g.nflverse_game_id = a.game_id
        WHERE a.season = ANY(%s) AND a.season_type = 'REG'
        ORDER BY a.game_id, a.game_seconds_remaining DESC""", (list(seasons),))
    schedule, schedule_path = _schedule_kickoffs(cache_dir)
    rows, sources, missing = [], {}, set()
    for record in cursor.fetchall():
        row = dict(zip(PLAY_COLUMNS, record[:-1]))
        if record[-1] is not None:
            row['kickoff'] = record[-1].astimezone(timezone.utc).isoformat()
            sources[row['game_id']] = 'nfl_season_games'
        elif row['game_id'] in schedule:
            row['kickoff'] = schedule[row['game_id']]
            sources[row['game_id']] = 'nflverse_games_csv'
        else:
            missing.add(row['game_id'])
            continue
        for key in ('score_differential', 'spread_line', 'total_line'):
            row[key] = None if row[key] is None else float(row[key])
        rows.append(row)
    connection.close()
    games = team_game_profiles(rows)
    for g in games:
        g['kickoff_source'] = sources[g['game_id']]
    return {'version': VERSION, 'built_at': datetime.now(timezone.utc).isoformat(), 'seasons': sorted(seasons),
            'plays': len(rows), 'games': games, 'games_without_kickoff': sorted(missing),
            'kickoff_sources': dict(__import__('collections').Counter(sources[g['game_id']] for g in games)),
            'schedule_file_sha256': digest(schedule_path.read_bytes().hex()),
            'source': 'nfl_pbp_archetypes (read-only) + nfl_season_games kickoff, nflverse games.csv fallback',
            'games_sha256': digest(games)}


def _crps(samples, observed):
    samples = np.asarray(samples, float)
    return float(np.mean(np.abs(samples - observed)) - 0.5 * np.mean(np.abs(samples[:, None] - samples[None, :])))


def _predictive(fit, train, neighbour_index, rng, draws, residual_pool):
    """Residual bootstrap: profile from the neighbourhood, dispersion from training residuals."""
    picks = rng.choice(neighbour_index, size=draws)
    profiles = np.array([train[int(i)]['home_profile'] for i in picks])
    out = {}
    for side, design in (('home', _design(profiles)), ('away', _design(profiles[:, list(FLIP)]))):
        for action in ACTIONS:
            mu = np.exp(design @ np.asarray(fit['coefficients'][action]))
            out[(side, action)] = mu * rng.choice(residual_pool[action], size=draws)
    return out


def dev_grid(games, train_through, develop, draws=400, seed=20261009):
    train = [g for g in games if _season(g) <= train_through]
    dev = [g for g in games if _season(g) == develop]
    if not train or not dev:
        raise ValueError('Empty training or development season')
    cutoff = min(timestamp(g['kickoff']) for g in dev).isoformat()
    rng = np.random.default_rng(seed)
    results = {'version': VERSION, 'train_through': train_through, 'develop': develop, 'train_games': len(train),
               'dev_games': len(dev), 'cutoff': cutoff, 'draws': draws, 'grid': []}
    for ridge in RIDGE_GRID:
        reference = fit_state_profile(train, cutoff, k=K_GRID[0], ridge=ridge)
        design_train = _design([g['home_profile'] for g in train] + [[g['home_profile'][i] for i in FLIP] for g in train])
        residual_pool = {}
        for action in ACTIONS:
            counts = np.array([g[f'home_{action}'] for g in train] + [g[f'away_{action}'] for g in train], float)
            mu = np.exp(design_train @ np.asarray(reference['coefficients'][action]))
            residual_pool[action] = counts / mu
        for k in (*K_GRID, None):
            fit = dict(reference, k=k or len(train))
            crps = {a: [] for a in ACTIONS}
            control = {a: [] for a in ACTIONS}
            for g in dev:
                market = {'home_spread': g['spread'], 'total': g['total'],
                          'evidence': {'source_ref': 'closing_line', 'captured_at': cutoff}}
                index = neighbours(fit, market, g['kickoff']) if k else np.arange(len(train))
                predictive = _predictive(fit, train, index, rng, draws, residual_pool)
                for side in ('home', 'away'):
                    for action in ACTIONS:
                        crps[action].append(_crps(predictive[(side, action)], g[f'{side}_{action}']))
            entry = {'k': k if k else 'all_training_games_control', 'ridge': ridge,
                     'crps': {a: float(np.mean(v)) for a, v in crps.items()},
                     'n_team_games': 2 * len(dev)}
            results['grid'].append(entry)
    # Paired comparison against the control at the same ridge.
    for entry in results['grid']:
        control = next(e for e in results['grid'] if e['ridge'] == entry['ridge'] and e['k'] == 'all_training_games_control')
        entry['crps_vs_control'] = {a: entry['crps'][a] - control['crps'][a] for a in ACTIONS}
    candidates = [e for e in results['grid'] if e['k'] != 'all_training_games_control']
    best = min(candidates, key=lambda e: sum(e['crps_vs_control'].values()))
    results['selected'] = {'k': best['k'], 'ridge': best['ridge'], 'rule': 'lowest summed CRPS delta vs control on the development season'}
    return results


def _season(game):
    stamp = timestamp(game['kickoff'])
    return stamp.year if stamp.month >= 8 else stamp.year - 1


def main():
    parser = argparse.ArgumentParser(description='Study 1: pregame market line -> state profile -> team opportunity')
    commands = parser.add_subparsers(dest='command', required=True)
    build = commands.add_parser('build-games')
    build.add_argument('--seasons', default='2016-2025')
    build.add_argument('--output', type=Path, required=True)
    fit = commands.add_parser('fit')
    fit.add_argument('--games', type=Path, required=True)
    fit.add_argument('--cutoff', required=True)
    fit.add_argument('--method', choices=('state_profile', 'market_weighted_blocks'), default='state_profile')
    fit.add_argument('--k', type=int, default=40)
    fit.add_argument('--ridge', type=float, default=10.)
    fit.add_argument('--output', type=Path, required=True)
    grid = commands.add_parser('dev-grid')
    grid.add_argument('--games', type=Path, required=True)
    grid.add_argument('--train-through', type=int, default=2023)
    grid.add_argument('--develop', type=int, default=2024)
    grid.add_argument('--draws', type=int, default=400)
    grid.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit(f'Refusing to overwrite {args.output}; use a new output name')
    if args.command == 'build-games':
        first, last = (int(x) for x in args.seasons.split('-'))
        result = build_games(range(first, last + 1))
    else:
        games = json.loads(args.games.read_text())['games']
        if args.command == 'fit':
            eligible = [g for g in games if timestamp(g['kickoff']) < timestamp(args.cutoff)]
            result = (fit_state_profile(eligible, args.cutoff, args.k, args.ridge) if args.method == 'state_profile'
                      else fit_market_weighted(eligible, args.cutoff, args.k))
            result['fit_sha256'] = digest(result)
        else:
            result = dev_grid(games, args.train_through, args.develop, args.draws)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=1, default=str))
    summary = {k: v for k, v in result.items() if k not in ('games', 'grid')}
    print(json.dumps(summary, indent=1, default=str)[:3000])
    if args.command == 'dev-grid':
        for entry in result['grid']:
            print(entry['k'], entry['ridge'], {a: round(v, 4) for a, v in entry['crps'].items()},
                  'vs control', {a: round(v, 4) for a, v in entry['crps_vs_control'].items()})


if __name__ == '__main__':
    main()
