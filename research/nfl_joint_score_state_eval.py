"""Study 1 registered evaluation: exact-set log loss on 2025, V1 and V2 against the fitted candidate.

    python -m research.nfl_joint_score_state_eval --snapshot <capture.json.gz> --games <games.json>
        --fit-v1 <fit.json> --fit-v2 <fit.json> --output <result.json> [--limit N] [--draws 5000]

One pass, one look. Every 2025 regular-season game the snapshot reconciles is
graded for the baseline (the fitted role/gain candidate with no market), V1
(market-weighted blocks) and V2 (state profile), with identical seeds, history,
roster and labels. The market input is the nflverse closing line for that game,
labelled retrospective; it is pregame by construction but it is the close, not a
capture at the decision time. Gates are read from the registration doc and
applied mechanically; the script prints the verdict and never changes a fit.
"""
from __future__ import annotations

import argparse
import gzip
import json
import time
from datetime import timedelta, timezone
from pathlib import Path

import numpy as np

from model.nfl_game_leaders import Settings, prepare
from model.nfl_joint_contracts import digest
from model.nfl_joint_evaluation import grade_bank, paired_bootstrap
from model.nfl_joint_outcomes import enrich_history, fit, generate
from model.nfl_longest_touchdown import timestamp
from research.nfl_game_leaders import expected_history, roster

STUDY = 'nfl-joint-score-state-v1'
FAMILIES = ('receptions', 'receiving_yards', 'rushing_yards', 'total_yards')
GATES = {'primary_loss_difference_upper_ci95': 0., 'maximum_relative_crps_degradation': .02,
         'p90_absolute_exceedance_error': .03, 'minimum_games': 200}
EXPOSED = {'2026': [1, 2, 3, 4]}


def load_snapshot(path):
    opener = gzip.open if str(path).endswith('.gz') else open
    with opener(path, 'rt', encoding='utf-8') as handle:
        return json.load(handle)


def summarize_grade(grade):
    out = {}
    for family in FAMILIES:
        m = grade['metrics'][family]
        out[family] = {'exact_set_log_loss': m['exact_set_log_loss'], 'leader_log_loss': m['leader_log_loss'],
                       'mean_crps': m['mean_crps'], 'p90_exceedance': m['p90_exceedance'],
                       'interval_80_coverage': m['interval_80_coverage'], 'status': m['status']}
    return out


def run(snapshot, lines, fit_v1, fit_v2, season=2025, draws=5000, seed=20261009, limit=None, log=print):
    latest = max(timestamp(g['kickoff']) for g in snapshot['games']) + timedelta(days=7)
    history, rejected = prepare(snapshot, latest.isoformat(), retrospective=True)
    labels = {h['game']['game_id']: h for h in history}
    ordered = sorted((g for g in snapshot['games'] if g['season'] == season and g.get('completed')),
                     key=lambda g: (timestamp(g['kickoff']), g['game_id']))
    if limit:
        ordered = ordered[:limit]
    settings = Settings(draws=draws, seed=seed)
    pairs, skipped = [], []
    for game in ordered:
        gid = game['game_id']
        started = time.time()
        try:
            if gid not in labels:
                raise ValueError('Target game not reconciled in snapshot')
            if gid not in lines:
                raise ValueError('No closing line for target game')
            cutoff = (timestamp(game['kickoff']) - timedelta(minutes=1)).isoformat()
            eligible = [h for h in history if timestamp(h['game']['kickoff']) + timedelta(hours=8) < timestamp(cutoff)]
            training = [h for h in history if timestamp(h['game']['kickoff']) < timestamp(cutoff)]
            request = {'game': game, 'decision_at': cutoff, 'players': roster(training, game),
                       'expected_prior_game_ids': expected_history(snapshot, game, cutoff, settings.recent_games),
                       'availability_verified': False, 'roster_evidence': 'reconstructed previous-three-game usage only'}
            eligible = enrich_history(eligible, snapshot)
            fitted = fit(eligible, cutoff)
            market = {'home_spread': lines[gid]['spread'], 'total': lines[gid]['total'],
                      'evidence': {'source_ref': f"nflverse_closing_line:{gid}", 'captured_at': cutoff,
                                   'basis': 'closing_line_retrospective_not_decision_time_capture'}}
            banks = {'baseline': generate(eligible, request, settings, fitted),
                     'v1': generate(eligible, request, settings, fitted, market, fit_v1),
                     'v2': generate(eligible, request, settings, fitted, market, fit_v2)}
            row = {'game_id': gid, 'kickoff': game['kickoff'], 'week': game['week'], 'seconds': None}
            for name, bank in banks.items():
                grade = grade_bank(bank['shared_draws'], labels[gid]['boxes'])
                row[name] = summarize_grade(grade)
                row[f'{name}_forecast_sha256'] = digest(bank)
            row['v1_block_report'] = banks['v1']['shared_draws'].get('volume_diagnostics')
            row['v2_ratios'] = {k: v for k, v in (banks['v2']['shared_draws'].get('volume_diagnostics') or {}).items() if k.endswith('mean_ratio')}
            row['seconds'] = round(time.time() - started, 1)
            pairs.append(row)
            log(json.dumps({'graded': gid, 'seconds': row['seconds'],
                            'total_yards_loss': {n: round(row[n]['total_yards']['exact_set_log_loss'], 3) if row[n]['total_yards']['exact_set_log_loss'] is not None else None for n in banks}}), flush=True)
        except ValueError as error:
            skipped.append({'game_id': gid, 'reason': str(error)})
            log(json.dumps({'skipped': gid, 'reason': str(error)}), flush=True)
    return {'pairs': pairs, 'skipped': skipped, 'source_rejections': rejected,
            'history_games': len(history), 'selected_games': len(ordered)}


def verdict(result, seed=20261009):
    pairs = result['pairs']
    comparisons = {}
    for variant in ('v1', 'v2'):
        comparisons[variant] = {}
        for family in FAMILIES:
            comparable = [r for r in pairs if r['baseline'][family]['exact_set_log_loss'] is not None and r[variant][family]['exact_set_log_loss'] is not None]
            if not comparable:
                comparisons[variant][family] = {'games': 0, 'status': 'no_resolved_joint_labels'}
                continue
            boot = paired_bootstrap({r['game_id']: r[variant][family]['exact_set_log_loss'] for r in comparable},
                                    {r['game_id']: r['baseline'][family]['exact_set_log_loss'] for r in comparable}, seed=seed)
            base_crps = np.mean([r['baseline'][family]['mean_crps'] for r in comparable])
            var_crps = np.mean([r[variant][family]['mean_crps'] for r in comparable])
            base_p90 = np.mean([r['baseline'][family]['p90_exceedance'] for r in comparable])
            var_p90 = np.mean([r[variant][family]['p90_exceedance'] for r in comparable])
            boot.update({'baseline_mean_loss': float(np.mean([r['baseline'][family]['exact_set_log_loss'] for r in comparable])),
                         'variant_mean_loss': float(np.mean([r[variant][family]['exact_set_log_loss'] for r in comparable])),
                         'relative_crps_change': float(var_crps / base_crps - 1) if base_crps else None,
                         'baseline_p90_exceedance': float(base_p90), 'variant_p90_exceedance': float(var_p90),
                         'p90_error_increase': float(abs(var_p90 - .1) - abs(base_p90 - .1))})
            comparisons[variant][family] = boot
    gates = {}
    for variant in ('v1', 'v2'):
        rows = [comparisons[variant][f] for f in FAMILIES if comparisons[variant][f].get('games')]
        n = min((r['games'] for r in rows), default=0)
        primary = all(r['ci95'][1] < GATES['primary_loss_difference_upper_ci95'] for r in rows) if rows else False
        crps_ok = all((r['relative_crps_change'] or 0) <= GATES['maximum_relative_crps_degradation'] for r in rows) if rows else False
        p90_ok = all(r['p90_error_increase'] <= GATES['p90_absolute_exceedance_error'] for r in rows) if rows else False
        floor_ok = n >= GATES['minimum_games']
        gates[variant] = {'games': n, 'floor': floor_ok, 'primary_all_families_ci_upper_below_zero': primary,
                          'crps_guard': crps_ok, 'p90_guard': p90_ok,
                          'families_passing_primary': [f for f in FAMILIES if comparisons[variant][f].get('games') and comparisons[variant][f]['ci95'][1] < 0],
                          'verdict': 'PROMOTE' if (floor_ok and primary and crps_ok and p90_ok) else ('NO_VERDICT_BELOW_FLOOR' if not floor_ok else 'NOT_PROMOTED')}
    return {'comparisons': comparisons, 'gates': gates, 'gate_constants': GATES}


def main():
    parser = argparse.ArgumentParser(description='Study 1 registered 2025 evaluation')
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--games', type=Path, required=True, help='profile games file with closing lines')
    parser.add_argument('--fit-v1', type=Path, required=True)
    parser.add_argument('--fit-v2', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--season', type=int, default=2025)
    parser.add_argument('--draws', type=int, default=5000)
    parser.add_argument('--seed', type=int, default=20261009)
    parser.add_argument('--limit', type=int)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit(f'Refusing to overwrite {args.output}')
    snapshot = load_snapshot(args.snapshot)
    lines = {g['game_id']: g for g in json.loads(args.games.read_text())['games']}
    fit_v1, fit_v2 = json.loads(args.fit_v1.read_text()), json.loads(args.fit_v2.read_text())
    for fitted in (fit_v1, fit_v2):
        if timestamp(fitted['training_cutoff']).year != args.season:
            raise SystemExit('Fit cutoff is not the first kickoff of the evaluation season')
    result = run(snapshot, lines, fit_v1, fit_v2, args.season, args.draws, args.seed, args.limit)
    result.update(verdict(result, args.seed))
    result.update({'study': STUDY, 'season': args.season, 'draws': args.draws, 'seed': args.seed,
                   'snapshot_sha256': digest(snapshot), 'fit_v1_sha256': fit_v1.get('fit_sha256'), 'fit_v2_sha256': fit_v2.get('fit_sha256'),
                   'exposed_weeks_excluded': EXPOSED, 'limit': args.limit,
                   'authority': 'registered_single_look_retrospective_closing_line' if not args.limit else 'partial_run_not_a_verdict'})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=1, default=str))
    print(json.dumps({'gates': result['gates'], 'graded': len(result['pairs']), 'skipped': len(result['skipped'])}, indent=1))
    for variant in ('v1', 'v2'):
        for family in FAMILIES:
            c = result['comparisons'][variant][family]
            if c.get('games'):
                print(variant, family, 'n', c['games'], 'mean diff', round(c['mean_difference'], 4), 'ci', [round(x, 4) for x in c['ci95']],
                      'crps', round(c['relative_crps_change'], 4), 'p90err', round(c['p90_error_increase'], 4))


if __name__ == '__main__':
    main()
