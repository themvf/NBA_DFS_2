"""Explain remaining discrepancies without forcing source totals to agree."""
from pathlib import Path
from collections import Counter
import argparse
import pandas as pd
from research.nfl_longest_touchdown import read, write


def audit(snapshot, reconciliation, cache_root):
    raw = pd.concat([pd.read_parquet(Path(cache_root) / f'play_by_play_{s}.parquet')
                     for s in sorted({p['season'] for p in snapshot['plays']})], ignore_index=True)
    captured = {(p['game_id'], p['play_id']) for p in snapshot['plays']}
    rows = []
    for failure in reconciliation['rejected']:
        game = raw[raw.game_id == failure['game_id']]
        game = game[(game.play_type != 'no_play') & (game.two_point_attempt.fillna(0) == 0) & (game.qb_spike.fillna(0) == 0)]
        for d in failure['discrepancies']:
            identity, field = d['identity'], d['field']
            receiver = game.receiver_player_id == identity; rusher = game.rusher_player_id == identity
            lat_rec = game.lateral_receiver_player_id == identity; lat_rush = game.lateral_rusher_player_id == identity
            if field == 'targets':
                candidates = game[receiver & (game.pass_attempt == 1)]; total = len(candidates)
            elif field == 'receptions':
                candidates = game[receiver & (game.complete_pass == 1)]; total = len(candidates)
            elif field == 'carries':
                candidates = game[rusher & (game.rush_attempt == 1)]; total = len(candidates)
            elif field == 'receiving_yards':
                candidates = game[receiver | lat_rec]
                total = game.loc[receiver, 'receiving_yards'].sum() + game.loc[lat_rec, 'lateral_receiving_yards'].sum()
            else:
                candidates = game[rusher | lat_rush]
                total = game.loc[rusher, 'rushing_yards'].sum() + game.loc[lat_rush, 'lateral_rushing_yards'].sum()
            category = 'capture_or_parser_loss' if abs(total - d['box']) < .01 else 'primary_source_box_disagreement'
            rows.append({'game_id': failure['game_id'], **d, 'primary_total': float(total), 'category': category,
                'candidate_plays': [{'play_id': int(p.play_id), 'captured': (p.game_id, int(p.play_id)) in captured,
                    'description': p.desc} for p in candidates.itertuples()]})
    return {'categories': dict(Counter(r['category'] for r in rows)), 'discrepancies': rows,
            'non_stat_reasons': [{'game_id': r['game_id'], 'reasons': r['reasons']} for r in reconciliation['rejected'] if not r['discrepancies']],
            'rule': 'Audit only. Primary/box disagreements are not repaired by inventing plays or yardage.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--root', type=Path, default=Path('artifacts/nfl-game-leaders'))
    parser.add_argument('--capture', default='repaired-capture-20261008.json.gz')
    parser.add_argument('--reconciliation', default='repaired-reconciliation-20261008.json')
    parser.add_argument('--output', default='remaining-repair-audit-20261008.json')
    args = parser.parse_args(); root = args.root
    result = audit(read(root / args.capture), read(root / args.reconciliation), root / 'raw-stat-credits')
    write(root / args.output, result)
    print(result['categories'])
