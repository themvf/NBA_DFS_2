import json, glob
from pathlib import Path
D = Path('artifacts/nfl-game-leaders/week5-scenarios-20261010')
pub = {g['game']['game_id']: g for g in json.load(open('artifacts/game-leaders-week5-published-20261010.json'))['games']}
def shares(ps):
    m = {}
    for p in ps:
        k = 'RESIDUAL' if p.get('residual') else p['name']
        m[k] = m.get(k, 0) + p['win_share']
    return m
rows = []
for f in sorted(D.glob('forecast-*-rule.json')):
    new = json.load(open(f)); gid = new['game']['game_id']; old = pub[gid]
    line = [gid]
    for metric in ('rushing_yards', 'receiving_yards', 'receptions', 'total_yards'):
        o, n = shares(old['metrics'][metric]['players']), shares(new['metrics'][metric]['players'])
        lead_o = max((k for k in o if k != 'RESIDUAL'), key=o.get); lead_n = max((k for k in n if k != 'RESIDUAL'), key=n.get)
        moved = sorted(((k, n.get(k, 0) - o.get(k, 0)) for k in set(o) | set(n) if k != 'RESIDUAL'), key=lambda kv: -abs(kv[1]))[:2]
        line.append((metric, o.get('RESIDUAL', 0), n.get('RESIDUAL', 0), lead_o, o[lead_o], lead_n, n[lead_n], moved))
    rows.append(line)
for line in rows:
    print('\n' + line[0])
    for metric, ro, rn, lo, po, ln, pn, moved in line[1:]:
        print(f"  {metric:16s} residual {ro:5.1%} -> {rn:5.1%} | leader {lo} {po:.1%} -> {ln} {pn:.1%} | biggest moves: " + ', '.join(f"{k} {v:+.1%}" for k, v in moved))
