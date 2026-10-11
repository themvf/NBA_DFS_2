"""Rule-based review scenarios for every week-5 game (2026-10-10). Review tool, not a calibrated role model.
Carries: vacated RB carries (out players' per-game rate) go to active backs 55/35/10 by FantasyPros depth
(Sleeper fallback); everyone else keeps their per-game rate in games played. Targets: per-game rate in games
played for every active player (missed weeks are absences, not zero usage); depth-chart newcomers with no
2026 usage get a small placeholder. Reserve 5% (carries) / 4% (targets) stays in the newcomer slots."""
import gzip, json, sys
from collections import defaultdict
from pathlib import Path
root = Path('artifacts/nfl-game-leaders')
out_dir = root / 'week5-scenarios-20261010'
reqs = json.load(open('artifacts/nfl-game-leaders-week5-requests-20261010.json'))
snap = json.load(gzip.open(root / 'final-repaired-capture-20261008.json.gz', 'rt', encoding='utf-8'))
boxes = defaultdict(list)
for b in snap['boxes']:
    boxes[(b['game_id'], b['team'])].append(b)
DEPTH_SPLIT = [.55, .35, .10]
summary = {}
for gid, r in reqs.items():
    if 'players' not in r:
        continue
    prior = r['expected_prior_game_ids']
    scen, notes = {}, {}
    for team in (r['game']['away'], r['game']['home']):
        players = [p for p in r['players'] if p['team'] == team]
        active = [p for p in players if p['status'] != 'out']
        outs = [p for p in players if p['status'] == 'out']
        games = prior[team]
        rate = defaultdict(lambda: {'g': 0, 'carries': 0., 'targets': 0.})
        for g in games:
            for b in boxes[(g, team)]:
                if b['carries'] + b['targets'] > 0:
                    rate[b['identity']]['g'] += 1
                    rate[b['identity']]['carries'] += b['carries']
                    rate[b['identity']]['targets'] += b['targets']
        pg = {i: {k: v[k] / v['g'] for k in ('carries', 'targets')} for i, v in rate.items() if v['g']}
        depth = {}
        for c in r['dual_depth'][team]['comparisons']:
            rk = c['fantasypros'].get('rank') or c['sleeper'].get('rank')
            if rk: depth[(c['name'], c['position'])] = rk
        for s in r['dual_depth'][team].get('sleeper_only', []):
            if s.get('depth_order'): depth[(s['name'], s['position'])] = s['depth_order']
        request_ids = {p['identity'] for p in players}
        departed = sorted({b['identity'] for g in games for b in boxes[(g, team)] if b['identity'] not in request_ids})
        roster = [p['identity'] for p in active] + departed + [f'NEW:{team}:{j}' for j in range(3)]
        vacated = sum(pg.get(p['identity'], {}).get('carries', 0.) for p in outs if p['position'] == 'RB')
        w_c = {p['identity']: pg.get(p['identity'], {}).get('carries', 0.) for p in active}
        backs = sorted([p for p in active if p['position'] == 'RB'], key=lambda p: depth.get((p['name'], 'RB'), 99))
        for p, f in zip(backs[:3], DEPTH_SPLIT):
            w_c[p['identity']] += vacated * f
        w_t = {}
        for p in active:
            t = pg.get(p['identity'], {}).get('targets')
            if t is None:
                rk = depth.get((p['name'], p['position']), 99)
                t = 0.5 if (p['position'] in ('WR', 'TE') and rk <= 3) or (p['position'] == 'RB' and rk <= 2) else 0.
            w_t[p['identity']] = t
        def field(w, reserve):
            s = sum(w.values()); f = {i: 0. for i in roster}
            for i, v in w.items(): f[i] = v / s * (1 - reserve)
            for j in range(3): f[f'NEW:{team}:{j}'] = reserve / 3
            return f
        scen[team] = {'carries': field(w_c, .05), 'targets': field(w_t, .04)}
        names = {p['identity']: p['name'] for p in players}
        notes[team] = {'out': [f"{p['name']} ({p['position']}: {pg.get(p['identity'], {}).get('carries', 0):.1f} car, {pg.get(p['identity'], {}).get('targets', 0):.1f} tgt /g)" for p in outs],
                       'vacated_rb_carries_per_game': round(vacated, 1), 'departed_zeroed': [next(b['name'] for g in games for b in boxes[(g, team)] if b['identity']==i) for i in departed],
                       'carries_to': {names[p['identity']]: round(f, 2) for p, f in zip(backs[:3], DEPTH_SPLIT)},
                       'top_target_shares': {names[i]: round(v, 3) for i, v in sorted(scen[team]['targets'].items(), key=lambda kv: -kv[1])[:6] if i in names}}
    captured = max(r['dual_depth'][t]['captured_at'] for t in scen)
    assert captured <= r['decision_at'], (gid, captured, r['decision_at'])
    new = dict(r, role_scenarios=scen,
               scenario_evidence={'source_ref': 'request.dual_depth (Sleeper + FantasyPros) and 2026 box scores in final-repaired-capture-20261008.json.gz',
                                  'captured_at': captured, 'description': __doc__.strip() + ' Notes: ' + json.dumps(notes)},
               roster_evidence=(r.get('roster_evidence') or '') + ' | rule-based review scenario 2026-10-10')
    json.dump(new, open(out_dir / f'request-{gid[8:]}-rule.json', 'w'), indent=1, default=str)
    summary[gid] = notes
json.dump(summary, open(out_dir / 'scenario-notes.json', 'w'), indent=1)
for gid, n in summary.items():
    for t, v in n.items():
        if v['out']: print(gid, t, 'vacated RB car/g', v['vacated_rb_carries_per_game'], '->', v['carries_to'], '| out:', '; '.join(v['out']))
