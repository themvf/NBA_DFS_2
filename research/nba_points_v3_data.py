"""Audited box-score features. No historical availability is inferred from DNPs."""
from collections import Counter, defaultdict
from datetime import date
from statistics import mean

from model.nba_points_ladder import player_key, team_key
from research.nba_points_ghost_v2 import history_features, valid_stats

FEATURES = [
    'p5', 'p10', 'p20', 'p_std10', 'minutes3', 'minutes10', 'minutes20',
    'minutes_growth', 'fga3', 'fga10', 'fg3a3', 'fg3a10', 'fta3', 'fta10',
    'fga_per_min10', 'points_per_min10', 'shot_share10', 'shots_growth',
    'ft_growth', 'scoring_trend', 'rest_days', 'back_to_back', 'home',
    'team_possessions48', 'opponent_possessions48', 'opponent_allowed_fga48',
    'opponent_allowed_fg3a48', 'opponent_allowed_fta48',
]
HISTORY_FEATURES = FEATURES[:20]
TARGETS = ['minutes', 'fga_per_min', 'fg3_fraction', 'fta_per_min']


def daydiff(a, b):
    return (date.fromisoformat(str(a))-date.fromisoformat(str(b))).days


def prepare(raw):
    """Fail on collisions; exclude invalid/incomplete games with audit counts."""
    names = {int(t['team_id']): t['name'] for t in raw['team_names']}
    counts = Counter(); players = []; seen = set(); identities = defaultdict(set)
    teams = {}
    for r in raw['teams']:
        if r['season_type'] != 'Regular Season':
            continue
        k = (str(r['game_id']), int(r['team_id']))
        if k in teams:
            raise ValueError('duplicate team-game identity')
        teams[k] = r
    for r in raw['players']:
        if r['season_type'] != 'Regular Season':
            counts['non_regular_season'] += 1; continue
        k = (str(r['game_id']), str(r['player_id']))
        if k in seen:
            raise ValueError('duplicate player-game identity')
        seen.add(k)
        identities[player_key(r['name'])].add(str(r['player_id']))
        if not valid_stats(r):
            counts['invalid_or_no_appearance'] += 1; continue
        if r.get('team_id') not in names or r.get('opponent_team_id') not in names:
            counts['missing_team_identity'] += 1; continue
        t = teams.get((str(r['game_id']), r['team_id']))
        o = teams.get((str(r['game_id']), r['opponent_team_id']))
        if not t or not o or str(t['game_date']) != str(r['game_date']) or str(o['game_date']) != str(r['game_date']):
            counts['missing_or_mismatched_team_game'] += 1; continue
        if t.get('opponent_team_id') != r['opponent_team_id'] or o.get('opponent_team_id') != r['team_id'] or t['is_home'] == o['is_home'] or r['is_home'] != t['is_home']:
            counts['team_join_disagreement'] += 1; continue
        players.append({**r, 'game_date': str(r['game_date']), 'team_fga': t['fga']})
    if any(len(ids) != 1 for ids in identities.values()):
        raise ValueError('ambiguous canonical player names')
    by_game = defaultdict(list)
    for r in players:
        by_game[str(r['game_id'])].append(r)
    good = []; contexts = defaultdict(list)
    for event, rows in by_game.items():
        sides = {r['team_id'] for r in rows}
        valid = len(sides) == 2
        for side in sides:
            t = teams[(event, side)]; own = [r for r in rows if r['team_id'] == side]
            valid &= abs(sum(r['points'] for r in own)-t['pts']) < .01
            valid &= abs(sum(r['fga'] for r in own)-t['fga']) < .01
            valid &= sum(r['minutes'] for r in own) >= 235
        if not valid:
            counts['unreconciled_games'] += 1; continue
        good.extend(rows)
        for side in sides:
            t = teams[(event, side)]; o = teams[(event, t['opponent_team_id'])]
            own = [r for r in rows if r['team_id'] == side]
            other = [r for r in rows if r['team_id'] == t['opponent_team_id']]
            # Team player-minutes include OT; normalize the prior observed game.
            scale = 240 / sum(r['minutes'] for r in own)
            oscale = 240 / sum(r['minutes'] for r in other)
            contexts[team_key(names[side])].append({
                'game_date': str(t['game_date']), 'game_id': event,
                'team_possessions48': (t['fga']+.44*t['fta']-t['oreb']+t['tov'])*scale,
                'opponent_possessions48': (o['fga']+.44*o['fta']-o['oreb']+o['tov'])*oscale,
                'opponent_allowed_fga48': o['fga']*oscale,
                'opponent_allowed_fg3a48': o['fg3a']*oscale,
                'opponent_allowed_fta48': o['fta']*oscale,
                'fetched_at': max(str(t.get('fetched_at', '')), str(o.get('fetched_at', ''))),
            })
    # Current ESPN boxes have distinct provider IDs and no offensive-rebound
    # field. Add reconciled attempt allowances, leave possessions unavailable.
    current = recent_context(raw, counts)
    for key, rs in current.items():
        contexts[key].extend(rs)
    for rs in contexts.values():
        rs.sort(key=lambda r:r['game_date'])
    audit = {
        'player_rows': len(raw['players']), 'regular_appearances_usable': len(good),
        'reconciled_games': len({r['game_id'] for r in good}),
        'dates': [min((r['game_date'] for r in good), default=None), max((r['game_date'] for r in good), default=None)],
        'exclusions': dict(counts), 'canonical_players': len(identities),
        'ingestion_after_game_rows': sum(str(r.get('fetched_at', ''))[:10] > r['game_date'] for r in good),
        'historical_timing': 'Retrospective date-ordered replay; historical pregame ingestion not verified.',
        'lineup_stints': 'unavailable', 'historical_pregame_injuries': 'unavailable',
        'tracking': 'unavailable', 'rest': 'days between recorded appearances; not confirmed team rest',
        'opponent_context': 'Prior team box allowances, not defender assignments or shot quality',
        'recent_box_context_team_games': sum(len(rs) for rs in current.values()),
    }
    return sorted(good, key=lambda r:(r['game_date'], str(r['game_id']), str(r['player_id']))), contexts, names, audit


def recent_context(raw, audit):
    """Strict per-provider identity and source-time joins, no NBA/ESPN ID mixing."""
    from research.nba_points_forward import aware
    proofs = defaultdict(list)
    for p in raw.get('classifications',[]):
        proofs[str(p['espn_event_id'])].append(p)
    by_event = defaultdict(list); seen = set()
    for r in raw.get('recent_players',[]):
        k = (str(r['espn_event_id']),str(r['espn_athlete_id']))
        if k in seen:
            raise ValueError('duplicate recent ESPN identity')
        seen.add(k)
        if r.get('played'):
            by_event[str(r['espn_event_id'])].append(r)
    result = defaultdict(list)
    for g in raw.get('recent_games',[]):
        eid = str(g['espn_event_id']); rs = by_event[eid]; ps = proofs[eid]
        if not g['completed'] or not ps or {str(p.get('season_type')) for p in ps} != {'2'}:
            audit['recent_game_classification_missing'] += 1; continue
        if any(str(p['game_date']) != str(g['game_date']) or team_key(p['home_team']) != team_key(g['home_team']) or team_key(p['away_team']) != team_key(g['away_team']) for p in ps):
            raise ValueError('recent classification game mismatch')
        if any(not valid_stats(r) for r in rs):
            audit['recent_game_invalid_stats'] += 1; continue
        if any(team_key(r['team']) != team_key(g['home_team'] if r['is_home'] else g['away_team']) for r in rs):
            raise ValueError('recent player-team mismatch')
        if any(str(r['game_date']) != str(g['game_date']) for r in rs):
            raise ValueError('recent player-game date mismatch')
        sides = {s:[r for r in rs if r['is_home'] is (s == 'home')] for s in ['home','away']}
        if any(sum(r['minutes'] for r in sides[s])<235 or abs(sum(r['points'] for r in sides[s])-g[s+'_score'])>.01 for s in sides):
            audit['recent_game_unreconciled'] += 1; continue
        stamp = max([aware(g['fetched_at'])]+[aware(r['fetched_at']) for r in rs]+[aware(p['captured_at']) for p in ps]).isoformat()
        for side,other in [('home','away'),('away','home')]:
            opp = sides[other]; scale = 240/sum(r['minutes'] for r in opp)
            result[team_key(g[side+'_team'])].append({'game_date':str(g['game_date']),
                'game_id':'espn:'+eid,'fetched_at':stamp,'team_possessions48':None,'opponent_possessions48':None,
                'opponent_allowed_fga48':sum(r['fga'] for r in opp)*scale,
                'opponent_allowed_fg3a48':sum(r['fg3a'] for r in opp)*scale,
                'opponent_allowed_fta48':sum(r['fta'] for r in opp)*scale})
    return result


def context_features(contexts, own, opponent, target, strict_cutoff=None):
    from research.nba_points_forward import aware
    out = {}; ages = {}
    for key, fields in [(own, ['team_possessions48']), (opponent, FEATURES[24:])]:
        rows = [r for r in contexts.get(team_key(key), []) if r['game_date'] < str(target) and daydiff(target,r['game_date']) <= 45
                and (strict_cutoff is None or (r.get('fetched_at') and aware(r['fetched_at']) < aware(strict_cutoff)))][-10:]
        age = daydiff(target, rows[-1]['game_date']) if rows else None
        ages[team_key(key)] = age
        for f in fields:
            field = 'team_possessions48' if f == 'opponent_possessions48' else f
            usable = [r[field] for r in rows if r.get(field) is not None]
            out[f] = mean(usable) if usable else None
    return out, ages


def features(history, target, home, own, opponent, contexts, strict_cutoff=None):
    if any(str(r['game_date']) >= str(target) for r in history):
        raise ValueError('future or same-day player input')
    h = history[-20:]; f = history_features(h)
    if f is None:
        return None, {}
    rest = daydiff(target, h[-1]['game_date'])-1
    ctx, ages = context_features(contexts, own, opponent, target, strict_cutoff)
    f.update(rest_days=min(14, rest), back_to_back=float(rest == 0), home=float(home), **ctx)
    return f, ages


def examples(players, contexts, names):
    histories = defaultdict(list); result = []
    for r in players:
        prior = histories[str(r['player_id'])]
        f, _ = features(prior, r['game_date'], r['is_home'], names[r['team_id']], names[r['opponent_team_id']], contexts)
        if f is not None:
            result.append({'date': r['game_date'], 'event': str(r['game_id']),
                'player_id': str(r['player_id']), 'player_key': player_key(r['name']), 'player': r['name'],
                'team_id': r['team_id'], 'home': bool(r['is_home']), 'features': f,
                'points': r['points'], 'minutes': r['minutes'], 'fga': r['fga'], 'fg3a': r['fg3a'], 'fta': r['fta'],
                'points_history': [h['points'] for h in prior[-20:]],
                'rates_history': [{k:h[k] for k in ['fga','fg3a','fgm','fg3m','fta','ftm']} for h in prior[-20:]],
                'targets': {'minutes': r['minutes'], 'fga_per_min': r['fga']/r['minutes'],
                            'fg3_fraction': r['fg3a']/r['fga'] if r['fga'] else 0,
                            'fta_per_min': r['fta']/r['minutes']}})
        prior.append(r)
    return result


def absence_scenarios(candidate, players, names, known_out, cutoff=None):
    """Observed co-participation analogues, never inferred absence probabilities."""
    groups = defaultdict(set)
    for r in players:
        if cutoff is not None:
            from research.nba_points_forward import aware
            if not r.get('fetched_at') or aware(r['fetched_at']) >= aware(cutoff):
                continue
        groups[(str(r['game_id']), team_key(names[r['team_id']]))].add(player_key(r['name']))
    out = []
    for teammate in known_out:
        absent = []; present = []
        for h in candidate['history']:
            if not h.get('team') or team_key(h['team']) != team_key(teammate['team']):
                continue
            # ESPN histories are not silently joined to NBA provider game IDs.
            if h.get('source') != 'nba_log':
                continue
            roster = groups.get((str(h['source_game_id']), team_key(h['team'])))
            if not roster or teammate['player_key'] == candidate['player_key']:
                continue
            (present if teammate['player_key'] in roster else absent).append(h)
        if len(present) >= 3 and len(absent) >= 3:
            out.append({'reported_out': teammate['player'], 'source_url': teammate['source_url'],
                'observed_at': teammate['observed_at'], 'absent_n': len(absent), 'present_n': len(present),
                'absent_means': {k:mean(r[k] for r in absent) for k in ['minutes','fga','fg3a','fta','points']},
                'present_means': {k:mean(r[k] for r in present) for k in ['minutes','fga','fg3a','fta','points']},
                'interpretation': 'Descriptive co-participation analogue; no confirmed historical injury, causal effect, forecast adjustment or scenario weight.'})
    return out
