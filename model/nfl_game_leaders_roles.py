"""Default role scenarios for the game-leader model (`nfl-game-leaders-roles-v1`).

Without a scenario the model leaves an OUT player's workload in unnamed
newcomer slots, so a team missing its lead back shows a large OTHER:<team>
share and no named leader. This module builds the full-field override the
model already accepts (`role_scenarios` + `scenario_evidence`) from evidence
frozen in the request: the dual depth chart and the team's recent box scores.

Rules (stated priors, not fitted; graded only by `research.nfl_game_leaders grade`):

* Usage is a per-game rate in games the player actually recorded a touch,
  shrunk toward his all-games average by games played:
  rate = (n * played_rate + K * all_games_rate) / (n + K), K = SHRINK_GAMES.
  A missed week is an absence, not a week of zero use; a two-game sample is
  not treated as a full-time role.
* Carries vacated by OUT running backs go to the active backs in depth order,
  split RB_DEPTH_SPLIT. Carries vacated by an OUT quarterback go to the
  highest-ranked active quarterback. Other vacated carries and all vacated
  targets are shared in proportion to the remaining players' rates.
* An active player with no recent touches who sits high on the depth chart
  (WR/TE rank <= 3, RB rank <= 2) gets NEWCOMER_TARGETS targets a game.
* Last season's usage enters as pseudo-games (`prior_weight`), but a player the
  window never saw touch the ball keeps that prior only while the depth chart
  gives him a role (QB 1, RB 1-2, WR/TE 1-3) and Sleeper does not list him
  NA or inactive (v2). v1 handed Josh Jacobs 34% of Green Bay's carries while
  away from the team with no 2026 game.
* A fixed reserve stays in the model's three newcomer slots.
* Players in the recent box scores who are no longer on the roster are named
  at zero, which the model requires for a full field.
"""
from __future__ import annotations

from collections import defaultdict

VERSION = 'nfl-game-leaders-roles-v2'
SHRINK_GAMES = 1.0
# v2 (2026-10-18): a player with no usage in the window keeps his prior-season
# rate only at these depth ranks and never when Sleeper lists him NA/inactive.
PRIOR_ROLE_DEPTH = {'QB': 1, 'RB': 2, 'WR': 3, 'TE': 3}
NO_PRIOR_STATUSES = ('NA', 'INACTIVE')
RB_DEPTH_SPLIT = (.55, .35, .10)
RESERVE = {'carries': .05, 'targets': .04}
NEWCOMER_TARGETS = .5
SURPRISE_SLOTS = 3


def prior_role_plausible(player, rank):
    """May a player with no usage in the window carry his prior-season rate?

    Only when the depth chart places him in a role (the same ranks the newcomer
    rule reads: QB 1, RB 1-2, WR/TE 1-3) and Sleeper does not list him away
    from the team (`NA`) or inactive. Status feeds are compared upper-case."""
    status = (player.get('sleeper_status') or '').upper()
    if status in NO_PRIOR_STATUSES:
        return False
    return rank <= PRIOR_ROLE_DEPTH.get(player['position'], 0)


def depth_ranks(dual_depth):
    """(name, position) -> rank. FantasyPros first, Sleeper as fallback."""
    ranks = {}
    for c in dual_depth.get('comparisons', []):
        rank = (c.get('fantasypros') or {}).get('rank') or (c.get('sleeper') or {}).get('rank')
        if rank:
            ranks[(c['name'], c['position'])] = rank
    for s in dual_depth.get('sleeper_only', []):
        if s.get('depth_order') and (s['name'], s['position']) not in ranks:
            ranks[(s['name'], s['position'])] = s['depth_order']
    return ranks


def usage_rates(team_boxes, games):
    """identity -> {'games_played', 'carries', 'targets'} with shrunk per-game rates."""
    played = defaultdict(lambda: {'n': 0, 'carries': 0., 'targets': 0.})
    for game_id in games:
        for b in team_boxes.get(game_id, []):
            if b['carries'] + b['targets'] > 0:
                played[b['identity']]['n'] += 1
                played[b['identity']]['carries'] += b['carries']
                played[b['identity']]['targets'] += b['targets']
    total = len(games)
    rates = {}
    for identity, v in played.items():
        out = {'games_played': v['n']}
        for action in ('carries', 'targets'):
            on = v[action] / v['n']
            overall = v[action] / total
            out[action] = (v['n'] * on + SHRINK_GAMES * overall) / (v['n'] + SHRINK_GAMES)
        rates[identity] = out
    return rates


def _field(weights, roster, team, reserve):
    total = sum(weights.values())
    if total <= 0:
        raise ValueError(f'No observed usage to build a role scenario for {team}')
    field = {identity: 0. for identity in roster}
    for identity, w in weights.items():
        field[identity] = w / total * (1 - reserve)
    for j in range(SURPRISE_SLOTS):
        field[f'NEW:{team}:{j}'] = reserve / SURPRISE_SLOTS
    return field


def blend_prior(rates, prior_rates, prior_weight):
    """Blend each player's current per-game rates with his prior-season rates as `prior_weight` pseudo-games.

    A player with no current usage but a prior season gets his prior rate. Off when prior_weight is 0.
    Evaluated in nfl-game-leaders-calibration-v1 (docs/nfl-game-leaders-calibration-study.md).
    """
    if not prior_weight or not prior_rates:
        return rates
    out = {}
    for identity in set(rates) | set(prior_rates):
        cur, prior = rates.get(identity), prior_rates.get(identity)
        n = cur['games_played'] if cur else 0
        if prior is None:
            out[identity] = cur
            continue
        out[identity] = {'games_played': n, **{a: ((n * cur[a] if cur else 0.) + prior_weight * prior[a]) / (n + prior_weight)
                                               for a in ('carries', 'targets')}}
    return out


def team_scenario(team, players, dual_depth, team_boxes, games, prior_rates=None, prior_weight=0.):
    """Full-field carries/targets shares for one team plus a plain-language note."""
    active = [p for p in players if p['status'] != 'out']
    outs = [p for p in players if p['status'] == 'out']
    ranks = depth_ranks(dual_depth)

    def rank(p):
        return ranks.get((p['name'], p['position']), 99)

    current = usage_rates(team_boxes, games)
    # v2: last season's rate is a prior for a player's CURRENT role, so a player
    # the window never saw touch the ball carries it only when the depth chart
    # and roster status say he has one now. Measured on 2024-25 active rosters
    # (research/nfl_game_leaders_calibration.py capture): such players touched
    # the ball in the next game 32% of the time for a mean 1.7% of team touches,
    # while the v1 blend handed them their whole prior-season share.
    denied = [p for p in players if p['identity'] not in current and p['identity'] in (prior_rates or {})
              and not prior_role_plausible(p, rank(p))]
    eligible_prior = {i: r for i, r in (prior_rates or {}).items() if i not in {p['identity'] for p in denied}}
    rates = blend_prior(current, eligible_prior, prior_weight)

    request_ids = {p['identity'] for p in players}
    departed = sorted({b['identity'] for g in games for b in team_boxes.get(g, []) if b['identity'] not in request_ids})
    roster = [p['identity'] for p in active] + departed + [f'NEW:{team}:{j}' for j in range(SURPRISE_SLOTS)]

    carries = {p['identity']: rates.get(p['identity'], {}).get('carries', 0.) for p in active}
    vacated_rb = sum(rates.get(p['identity'], {}).get('carries', 0.) for p in outs if p['position'] == 'RB')
    vacated_qb = sum(rates.get(p['identity'], {}).get('carries', 0.) for p in outs if p['position'] == 'QB')
    backs = sorted((p for p in active if p['position'] == 'RB'), key=rank)[:len(RB_DEPTH_SPLIT)]
    if vacated_rb and backs:
        split = RB_DEPTH_SPLIT[:len(backs)]
        for p, share in zip(backs, split):
            carries[p['identity']] += vacated_rb * share / sum(split)
    quarterbacks = sorted((p for p in active if p['position'] == 'QB'), key=rank)
    if vacated_qb and quarterbacks:
        carries[quarterbacks[0]['identity']] += vacated_qb

    targets = {}
    for p in active:
        rate = rates.get(p['identity'], {}).get('targets')
        if rate is None:
            high = (p['position'] in ('WR', 'TE') and rank(p) <= 3) or (p['position'] == 'RB' and rank(p) <= 2)
            rate = NEWCOMER_TARGETS if high else 0.
        targets[p['identity']] = rate

    scenario = {'carries': _field(carries, roster, team, RESERVE['carries']),
                'targets': _field(targets, roster, team, RESERVE['targets'])}
    name = {p['identity']: p['name'] for p in players}
    note = {'out_with_usage': [f"{p['name']} ({p['position']}, {rates[p['identity']]['carries']:.1f} car, "
                               f"{rates[p['identity']]['targets']:.1f} tgt a game)" for p in outs if p['identity'] in rates],
            'vacated_rb_carries': round(vacated_rb, 1),
            'rb_depth_order': [p['name'] for p in backs],
            'vacated_qb_carries': round(vacated_qb, 1),
            'qb_receiving_carries': quarterbacks[0]['name'] if vacated_qb and quarterbacks else None,
            'departed_named_at_zero': len(departed),
            'prior_role_denied': [f"{p['name']} ({p['position']} rank {rank(p)}, {p.get('sleeper_status') or 'no status'})"
                                  for p in denied],
            'top_carries': {name[i]: round(v, 3) for i, v in sorted(scenario['carries'].items(), key=lambda kv: -kv[1])[:3] if i in name},
            'top_targets': {name[i]: round(v, 3) for i, v in sorted(scenario['targets'].items(), key=lambda kv: -kv[1])[:4] if i in name}}
    return scenario, note


def attach(request, boxes_by_game_team, prior_games, prior_rates=None, prior_weight=0.):
    """Return a copy of `request` with role scenarios and evidence. Never edits an existing scenario."""
    if request.get('role_scenarios'):
        return request
    game = request['game']
    scenarios, notes = {}, {}
    for team in (game['away'], game['home']):
        team_boxes = {g: boxes_by_game_team.get((g, team), []) for g in prior_games[team]}
        players = [p for p in request['players'] if p['team'] == team]
        scenarios[team], notes[team] = team_scenario(team, players, request['dual_depth'][team],
                                                     team_boxes, prior_games[team], prior_rates, prior_weight)
    captured = max(request['dual_depth'][t]['captured_at'] for t in scenarios)
    if captured > request['decision_at']:
        raise ValueError('Depth evidence captured after the decision time')
    games = sorted({g for v in prior_games.values() for g in v})
    evidence = {'source_ref': f'{VERSION}: request.dual_depth + recent box scores {games}',
                'captured_at': captured, 'method': VERSION, 'prior_weight': prior_weight,
                'description': 'Default role scenario (stated priors, not fitted). See model/nfl_game_leaders_roles.py.',
                'notes': notes}
    return {**request, 'role_scenarios': scenarios, 'scenario_evidence': evidence}


def boxes_index(snapshot):
    index = defaultdict(list)
    for b in snapshot['boxes']:
        index[(b['game_id'], b['team'])].append(b)
    return index
