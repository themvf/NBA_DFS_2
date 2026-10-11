"""Market-implied game leaders from sportsbook over/under props (`nfl-leaders-market-implied-v1`).

The Odds API has no NFL "most yards" market, only per-player over/unders. This
turns those into leader probabilities while keeping the model's own joint
structure (shared game script, competition for touches):

1. Re-create each saved forecast with its simulated draws (same seed). The
   re-created leader shares must equal the published ones, or the run fails.
2. For each player with a pregame line, de-vig the over/under at the preferred
   book (DraftKings first) and shift that player's draws so P(X > line) equals
   the market's fair over probability. Players without a line keep the model's
   draws.
3. Re-resolve the leader per draw (ties split equally, unnamed identities
   grouped after resolving, exactly as the model does).

The result is a benchmark, not a forecast: it depends on the model's
distribution shapes and correlations, and on which players the book lists.

    python -m research.nfl_game_leaders_market refresh [--within-hours 2] [--write-db]
        Prices upcoming games on the published page and stores them in
        nfl_game_leader_market (the page reads the latest row set per game).
    python -m research.nfl_game_leaders_market export --input <capture.json.gz> --requests <requests.json> --output <dir>
    python -m research.nfl_game_leaders_market compare --draws <dir> --published <published.json> --output <report.json>
"""
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

VERSION = 'nfl-leaders-market-implied-v1'
MARKETS = {'rushing_yards': ('player_rush_yds', 'rushYds'),
           'receiving_yards': ('player_reception_yds', 'recYds'),
           'receptions': ('player_receptions', 'receptions')}
BOOK_ORDER = ('draftkings', 'fanduel', 'betmgm', 'caesars', 'williamhill_us', 'espnbet', 'betrivers', 'fanatics')
PUBLISHED = Path('web/src/data/game-leaders.json')
MIN_EVENT_OVERLAP = 5


def norm(name):
    s = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode().lower()
    s = re.sub(r'\b(jr|sr|ii|iii|iv|v)\b', '', s)
    return re.sub(r'[^a-z]', '', s)


def american_to_prob(price):
    return 100 / (price + 100) if price > 0 else -price / (-price + 100)


def fair_over(book):
    """De-vigged P(over) from one book's paired over/under, or None."""
    if book.get('over') is None or book.get('under') is None or book.get('line') is None:
        return None
    o, u = american_to_prob(book['over']), american_to_prob(book['under'])
    return o / (o + u)


def pick_quote(books):
    for key in BOOK_ORDER:
        if key in books and fair_over(books[key]) is not None:
            b = books[key]
            return {'book': key, 'line': b['line'], 'over': b['over'], 'under': b['under'], 'fair_over': fair_over(b)}
    return None


def shift_to_market(draws, line, p_over):
    """Additive shift so the share of draws above `line` equals p_over."""
    q = float(np.quantile(draws, 1 - p_over))
    return draws + (line - q)


def leader_shares(players, arrays):
    """Win share per identity: ties split equally across all tied identities."""
    matrix = np.vstack(arrays)
    best = matrix.max(axis=0)
    tied = matrix == best
    return (tied / tied.sum(axis=0)).mean(axis=1)


def group_key(player):
    return f"OTHER:{player['team']}" if player.get('residual') else player['identity']


def match_event(players, props):
    """The sportsbook event whose listed players overlap this game most (several games share a date)."""
    names = {norm(p['name']) for p in players if not p.get('residual')}
    by_event = {}
    for r in props:
        by_event.setdefault(r['event_id'], []).append(r)
    overlap = {e: sum(norm(r['player']) in names for r in rows) for e, rows in by_event.items()}
    if not overlap:
        return None, []
    event_id = max(overlap, key=overlap.get)
    return (event_id, by_event[event_id]) if overlap[event_id] >= MIN_EVENT_OVERLAP else (None, [])


def price_game(players, game_props):
    """Per metric: one row per grouped identity with model and market-implied leader share, plus the line used."""
    index = {norm(p['name']): j for j, p in enumerate(players) if not p.get('residual')}
    out = {}
    for metric, (market_key, field) in MARKETS.items():
        arrays = [np.asarray(p['draws'][field], float) for p in players]
        adjusted, quotes = list(arrays), {}
        for r in game_props:
            if r['market'] != market_key:
                continue
            j = index.get(norm(r['player']))
            quote = pick_quote(r['books']) if j is not None else None
            if quote is None:
                continue
            adjusted[j] = shift_to_market(arrays[j], quote['line'], quote['fair_over'])
            quotes[j] = {**quote, 'model_p_over': float((arrays[j] > quote['line']).mean()), 'captured_at': r['captured_at']}
        model, market = leader_shares(players, arrays), leader_shares(players, adjusted)
        rows = {}
        for j, p in enumerate(players):
            key = group_key(p)
            row = rows.setdefault(key, {'identity': key, 'player': key if p.get('residual') else p['name'], 'team': p['team'],
                                        'model_leader_share': 0., 'market_leader_share': 0., 'quote': None})
            row['model_leader_share'] += float(model[j])
            row['market_leader_share'] += float(market[j])
            if j in quotes:
                row['quote'] = quotes[j]
        listed = {norm(r['player']) for r in game_props if r['market'] == market_key}
        out[metric] = {'rows': list(rows.values()), 'unmatched_market_players': sorted({r['player'] for r in game_props
                       if r['market'] == market_key and norm(r['player']) not in index}), 'book_listed': len(listed)}
    return out


def forecast_bank(history, snapshot, request):
    """Re-create the saved forecast with draws; returns (players, published-style win shares by metric)."""
    from model.nfl_game_leaders import Settings, forecast
    from research.nfl_game_leaders import expected_history
    cfg = Settings()
    request = {**request, 'expected_prior_game_ids': expected_history(snapshot, request['game'], request['decision_at'], cfg.recent_games)}
    result = forecast(history, request, cfg, include_draws=True)
    players = [{'identity': p['identity'], 'name': p['name'], 'team': p['team'], 'residual': p.get('residual', False),
                'draws': {f: p['draws'][f] for _, f in MARKETS.values()}} for p in result['shared_draws']['players']]
    shares = {m: {p['identity']: p['win_share'] for p in result['metrics'][m]['players']} for m in MARKETS}
    return players, shares, result['decision_at']


def check_matches_published(game_id, shares, published_game):
    """The page must describe the same forecast the market is applied to."""
    for metric in MARKETS:
        published = {p['identity']: p['win_share'] for p in published_game['metrics'][metric]['players']}
        if set(published) != set(shares[metric]) or any(abs(published[i] - s) > 1e-9 for i, s in shares[metric].items()):
            raise ValueError(f'{game_id} {metric}: re-created forecast differs from the published one; republish before pricing')


def _connect():
    import os
    import psycopg2
    url = os.environ.get('DATABASE_URL')
    if not url:
        from dotenv import load_dotenv
        load_dotenv('.env')
        url = os.environ.get('DATABASE_URL')
    if not url:
        raise SystemExit('DATABASE_URL is required')
    return psycopg2.connect(url)


def load_props(cur):
    """Latest pregame capture per (event, market, player)."""
    cur.execute("""SELECT DISTINCT ON (event_id, market, player) event_id, commence_time, market, player, books, captured_at
                   FROM prop_odds_history WHERE sport='nfl' AND captured_at < commence_time
                   ORDER BY event_id, market, player, captured_at DESC""")
    return [dict(zip(('event_id', 'commence_time', 'market', 'player', 'books', 'captured_at'), r)) for r in cur.fetchall()]


def refresh(published_path=PUBLISHED, within_hours=0., write_db=False, now=None, log=print, include_started=False, games_filter=None):
    from research.nfl_game_leaders import read
    from model.nfl_game_leaders import prepare
    from model.nfl_longest_touchdown import timestamp
    now = now or datetime.now(timezone.utc)
    published = json.loads(Path(published_path).read_text())
    # A page can mix runs (a game re-run before kickoff, one frozen after it): each game names its own inputs.
    def sources_for(game):
        src = game.get('sources') or published.get('sources')
        if not src:
            raise SystemExit('Published page has no sources (snapshot/requests paths); republish with --snapshot and --requests')
        return src
    # Started games are priced only on request: their lines must still be pregame captures (load_props).
    games = [g for g in published['games'] if (include_started or timestamp(g['game']['kickoff']) > now) and (
        not within_hours or timestamp(g['game']['kickoff']) <= now + timedelta(hours=within_hours))
        and (not games_filter or g['game']['game_id'] in games_filter)]
    if not games:
        log(json.dumps({'priced': 0, 'reason': 'no upcoming published game in the window'}))
        return []
    loaded = {}

    def inputs(game):
        src = sources_for(game)
        key = (src['snapshot'], src['requests'])
        if key not in loaded:
            loaded[key] = (read(Path(src['snapshot'])), read(Path(src['requests'])))
        return loaded[key]

    conn = _connect()
    cur = conn.cursor()
    props = load_props(cur)
    conn.rollback()
    histories, written = {}, []
    for g in games:
        gid = g['game']['game_id']
        snapshot, requests = inputs(g)
        request = requests[gid]
        if (id(snapshot), request['decision_at']) not in histories:
            histories[id(snapshot), request['decision_at']] = prepare(snapshot, request['decision_at'])[0]
        players, shares, decision_at = forecast_bank(histories[id(snapshot), request['decision_at']], snapshot, request)
        check_matches_published(gid, shares, g)
        event_id, game_props = match_event(players, props)
        if not event_id:
            log(json.dumps({'skipped': gid, 'reason': 'no captured sportsbook event matched this game'}), flush=True)
            continue
        captured = max(r['captured_at'] for r in game_props)
        # Unchanged only when both the lines and the saved forecast are the ones already priced.
        cur.execute('SELECT max(prop_captured_at) FROM nfl_game_leader_market WHERE game_id=%s AND forecast_decision_at=%s',
                    (gid, decision_at))
        previous = cur.fetchone()[0]
        conn.rollback()
        if previous is not None and previous >= captured:
            log(json.dumps({'skipped': gid, 'reason': 'no new capture or forecast since the last pricing'}), flush=True)
            continue
        priced = price_game(players, game_props)
        computed = datetime.now(timezone.utc)
        rows = [(gid, metric, r['identity'], r['player'], r['team'],
                 *((q['book'], q['line'], q['over'], q['under'], q['fair_over'], q['model_p_over']) if (q := r['quote']) else (None,) * 6),
                 r['model_leader_share'], r['market_leader_share'], captured, decision_at, computed, VERSION)
                for metric, v in priced.items() for r in v['rows']]
        if write_db:
            cur.executemany("""INSERT INTO nfl_game_leader_market (game_id, metric, identity, player, team, book, line,
                    over_price, under_price, fair_over, model_p_over, model_leader_share, market_leader_share,
                    prop_captured_at, forecast_decision_at, computed_at, version)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rows)
            conn.commit()
        written.append(gid)
        log(json.dumps({'priced': gid, 'rows': len(rows), 'lines': {m: sum(r['quote'] is not None for r in v['rows'])
                        for m, v in priced.items()}, 'written': write_db}), flush=True)
    conn.close()
    return written


def export(snapshot_path, requests_path, out_dir):
    from model.nfl_game_leaders import prepare
    from research.nfl_game_leaders import read
    snapshot, requests = read(snapshot_path), read(requests_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    histories = {}
    for game_id, req in requests.items():
        if 'players' not in req:
            continue
        if req['decision_at'] not in histories:
            histories[req['decision_at']] = prepare(snapshot, req['decision_at'])[0]
        players, shares, decision_at = forecast_bank(histories[req['decision_at']], snapshot, req)
        (out_dir / f'{game_id}.json').write_text(json.dumps({'game': req['game'], 'decision_at': decision_at,
                                                              'win_shares': shares, 'players': players}))
        print(json.dumps({'exported': game_id}), flush=True)


def compare(draw_dir, published_path, output):
    published = {g['game']['game_id']: g for g in json.loads(Path(published_path).read_text())['games']}
    conn = _connect()
    props = load_props(conn.cursor())
    conn.close()
    report = {'version': VERSION, 'created_at': datetime.now(timezone.utc).isoformat(), 'games': {}}
    for path in sorted(draw_dir.glob('*.json')):
        bank = json.loads(path.read_text())
        gid = bank['game']['game_id']
        check_matches_published(gid, bank['win_shares'], published[gid])
        event_id, game_props = match_event(bank['players'], props)
        if not event_id:
            print(json.dumps({'skipped': gid}), flush=True)
            continue
        priced = price_game(bank['players'], game_props)
        report['games'][gid] = {m: {
            'model': {r['player']: r['model_leader_share'] for r in v['rows']},
            'market_implied': {r['player']: r['market_leader_share'] for r in v['rows']},
            'matched_lines': [{'player': r['player'], 'book': r['quote']['book'], 'line': r['quote']['line'],
                               'fair_over': round(r['quote']['fair_over'], 4), 'model_p_over': round(r['quote']['model_p_over'], 4),
                               'captured_at': str(r['quote']['captured_at'])} for r in v['rows'] if r['quote']],
            'unmatched_market_players': v['unmatched_market_players'], 'book_listed': v['book_listed']}
            for m, v in priced.items()}
        print(json.dumps({'compared': gid}), flush=True)
    output.write_text(json.dumps(report, indent=1))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    r = sub.add_parser('refresh')
    r.add_argument('--published', type=Path, default=PUBLISHED)
    r.add_argument('--within-hours', type=float, default=0.)
    r.add_argument('--write-db', action='store_true')
    r.add_argument('--include-started', action='store_true', help='also price games already under way (pregame lines only)')
    r.add_argument('--game', action='append', help='limit to these game ids')
    e = sub.add_parser('export')
    e.add_argument('--input', type=Path, required=True)
    e.add_argument('--requests', type=Path, required=True)
    e.add_argument('--output', type=Path, required=True)
    c = sub.add_parser('compare')
    c.add_argument('--draws', type=Path, required=True)
    c.add_argument('--published', type=Path, required=True)
    c.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'refresh':
        refresh(args.published, args.within_hours, args.write_db, include_started=args.include_started,
                games_filter=set(args.game) if args.game else None)
    elif args.command == 'export':
        export(args.input, args.requests, args.output)
    else:
        if args.output.exists():
            raise SystemExit(f'Refusing to overwrite {args.output}')
        compare(args.draws, args.published, args.output)


if __name__ == '__main__':
    main()
