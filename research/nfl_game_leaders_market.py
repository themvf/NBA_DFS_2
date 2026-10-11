"""Market-implied game leaders from sportsbook over/under props (`nfl-leaders-market-implied-v1`).

The Odds API has no NFL "most yards" market, only per-player over/unders. This
turns those into leader probabilities while keeping the model's own joint
structure (shared game script, competition for touches):

1. Re-create each frozen forecast with its simulated draws (same seed, so the
   win shares must equal the published ones; checked).
2. For each player with a pregame line, de-vig the over/under at the chosen
   book and shift that player's draws so P(X > line) equals the market's
   fair over probability. Players without a line keep the model's draws.
3. Re-resolve the leader per draw (ties split equally, unnamed identities
   grouped after resolving, exactly as the model does).

The result is a benchmark, not a forecast: it depends on the model's
distribution shapes and correlations, and on which players the books list.

    python -m research.nfl_game_leaders_market export --input <capture.json.gz> --requests <requests.json> --output <dir>
    python -m research.nfl_game_leaders_market compare --draws <dir> --published <published.json> --output <report.json>
"""
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

VERSION = 'nfl-leaders-market-implied-v1'
MARKETS = {'rushing_yards': ('player_rush_yds', 'rushYds'),
           'receiving_yards': ('player_reception_yds', 'recYds'),
           'receptions': ('player_receptions', 'receptions')}
BOOK_ORDER = ('draftkings', 'fanduel', 'betmgm', 'caesars', 'williamhill_us', 'espnbet', 'betrivers', 'fanatics')


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
            return key, books[key]['line'], fair_over(books[key])
    return None


def shift_to_market(draws, line, p_over):
    """Additive shift so the share of draws above `line` equals p_over."""
    q = float(np.quantile(draws, 1 - p_over))
    return draws + (line - q)


def leader_shares(players, arrays):
    """Win share per player: ties split equally across all tied identities."""
    matrix = np.vstack(arrays)
    best = matrix.max(axis=0)
    tied = matrix == best
    shares = (tied / tied.sum(axis=0)).mean(axis=1)
    out = {}
    for p, s in zip(players, shares):
        key = f"OTHER:{p['team']}" if p.get('residual') else p['name']
        out[key] = out.get(key, 0.) + float(s)
    return out


def export(snapshot_path, requests_path, out_dir):
    from model.nfl_game_leaders import Settings, forecast, prepare
    from research.nfl_game_leaders import expected_history, read
    snapshot = read(snapshot_path)
    requests = read(requests_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    cache = {}
    for game_id, req in requests.items():
        if 'players' not in req:
            continue
        if req['decision_at'] not in cache:
            cache[req['decision_at']] = prepare(snapshot, req['decision_at'])[0]
        history = cache[req['decision_at']]
        cfg = Settings()
        req = {**req, 'expected_prior_game_ids': expected_history(snapshot, req['game'], req['decision_at'], cfg.recent_games)}
        result = forecast(history, req, cfg, include_draws=True)
        bank = result['shared_draws']
        compact = {'game': result['game'], 'decision_at': result['decision_at'],
                   'win_shares': {m: {p['identity']: p['win_share'] for p in result['metrics'][m]['players']} for m in MARKETS},
                   'players': [{'identity': p['identity'], 'name': p['name'], 'team': p['team'], 'residual': p.get('residual', False),
                                'draws': {f: p['draws'][f] for _, f in MARKETS.values()}} for p in bank['players']]}
        (out_dir / f'{game_id}.json').write_text(json.dumps(compact))
        print(json.dumps({'exported': game_id}), flush=True)


def load_props():
    import os
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv('.env')
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()
    cur.execute('BEGIN READ ONLY')
    cur.execute("""SELECT DISTINCT ON (event_id, market, player) event_id, home_team_name, away_team_name, commence_time,
                          market, player, books, captured_at
                   FROM prop_odds_history WHERE sport='nfl' AND captured_at < commence_time
                   ORDER BY event_id, market, player, captured_at DESC""")
    rows = cur.fetchall()
    cur.execute('ROLLBACK')
    conn.close()
    return rows


def compare(draw_dir, published_path, output):
    published = {g['game']['game_id']: g for g in json.loads(Path(published_path).read_text())['games']}
    props = load_props()
    report = {'version': VERSION, 'created_at': datetime.now(timezone.utc).isoformat(), 'games': {}}
    for path in sorted(draw_dir.glob('*.json')):
        bank = json.loads(path.read_text())
        gid = bank['game']['game_id']
        players = bank['players']
        # Re-created shares must equal the published ones, or the comparison is against a different forecast.
        pub = {p['identity']: p['win_share'] for p in published[gid]['metrics']['rushing_yards']['players']}
        assert all(abs(pub[i] - s) < 1e-9 for i, s in bank['win_shares']['rushing_yards'].items()), gid
        names = {norm(p['name']): j for j, p in enumerate(players) if not p['residual']}
        # Several games share a date: take the sportsbook event whose listed players overlap this game most.
        by_event = {}
        for r in props:
            by_event.setdefault(r[0], []).append(r)
        overlap = {e: sum(norm(r[5]) in names for r in rows) for e, rows in by_event.items()}
        event_id = max(overlap, key=overlap.get) if overlap else None
        if not event_id or overlap[event_id] < 5:
            print(json.dumps({'skipped': gid, 'reason': 'no sportsbook event matched'}), flush=True)
            continue
        game_props = by_event[event_id]
        entry = {}
        for metric, (market_key, field) in MARKETS.items():
            arrays = [np.asarray(p['draws'][field], float) for p in players]
            model = leader_shares(players, arrays)
            adjusted, matched, unmatched = list(arrays), [], []
            for event_id, home, away, commence, market, player, books, captured in game_props:
                if market != market_key:
                    continue
                j = names.get(norm(player))
                if j is None:
                    continue
                quote = pick_quote(books)
                if quote is None:
                    continue
                book, line, p_over = quote
                adjusted[j] = shift_to_market(arrays[j], line, p_over)
                matched.append({'player': players[j]['name'], 'book': book, 'line': line, 'fair_over': round(p_over, 4),
                                'model_p_over': round(float((arrays[j] > line).mean()), 4), 'captured_at': str(captured)})
            listed = {norm(r[5]) for r in game_props if r[4] == market_key}
            unmatched = sorted(r[5] for r in game_props if r[4] == market_key and norm(r[5]) not in names)
            market_shares = leader_shares(players, adjusted)
            entry[metric] = {'model': model, 'market_implied': market_shares, 'matched_lines': matched,
                             'unmatched_market_players': sorted(set(unmatched)), 'players_with_lines': len(matched),
                             'book_listed': len(listed)}
        report['games'][gid] = entry
        print(json.dumps({'compared': gid, 'lines': {m: entry[m]['players_with_lines'] for m in entry}}), flush=True)
    output.write_text(json.dumps(report, indent=1))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    e = sub.add_parser('export')
    e.add_argument('--input', type=Path, required=True)
    e.add_argument('--requests', type=Path, required=True)
    e.add_argument('--output', type=Path, required=True)
    c = sub.add_parser('compare')
    c.add_argument('--draws', type=Path, required=True)
    c.add_argument('--published', type=Path, required=True)
    c.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'export':
        export(args.input, args.requests, args.output)
    else:
        if args.output.exists():
            raise SystemExit(f'Refusing to overwrite {args.output}')
        compare(args.draws, args.published, args.output)


if __name__ == '__main__':
    main()
