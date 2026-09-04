"""Lossless field-level differences between saved CFB pregame snapshots.

These are observations, not betting signals. No size/time/book-count threshold.
Observed timestamps bound a change; they do not establish its exact market time.
"""
from __future__ import annotations

from collections import defaultdict

FIELDS = {
    "spread": ("spread_home", "spread_away", "spread_home_price", "spread_away_price"),
    "total": ("total_line", "over", "under"),
    "moneyline": ("ml_home", "ml_away"),
}


def movements(history):
    """Yield every observed field transition, including availability transitions.

    First snapshot is a baseline, not an invented move. A quote returning after
    absence is 'appeared', never a price change across an unobserved interval.
    """
    previous = None
    for current in sorted(history, key=lambda r: (r["captured_at"], r["history_id"])):
        if previous is not None:
            old_books, new_books = previous.get("books") or {}, current.get("books") or {}
            for book in sorted(set(old_books) | set(new_books)):
                old, new = old_books.get(book) or {}, new_books.get(book) or {}
                for market, fields in FIELDS.items():
                    for field in fields:
                        before, after = old.get(field), new.get(field)
                        if before == after:
                            continue
                        yield {
                            "previous_history_id": previous["history_id"],
                            "history_id": current["history_id"], "book": book,
                            "market": market, "field": field,
                            "kind": "appeared" if before is None else "disappeared" if after is None else "changed",
                            "before": before, "after": after,
                        }
        previous = current


def record_movements(db):
    """Idempotently reconcile all saved history, including completed games.

    Replay-derived rows retain source capture IDs; recorded_at is processing time,
    not a claim that a historical observation was detected prospectively.
    """
    from psycopg2.extras import Json, execute_values

    with db.connect() as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT pg_advisory_xact_lock(73402191)")
        cursor.execute("""SELECT h.id AS history_id,h.matchup_id,h.books,h.captured_at
            FROM game_odds_history h JOIN cfb_matchups m ON m.id=h.matchup_id
            WHERE h.sport='cfb' AND h.captured_at<m.commence_time
            ORDER BY h.matchup_id,h.captured_at,h.id""")
        histories = defaultdict(list)
        for row in cursor.fetchall():
            histories[row["matchup_id"]].append(dict(row))
        expected = []
        for matchup_id, history in histories.items():
            for move in movements(history):
                expected.append((matchup_id, move["previous_history_id"], move["history_id"],
                                 move["book"], move["market"], move["field"], move["kind"],
                                 Json(move["before"]), Json(move["after"])))
        if expected:
            execute_values(cursor, """INSERT INTO cfb_quote_movements
                (matchup_id,previous_history_id,history_id,book,market,field,kind,before_value,after_value)
                VALUES %s ON CONFLICT (history_id,book,field) DO NOTHING""", expected)
        cursor.execute("SELECT * FROM cfb_quote_movements")
        actual = {(r["history_id"], r["book"], r["field"]): dict(r) for r in cursor.fetchall()}
        expected_keys = set()
        for matchup_id, history in histories.items():
            for move in movements(history):
                key = (move["history_id"], move["book"], move["field"])
                expected_keys.add(key)
                saved = actual.get(key)
                if saved is None or any(saved[k] != move[k] for k in
                        ("previous_history_id", "market", "kind")) or (
                        saved["matchup_id"] != matchup_id or saved["before_value"] != move["before"]
                        or saved["after_value"] != move["after"]):
                    raise ValueError(f"CFB movement evidence mismatch: {key}")
        if set(actual) != expected_keys:
            raise ValueError("CFB movement ledger contains transitions not supported by pregame history")
        return {"integrity_status": "pass", "snapshots": sum(map(len, histories.values())),
                "field_transitions": len(expected), "games": len(histories)}


if __name__ == "__main__":
    from config import load_config
    from db.database import DatabaseManager
    print(record_movements(DatabaseManager(load_config().database_url)))
