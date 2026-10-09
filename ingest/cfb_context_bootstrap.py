"""Bootstrap typed identities and migrate legacy CFB quote evidence.

This migration does not acquire data and does not claim raw-retention rights.
Existing normalized rows are classified ``legacy_unverified`` until a reviewed
provider evidence policy permits stronger replay claims. The optional auto
origin mode preserves pre-pilot rows as legacy and marks newly stored captures
from the frozen pilot start as prospective without changing provider rights.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
from uuid import UUID, uuid5

from config import load_config


NAMESPACE = UUID("9fa3a542-a6a6-4fd6-b759-82126722e5e0")
NORMALIZATION_VERSION = "legacy-game-odds-history-v1"
PILOT_START = datetime(2026, 9, 25, tzinfo=timezone.utc)


def capture_origin(captured_at: datetime, mode: str) -> str:
    if mode == "legacy":
        return "legacy"
    if mode in {"auto", "prospective"} and captured_at.tzinfo is not None:
        return "prospective" if captured_at >= PILOT_START else "legacy"
    raise ValueError("auto origin classification requires a timezone-aware capture time")


def _id(kind: str, key: object) -> UUID:
    return uuid5(NAMESPACE, f"{kind}:{key}")


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _digest(value: object) -> str:
    return sha256(_json(value).encode()).hexdigest()


def _decimal(american: object) -> float | None:
    try:
        price = float(american)
    except (TypeError, ValueError):
        return None
    if price == 0:
        return None
    return 1 + (100 / abs(price) if price < 0 else price / 100)


def _artifacts() -> list[tuple]:
    values = [
        ("schema", "cfb-context-payload-schema-v1", "schema"),
        ("code", "cfb-market-movement-context-v1", "code"),
        ("code", "cfb-offensive-drive-volume-v1", "code"),
        ("configuration", "cfb-market-allowlist-v1", "configuration"),
        ("report", "collegefootballdata-terms-unresolved-v1", "report"),
        ("report", "the-odds-api-terms-unresolved-v1", "report"),
        ("schema", "legacy-cfb-schedule-projection-v1", "schema"),
        ("schema", "legacy-cfb-quote-projection-v1", "schema"),
    ]
    return [(_id("artifact", name), kind, _digest({"name": name}), None, representation, 0,
             json.dumps({"name": name, "contract_revision": 3})) for kind, name, representation in values]


def _quotes(capture_id: UUID, source_id: UUID, observed_at: datetime, books: dict) -> list[tuple]:
    output = []
    for book, quote in sorted((books or {}).items()):
        updated = quote.get("last_update")
        markets = [
            ("spread", "home", quote.get("spread_home"), quote.get("spread_home_price")),
            ("spread", "away", quote.get("spread_away"), quote.get("spread_away_price")),
            ("total", "over", quote.get("total_line"), quote.get("over")),
            ("total", "under", quote.get("total_line"), quote.get("under")),
            ("moneyline", "home", None, quote.get("ml_home")),
            ("moneyline", "away", None, quote.get("ml_away")),
        ]
        for market, selection, line, american in markets:
            price = _decimal(american)
            if price is None or price <= 1 or (market != "moneyline" and line is None):
                continue
            locator = f"{book}:{market}:{selection}"
            payload = {"book": book, "market": market, "selection": selection, "line": line,
                       "decimal_price": round(price, 10), "bookmaker_updated_at": updated}
            output.append((_id("quote", f"{capture_id}:{locator}"), capture_id, source_id, locator,
                           book, market, selection, line, price, updated, observed_at, None, "main", _digest(payload)))
    return output


def bootstrap(database_url: str, *, apply: bool, new_origin: str = "legacy") -> dict:
    import psycopg2
    from psycopg2.extras import Json, RealDictCursor, execute_values, register_uuid

    register_uuid()

    with psycopg2.connect(database_url, cursor_factory=RealDictCursor) as connection:
        connection.autocommit = False
        cursor = connection.cursor()
        cursor.execute("SET LOCAL lock_timeout='30s'")
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("cfb-context-bootstrap-v1",))
        cursor.execute("SELECT team_id FROM cfb_teams ORDER BY team_id")
        teams = list(cursor.fetchall())
        cursor.execute("SELECT * FROM cfb_matchups ORDER BY id")
        games = list(cursor.fetchall())
        cursor.execute("""SELECT h.id,h.matchup_id,h.capture_key,h.books,h.captured_at
          FROM game_odds_history h WHERE h.sport='cfb' AND NOT EXISTS (
            SELECT 1 FROM cfb_engine_captures c
            WHERE c.history_id=h.id AND c.normalization_version=%s)
          ORDER BY h.id""", (NORMALIZATION_VERSION,))
        histories = list(cursor.fetchall())
        if not apply:
            connection.rollback()
            return {"mode": "plan", "teams": len(teams), "events": len(games), "captures": len(histories),
                    "estimated_quotes": sum(len(_quotes(_id('capture', row['id']), _id('source', row['id']), row['captured_at'], row['books'] or {})) for row in histories)}

        execute_values(cursor, """INSERT INTO cfb_engine_artifacts
          (artifact_id,kind,digest,uri,representation,byte_count,metadata) VALUES %s
          ON CONFLICT DO NOTHING""", _artifacts())
        policy_rows = []
        for provider, terms in (("collegefootballdata", "collegefootballdata-terms-unresolved-v1"),
                                ("the-odds-api", "the-odds-api-terms-unresolved-v1")):
            policy_rows.append((_id("evidence-policy", f"{provider}:1"), provider, 1, "unknown",
                                _id("artifact", terms), None, None, None,
                                Json({"status": "unresolved", "normalized_legacy_migration_only": True})))
        execute_values(cursor, """INSERT INTO cfb_engine_evidence_policies
          (policy_id,provider,version,retention_mode,terms_artifact_id,approved_by,approved_at,retain_until,scope)
          VALUES %s ON CONFLICT(provider,version) DO NOTHING""", policy_rows)

        subjects = [(f"cfb:team:{row['team_id']}", "cfb", "team") for row in teams]
        subjects += [(f"cfb:event:{row['id']}", "cfb", "event") for row in games]
        execute_values(cursor, "INSERT INTO cfb_engine_subjects(subject_key,namespace,entity_type) VALUES %s ON CONFLICT DO NOTHING", subjects)
        execute_values(cursor, "INSERT INTO cfb_engine_teams(team_key,entity_type,team_id) VALUES %s ON CONFLICT DO NOTHING",
                       [(f"cfb:team:{row['team_id']}", "team", row["team_id"]) for row in teams])
        execute_values(cursor, "INSERT INTO cfb_engine_events(event_key,entity_type,matchup_id) VALUES %s ON CONFLICT DO NOTHING",
                       [(f"cfb:event:{row['id']}", "event", row["id"]) for row in games])

        payload_schema = _id("artifact", "cfb-context-payload-schema-v1")
        definitions = [
            ("cfb_market_movement_context", 1, payload_schema, _id("artifact", "cfb-market-movement-context-v1"),
             "event", "line_points", "observed", Json(["paired main lines", "bookmaker_updated_at"]), 300, 300,
             Json({"start_age_seconds": [900, 1800], "minimum_common_books": 4}), "cfb-research-owner"),
            ("cfb_offensive_drive_volume", 1, payload_schema, _id("artifact", "cfb-offensive-drive-volume-v1"),
             "team", "offensive_drives_per_game", "observed", Json(["cfbd_drive_id", "offense_team_id", "ingested_at"]),
             604800, 31536000, Json({"history_window_games": 4}), "cfb-research-owner"),
        ]
        execute_values(cursor, """INSERT INTO cfb_context_definitions
          (definition_id,definition_version,payload_schema_id,calculation_artifact_id,subject_type,unit,measurement_kind,
           required_source_fields,default_freshness_seconds,default_source_age_seconds,parameters,owner_role)
          VALUES %s ON CONFLICT DO NOTHING""", definitions)

        consumer_specs = [
            ("cfb-terminal", "descriptive", "event", "resolve", "cfb_market_movement_context", 1),
            ("cfb-shadow-study", "shadow-predictive", "team", "resolve", "cfb_offensive_drive_volume", 1),
            ("cfb-postgame-export", "descriptive", "event", "resolve", "cfb_market_movement_context", 1),
            ("pickem", "decision-denied", "event", "deny", None, None),
            ("survivor", "decision-denied", "event", "deny", None, None),
        ]
        policy_rows, bindings, current_bindings, steps, slots, versions, pointers = [], [], [], [], [], [], []
        for consumer, usage, subject_type, action, definition_id, definition_version in consumer_specs:
            policy_id = _id("consumer-policy", f"{consumer}:1")
            policy_rows.append((policy_id, consumer, 1, Json({"subject_type": subject_type}), usage,
                                Json(["prospective", "legacy"] if usage != "decision-denied" else []),
                                1800, 31536000, "cfb-policy-owner"))
            bindings.append((consumer, 1, policy_id, datetime.now().astimezone()))
            current_bindings.append((consumer, 1, datetime.now().astimezone()))
            steps.append((policy_id, 0, action, None, payload_schema, Json({"scenario_key": "main"})))
            if action == "resolve":
                slots.append((policy_id, 0, "primary", True, 1800, 31536000))
                versions.append((policy_id, 0, "primary", 0, definition_id, definition_version, payload_schema))
            pointers.append((consumer, 1, "default", None, 1, "unavailable", datetime.now().astimezone()))
        execute_values(cursor, """INSERT INTO cfb_context_consumer_policies
          (policy_id,consumer_id,policy_version,subject_scope,usage,allowed_origins,max_context_age_seconds,max_source_age_seconds,owner_role)
          VALUES %s ON CONFLICT(consumer_id,policy_version) DO NOTHING""", policy_rows)
        execute_values(cursor, """INSERT INTO cfb_context_binding_revisions
          (consumer_id,policy_generation,policy_id,effective_at) VALUES %s ON CONFLICT DO NOTHING""", bindings)
        execute_values(cursor, """INSERT INTO cfb_context_policy_bindings(consumer_id,generation,updated_at)
          VALUES %s ON CONFLICT(consumer_id) DO NOTHING""", current_bindings)
        execute_values(cursor, """INSERT INTO cfb_policy_resolution_steps
          (policy_id,step_index,action,baseline_manifest_id,compatibility_schema_id,compatibility_parameters)
          VALUES %s ON CONFLICT DO NOTHING""", steps)
        if slots:
            execute_values(cursor, """INSERT INTO cfb_policy_dependency_slots
              (policy_id,step_index,slot,required,max_context_age_seconds,max_source_age_seconds)
              VALUES %s ON CONFLICT DO NOTHING""", slots)
            execute_values(cursor, """INSERT INTO cfb_policy_slot_versions
              (policy_id,step_index,slot,preference,definition_id,definition_version,payload_schema_id)
              VALUES %s ON CONFLICT DO NOTHING""", versions)
        execute_values(cursor, """INSERT INTO cfb_context_release_pointers
          (consumer_id,policy_generation,scope_key,manifest_id,generation,availability,updated_at)
          VALUES %s ON CONFLICT DO NOTHING""", pointers)

        schedule_schema = _id("artifact", "legacy-cfb-schedule-projection-v1")
        source_rows, revision_rows = [], []
        for game in games:
            payload = {key: game.get(key) for key in ("id", "cfbd_game_id", "commence_time", "fetched_at", "odds_event_id")}
            revision = _digest(payload)
            source_id = _id("schedule-source", f"{game['id']}:{revision}")
            source_rows.append((source_id, "legacy-cfbd", str(game["cfbd_game_id"]), revision, schedule_schema,
                                "legacy_unverified", game.get("commence_time"), None, game.get("fetched_at") or datetime.now().astimezone(),
                                "legacy", schedule_schema))
            revision_rows.append((_id("schedule-revision", f"{game['id']}:{revision}"), f"cfb:event:{game['id']}", source_id,
                                  game.get("commence_time"), game.get("fetched_at") or datetime.now().astimezone(), revision))
        execute_values(cursor, """INSERT INTO cfb_engine_sources
          (source_id,provider,provider_record_key,revision_key,artifact_id,representation,event_at,published_at,observed_at,origin,projection_schema_id)
          VALUES %s ON CONFLICT(provider,provider_record_key,revision_key) DO NOTHING""", source_rows, page_size=1000)
        execute_values(cursor, """INSERT INTO cfb_engine_schedule_revisions
          (schedule_revision_id,event_key,source_id,scheduled_kickoff,observed_at,revision_digest) VALUES %s
          ON CONFLICT(event_key,revision_digest) DO NOTHING""", revision_rows, page_size=1000)
        revisions_by_game = {game["id"]: revision_rows[index][0] for index, game in enumerate(games)}
        games_by_id = {game["id"]: game for game in games}

        quote_schema = _id("artifact", "legacy-cfb-quote-projection-v1")
        capture_sources, capture_artifacts, captures, quotes = [], [], [], []
        for history in histories:
            # Only captures actually stored during the frozen pilot may enter
            # the prospective cohort. Earlier rows retain their legacy origin.
            origin = capture_origin(history["captured_at"], new_origin)
            normalized = {"history_id": history["id"], "matchup_id": history["matchup_id"],
                          "captured_at": history["captured_at"], "books": history["books"] or {}}
            digest = _digest(normalized)
            artifact_id = _id("capture-artifact", f"{history['id']}:{digest}")
            source_id = _id("capture-source", f"{history['id']}:{digest}")
            capture_id = _id("capture", f"{history['id']}:{NORMALIZATION_VERSION}")
            capture_artifacts.append((artifact_id, "legacy-normalized-cfb-capture", digest, None, "normalized",
                                      len(_json(normalized).encode()), Json({"table": "game_odds_history", "id": history["id"], "classification": "legacy_unverified"})))
            capture_sources.append((source_id, "the-odds-api", str(history["id"]), digest, artifact_id,
                                    "legacy_unverified", None, None, history["captured_at"], origin, quote_schema))
            kickoff = games_by_id[history["matchup_id"]].get("commence_time")
            pregame_state = "unknown" if kickoff is None else "pregame" if history["captured_at"] < kickoff else "in_play"
            captures.append((capture_id, f"cfb:event:{history['matchup_id']}", history["id"], source_id,
                             revisions_by_game[history["matchup_id"]], "the-odds-api", history["capture_key"] or str(history["id"]),
                             history["captured_at"], origin, pregame_state, NORMALIZATION_VERSION))
            quotes.extend(_quotes(capture_id, source_id, history["captured_at"], history["books"] or {}))
        execute_values(cursor, """INSERT INTO cfb_engine_artifacts
          (artifact_id,kind,digest,uri,representation,byte_count,metadata) VALUES %s ON CONFLICT DO NOTHING""", capture_artifacts, page_size=500)
        execute_values(cursor, """INSERT INTO cfb_engine_sources
          (source_id,provider,provider_record_key,revision_key,artifact_id,representation,event_at,published_at,observed_at,origin,projection_schema_id)
          VALUES %s ON CONFLICT(provider,provider_record_key,revision_key) DO NOTHING""", capture_sources, page_size=500)
        execute_values(cursor, """INSERT INTO cfb_engine_captures
          (capture_id,event_key,history_id,source_id,schedule_revision_id,provider,request_key,observed_at,origin,pregame_state,normalization_version)
          VALUES %s ON CONFLICT(provider,request_key,event_key,normalization_version) DO NOTHING""", captures, page_size=500)
        cursor.execute("""UPDATE cfb_engine_captures c SET pregame_state=CASE
            WHEN r.scheduled_kickoff IS NULL THEN 'unknown'
            WHEN c.observed_at<r.scheduled_kickoff THEN 'pregame' ELSE 'in_play' END
          FROM cfb_engine_schedule_revisions r WHERE r.schedule_revision_id=c.schedule_revision_id
            AND c.normalization_version=%s""", (NORMALIZATION_VERSION,))
        execute_values(cursor, """INSERT INTO cfb_engine_quote_observations
          (quote_id,capture_id,source_id,source_locator,book,market,selection,line,decimal_price,bookmaker_updated_at,
           system_observed_at,settlement_rule_id,line_role,quote_digest) VALUES %s ON CONFLICT DO NOTHING""", quotes, page_size=1000)
        connection.commit()
        return {"mode": "applied", "teams": len(teams), "events": len(games), "captures": len(captures), "quotes": len(quotes),
                "origin_mode": new_origin, "prospective_captures": sum(row[8] == "prospective" for row in captures)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--origin-mode", choices=("legacy", "auto"), default="legacy")
    args = parser.parse_args()
    print(json.dumps(bootstrap(load_config().database_url or "", apply=args.apply, new_origin=args.origin_mode), indent=2))


if __name__ == "__main__":
    main()
