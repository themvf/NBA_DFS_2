"""Build immutable 2023+ overall and surface Elo events (SCRUM-27).

The v1 contract is frozen in CLAUDE.md.  Date-only source rows are evaluated in
same-day batches so arbitrary file order cannot make one same-day result a
feature for another.  The script writes full before/expected/delta/batch-after
events and pre-match feature snapshots; it never updates an Elo event in place.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from itertools import groupby
from typing import Any

from psycopg2.extras import execute_values

from config import load_config
from db.database import DatabaseManager
from ingest.tennis_foundation import checksum, stable_json

ALGORITHM_VERSION = "tennis-surface-elo-v1"
FEATURE_VERSION_BASE = "tennis-surface-elo-features-v2"
PRIOR = 1500.0
K_FACTOR = 32.0
SHRINKAGE_MATCHES = 20.0
START_DATE = date(2023, 1, 1)


def expected(rating_a: float, rating_b: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((rating_b - rating_a) / 400.0))


def surface_bucket(surface: str) -> str:
    if surface == "indoor_hard":
        return "hard"
    if surface not in {"hard", "clay", "grass"}:
        raise ValueError(f"Unsupported Elo surface: {surface!r}")
    return surface


def reliability(n: int) -> float:
    return n / (n + SHRINKAGE_MATCHES)


def reliability_label(n: int) -> str:
    if n < 5:
        return "insufficient"
    if n < 20:
        return "developing"
    return "established"


def blended_surface(overall: float, raw_surface: float, n: int) -> float:
    weight = reliability(n)
    return overall + weight * (raw_surface - overall)


@dataclass
class PlayerState:
    overall: float = PRIOR
    overall_matches: int = 0
    surfaces: dict[str, float] = field(default_factory=lambda: {
        "hard": PRIOR, "clay": PRIOR, "grass": PRIOR,
    })
    surface_matches: dict[str, int] = field(default_factory=lambda: {
        "hard": 0, "clay": 0, "grass": 0,
    })
    last_eligible_date: date | None = None
    outcomes: deque[tuple[date, bool]] = field(default_factory=deque)


@dataclass
class PerformanceState:
    observations: deque[tuple[date, float, int, float, int]] = field(default_factory=deque)
    serve_won: float = 0.0
    serve_points: int = 0
    return_won: float = 0.0
    return_points: int = 0

    def add(self, observation: tuple[date, float, int, float, int]) -> None:
        self.observations.append(observation)
        _d, serve_pct, serve_n, return_pct, return_n = observation
        self.serve_won += serve_pct * serve_n
        self.serve_points += serve_n
        self.return_won += return_pct * return_n
        self.return_points += return_n

    def view(self, cutoff_date: date) -> tuple[float | None, float | None, int, int]:
        lower = cutoff_date - timedelta(days=365)
        while self.observations and self.observations[0][0] < lower:
            _d, serve_pct, serve_n, return_pct, return_n = self.observations.popleft()
            self.serve_won -= serve_pct * serve_n
            self.serve_points -= serve_n
            self.return_won -= return_pct * return_n
            self.return_points -= return_n
        serve = self.serve_won / self.serve_points if self.serve_points else None
        ret = self.return_won / self.return_points if self.return_points else None
        return serve, ret, self.serve_points, self.return_points


def _cutoff(match_date: date) -> datetime:
    return datetime.combine(match_date, time.min, tzinfo=timezone.utc)


def _stats_through(last_date: date | None) -> datetime:
    if last_date is None:
        return datetime(2022, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    return datetime.combine(last_date, time.max, tzinfo=timezone.utc)


def _recent_form(state: PlayerState) -> float | None:
    recent = list(state.outcomes)[-10:]
    return sum(1 for _d, won in recent if won) / len(recent) if recent else None


def _recent_load(state: PlayerState, cutoff_date: date) -> int:
    lower = cutoff_date - timedelta(days=14)
    return sum(1 for d, _won in state.outcomes if lower <= d < cutoff_date)


def _eligible(row: dict) -> tuple[bool, str | None]:
    if row["walkover"]:
        return False, "walkover"
    if row["retired"] or row["completion_status"] == "retired":
        return False, "retirement"
    if row["completion_status"] != "completed":
        return False, f"completion_status:{row['completion_status']}"
    return True, None


def _load_matches(db: DatabaseManager) -> list[dict]:
    return db.execute(
        """
        SELECT id, tour, match_date, start_time, tournament, round, surface,
               winner_player_id, loser_player_id, completion_status, retired, walkover,
               winner_rank, loser_rank, winner_rank_points, loser_rank_points,
               winner_decimal_odds, loser_decimal_odds, odds_source,
               source_available_at, stats_through_at, captured_at,
               transformation_version, raw_checksum
        FROM tennis_historical_matches
        WHERE is_current AND source='tennis_data'
          AND match_date >= DATE '2023-01-01'
          AND surface IN ('hard','indoor_hard','clay','grass')
        ORDER BY tour, match_date, id
        """
    )


def _load_atp_performance(db: DatabaseManager) -> list[tuple]:
    rows = db.execute(
        """
        SELECT hm.match_date, ps.player_id,
               ps.serve_points_won_pct, ps.sample_size AS serve_points,
               ps.return_points_won_pct, opp.sample_size AS return_points
        FROM tennis_historical_matches hm
        JOIN tennis_player_match_stats ps ON ps.historical_match_id=hm.id
        JOIN tennis_player_match_stats opp ON opp.historical_match_id=hm.id
             AND opp.player_id=ps.opponent_player_id
        WHERE hm.is_current AND hm.source='tml_database' AND hm.tour='ATP'
          AND hm.match_date >= DATE '2023-01-01'
          AND hm.raw_payload->'enrichment' IS NOT NULL
          AND hm.completion_status='completed' AND NOT hm.retired AND NOT hm.walkover
          AND ps.stats_available AND ps.serve_points_won_pct IS NOT NULL
          AND ps.return_points_won_pct IS NOT NULL
          AND ps.sample_size > 0 AND opp.sample_size > 0
        ORDER BY hm.match_date, hm.id, ps.player_id
        """
    )
    return [
        (row["match_date"], row["player_id"], float(row["serve_points_won_pct"]),
         int(row["serve_points"]), float(row["return_points_won_pct"]),
         int(row["return_points"]))
        for row in rows
    ]


def _source_checksum(rows: list[dict]) -> str:
    return checksum([
        (row["id"], row["raw_checksum"], row["transformation_version"])
        for row in rows
    ])


def _start_run(db: DatabaseManager, rows: list[dict], source_checksum: str) -> int:
    config = {
        "prior": PRIOR,
        "k_factor": K_FACTOR,
        "shrinkage_matches": SHRINKAGE_MATCHES,
        "start_date": START_DATE.isoformat(),
        "retirements": "excluded",
        "walkovers": "excluded",
        "date_only_ordering": "same_day_batch",
        "indoor_hard_bucket": "hard",
    }
    row = db.execute_one(
        """
        INSERT INTO tennis_elo_runs (
            algorithm_version, source_start_date, source_end_date,
            source_match_count, source_checksum, config, status
        ) VALUES (%s,%s,%s,%s,%s,%s::jsonb,'running')
        ON CONFLICT (algorithm_version, source_checksum) DO UPDATE SET
            status=CASE WHEN tennis_elo_runs.status='complete' THEN 'complete' ELSE 'running' END,
            error_message=NULL
        RETURNING id
        """,
        (ALGORITHM_VERSION, rows[0]["match_date"], rows[-1]["match_date"],
         len(rows), source_checksum, stable_json(config)),
    )
    return row["id"]


def _player_view(state: PlayerState, bucket: str, match_date: date) -> dict[str, Any]:
    raw_surface = state.surfaces[bucket]
    surface_n = state.surface_matches[bucket]
    return {
        "overall": state.overall,
        "overall_n": state.overall_matches,
        "surface": raw_surface,
        "surface_n": surface_n,
        "blended": blended_surface(state.overall, raw_surface, surface_n),
        "reliability": reliability(surface_n),
        "label": reliability_label(surface_n),
        "last_date": state.last_eligible_date,
        "inactivity": ((match_date - state.last_eligible_date).days
                       if state.last_eligible_date else None),
        "stats_through": _stats_through(state.last_eligible_date),
        "recent_form": _recent_form(state),
        "recent_load": _recent_load(state, match_date),
    }


def build(db: DatabaseManager) -> dict:
    rows = _load_matches(db)
    if not rows:
        raise RuntimeError("No canonical 2023+ tennis-data rows are available")
    source_checksum = _source_checksum(rows)
    run_id = _start_run(db, rows, source_checksum)
    feature_version = f"{FEATURE_VERSION_BASE}:{source_checksum[:12]}"
    performance_rows = _load_atp_performance(db)
    performance_index = 0
    performance_states: dict[int, PerformanceState] = defaultdict(PerformanceState)

    states: dict[tuple[str, int], PlayerState] = defaultdict(PlayerState)
    event_rows: list[tuple] = []
    feature_rows: list[tuple] = []
    eligible_matches = excluded_matches = 0

    for (tour, match_date), day_iter in groupby(rows, key=lambda r: (r["tour"], r["match_date"])):
        day_rows = list(day_iter)
        while (performance_index < len(performance_rows)
               and performance_rows[performance_index][0] < match_date):
            perf = performance_rows[performance_index]
            performance_states[perf[1]].add((perf[0], perf[2], perf[3], perf[4], perf[5]))
            performance_index += 1
        appearances = Counter(
            player_id
            for row in day_rows
            for player_id in (row["winner_player_id"], row["loser_player_id"])
        )
        overall_deltas: dict[int, float] = defaultdict(float)
        surface_deltas: dict[tuple[int, str], float] = defaultdict(float)
        eligible_counts: Counter[int] = Counter()
        surface_counts: Counter[tuple[int, str]] = Counter()
        pending: list[dict] = []

        for row in day_rows:
            bucket = surface_bucket(row["surface"])
            winner_id, loser_id = row["winner_player_id"], row["loser_player_id"]
            winner_state = states[(tour, winner_id)]
            loser_state = states[(tour, loser_id)]
            w = _player_view(winner_state, bucket, match_date)
            l = _player_view(loser_state, bucket, match_date)
            exp_overall_w = expected(w["overall"], l["overall"])
            exp_surface_w = expected(w["surface"], l["surface"])
            exp_blended_w = expected(w["blended"], l["blended"])
            is_eligible, exclusion = _eligible(row)
            if is_eligible:
                eligible_matches += 1
                w_overall_delta = K_FACTOR * (1.0 - exp_overall_w)
                l_overall_delta = -w_overall_delta
                w_surface_delta = K_FACTOR * (1.0 - exp_surface_w)
                l_surface_delta = -w_surface_delta
                for player_id, od, sd in (
                    (winner_id, w_overall_delta, w_surface_delta),
                    (loser_id, l_overall_delta, l_surface_delta),
                ):
                    overall_deltas[player_id] += od
                    surface_deltas[(player_id, bucket)] += sd
                    eligible_counts[player_id] += 1
                    surface_counts[(player_id, bucket)] += 1
            else:
                excluded_matches += 1
                w_overall_delta = l_overall_delta = 0.0
                w_surface_delta = l_surface_delta = 0.0

            pending.append({
                "row": row, "bucket": bucket, "eligible": is_eligible,
                "exclusion": exclusion, "winner_view": w, "loser_view": l,
                "expected": (exp_overall_w, exp_surface_w, exp_blended_w),
                "deltas": (w_overall_delta, l_overall_delta,
                           w_surface_delta, l_surface_delta),
            })

        for item in pending:
            row, bucket = item["row"], item["bucket"]
            cutoff = _cutoff(match_date)
            expected_w = item["expected"]
            ids = (row["winner_player_id"], row["loser_player_id"])
            views = (item["winner_view"], item["loser_view"])
            overall_ds = item["deltas"][:2]
            surface_ds = item["deltas"][2:]
            ranks = (row["winner_rank"], row["loser_rank"])
            rank_points = (row["winner_rank_points"], row["loser_rank_points"])
            for side in (0, 1):
                player_id, opponent_id = ids[side], ids[1 - side]
                view = views[side]
                expected_values = expected_w if side == 0 else tuple(1.0 - value for value in expected_w)
                overall_after = view["overall"] + overall_deltas[player_id]
                surface_after = view["surface"] + surface_deltas[(player_id, bucket)]
                overall_n_after = view["overall_n"] + eligible_counts[player_id]
                surface_n_after = view["surface_n"] + surface_counts[(player_id, bucket)]
                ordering = "date_only_batch_ambiguous" if appearances[player_id] > 1 else "date_only_batch"
                event_payload = {
                    "run_id": run_id, "match_id": row["id"], "player_id": player_id,
                    "opponent_id": opponent_id, "overall_before": view["overall"],
                    "overall_after": overall_after, "surface_before": view["surface"],
                    "surface_after": surface_after, "algorithm": ALGORITHM_VERSION,
                }
                event_rows.append((
                    run_id, row["id"], player_id, opponent_id, tour, match_date,
                    cutoff, view["stats_through"], row["surface"], bucket, side == 0,
                    item["eligible"], item["exclusion"], view["overall"], overall_ds[side],
                    overall_after, view["surface"], surface_ds[side], surface_after,
                    view["blended"], expected_values[0], expected_values[1], expected_values[2],
                    view["overall_n"], overall_n_after, view["surface_n"], surface_n_after,
                    view["reliability"], view["label"], view["last_date"], view["inactivity"],
                    True, appearances[player_id], ordering, PRIOR, K_FACTOR,
                    SHRINKAGE_MATCHES, ALGORITHM_VERSION, row["raw_checksum"], checksum(event_payload),
                ))
                source_availability = {
                    "history": "tennis_data",
                    "rank": "available" if ranks[side] is not None else "missing",
                    "rank_points": "available" if rank_points[side] is not None else "missing",
                    "start_time": "available" if row["start_time"] else "source_unavailable_date_only",
                    "market_odds": "representative_close" if row["odds_source"] else "missing",
                }
                serve_pct = return_pct = None
                serve_sample = return_sample = 0
                if tour == "ATP":
                    serve_pct, return_pct, serve_sample, return_sample = performance_states[player_id].view(match_date)
                    source_availability["serve_return"] = (
                        "tml_rolling_365d" if serve_pct is not None and return_pct is not None
                        else "tml_no_prior_365d_sample"
                    )
                else:
                    source_availability["serve_return"] = "source_unavailable"
                missingness = {
                    key: value for key, value in source_availability.items()
                    if value in {"missing", "source_unavailable", "source_unavailable_date_only", "tml_no_prior_365d_sample"}
                }
                provenance = {
                    "run_id": run_id, "historical_match_id": row["id"],
                    "source_raw_checksum": row["raw_checksum"],
                    "source_transformation_version": row["transformation_version"],
                    "ordering_status": ordering,
                    "performance_formula": "weighted_trailing_365d_v1" if tour == "ATP" else None,
                    "serve_points_sample": serve_sample,
                    "return_points_sample": return_sample,
                }
                feature_payload = {
                    "player_id": player_id, "match_id": row["id"], "cutoff": cutoff,
                    "overall": view["overall"], "surface": view["surface"],
                    "serve": serve_pct, "return": return_pct,
                    "feature_version": feature_version,
                }
                feature_rows.append((
                    player_id, opponent_id, row["id"], cutoff, view["stats_through"],
                    row["surface"], view["overall"], view["surface"], view["recent_form"],
                    view["inactivity"], view["recent_load"], serve_pct, return_pct, ranks[side],
                    rank_points[side], view["surface_n"], feature_version,
                    stable_json(source_availability), stable_json(missingness),
                    stable_json(provenance), checksum(feature_payload),
                ))

        for player_id, delta in overall_deltas.items():
            state = states[(tour, player_id)]
            state.overall += delta
            state.overall_matches += eligible_counts[player_id]
        for (player_id, bucket), delta in surface_deltas.items():
            state = states[(tour, player_id)]
            state.surfaces[bucket] += delta
            state.surface_matches[bucket] += surface_counts[(player_id, bucket)]
        for item in pending:
            if not item["eligible"]:
                continue
            row = item["row"]
            winner_state = states[(tour, row["winner_player_id"])]
            loser_state = states[(tour, row["loser_player_id"])]
            winner_state.outcomes.append((match_date, True))
            loser_state.outcomes.append((match_date, False))
            winner_state.last_eligible_date = match_date
            loser_state.last_eligible_date = match_date

    try:
        with db.connect() as conn:
            cur = conn.cursor()
            inserted_events = len(execute_values(
                cur,
                """
                INSERT INTO tennis_elo_rating_events (
                    run_id,historical_match_id,player_id,opponent_player_id,tour,match_date,
                    cutoff_at,stats_through_at,surface,surface_bucket,is_winner,eligible,
                    exclusion_reason,overall_before,overall_delta,overall_after,surface_before,
                    surface_delta,surface_after,blended_surface_before,expected_overall,
                    expected_surface,expected_blended,overall_matches_before,overall_matches_after,
                    surface_matches_before,surface_matches_after,surface_reliability,reliability_label,
                    last_eligible_match_date,inactivity_days,same_day_batch,same_day_match_count,
                    ordering_status,prior_rating,k_factor,shrinkage_matches,algorithm_version,
                    source_raw_checksum,event_checksum
                ) VALUES %s ON CONFLICT (run_id,historical_match_id,player_id) DO NOTHING
                RETURNING id
                """,
                event_rows, page_size=1000, fetch=True,
            ))
            inserted_features = len(execute_values(
                cur,
                """
                INSERT INTO tennis_player_feature_snapshots (
                    player_id,opponent_player_id,historical_match_id,cutoff_at,stats_through_at,
                    surface,overall_elo,surface_elo,recent_form,rest_days,recent_match_load,
                    serve_points_won_pct,return_points_won_pct,rank,rank_points,sample_size,
                    feature_version,source_availability,missingness,provenance,raw_checksum
                ) VALUES %s
                ON CONFLICT DO NOTHING RETURNING id
                """,
                feature_rows,
                template="(" + ",".join(["%s"] * 17 + ["%s::jsonb"] * 3 + ["%s"]) + ")",
                page_size=1000, fetch=True,
            ))
            cur.execute(
                """
                UPDATE tennis_elo_runs SET status='complete', eligible_match_count=%s,
                    excluded_match_count=%s, event_count=%s, completed_at=NOW(), error_message=NULL
                WHERE id=%s
                """,
                (eligible_matches, excluded_matches, len(event_rows), run_id),
            )
    except Exception as exc:
        db.execute(
            "UPDATE tennis_elo_runs SET status='fail', error_message=%s, completed_at=NOW() WHERE id=%s",
            (str(exc), run_id),
        )
        raise

    report = {
        "run_id": run_id,
        "algorithm_version": ALGORITHM_VERSION,
        "feature_version": feature_version,
        "source_matches": len(rows),
        "eligible_matches": eligible_matches,
        "excluded_matches": excluded_matches,
        "events": len(event_rows),
        "events_inserted": inserted_events,
        "features": len(feature_rows),
        "features_inserted": inserted_features,
        "source_checksum": source_checksum,
        "atp_performance_observations": len(performance_rows),
    }
    print(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build immutable 2023+ Tennis surface Elo events")
    parser.parse_args()
    build(DatabaseManager(load_config().database_url))
