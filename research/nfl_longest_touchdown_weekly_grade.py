"""Grade the saved Longest Touchdown page forecasts after each game finishes.

python -m research.nfl_longest_touchdown_weekly_grade [--capture CAPTURE.json.gz]

Reads the published index (never re-forecasts) and grades every game that has
kicked off with ``research.nfl_longest_touchdown.grade``. Results go to a
tracked JSON store the page reads:

- ``records`` is append-only. A ``graded`` record is final; the same game is
  never graded twice. A ``needs_review`` record stays visible and unscored; a
  later run appends a new record only if the PBP changed its outcome.
  ``outcome_unknown`` (game not final, PBP missing) writes nothing and the
  game is retried next run.
- ``weeks`` is derived from the latest record per game and rebuilt every run.

Tracking only: one week of results is not calibration and no gate reads this.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import tempfile

from model.nfl_longest_touchdown import timestamp
from research.nfl_longest_touchdown import capture, grade, read

INDEX = Path("web/src/data/longest-touchdown-weeks.json")
STORE = Path("web/src/data/longest-touchdown-grades.json")
VERSION = "nfl-longest-td-weekly-grade-v1"
FINAL = "graded"


def as_forecast(pub: dict) -> dict:
    """The published game in the shape the research grader expects."""
    rows = [{"identity": p["identity"], "name": p["name"], "longest_td_win_share": p["longestShare"],
             "any_td_probability": p["anyTd"], "td_40_plus_probability": p["longTd"]}
            for p in pub["players"] + pub["residual"]]
    rows.sort(key=lambda r: -r["longest_td_win_share"])
    return {"game": pub["game"], "decision_at": pub["decisionAt"], "players": rows,
            "no_scrimmage_td_probability": pub["noTd"], "outcome_scope": "regulation_scrimmage",
            "request_sha256": pub["provenance"]["request_sha256"],
            "implementation_sha256": pub["provenance"]["implementation_sha256"]}


def proportional_baseline(forecast: dict) -> dict:
    """Split the touchdown mass by each row's any-TD chance; keep the no-TD share.

    The naive alternative the model must beat: whoever is likeliest to score
    at all is likeliest to score the longest. Uniform when no row can score.
    """
    rows = forecast["players"]
    total = sum(r["any_td_probability"] for r in rows)
    mass = 1 - forecast["no_scrimmage_td_probability"]
    players = [{**r, "longest_td_win_share": mass * (r["any_td_probability"] / total if total > 0 else 1 / len(rows))}
               for r in rows]
    players.sort(key=lambda r: -r["longest_td_win_share"])
    return {**forecast, "players": players}


def grade_game(snapshot: dict, pub: dict, graded_at: datetime) -> dict:
    forecast = as_forecast(pub)
    game = pub["game"]
    result = grade(snapshot, forecast)
    record = {"version": VERSION, "game_id": game["game_id"], "season": game["season"], "week": game["week"],
              "away": game["away"], "home": game["home"], "kickoff": game["kickoff"],
              "decision_at": pub["decisionAt"], "model_version": pub["modelVersion"],
              "request_sha256": forecast["request_sha256"], "graded_at": graded_at.isoformat(),
              "capture_at": snapshot.get("captured_at"), "status": result["status"], "reason": result.get("reason")}
    if result["status"] == "needs_review":
        record["quarantined_td_plays"] = result["quarantined_td_plays"]
    if result["status"] != FINAL:
        return record
    model = result["model"]
    probs = {r["identity"]: r["longest_td_win_share"] for r in forecast["players"]}
    probs["NO_TD"] = forecast["no_scrimmage_td_probability"]
    names = {r["identity"]: r["name"] for r in forecast["players"]}
    ranked = sorted(probs, key=lambda k: -probs[k])
    winners = model["winners"] or ["NO_TD"]
    observed = {r["identity"]: r["observed_share"] for r in model["calibration_rows"]}
    baseline = {r["identity"]: r["longest_td_win_share"] for r in proportional_baseline(forecast)["players"]}
    baseline["NO_TD"] = forecast["no_scrimmage_td_probability"]
    baseline_log_loss = -sum(share * math.log(max(baseline[i], 1e-12)) for i, share in observed.items())
    named = [r for r in model["td_event_rows"] if not r["identity"].startswith("OTHER:")]
    record.update(
        longest_yards=model["longest_td_yards"],
        winners=[names.get(w, "No touchdown" if w == "NO_TD" else w) for w in winners],
        winner_probability=sum(probs[w] for w in winners),
        winner_rank=min(ranked.index(w) + 1 for w in winners), outcomes=len(probs),
        unmodeled_winner=model["winner_was_unmodeled"],
        top_pick=names.get(ranked[0], ranked[0]), top_probability=probs[ranked[0]],
        top_hit=ranked[0] in winners,
        log_loss=model["log_loss"], baseline_log_loss=baseline_log_loss, uniform_log_loss=math.log(len(probs)),
        brier=model["brier_score"],
        any_td_expected=sum(r["any_td_probability"] for r in named),
        any_td_scored=sum(1 for r in named if r["scored"]),
        long_td_expected=sum(r["td_40_plus_probability"] for r in named),
        long_td_scored=sum(1 for r in named if r["scored_40_plus"]),
        named_players=len(named))
    return record


def _latest(records: list[dict]) -> dict[str, dict]:
    latest = {}
    for record in records:
        latest[record["game_id"]] = record
    return latest


def started_games(index: dict, now: datetime) -> list[dict]:
    seasons = [*index.get("priorSeasons", []), index]
    return [pub for season in seasons for week in season["weeks"] for pub in week["games"]
            if timestamp(pub["game"]["kickoff"]) <= now]


def update(store: dict | None, index: dict, snapshot: dict, now: datetime) -> tuple[dict, dict]:
    """Return the next store and a report. Earlier records are never changed."""
    records = list((store or {}).get("records", []))
    latest = _latest(records)
    report = {"graded": [], "needs_review": [], "awaiting": [], "unchanged": []}
    for pub in started_games(index, now):
        game_id = pub["game"]["game_id"]
        prior = latest.get(game_id)
        if prior and prior["request_sha256"] != pub["provenance"]["request_sha256"]:
            raise ValueError(f"Saved forecast for started game {game_id} changed after grading began")
        if prior and prior["status"] == FINAL:
            report["unchanged"].append(game_id)
            continue
        record = grade_game(snapshot, pub, now)
        if record["status"] == "outcome_unknown":
            report["awaiting"].append(game_id)
            continue
        if (prior and prior["status"] == record["status"]
                and prior.get("quarantined_td_plays") == record.get("quarantined_td_plays")):
            report["unchanged"].append(game_id)
            continue
        records.append(record)
        latest[game_id] = record
        report["graded" if record["status"] == FINAL else "needs_review"].append(game_id)
    return {"schemaVersion": 1, "version": VERSION, "updatedAt": now.isoformat(),
            "authority": "tracking_only_not_calibration",
            "weeks": summarize(records), "records": records}, report


def summarize(records: list[dict]) -> list[dict]:
    by_week = defaultdict(list)
    for record in _latest(records).values():
        by_week[(record["season"], record["week"])].append(record)
    weeks = []
    for (season, week), rows in sorted(by_week.items()):
        graded = sorted((r for r in rows if r["status"] == FINAL), key=lambda r: r["kickoff"])
        review = sorted((r for r in rows if r["status"] == "needs_review"), key=lambda r: r["kickoff"])
        n = len(graded)
        mean = (lambda key: sum(r[key] for r in graded) / n) if n else (lambda key: None)
        weeks.append({"season": season, "week": week, "graded": n,
            "needsReview": [{"gameId": r["game_id"], "away": r["away"], "home": r["home"], "reason": r["reason"],
                             "plays": [q["description"] for q in r.get("quarantined_td_plays", [])]} for r in review],
            "topHits": sum(1 for r in graded if r["top_hit"]),
            "topExpected": sum(r["top_probability"] for r in graded),
            "logLoss": mean("log_loss"), "baselineLogLoss": mean("baseline_log_loss"),
            "uniformLogLoss": mean("uniform_log_loss"),
            "anyTdExpected": sum(r["any_td_expected"] for r in graded),
            "anyTdScored": sum(r["any_td_scored"] for r in graded),
            "longTdExpected": sum(r["long_td_expected"] for r in graded),
            "longTdScored": sum(r["long_td_scored"] for r in graded),
            "games": [{"gameId": r["game_id"], "away": r["away"], "home": r["home"],
                       "longestYards": r["longest_yards"], "winners": r["winners"],
                       "winnerProbability": r["winner_probability"], "winnerRank": r["winner_rank"],
                       "outcomes": r["outcomes"], "topPick": r["top_pick"], "topProbability": r["top_probability"],
                       "topHit": r["top_hit"]} for r in graded]})
    return weeks


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, prefix=".grades-",
                                     suffix=".json", delete=False) as file:
        json.dump(payload, file, indent=1, default=str, allow_nan=False)
        file.write("\n")
        temporary = Path(file.name)
    os.replace(temporary, path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", type=Path, default=INDEX)
    parser.add_argument("--store", type=Path, default=STORE)
    parser.add_argument("--capture", type=Path, help="Saved capture; otherwise read the index season from the DB")
    args = parser.parse_args()
    index = read(args.index)
    store = read(args.store) if args.store.exists() else None
    if args.capture:
        snapshot = read(args.capture)
    else:
        from config import load_config
        from db.database import DatabaseManager
        seasons = [s["season"] for s in [*index.get("priorSeasons", []), index]]
        db = DatabaseManager(load_config().database_url, initialize_schema=False)
        with db.reuse_connection():
            snapshot = capture(db, min(seasons), max(seasons))
    now = datetime.now(timezone.utc)
    next_store, report = update(store, index, snapshot, now)
    if store is not None and next_store["records"][:len(store["records"])] != store["records"]:
        raise AssertionError("Grade records are append-only")
    write_json(args.store, next_store)
    for key, games in report.items():
        print(f"{key}: {len(games)} {' '.join(games)}")
    for week in next_store["weeks"]:
        if week["graded"]:
            print(f"{week['season']} W{week['week']}: {week['graded']} graded, top pick {week['topHits']}/{week['graded']} "
                  f"(expected {week['topExpected']:.1f}), log loss {week['logLoss']:.2f} vs any-TD {week['baselineLogLoss']:.2f}, "
                  f"any TD {week['anyTdScored']} scored vs {week['anyTdExpected']:.1f} expected, "
                  f"{len(week['needsReview'])} need review")


if __name__ == "__main__":
    main()
