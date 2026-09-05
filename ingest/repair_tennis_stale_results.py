"""One-time, evidence-backed repair of the July-September 2026 Tennis backlog.

The normal provider workers preserve raw observations and never guess when a
scheduled fixture disappears after a draw replacement.  This manifest records
the reviewed result/withdrawal evidence through the same immutable settlement
ledger as automated providers.  It is idempotent and guarded by player names.
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone

from config import load_config
from db.database import DatabaseManager
from ingest.tennis_result_settlement import ResultObservation, record_observation_and_settle


@dataclass(frozen=True)
class Repair:
    match_id: int
    home: str
    away: str
    winner: str | None
    status: str
    source_url: str
    reason: str
    home_sets: int | None = None
    away_sets: int | None = None
    home_games: int | None = None
    away_games: int | None = None


REPAIRS = (
    Repair(1147, "Alex Michelsen", "Jack Draper", None, "cancelled",
           "https://www.lbc.co.uk/article/jack-draper-pulls-out-dc-open-arm-injury-setback-5HjdfDq_2/",
           "Draper withdrew before the match and Michelsen played replacement Mackenzie McDonald."),
    Repair(1453, "Martin Damm Jr.", "Henrique Rocha", "home", "completed",
           "https://nationalbankopen.com/matches-players/results/2026/damm_vs_rocha",
           "Official National Bank Open result: Damm defeated Rocha 6-3, 7-6(3).",
           2, 0, 13, 9),
    Repair(2090, "Titouan Droguet", "Felix Auger-Aliassime", None, "walkover",
           "https://as.com/tenis/masters_1000/montreal-queda-huerfano-de-minaur-y-shelton-salvan-al-top-10-f202608-n/",
           "Auger-Aliassime withdrew before taking the court; sportsbook selections are void."),
    Repair(2729, "Mackenzie McDonald", "Wu Yibing", "away", "completed",
           "https://www.tennis.com/tournaments/cincinnati-open/matches/m-mcdonald-vs-y-wu-2026-08-11",
           "Cincinnati result: Wu defeated McDonald 7-6(9), 1-6, 7-5.", 1, 2, 17, 15),
    Repair(2736, "JJ Wolf", "Toby Samuel", None, "cancelled",
           "https://www.atptour.com/en/scores/current/cincinnati/422/daily-schedule?day=1",
           "The final Cincinnati schedule replaced Samuel with Shintaro Mochizuki."),
    Repair(2795, "Jaime Faria", "Wu Yibing", "home", "completed",
           "https://www.atpschedule.com/matches/ZXNwbi1hdHAtMTg0NDMx/wu-yibing-vs-jaime-faria",
           "Cincinnati result: Faria defeated Wu 6-3, 7-5.", 2, 0, 13, 8),
    Repair(2851, "Botic van de Zandschulp", "Tallon Griekspoor", None, "cancelled",
           "https://www.365scores.com/tennis/match/hertogenbosch-213/botic-van-de-zandschulp-tallon-griekspoor-47312-47315-213",
           "The listed Cincinnati fixture was cancelled and produced no match result."),
    Repair(2854, "Corentin Moutet", "Hubert Hurkacz", None, "cancelled",
           "https://www.tennisstats247.com/matches/Cincinnati/C-Moutet-H-Hurkacz-2624201/",
           "The Cincinnati fixture is recorded as cancelled."),
    Repair(2888, "Maria Timofeeva", "Katerina Siniakova", None, "cancelled",
           "https://www.wtatennis.com/news/4558323/naomi-osaka-pulls-out-of-cincinnati-open-citing-fatigue",
           "Official draw change moved Siniakova and assigned Timofeeva a qualifier/lucky loser."),
    Repair(3976, "Anna Bond\u00e1r", "Liang En-shuo", "home", "completed",
           "https://www.wtatennis.com/tournaments/1039/monterrey/2026/scores/RS005",
           "Official WTA result: Bondar defeated Liang 6-3, 4-6, 6-1.", 2, 1, 16, 10),
    Repair(3981, "Maria Camila Osorio Serrano", "Ann Li", "away", "completed",
           "https://www.tennisstats247.com/matches/Monterrey/C-Osorio-A-Li-2626561/",
           "Monterrey result: Ann Li defeated Camila Osorio in straight sets.", 0, 2),
    Repair(3999, "Wu Yibing", "Adam Walton", "home", "completed",
           "https://www.tennis.com/tournaments/us-open/matches/y-wu-vs-a-walton-2026-08-30",
           "US Open result: Wu defeated Walton 7-6(2), 6-2, 7-5.", 3, 0, 20, 13),
    Repair(4022, "Marin Cilic", "Andrey Rublev", None, "cancelled",
           "https://www.atptour.com/en/news/ruud-us-open-2026-withdrawal",
           "ATP reported Cilic withdrew; Otto Virtanen replaced him against Rublev."),
    Repair(4090, "Aryna Sabalenka", "Maria Camila Osorio Serrano", "home", "completed",
           "https://www.wtatennis.com/news/4569739/sabalenka-starts-us-open-title-defense-with-straight-sets-win-over-osorio",
           "Official WTA result: Sabalenka defeated Osorio 6-2, 6-4.", 2, 0, 12, 6),
    Repair(4042, "Martin Damm Jr.", "Frances Tiafoe", "away", "completed",
           "https://www.atptour.com/en/news/tiafoe-damm-us-open-2026-r1",
           "Official ATP result: Tiafoe defeated Damm in five sets.", 2, 3, 23, 25),
    Repair(4049, "Juan Manuel Cerundolo", "Casper Ruud", None, "cancelled",
           "https://www.atptour.com/en/news/ruud-us-open-2026-withdrawal",
           "ATP reported Ruud withdrew; Arthur Gea replaced him against Cerundolo."),
    Repair(4059, "Rafael Jodar", "Thanasi Kokkinakis", None, "cancelled",
           "https://www.usopen.org/amp/en_US/news/articles/2026-09-03/who_is_bu_yunchaokete_the_lucky_loser_who_upset_rafael_jodar_at_the_2026_us_open.html",
           "Official US Open report: Kokkinakis withdrew and Bu replaced him."),
    Repair(4108, "Anastasia Potapova", "Tereza Valentova", None, "cancelled",
           "https://www.wtatennis.com/tournaments/905/us-open/2026/scores/LS74249810",
           "Valentova withdrew; Potapova played replacement Darja Semenistaja."),
)


def apply_repairs(db: DatabaseManager, *, dry_run: bool) -> list[dict]:
    output: list[dict] = []
    for repair in REPAIRS:
        match = db.execute_one(
            "SELECT id, match_date, home_player, away_player, winner, completion_status "
            "FROM tennis_matches WHERE id=%s", (repair.match_id,),
        )
        if not match:
            raise RuntimeError(f"Missing Tennis match {repair.match_id}")
        actual = (match["home_player"], match["away_player"])
        if actual != (repair.home, repair.away):
            raise RuntimeError(f"Identity guard failed for {repair.match_id}: {actual!r}")
        if match["winner"] is not None or match["completion_status"] not in {"scheduled", "unknown"}:
            output.append({"match_id": repair.match_id, "state": "already_resolved"})
            continue
        if dry_run:
            output.append({"match_id": repair.match_id, "state": "would_apply", "status": repair.status})
            continue
        result = record_observation_and_settle(db, ResultObservation(
            match_id=repair.match_id,
            provider="manual_research",
            winner_side=repair.winner,
            completion_status=repair.status,
            status_evidence=True,
            observed_match_date=match["match_date"],
            home_sets=repair.home_sets,
            away_sets=repair.away_sets,
            home_games=repair.home_games,
            away_games=repair.away_games,
            source_url=repair.source_url,
            source_available_at=datetime.now(timezone.utc),
            parser_version="stale-tennis-repair-2026-09-05-v1",
            raw_payload={"reviewed_on": date.today().isoformat(), "home": repair.home, "away": repair.away},
            match_method="manual_exact_id_name_guard",
            match_confidence=1.0,
            actor="codex_evidence_repair",
            reason=repair.reason,
            evidence_url=repair.source_url,
        ))
        output.append({"match_id": repair.match_id, **result})
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Repair reviewed stale Tennis results")
    parser.add_argument("--apply", action="store_true", help="Write immutable observations and settle ledgers")
    parser.add_argument("--existing-schema", action="store_true")
    args = parser.parse_args()
    config = load_config()
    manager = DatabaseManager(config.database_url, initialize_schema=not args.existing_schema)
    print(json.dumps(apply_repairs(manager, dry_run=not args.apply), indent=2, default=str))
