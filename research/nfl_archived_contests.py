"""Strict, post-lock-only contest evidence import and saved-portfolio grader.

Outputs are evaluation artifacts, never forecast features or ownership forecasts.
Already imported database records are reconciled, not duplicated or overwritten.
Entry names and entry IDs are deliberately not retained.
"""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from config import load_config
from db.database import DatabaseManager


def normalized(value):
    return re.sub(r"[^a-z0-9]", "", value.lower())


def read_archive(path):
    curve, actuals, ownership = Counter(), {}, {}
    entries = 0
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not {"Points", "Lineup", "Player", "Roster Position", "%Drafted", "FPTS"}.issubset(reader.fieldnames or []):
            raise ValueError("Unsupported standings headers")
        for row in reader:
            if row["Points"].strip():
                score = float(row["Points"])
                curve[round(score, 2)] += 1
                entries += 1
            if row["Player"].strip():
                key = (normalized(row["Player"]), row["Roster Position"].upper())
                points = float(row["FPTS"])
                if key in actuals and abs(actuals[key] - points) > .001:
                    raise ValueError("Conflicting actual points for the same name/slot")
                actuals[key] = points
                ownership[key] = float(row["%Drafted"].rstrip("%"))
    return {"entries": entries, "curve": curve, "actuals": actuals, "ownership": ownership}


def grade_slots(slots, actuals, format):
    """Showdown supplied CPT actuals already include 1.5; never multiply twice."""
    points, missing = [], []
    for slot in slots:
        name = normalized(slot["name"])
        if format == "showdown":
            role = "CPT" if slot["slot"] == "CPT" else "FLEX"
            found = [actuals[(name, role)]] if (name, role) in actuals else []
        else:
            role = re.sub(r"\d+$", "", slot["slot"])
            found = [actuals[(name, role)]] if (name, role) in actuals else []
            if not found:
                # Classic FLEX has the same scoring as its underlying position.
                # A missing slot is usable only if all observed rows agree.
                found = list({value for (player, _), value in actuals.items() if player == name})
        if len(found) != 1:
            missing.append({"player": slot["name"], "slot": slot["slot"], "reason": "actual_missing_or_ambiguous"})
        else:
            points.append(found[0])
    return {"status": "withheld" if missing else "graded", "points": None if missing else round(sum(points), 4), "missing": missing}


def rank_against_field(points, curve):
    if points is None:
        return None
    total = sum(curve.values())
    greater = sum(count for score, count in curve.items() if score > points + .005)
    tied = sum(count for score, count in curve.items() if abs(score - points) <= .005)
    return {"strictly_better_entries": greater, "tied_entries": tied,
            "hypothetical_rank": greater + 1, "field_entries": total,
            "percentile_midrank": 100 * (total - greater - tied / 2) / total,
            "definition": "Comparison to archived field; submitted entry identity is unknown."}


def lock_at(salary):
    starts = []
    for row in salary:
        match = re.search(r"(\d{2}/\d{2}/\d{4}) (\d{2}:\d{2}[AP]M) ET", row["game_info"] or "")
        if not match:
            raise ValueError("Cannot establish archived pre-lock cutoff")
        starts.append(datetime.strptime(" ".join(match.groups()), "%m/%d/%Y %I:%M%p").replace(tzinfo=ZoneInfo("America/New_York")))
    return min(starts).astimezone(timezone.utc)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    output = {"version": "nfl-archived-contest-evaluation-v1", "captured_at": datetime.now(timezone.utc).isoformat(),
              "authority": "postlock_evaluation_only", "forecast_inputs_allowed": False, "contests": [], "saved_slates": []}
    with db.reuse_connection():
        for contest_id in ("195648006", "195786073"):
            path = Path(f"contest-standings-{contest_id}/contest-standings-{contest_id}.csv")
            archive = read_archive(path)
            records = db.execute("SELECT * FROM nfl_dfs_field_contests WHERE contest_id=%s", (contest_id,))
            if len(records) != 1:
                raise ValueError("Expected one previously imported contest")
            record = records[0]
            digest = sha256(path.read_bytes()).hexdigest()
            if digest != record["file_digest"] or archive["entries"] != record["entry_count"]:
                raise ValueError("Archive differs from imported field record")
            salary = db.execute("SELECT * FROM nfl_dfs_slate_players WHERE upload_id=%s", (record["slate_upload_id"],))
            lock = lock_at(salary)
            runs = db.execute("SELECT * FROM nfl_dfs_optimizer_runs WHERE upload_id=%s ORDER BY created_at", (record["slate_upload_id"],))
            grades = []
            for run in runs:
                saved = db.execute("SELECT lineup_number,slots,projected_fpts FROM nfl_dfs_lineups WHERE run_id=%s ORDER BY lineup_number", (run["run_id"],))
                if run["created_at"] >= lock:
                    grades.append({"run_id": run["run_id"], "status": "excluded_postlock_creation", "created_at": run["created_at"]})
                    continue
                lineups = []
                for lineup in saved:
                    grade = grade_slots(lineup["slots"], archive["actuals"], record["format"])
                    lineups.append({"lineup_number": lineup["lineup_number"], "projected_points": lineup["projected_fpts"],
                                    **grade, "field_comparison": rank_against_field(grade["points"], archive["curve"])})
                scored = [row["points"] for row in lineups if row["points"] is not None]
                grades.append({"run_id": run["run_id"], "created_at": run["created_at"], "status": "graded" if len(scored) == len(saved) else "partial_actuals",
                               "requested": run["requested_lineups"], "generated": len(saved), "graded": len(scored),
                               "best_points": max(scored) if scored else None, "mean_points": sum(scored) / len(scored) if scored else None,
                               "lineups": lineups, "roi": None, "prize": None})
            output["contests"].append({"contest_id": contest_id, "format": record["format"], "upload_id": record["slate_upload_id"],
                "source_file": str(path), "source_sha256": digest, "already_imported": True, "imported_at": record["imported_at"],
                "entries": archive["entries"], "observed_ownership_rows": len(archive["ownership"]),
                "actual_player_slot_rows": len(archive["actuals"]), "lock_at": lock,
                "score_curve": [{"score": score, "count": count} for score, count in sorted(archive["curve"].items(), reverse=True)],
                "slot_actuals": [{"normalized_name": key[0], "slot": key[1], "points": value, "observed_ownership_pct": archive["ownership"][key]} for key, value in sorted(archive["actuals"].items())],
                "saved_portfolio_grades": grades, "missing": ["entry fee", "payout table", "our submitted entry identities", "prospectively qualified field model"],
                "future_ownership_authority": False})
        # Freeze three genuinely distinct saved pregame input pools for regression.
        for upload_id in ("f067c8b9-f920-4b26-b77c-36f43a2c9df0", "746487fd-2ebe-444a-b74c-214e6d8854e3", "81996971-0d2c-4d65-b5fc-a2109d18e755"):
            upload = db.execute("SELECT * FROM nfl_dfs_slate_uploads WHERE upload_id=%s", (upload_id,))[0]
            run = db.execute("SELECT * FROM nfl_dfs_optimizer_runs WHERE upload_id=%s ORDER BY created_at LIMIT 1", (upload_id,))[0]
            salary = db.execute("SELECT * FROM nfl_dfs_slate_players WHERE upload_id=%s ORDER BY dk_player_id", (upload_id,))
            if run["created_at"] >= lock_at(salary):
                raise ValueError("Regression input was not frozen before lock")
            output["saved_slates"].append({"upload": upload, "run": run, "salary": salary})
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.with_name("three-saved-slate-inputs.json").write_text(json.dumps(output.pop("saved_slates"), default=str, separators=(",", ":")), encoding="utf-8")
    target.write_text(json.dumps(output, default=str, indent=2), encoding="utf-8")
    print(json.dumps({"contests": [{"id": row["contest_id"], "entries": row["entries"], "portfolios": [{key: grade.get(key) for key in ("run_id", "status", "generated", "graded", "best_points")} for grade in row["saved_portfolio_grades"]]} for row in output["contests"]]}, default=str))


if __name__ == "__main__":
    main()
