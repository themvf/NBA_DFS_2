"""Local NFL ownership workflow: snapshot, import, fit, forecast and evaluate.

Run ``python -m research.nfl_ownership --help``. Outputs are content-addressed
JSON and contain no entrant names, entry IDs or database credentials.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import csv
from datetime import datetime, timezone
from hashlib import sha256
import json
import io
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from model.nfl_ownership import (VERSION, code_digest, digest, finite, fit, forecast, metrics, seal,
                                 stamp, validate_contest, validate_snapshot, verify, walk_forward, write_artifact)


def normalized(name):
    return re.sub(r"[^a-z0-9]", "", name.lower())


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def file_digest(path):
    return sha256(Path(path).read_bytes()).hexdigest()


def lock_from_games(players):
    starts = []
    for p in players:
        match = re.search(r"(\d{2}/\d{2}/\d{4}) (\d{2}:\d{2}[AP]M) ET", p.get("gameInfo") or "")
        if not match:
            raise ValueError("Salary game kickoff is missing or invalid")
        starts.append(datetime.strptime(" ".join(match.groups()), "%m/%d/%Y %I:%M%p")
                      .replace(tzinfo=ZoneInfo("America/New_York")))
    return min(starts).astimezone(timezone.utc).isoformat()


def freeze_snapshot(slate_id, fmt, captured_at, players, provenance):
    provenance = {**provenance, "adapter_implementation_digest": code_digest(__file__)}
    canonical = []
    for p in players:
        source_asof = (p.get("baselineSource") or {}).get("asOf")
        if source_asof and stamp(source_asof) > stamp(captured_at):
            raise ValueError("Projection source was not available at capture time")
        canonical.append({"player_id": p["dkPlayerId"], "captain_player_id": p.get("captainDkPlayerId"),
                          "name": p["name"], "team": p["team"], "position": p["position"],
                          "salary": p["salary"], "projection": p.get("ourProj"), "dk_average": p.get("dkAvg"),
                          "is_out": p["isOut"], "role": (p.get("availability") or {}).get("role"),
                          "projection_as_of": source_asof})
    s = {"version": VERSION, "kind": "snapshot", "slate_id": slate_id, "format": fmt,
         "captured_at": captured_at, "lock_at": lock_from_games(players),
         "players": sorted(canonical, key=lambda p: str(p["player_id"])),
         "source_digest": digest(provenance), "provenance": provenance}
    validate_snapshot(s)
    return seal(s)


def read_complete_field(path, snapshot, expected_entries, overrides=None):
    """Recount roster appearances, making zero labels observable rather than assumed.

    Classic %Drafted is slot-specific. Missing FLEX/base rows cannot simply be
    treated as zero without checking the complete field. Entrant IDs are used
    transiently to reject duplicate entries and never written to an artifact.
    """
    overrides = overrides or {}
    fmt = snapshot["format"]
    pattern = re.compile(r"(?:^|\s)(QB|RB|WR|TE|FLEX|DST|CPT)\s+")
    by_name = defaultdict(list)
    for p in snapshot["players"]:
        by_name[normalized(p["name"])].append(p)
    by_id = {p["player_id"]: p for p in snapshot["players"]}
    counts, entry_ids, resolution = defaultdict(int), set(), {}
    empty_entries = 0
    published = []
    with Path(path).open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not {"EntryId", "Lineup", "Player", "Roster Position", "%Drafted"}.issubset(reader.fieldnames or []):
            raise ValueError("Expected complete DraftKings standings export")
        for row in reader:
            if row["Player"].strip():
                published.append((normalized(row["Player"]), row["Roster Position"].strip(), float(row["%Drafted"].rstrip("%"))))
            entry = row["EntryId"].strip()
            if not entry:
                if row["Lineup"].strip():
                    raise ValueError("Lineup without entry identity")
                continue
            if entry in entry_ids:
                raise ValueError("Duplicate contest entry")
            entry_ids.add(entry)
            text = row["Lineup"]
            if not text.strip():
                if float(row.get("Points") or 0) != 0:
                    raise ValueError("Missing roster for a scoring entry")
                empty_entries += 1
                continue
            matches = list(pattern.finditer(text))
            slots = [m.group(1) for m in matches]
            expected_slots = ["QB", "RB", "RB", "WR", "WR", "WR", "TE", "FLEX", "DST"] if fmt == "classic" else ["CPT"] + ["FLEX"] * 5
            if sorted(slots) != sorted(expected_slots):
                raise ValueError("Incomplete or invalid roster in contest export")
            picked = set()
            for i, match in enumerate(matches):
                slot = match.group(1)
                name = normalized(text[match.end():matches[i + 1].start() if i + 1 < len(matches) else len(text)].strip())
                key = (name, slot)
                if key not in resolution:
                    candidates = by_name.get(name, [])
                    if name in overrides:
                        override = overrides[name]
                        if not override.get("reason") or override.get("player_id") not in by_id:
                            raise ValueError("Invalid identity override")
                        candidates = [by_id[override["player_id"]]]
                    if fmt == "classic":
                        candidates = [p for p in candidates if p["position"] == slot or slot == "FLEX" and p["position"] in ("RB", "WR", "TE")]
                    if len(candidates) != 1:
                        raise ValueError(f"Roster identity unresolved: {name}/{slot}")
                    resolution[key] = candidates[0]["player_id"]
                pid = resolution[key]
                if pid in picked:
                    raise ValueError("Duplicate player within an entry")
                picked.add(pid)
                counts[(pid, slot)] += 1
    n = len(entry_ids)
    if not n or n != expected_entries:
        raise ValueError(f"Field is incomplete: {n} entries; expected {expected_entries}")
    valid_entries = n - empty_entries
    if not valid_entries:
        raise ValueError("No complete rosters in the field")
    discrepancies = []
    for name, slot, pct in published:
        candidates = by_name.get(name, [])
        if name in overrides:
            candidates = [by_id[overrides[name]["player_id"]]]
        if fmt == "classic" and slot != "FLEX":
            candidates = [p for p in candidates if p["position"] == slot]
        if len(candidates) != 1:
            raise ValueError("Published ownership identity unresolved")
        observed = 100 * counts[(candidates[0]["player_id"], slot)] / n
        discrepancies.append(abs(observed - pct))
        if abs(observed - pct) > .011:
            raise ValueError(f"Roster recount disagrees with published ownership: {name}/{slot}")
    labels = []
    for p in snapshot["players"]:
        slots = ("CPT", "FLEX") if fmt == "showdown" else ((p["position"], "FLEX") if p["position"] in ("RB", "WR", "TE") else (p["position"],))
        for slot in slots:
            labels.append({"normalized_name": normalized(p["name"]), "slot": slot,
                           "observed_ownership_pct": 100 * counts[(p["player_id"], slot)] / valid_entries})
    return labels, n, {"entries": n, "empty_entries": empty_entries, "complete_lineups": valid_entries,
                       "label_denominator": "complete_lineups", "published_denominator": "all_entries",
                       "published_rows_checked": len(published), "max_published_difference_pp": max(discrepancies, default=0)}


def import_labels(snapshot, labels, contest_id, available_at, source_digest, entries=None, overrides=None):
    validate_snapshot(snapshot)
    by_name = defaultdict(list)
    by_id = {p["player_id"]: p for p in snapshot["players"]}
    for p in snapshot["players"]:
        by_name[normalized(p["name"])].append(p)
    overrides = overrides or {}
    matched, raw_seen, raw_totals, flex = defaultdict(dict), set(), defaultdict(float), defaultdict(float)
    mapping = []
    for row in labels:
        name, slot, pct = row["normalized_name"], row["slot"], row["observed_ownership_pct"]
        if (name, slot) in raw_seen:
            raise ValueError(f"Duplicate player-slot label: {name}/{slot}")
        raw_seen.add((name, slot))
        if not finite(pct) or not 0 <= pct <= 100:
            raise ValueError("Invalid observed ownership")
        candidates = by_name.get(name, [])
        if name in overrides:
            override = overrides[name]
            if not override.get("reason") or override.get("player_id") not in by_id:
                raise ValueError("Identity override needs a valid player ID and reason")
            candidates = [by_id[override["player_id"]]]
        elif snapshot["format"] == "classic" and slot != "FLEX":
            candidates = [p for p in candidates if p["position"] == slot]
        if len(candidates) != 1:
            raise ValueError(f"Missing or ambiguous identity: {name}/{slot}; supply an audited identity override")
        p = candidates[0]
        if snapshot["format"] == "classic":
            if slot not in (p["position"], "FLEX") or slot == "FLEX" and p["position"] not in ("RB", "WR", "TE"):
                raise ValueError("Classic ownership slot does not match player position")
            if slot == "FLEX":
                flex[p["position"]] += pct
        elif slot not in ("CPT", "FLEX"):
            raise ValueError("Showdown requires separate Captain and FLEX labels")
        if slot in matched[p["player_id"]]:
            raise ValueError("Multiple names resolved to the same player-slot")
        matched[p["player_id"]][slot] = pct
        raw_totals[slot] += pct
        mapping.append({"label_name": name, "slot": slot, "player_id": p["player_id"],
                        "method": "explicit_override" if name in overrides else "unique_snapshot_name_position",
                        "reason": overrides.get(name, {}).get("reason")})
    expected = {"QB": 100, "RB": 200, "WR": 300, "TE": 100, "DST": 100, "FLEX": 100} if snapshot["format"] == "classic" else {"CPT": 100, "FLEX": 500}
    for slot, total in expected.items():
        if abs(raw_totals[slot] - total) > max(2, total * .01):
            raise ValueError(f"Incomplete or incompatible ownership export: {slot} totals {raw_totals[slot]:.2f}, expected about {total}")
    output = []
    for pid, slots in matched.items():
        if snapshot["format"] == "classic":
            output.append({"player_id": pid, "slot": "OVERALL", "ownership_pct": sum(slots.values())})
        else:
            output.extend({"player_id": pid, "slot": slot, "ownership_pct": pct} for slot, pct in slots.items())
    c = {"contest_id": str(contest_id), "snapshot": snapshot, "labels_available_at": available_at,
         "source_digest": source_digest, "field_entries": entries, "contest_segment": "unknown_fee_and_entry_limit",
         "labels": sorted(output, key=lambda r: (str(r["player_id"]), r["slot"])),
         "flex_allocation": {p: flex[p] / sum(flex.values()) for p in ("RB", "WR", "TE")} if flex else {},
         "audit": {"identity_mapping": mapping, "reported_slot_totals": dict(raw_totals),
                   "importer_implementation_digest": code_digest(__file__),
                   "missing_label_player_ids": sorted(set(by_id) - set(matched)),
                   "missing_label_policy": "Missing rows remain unlabeled; never imputed zero",
                   "feature_policy": "Pregame salary and saved projection features only; observed ownership is a historical target",
                   "ownership_on_pregame_out": [r for r in output if by_id[r["player_id"]]["is_out"] and r["ownership_pct"] > 0]}}
    validate_contest(c)
    return c


def import_archives(grades_path, snapshots_path, standings_root):
    grades, saved = read(grades_path), read(snapshots_path)
    contests = []
    for c in grades["contests"]:
        matches = [s for s in saved if s["upload"]["upload_id"] == c["upload_id"]]
        if len(matches) != 1:
            raise ValueError("Historical contest requires one matching frozen salary snapshot")
        run = matches[0]["run"]
        snap = freeze_snapshot(c["upload_id"], c["format"], run["created_at"], run["input_snapshot"],
                               {"saved_run_id": run["run_id"], "saved_input_digest": run["input_digest"],
                                "snapshot_archive_sha256": file_digest(snapshots_path)})
        if stamp(snap["lock_at"]) != stamp(c["lock_at"]):
            raise ValueError("Archive and frozen slate lock disagree")
        path = Path(standings_root) / c["source_file"].replace("\\", "/")
        if file_digest(path) != c["source_sha256"]:
            raise ValueError("Raw standings differ from archived evidence")
        labels, entries, recount = read_complete_field(path, snap, c["entries"])
        contest = import_labels(snap, labels, c["contest_id"], c["imported_at"], c["source_sha256"], entries)
        contest["audit"]["label_method"] = "Exact recount of all verified field entries; zero means no appearances"
        contest["audit"]["field_recount"] = recount
        contests.append(contest)
    return seal({"version": VERSION, "kind": "history", "source_archive_digest": file_digest(grades_path), "contests": contests})


def database_snapshot(upload_id, env_file):
    """Read-only transaction; credentials and entrant data never reach outputs."""
    import os
    from dotenv import dotenv_values
    import psycopg2
    from psycopg2.extras import RealDictCursor
    url = os.environ.get("DATABASE_URL") or dotenv_values(env_file).get("DATABASE_URL")
    if not url:
        raise ValueError("DATABASE_URL unavailable")
    with psycopg2.connect(url) as conn:
        conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT u.upload_id,u.format,u.file_digest,u.projection_run_id,r.as_of_at FROM nfl_dfs_slate_uploads u LEFT JOIN nfl_dfs_projection_runs r ON r.run_id=u.projection_run_id WHERE u.upload_id=%s", (upload_id,))
            upload = cur.fetchone()
            if not upload:
                raise ValueError("Saved salary slate not found")
            cur.execute("SELECT dk_player_id,captain_dk_player_id,name,team,position,salary,our_proj,avg_fpts_dk,is_out,game_info,updated_at FROM nfl_dfs_slate_players WHERE upload_id=%s ORDER BY dk_player_id", (upload_id,))
            raw = [dict(p) for p in cur.fetchall()]
    captured = datetime.now(timezone.utc).isoformat()
    players = [{"dkPlayerId": p["dk_player_id"], "captainDkPlayerId": p["captain_dk_player_id"], "name": p["name"], "team": p["team"],
                "position": p["position"], "salary": p["salary"], "ourProj": None if p["our_proj"] is None else float(p["our_proj"]),
                "dkAvg": None if p["avg_fpts_dk"] is None else float(p["avg_fpts_dk"]), "isOut": bool(p["is_out"]), "gameInfo": p["game_info"],
                "baselineSource": {"asOf": upload["as_of_at"].isoformat() if upload["as_of_at"] else None}} for p in raw]
    return freeze_snapshot(str(upload["upload_id"]), upload["format"], captured, players,
                           {"source": "read_only_saved_salary_snapshot", "file_digest": upload["file_digest"],
                            "projection_run_id": str(upload["projection_run_id"]),
                            "latest_salary_row_update": max(p["updated_at"] for p in raw).isoformat(),
                            "availability": "Saved is_out only; no live injury refresh performed"})


def load_history(paths):
    contests = []
    by_id = {}
    for path in paths:
        artifact = verify(read(path))
        for c in artifact["contests"]:
            key = c["contest_id"]
            if key in by_id and digest(by_id[key]) != digest(c):
                raise ValueError(f"Conflicting imports of contest {key}")
            by_id[key] = c
    contests = sorted(by_id.values(), key=lambda c: c["contest_id"])
    return contests


def write_forecast_csv(directory, prediction):
    verify(prediction)
    stream = io.StringIO(newline="")
    fields = ["player_id", "name", "team", "position", "slot", "salary", "ownership_pct", "method", "status", "model_digest", "forecast_digest"]
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    for row in sorted(prediction["players"], key=lambda r: (-r["ownership_pct"], str(r["player_id"]), r["slot"])):
        writer.writerow({**{key: row[key] for key in fields[:8]}, "status": prediction["status"],
                         "model_digest": prediction["model_digest"], "forecast_digest": prediction["artifact_digest"]})
    path = Path(directory) / f"forecast-{prediction['artifact_digest']}.csv"
    content = stream.getvalue()
    if path.exists():
        with path.open(encoding="utf-8", newline="") as handle:
            if handle.read() != content:
                raise ValueError("Refusing to overwrite forecast CSV")
    else:
        with path.open("x", encoding="utf-8", newline="") as handle:
            handle.write(content)
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    archive = sub.add_parser("import-archives", help="Adapt existing frozen contest evidence")
    archive.add_argument("--grades", required=True)
    archive.add_argument("--snapshots", required=True)
    archive.add_argument("--standings-root", required=True)
    imp = sub.add_parser("import-contest", help="Attach post-contest ownership to a saved pregame snapshot")
    imp.add_argument("--snapshot", required=True)
    imp.add_argument("--standings", required=True)
    imp.add_argument("--contest-id", required=True)
    imp.add_argument("--labels-available-at", required=True)
    imp.add_argument("--expected-entries", type=int, required=True)
    imp.add_argument("--identity-overrides")
    snap = sub.add_parser("snapshot", help="Freeze an existing saved salary slate using SELECT only")
    snap.add_argument("--upload-id", required=True)
    snap.add_argument("--env-file", required=True)
    train = sub.add_parser("fit")
    train.add_argument("--history", nargs="+", required=True)
    train.add_argument("--as-of", required=True)
    predict = sub.add_parser("forecast")
    predict.add_argument("--model", required=True)
    predict.add_argument("--snapshot", required=True)
    predict.add_argument("--as-of", required=True)
    evaluate = sub.add_parser("evaluate", help="Chronological slate-grouped evaluation")
    evaluate.add_argument("--history", nargs="+", required=True)
    qualify = sub.add_parser("qualify", help="Grade the frozen accuracy gate on independent chronological slates")
    qualify.add_argument("--history", nargs="+", required=True)
    grade = sub.add_parser("grade", help="Score a frozen forecast after results import")
    grade.add_argument("--forecast", required=True)
    grade.add_argument("--history", nargs="+", required=True)
    grade.add_argument("--contest-id", required=True)
    for command in (archive, imp, snap, train, predict, evaluate, qualify, grade):
        command.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    if getattr(args, "as_of", None) and stamp(args.as_of) > datetime.now(timezone.utc):
        raise ValueError("Decision as-of cannot be in the future")
    if args.command == "import-archives":
        output = import_archives(args.grades, args.snapshots, args.standings_root)
    elif args.command == "import-contest":
        snapshot = verify(read(args.snapshot))
        overrides = read(args.identity_overrides) if args.identity_overrides else None
        labels, count, recount = read_complete_field(args.standings, snapshot, args.expected_entries, overrides)
        c = import_labels(snapshot, labels, args.contest_id, args.labels_available_at,
                          file_digest(args.standings), count, overrides)
        c["audit"]["label_method"] = "Exact recount of all verified field entries; zero means no appearances"
        c["audit"]["field_recount"] = recount
        output = seal({"version": VERSION, "kind": "history", "contests": [c]})
    elif args.command == "snapshot":
        output = database_snapshot(args.upload_id, args.env_file)
    elif args.command == "fit":
        output = fit(load_history(args.history), args.as_of)
    elif args.command == "forecast":
        snapshot = verify(read(args.snapshot))
        if datetime.now(timezone.utc) >= stamp(snapshot["lock_at"]):
            raise ValueError("Cannot create a new prospective forecast after lock; use evaluate for historical replay")
        output = forecast(verify(read(args.model)), snapshot, args.as_of)
        if output["timing"] != "pregame_frozen":
            raise ValueError("Slate locked during forecast; no prospective output saved")
    elif args.command == "evaluate":
        output = walk_forward(load_history(args.history))
    elif args.command == "qualify":
        from model.nfl_ownership_qualification import qualify as grade_qualification
        output = grade_qualification(load_history(args.history))
    else:
        prediction = verify(read(args.forecast))
        candidates = [c for c in load_history(args.history) if c["contest_id"] == args.contest_id]
        if len(candidates) != 1 or candidates[0]["snapshot"]["slate_id"] != prediction["slate_id"]:
            raise ValueError("Forecast and contest slate do not match")
        contest = candidates[0]
        if digest(contest["snapshot"]) != prediction["snapshot_digest"]:
            raise ValueError("Grade requires the exact snapshot used by the frozen forecast")
        output = seal({"version": VERSION, "kind": "grade", "forecast_digest": prediction["artifact_digest"],
                       "contest_id": args.contest_id, "source_digest": contest["source_digest"],
                       "timing": prediction.get("timing", "unverified_creation_time"),
                       "forecast_created_at": prediction.get("created_at"), "metrics": metrics(prediction, contest)})
    path = write_artifact(args.output_dir, output["kind"], output)
    csv_path = write_forecast_csv(args.output_dir, output) if output["kind"] == "forecast" else None
    print(json.dumps({"path": str(path), "digest": output["artifact_digest"], "kind": output["kind"],
                      "status": output.get("status", output.get("qualification")), "players": len(output.get("players", [])),
                      "csv": str(csv_path) if csv_path else None}))


if __name__ == "__main__":
    main()
