"""Resolve the attribution studies' "no recorded score" players against nflverse (read-only).

A pool player with neither a DraftKings contest number nor a stat row is
excluded from the hindsight oracle. This classifies each one per week from
two nflverse files, kept with their SHA-256 in `resolution.json`:

* `roster_weekly_{season}.csv`: not on the active roster that week
  (status other than ACT/A: inactive, reserve, practice squad, cut, or absent)
  => verified non-participant, DraftKings 0;
* `snap_counts_{season}.csv`: active but no offensive snaps (kickers: no
  special-teams snaps either) => verified 0, since DraftKings scores only
  recorded statistics;
* active with offensive (kicker: special-teams) snaps and still no stat row
  => UNRESOLVED (a possible stats-feed gap); never scored.

Writes `resolution.json` (per week: dkPlayerId -> class) next to the slate files.

    python -m research.nfl_attribution_resolve_missing artifacts/nfl-classic-attribution-2026-10-13 <roster.csv> <snaps.csv>
"""

from __future__ import annotations

import csv
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from model.nfl_dfs_dk_pool_match import normalize_name

TEAM = {"JAC": "JAX", "AZ": "ARI", "LA": "LAR", "WSH": "WAS", "OAK": "LV", "SD": "LAC", "STL": "LAR"}
ACTIVE = {"ACT", "A"}


def team(t):
    t = (t or "").strip().upper()
    return TEAM.get(t, t)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    d, roster_path, snaps_path = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    gsis_of = {r["id"]: r["gsis_id"] for r in db.execute("SELECT id, gsis_id FROM ff_players WHERE gsis_id IS NOT NULL")}
    roster = defaultdict(dict)       # week -> key -> status ; key = gsis or (name, team)
    for r in csv.DictReader(roster_path.open(encoding="utf-8")):
        if r.get("game_type") not in (None, "", "REG"):
            continue
        w = int(r["week"])
        st = (r.get("status") or "").upper()
        if r.get("gsis_id"):
            roster[w][r["gsis_id"]] = st
        roster[w][(normalize_name(r.get("full_name") or ""), team(r["team"]))] = st
    snaps = defaultdict(dict)        # week -> (name, team) -> (offense, st)
    for r in csv.DictReader(snaps_path.open(encoding="utf-8")):
        if r.get("game_type") != "REG":
            continue
        snaps[int(r["week"])][(normalize_name(r["player"]), team(r["team"]))] = (float(r["offense_snaps"] or 0), float(r["st_snaps"] or 0))
    out = {"sources": {roster_path.name: sha(roster_path), snaps_path.name: sha(snaps_path)}, "weeks": {}}
    for f in sorted(d.glob("*.json")):
        if f.name in ("resolution.json", "exclusions.json", "cbc.json", "results.json", "coverage.json"):
            continue
        s = json.loads(f.read_text(encoding="utf-8"))
        w = s["week"]
        res, counts = {}, defaultdict(int)
        for p in s["players"]:
            if p.get("actual") is not None or p["eligibilityClass"] == "availability" or p["position"] == "DST":
                continue
            g = gsis_of.get(p.get("ffPlayerId"))
            k = (normalize_name(p["name"]), team(p.get("team")))
            st = roster[w].get(g) if g else None
            st = st if st is not None else roster[w].get(k)
            off, stsn = snaps[w].get(k, (0.0, 0.0))
            if st is None or st not in ACTIVE:
                cls = "verified_zero_not_active"
            elif (p["position"] == "K" and off == 0 and stsn == 0) or (p["position"] != "K" and off == 0):
                cls = "verified_zero_no_snaps"
            else:
                cls = "unresolved_played_without_stat_row"
            res[str(p["dkPlayerId"])] = {"class": cls, "rosterStatus": st, "offenseSnaps": off, "stSnaps": stsn, "name": p["name"]}
            counts[cls] += 1
        out["weeks"][f"{f.stem}"] = {"week": w, "counts": dict(counts), "players": res}
        print(f.stem, dict(counts), [v["name"] for v in res.values() if v["class"].startswith("unresolved")][:12])
    (d / "resolution.json").write_text(json.dumps(out, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
