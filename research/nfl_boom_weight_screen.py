"""Hindsight boom-weight screen (DISCOVERY ONLY; docs/nfl-ghost-factorial-study.md, amendment 2026-10-30).

    npx tsx --conditions=react-server scripts/research-nfl-boom-weight-screen.ts   (in web/)
    python -m research.nfl_boom_weight_screen

Grades the house portfolio rebuilt at GPP boom weight 0, 1, 2 and 4 on every frozen ghost
slate that has a gradable contest (complete standings AND a payout table, the same rule as
the Tuesday grade: `research.nfl_ghost_grade.contest_for`). It reads the lineups the
TypeScript builder wrote to artifacts/nfl-ghost-extra-arms-v1/boom-weight-screen/ and
scores them exactly as the ghost grader scores an arm. It stores nothing in the database
and writes one summary JSON beside the inputs.

What it is: a look at the slates that were already seen, to license the registered forward
test (the boom-weight arm pair, levels 1 and 4 against the house's 2). What it is not: a
result. The slates were seen before the levels were chosen, several levels are read at once,
and a constant moves only if its effect is a confirmed candidate on 8 fresh slates.
"""

from __future__ import annotations

import glob
import json
import os
import sys

from config import load_config
from db.database import DatabaseManager
from model import nfl_ghost_grade as g
from model.nfl_dfs_field_audit import normalize_name
from research.nfl_ghost_grade import contest_for, points_lookup

SCREEN_DIR = os.path.join(os.path.dirname(__file__), "..", "artifacts", "nfl-ghost-extra-arms-v1", "boom-weight-screen")
LEVELS = ("0", "1", "2", "4")
HOUSE_LEVEL = "2"


def freeze_row(db: DatabaseManager, freeze_id: str) -> dict | None:
    rows = db.execute(
        """SELECT freeze_id, format, first_kickoff, season, week, draft_group_id,
                  (SELECT jsonb_agg(jsonb_build_object('name', p->>'name', 'dkPlayerId', p->'dkPlayerId', 'ffPlayerId', p->'ffPlayerId'))
                     FROM jsonb_array_elements(input_snapshot) p) AS input_snapshot
             FROM nfl_dfs_ghost_freezes WHERE freeze_id = %s""", (freeze_id,))
    return dict(rows[0]) if rows else None


def main(argv=None) -> int:
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    files = sorted(glob.glob(os.path.join(SCREEN_DIR, "*.json")))
    files = [f for f in files if not f.endswith("summary.json")]
    if not files:
        print(f"no screen inputs in {SCREEN_DIR}; run the TypeScript builder first")
        return 1
    summary = {"version": "nfl-ghost-extra-arms-v1", "screen": "boom-weight-hindsight", "discovery_only": True,
               "grader": g.GRADER_VERSION, "slates": []}
    print("Hindsight boom-weight screen: DISCOVERY ONLY (slates seen before the levels were chosen).")
    for path in files:
        with open(path, encoding="utf-8") as fh:
            built = json.load(fh)
        freeze = freeze_row(db, built["freezeId"])
        label = f"{built['season']} w{built['week']} {built['format']} {built['draftGroupId']}"
        if freeze is None:
            print(f"{label}: freeze row missing")
            continue
        contest, reason = contest_for(db, freeze)
        entry = {"freeze_id": built["freezeId"], "format": built["format"], "season": built["season"], "week": built["week"],
                 "draft_group_id": built["draftGroupId"], "requested": built["requested"],
                 "house_reproduced": (built["levels"][HOUSE_LEVEL].get("matchesStoredHouse"), built["levels"][HOUSE_LEVEL].get("storedHouseLineups"))}
        if contest is None:
            entry["graded"] = False
            entry["reason"] = reason
            summary["slates"].append(entry)
            repro = entry["house_reproduced"]
            print(f"{label}: NOT GRADED - {reason} (house rebuilt: {repro[0]}/{repro[1]} lineups match stored arm 0)")
            continue
        by_name, by_ff = points_lookup(db, contest["contest_id"], freeze["season"], freeze["week"])
        snap = {p["dkPlayerId"]: p for p in freeze["input_snapshot"]}

        def points_for(slot: dict):
            name = normalize_name(slot.get("name") or "")
            if name in by_name:
                return by_name[name]
            ff = (snap.get(slot.get("dkPlayerId")) or {}).get("ffPlayerId")
            return by_ff.get(int(ff)) if ff is not None else None

        entry.update({"graded": True, "contest": contest["name"], "entries": contest["entries"], "levels": {}})
        graded = {}
        for lv in LEVELS:
            lineups = [l["slots"] for l in built["levels"][lv]["lineups"]]
            m = g.grade_subject(lineups, built["requested"], points_for, contest)
            o = g.outcomes(m)
            graded[lv] = {"built": m["built"], "scored": m["scored"], "top1_hits": m["top1_hits"], "top01_hits": m["top01_hits"],
                          "cashes": m["cashes"], "payout": m["payout"], "best_score": m["best_score"], "mean_score": m["mean_score"],
                          "top1_rate": o["top1_rate"], "roi": o["roi"], "unscored_players": m["detail"]["unscored_players"]}
        house = graded[HOUSE_LEVEL]
        for lv in LEVELS:
            r = graded[lv]
            r["diff_vs_house"] = {k: (None if r[k] is None or house[k] is None else round(r[k] - house[k], 4)) for k in ("top1_rate", "roi", "mean_score")}
        entry["levels"] = graded
        summary["slates"].append(entry)
        repro = entry["house_reproduced"]
        print(f"{label}: {contest['name']} ({contest['entries']:,} entries); house rebuilt {repro[0]}/{repro[1]} = stored arm 0")
        print(f"  {'boom':>5} {'built':>5} {'scored':>6} {'top1%':>6} {'top.1%':>6} {'cash':>5} {'payout':>8} {'ROI':>7} {'mean':>6} {'best':>6}  vs house(top1%, ROI, mean)")
        for lv in LEVELS:
            r = graded[lv]
            roi = "-" if r["roi"] is None else f"{r['roi']:+.0%}"
            d = r["diff_vs_house"]
            print(f"  {lv:>5} {r['built']:>5} {r['scored']:>6} {r['top1_hits']:>6} {r['top01_hits']:>6} {r['cashes']:>5} {r['payout'] if r['payout'] is not None else '-':>8} {roi:>7} "
                  f"{r['mean_score'] if r['mean_score'] is not None else 0:>6.1f} {r['best_score'] if r['best_score'] is not None else 0:>6.1f}  "
                  f"{d['top1_rate']:+.3f} {d['roi'] if d['roi'] is not None else float('nan'):+.3f} {d['mean_score']:+.2f}")
            if r["unscored_players"]:
                print(f"        unscored: {', '.join(r['unscored_players'][:6])}")
    out = os.path.join(SCREEN_DIR, "summary.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=1)
    graded_n = sum(1 for s in summary["slates"] if s.get("graded"))
    print(f"\n{graded_n} of {len(summary['slates'])} frozen slates gradable; summary at {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
