"""Add ownership to every case in the Weeks 1-6 value review (2026-10-22).

For every player row in assessment.json this records two things:

- predicted: what our frozen forecast said, read from the selected pre-lock
  run's input snapshot (`ownership` per player: total / captain / flex and the
  prior version). Older runs stored no ownership; those rows say so.
- field: what the contest actually showed, with its label class from
  `nfl_ownership_label_provenance` (docs/nfl-ownership-scorecard-v1.md
  section 3). A player missing from a complete DraftKings export is a
  reconciled zero; a stat-api import below the mass floor leaves him
  unresolved; a slate with no imported contest says so.

Reads retained evidence plus SELECT-only database queries; writes
ownership.json beside assessment.json. Run from the repository root.
"""
from __future__ import annotations

import gzip
import json
import os
import re
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path("artifacts/nfl-value-history-20261021").resolve()


def norm(name: str) -> str:
    return re.sub(r"[^a-z]", "", str(name or "").lower())


def main() -> None:
    load_dotenv("web/.env.local")
    assessment = json.loads((ROOT / "assessment.json").read_text(encoding="utf-8"))
    inputs = json.load(gzip.open(ROOT / "inputs.json.gz", "rt", encoding="utf-8"))
    runs = {r["run_id"]: r for r in inputs["selected_runs"]}
    contests = {str(c["contest_id"]): c for c in inputs["contests"]}

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '90s'")
    ids = sorted({str(c["contest_id"]) for g in assessment["reports"] for c in g["contests"]})
    cur.execute("""SELECT contest_id, file_name, entry_count, score_curve IS NOT NULL
                   FROM nfl_dfs_field_contests WHERE contest_id = ANY(%s)""", (ids,))
    meta = {r[0]: {"file_name": r[1] or "", "entries": r[2], "full": r[3]} for r in cur.fetchall()}
    cur.execute("""SELECT contest_id, normalized_name, slot, label_class, label_pct
                   FROM nfl_ownership_label_provenance WHERE contest_id = ANY(%s)""", (ids,))
    prov: dict[tuple[str, str, str], tuple[str, float | None]] = {}
    for cid, name, slot, cls, pct in cur.fetchall():
        prov[(cid, name, slot)] = (cls, None if pct is None else float(pct))
    cur.execute("""SELECT contest_id, normalized_name, drafted_pct, drafted_by_slot
                   FROM nfl_dfs_field_ownership WHERE contest_id = ANY(%s)""", (ids,))
    labels: dict[tuple[str, str], tuple[float, dict]] = {}
    for cid, name, pct, by_slot in cur.fetchall():
        labels[(cid, name)] = (float(pct), by_slot or {})
    conn.close()

    def reference(g: dict) -> str | None:
        """The slate's largest imported contest (scorecard: the reference contest)."""
        known = [str(c["contest_id"]) for c in g["contests"] if str(c["contest_id"]) in meta
                 and not str(c["contest_id"]).startswith("dfsdb-")]
        return max(known, key=lambda cid: meta[cid]["entries"] or 0) if known else None

    def field(cid: str | None, name: str, showdown: bool) -> dict:
        if cid is None:
            return {"class": "no_contest"}
        key = norm(name)
        source = "stat_api" if meta[cid]["file_name"].startswith("stat-api") else "draftkings"
        out: dict = {"contest_id": cid, "source": source}
        hit = labels.get((cid, key))
        if hit:
            out["total"] = hit[0]
            if showdown:
                out["captain"] = hit[1].get("CPT")
                out["flex"] = hit[1].get("FLEX")
        stored = prov.get((cid, key, "overall"))
        if stored:
            out["class"] = stored[0]
            if showdown:
                out["captain_class"] = (prov.get((cid, key, "CPT")) or ("unclassified", None))[0]
                out["flex_class"] = (prov.get((cid, key, "FLEX")) or ("unclassified", None))[0]
        elif hit:
            # Imported after the provenance run (week 6). A DraftKings export row is
            # observed; a stat-api seat row is observed for the overall share.
            out["class"] = "observed"
        elif source == "draftkings":
            out["class"] = "reconciled_zero"  # complete export: absent means nobody drafted him
            out["total"] = 0.0
        else:
            # stat-api import the provenance run has not classified (imported later):
            # absent from the fetched lineups is not evidence of zero.
            out["class"] = "unresolved"
        if out.get("class") == "reconciled_zero":
            out["total"] = 0.0
        return out

    result: dict = {"captured_at": assessment["captured_at"], "reports": {}}
    for g in assessment["reports"]:
        showdown = g["format"] == "showdown"
        run = runs.get(g.get("selected_run_id") or "")
        snapshot = {p.get("dkPlayerId"): p for p in (run or {}).get("input_snapshot") or []}
        cid = reference(g)
        rows = {}
        for p in g["rows"]:
            own = (snapshot.get(p["dk_player_id"]) or {}).get("ownership")
            predicted = ({"total": own.get("total"), "captain": own.get("captain"), "flex": own.get("flex"),
                          "version": own.get("source")} if own else
                         {"version": None, "reason": "no saved build" if run is None else "run stored no ownership"})
            rows[str(p["dk_player_id"])] = {"predicted": predicted, "field": field(cid, p["name"], showdown)}
        result["reports"][g["key"]] = {"reference_contest": cid, "rows": rows}
    (ROOT / "ownership.json").write_text(json.dumps(result, indent=1), encoding="utf-8")
    total = sum(len(r["rows"]) for r in result["reports"].values())
    with_pred = sum(r["predicted"].get("version") is not None for x in result["reports"].values() for r in x["rows"].values())
    by_class: dict[str, int] = {}
    for x in result["reports"].values():
        for r in x["rows"].values():
            by_class[r["field"]["class"]] = by_class.get(r["field"]["class"], 0) + 1
    print(json.dumps({"rows": total, "predicted_present": with_pred, "field_classes": by_class}))


if __name__ == "__main__":
    main()
