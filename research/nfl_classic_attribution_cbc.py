"""Independent exact oracle for the Classic attribution study (CBC via pulp, read-only).

Reads the frozen slate files and `exclusions.json` (rule configurations and
the product's own Winners pool exclusions, written by
`web/scripts/research-nfl-classic-attribution.ts exclusions`) and solves the
best single lineup of actual DraftKings points under every configuration, with
and without the cheap-player rule. Writes `cbc.json`: one entry per
(week, cheap-player setting, configuration) with the lineup, CBC's status,
objective, best bound, relative gap and solve time read from CBC's own log.
A number is used by the report only when CBC proved it optimal with zero gap.

The model mirrors the optimizer's Classic constraints line for line: QB 1, RB
2-3, WR 3-4, TE 1-2, DST 1, RB+WR+TE 7; cap $50,000 and the configuration's
floor; at most 8 players from one game; a defence with at most 3 opposing
QB/RB/WR/TE (`dst_script`, 6*DST + opposing offense <= 9) unless the
configuration is DraftKings-rules-only; the QB stack (WR/TE teammate) together
with its coupled QB-versus-opposing-DST ban; the bring-back (opposing RB/WR/TE)
when required; and the strict DST-versus-own-offense ban for that diagnostic.

    python -m research.nfl_classic_attribution_cbc artifacts/nfl-classic-attribution-2026-10-13
"""

from __future__ import annotations

import json
import re
import sys
import tempfile
import time
from pathlib import Path

import pulp

CAP, FLOOR = 50_000, 49_500


def opponent(p: dict) -> str | None:
    gk = p.get("gameKey") or ""
    return next((t for t in gk.split("@") if t != p["team"]), None) if "@" in gk else None


def solve(slate: dict, excl: dict, cfg: dict, punts: bool, zeros: set | None = None) -> dict:
    r = cfg["rules"]
    banned = set()
    for k in ("rbPool", "wrPool", "tePool", "dstPool"):
        if r[k]:
            banned |= set(excl["byRule"][k])
    zeros = zeros or set()
    pool = [dict(p, actual=0.0) if p.get("actual") is None and str(p["dkPlayerId"]) in zeros else p for p in slate["players"]]
    pool = [p for p in pool if p.get("actual") is not None and p["dkPlayerId"] not in banned
            and (p["eligibilityClass"] == "available" or (punts and p["eligibilityClass"] == "punt"))]
    prob = pulp.LpProblem("lineup", pulp.LpMaximize)
    x = {p["dkPlayerId"]: pulp.LpVariable(f"x_{p['dkPlayerId']}", cat="Binary") for p in pool}
    by = lambda pred: [x[p["dkPlayerId"]] for p in pool if pred(p)]
    prob += pulp.lpSum(p["actual"] * x[p["dkPlayerId"]] for p in pool)
    pos = lambda q: by(lambda p: p["position"] == q)
    prob += pulp.lpSum(pos("QB")) == 1
    prob += pulp.lpSum(pos("DST")) == 1
    prob += pulp.lpSum(pos("RB")) >= 2
    prob += pulp.lpSum(pos("RB")) <= 3
    prob += pulp.lpSum(pos("WR")) >= 3
    prob += pulp.lpSum(pos("WR")) <= 4
    prob += pulp.lpSum(pos("TE")) >= 1
    prob += pulp.lpSum(pos("TE")) <= 2
    prob += pulp.lpSum(pos("RB") + pos("WR") + pos("TE")) == 7
    salary = pulp.lpSum(p["salary"] * x[p["dkPlayerId"]] for p in pool)
    prob += salary <= CAP
    floor = FLOOR if r["floor"] else (slate["run"]["settings"].get("minSalary") or 0) if r.get("standardFloor") else 0
    if floor:
        prob += salary >= floor
    for g in {p.get("gameKey") for p in pool if p.get("gameKey")}:
        prob += pulp.lpSum(by(lambda p, g=g: p.get("gameKey") == g)) <= 8
    offense = ("QB", "RB", "WR", "TE")
    for d in [p for p in pool if p["position"] == "DST"]:
        opp = opponent(d)
        facing = by(lambda p, opp=opp: p["team"] == opp and p["position"] in offense)
        if not r["dkOnly"]:
            prob += 6 * x[d["dkPlayerId"]] + pulp.lpSum(facing) <= 9
        if r["noDstVsOffense"]:
            prob += 9 * x[d["dkPlayerId"]] + pulp.lpSum(facing) <= 9
    if r["stack"] or r["bringBack"]:
        for q in [p for p in pool if p["position"] == "QB"]:
            xq, opp = x[q["dkPlayerId"]], opponent(q)
            if r["stack"]:
                prob += pulp.lpSum(by(lambda p, t=q["team"]: p["team"] == t and p["position"] in ("WR", "TE"))) - xq >= 0
                prob += xq + pulp.lpSum(by(lambda p, opp=opp: p["position"] == "DST" and p["team"] == opp)) <= 1
            if r["bringBack"]:
                prob += pulp.lpSum(by(lambda p, opp=opp: p["team"] == opp and p["position"] in ("RB", "WR", "TE"))) - xq >= 0
    with tempfile.TemporaryDirectory() as tmp:
        log = Path(tmp) / "cbc.log"
        t0 = time.time()
        prob.solve(pulp.PULP_CBC_CMD(msg=False, gapRel=0, timeLimit=300, logPath=str(log)))
        seconds = time.time() - t0
        text = log.read_text(errors="ignore") if log.exists() else ""
    status = pulp.LpStatus[prob.status]
    objective = pulp.value(prob.objective)
    result = (re.findall(r"Result - (.+)", text) or [None])[-1]
    nodes = (re.findall(r"Enumerated nodes:\s+(\d+)", text) or [None])[-1]
    root = (re.findall(r"best possible ([-\d.e+]+)", text) or [None])[0]
    proven = status == "Optimal" and result is not None and result.strip().startswith("Optimal solution found")
    # gapRel=0: CBC stops with "Optimal solution found" only when its bound equals the incumbent.
    bound = round(objective, 2) if proven and objective is not None else None
    gap = 0.0 if proven else None
    chosen = [p for p in pool if (x[p["dkPlayerId"]].value() or 0) > 0.5]
    return {"status": "Optimal" if proven else (result or status), "cbcResult": result, "nodes": int(nodes) if nodes else None,
            "rootBound": abs(float(root)) if root else None,
            "objective": round(objective, 2) if objective is not None and status != "Infeasible" else None, "bound": bound, "gap": gap,
            "seconds": round(seconds, 2), "poolSize": len(pool),
            "lineup": [{"dkPlayerId": p["dkPlayerId"], "name": p["name"], "position": p["position"], "team": p["team"],
                        "salary": p["salary"], "actual": p["actual"]} for p in chosen]}


def main() -> None:
    d = Path(sys.argv[1])
    excl = json.loads((d / "exclusions.json").read_text(encoding="utf-8"))
    resolution = json.loads((d / "resolution.json").read_text(encoding="utf-8")) if (d / "resolution.json").exists() else None
    out = {}
    for f in sorted(d.glob("*-classic.json")):
        slate = json.loads(f.read_text(encoding="utf-8"))
        e = excl["slates"][str(slate["week"])]
        verified = set()
        if resolution:
            verified = {k for k, v in resolution["weeks"][f.stem]["players"].items() if v["class"].startswith("verified_zero")}
        for punts in (False, True):
            for cfg in excl["configs"]:
                res = solve(slate, e, cfg, punts)
                out[f"{slate['week']}|{'punts' if punts else 'noPunts'}|{cfg['key']}"] = res
                # Sensitivity: verified non-participants (nflverse) enter the pool at DraftKings' 0.
                if resolution:
                    out[f"{slate['week']}|{'punts' if punts else 'noPunts'}|{cfg['key']}|verifiedZeros"] = solve(slate, e, cfg, punts, verified)
                print(slate["week"], "punts" if punts else "noPunts", cfg["key"], res["status"], res["objective"], res["gap"], res["seconds"])
    (d / "cbc.json").write_text(json.dumps(out, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
