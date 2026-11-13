"""Fit `nfl-ownership-field-showdown-v1` and grade it.

    python -m research.nfl_showdown_field_model --slates PATH             # grade, write model + fixture
    python -m research.nfl_showdown_field_model --slates PATH --no-write

PATH is a local JSON list of training slates. Each is one Showdown pool exactly
as the app's ownership step saw it before lock (a ghost freeze: position, team,
salary, captain_salary, is_out, pts, anchor, team_implied, opponent_implied)
with the real field's captain and flex ownership in percent (cpt_act, flex_act)
and a consensus projection (sa_pts) for the fringe factors. The file joins our
frozen slates to licensed contest data, so it is not in the repository.

Grading is leave-one-slate-out on the slates whose sportsbook lines were in
(the state the page is in near lock); those are also the slates the shipped
numbers are fitted on. Slates frozen before the lines were in are scored with
the shipped numbers as a second, untouched check. Needs torch, which the
pipeline does not.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path

import numpy as np

from model import nfl_showdown_field_model as M

MODEL_PATH = Path("web/src/data/nfl-showdown-field-model-v1.json")
FIXTURE_PATH = Path("tests/fixtures/nfl_showdown_field_model_fixture.json")
#: Consensus kicker points on his team's Vegas points, and defense points on the
#: opponent's: least squares over 60 kickers and 60 defenses on 30 Showdown slates
#: (2026 weeks 1-10), fitted 2026-11-13.
KICKER = {"intercept": 3.5306, "slope": 0.2325}
DEFENSE = {"intercept": 14.2257, "slope": -0.3630}
INPUT_KEYS = ("position", "team", "salary", "captain_salary", "is_out", "pts", "anchor", "team_implied", "opponent_implied")


def fringe_factors(slates: list[dict]) -> dict:
    """Consensus points over ours for players without a full set of lines, on slates that have lines."""
    out = {}
    for group in ("td_only", "none"):
        ours = theirs = 0.0
        for s in slates:
            if not M.has_lines(s["players"]):
                continue
            for p in s["players"]:
                kind = p.get("anchor") or "none"
                if M.eligible(p) and p["position"] not in ("K", "DST") and kind == group and p.get("sa_pts") is not None:
                    ours += p["pts"]; theirs += p["sa_pts"]
        out[group] = round(theirs / ours, 4) if ours else 1.0
    return out


def _tensors(slate: dict, crowd: dict):
    import torch
    built = M.build(slate["players"], crowd)
    t = lambda a, kind=torch.float64: torch.tensor(np.asarray(a), dtype=kind)
    players = slate["players"]
    return {"built": built, "pts": t(built["pts"]), "pos": t(built["position"], torch.long), "cap": t(built["cap"], torch.long),
            "flex": t(built["flex"], torch.long), "left": t(built["left"]), "stack": t(built["stack"]), "n": built["n"],
            "act_flex": t([players[i].get("flex_act") or 0.0 for i in built["index"]]),
            "act_cpt": t([players[i].get("cpt_act") or 0.0 for i in built["index"]])}


def _shares(b: dict, th: dict):
    import torch
    fo, co = th["fo"] - th["fo"][2], th["co"] - th["co"][2]          # wide receivers are the reference position
    value, captain_value = b["pts"] + fo[b["pos"]], M.CAPTAIN_MULTIPLIER * b["pts"] + co[b["pos"]]
    utility = value[b["flex"]].sum(1) + captain_value[b["cap"]] + th["b"] * b["left"] + th["s"] * b["stack"]
    p = torch.softmax(utility / torch.exp(th["log_tau"]), 0)
    captain = torch.zeros(b["n"], dtype=torch.float64).scatter_add(0, b["cap"], p) * 100
    flex = torch.zeros(b["n"], dtype=torch.float64).scatter_add(0, b["flex"].reshape(-1), p.repeat_interleave(5)) * 100
    return captain, flex


def _loss(batches: list[dict], th: dict):
    total = 0.0
    for b in batches:
        captain, flex = _shares(b, th)
        # A captain point counts four times a flex point: the captain total is a fifth of the flex total.
        total = total + ((flex - b["act_flex"]) ** 2).sum() + 4 * ((captain - b["act_cpt"]) ** 2).sum()
    return total / len(batches)


def fit(batches: list[dict], crowd: dict, max_steps: int = 12) -> dict:
    """Least squares on captain and flex ownership; returns the model dict."""
    import torch
    th = {"log_tau": torch.tensor(1.2, dtype=torch.float64, requires_grad=True), "b": torch.tensor(-1.0, dtype=torch.float64, requires_grad=True),
          "s": torch.tensor(0.0, dtype=torch.float64, requires_grad=True),
          "fo": torch.zeros(6, dtype=torch.float64, requires_grad=True), "co": torch.zeros(6, dtype=torch.float64, requires_grad=True)}
    opt = torch.optim.LBFGS(list(th.values()), lr=0.5, max_iter=25, history_size=20, line_search_fn="strong_wolfe")

    def closure():
        opt.zero_grad()
        loss = _loss(batches, th)
        loss.backward()
        return loss
    last = math.inf
    for _ in range(max_steps):
        opt.step(closure)
        now = _loss(batches, th).item()
        if last - now < 1e-4 * max(1.0, now):
            break
        last = now
    fo, co = (th["fo"] - th["fo"][2]).tolist(), (th["co"] - th["co"][2]).tolist()
    return {"version": M.VERSION, "tau": round(math.exp(th["log_tau"].item()), 5), "unused_salary": round(th["b"].item(), 5),
            "qb_stack": round(th["s"].item(), 5),
            "flex_offsets": {p: round(v, 5) for p, v in zip(M.POSITIONS, fo)}, "captain_offsets": {p: round(v, 5) for p, v in zip(M.POSITIONS, co)},
            **crowd, "loss": round(_loss(batches, th).item(), 3)}


def errors(model: dict, slate: dict) -> tuple[float, float, int]:
    """(captain, flex) mean absolute miss in percentage points over the slate's eligible players."""
    got = M.predict(model, slate["players"])
    rows = [(g, p) for g, p in zip(got, slate["players"]) if M.eligible(p)]
    return (float(np.mean([abs(g[0] - (p.get("cpt_act") or 0.0)) for g, p in rows])),
            float(np.mean([abs(g[1] - (p.get("flex_act") or 0.0)) for g, p in rows])), len(rows))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--slates", required=True, type=Path)
    ap.add_argument("--no-write", action="store_true")
    args = ap.parse_args(argv)
    slates = json.loads(args.slates.read_text(encoding="utf-8"))
    lined = [s for s in slates if M.has_lines(s["players"])]
    unlined = [s for s in slates if not M.has_lines(s["players"])]
    print(f"{M.VERSION}: {len(slates)} slates, {len(lined)} with the books' lines in")
    held = []
    for i, s in enumerate(lined):
        others = [x for j, x in enumerate(lined) if j != i]
        crowd = {"kicker": KICKER, "defense": DEFENSE, "fringe": fringe_factors(others)}
        model = fit([_tensors(o, crowd) for o in others], crowd)
        c, f, n = errors(model, s)
        held.append((c, f))
        print(f"  held out {s['date']} {s['name'][:22]:<22} {n} players  captain {c:.2f}  flex {f:.2f}")
    held_c, held_f = float(np.mean([h[0] for h in held])), float(np.mean([h[1] for h in held]))
    print(f"  leave-one-slate-out, {len(lined)} slates: captain {held_c:.2f}  flex {held_f:.2f}")

    crowd = {"kicker": KICKER, "defense": DEFENSE, "fringe": fringe_factors(lined)}
    full = fit([_tensors(s, crowd) for s in lined], crowd)
    for s in unlined:
        c, f, n = errors(full, s)
        print(f"  not in the fit (frozen before the lines) {s['date']} {s['name'][:22]:<22} {n} players  captain {c:.2f}  flex {f:.2f}")
    full.update({"fitted_at": datetime.now(timezone.utc).isoformat(), "slates": len(lined),
                 "training": [f"{s['date']} {s['name']}" for s in lined],
                 "held_out": {"slates": len(lined), "captain_mae": round(held_c, 3), "flex_mae": round(held_f, 3)}})
    print("  " + json.dumps({k: full[k] for k in ("tau", "unused_salary", "qb_stack", "flex_offsets", "captain_offsets", "fringe", "loss")}))
    if not args.no_write:
        MODEL_PATH.write_text(json.dumps(full, indent=1) + "\n", encoding="utf-8")
        s = lined[-1]
        got = M.predict(full, s["players"])
        FIXTURE_PATH.write_text(json.dumps({"slate": f"{s['date']} {s['name']}", "players": [
            {**{k: p.get(k) for k in INPUT_KEYS}, "captain": round(g[0], 4), "flex": round(g[1], 4)}
            for p, g in zip(s["players"], got)]}, indent=1) + "\n", encoding="utf-8")
        print(f"\n  wrote {MODEL_PATH} and {FIXTURE_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
