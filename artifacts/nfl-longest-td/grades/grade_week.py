"""Grade a published Longest TD week against final play-by-play.

usage: python grade_week.py CAPTURE.json.gz INDEX.json WEEK OUT.json
Reads the saved publication (never re-forecasts) and the research grader.
"""
import json
import math
import sys
from pathlib import Path

sys.path.insert(0, r"C:\Docs\_AI Python Projects\NBADFS_v2")
from research.nfl_longest_touchdown import read, grade  # noqa: E402


def as_forecast(pub):
    rows = []
    for p in pub["players"] + pub["residual"]:
        rows.append({"identity": p["identity"], "name": p["name"],
                     "longest_td_win_share": p["longestShare"],
                     "any_td_probability": p["anyTd"], "td_40_plus_probability": p["longTd"]})
    rows.sort(key=lambda r: -r["longest_td_win_share"])
    return {"game": pub["game"], "decision_at": pub["decisionAt"], "players": rows,
            "no_scrimmage_td_probability": pub["noTd"], "outcome_scope": "regulation_scrimmage",
            "request_sha256": pub["provenance"]["request_sha256"],
            "implementation_sha256": pub["provenance"]["implementation_sha256"]}


snapshot = read(Path(sys.argv[1]))
index = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
week = next(w for w in index["weeks"] if w["week"] == int(sys.argv[3]))
out = []
for pub in week["games"]:
    f = as_forecast(pub)
    g = grade(snapshot, f)
    row = {"game": pub["game"]["game_id"], "status": g["status"], "reason": g.get("reason"),
           "quarantined": g.get("quarantined_td_plays")}
    if g["status"] == "graded":
        m = g["model"]
        probs = {r["identity"]: r["longest_td_win_share"] for r in f["players"]}
        probs["NO_TD"] = f["no_scrimmage_td_probability"]
        ranked = sorted(probs, key=lambda k: -probs[k])
        names = {r["identity"]: r["name"] for r in f["players"]}
        winners = m["winners"] or ["NO_TD"]
        n_out = len(probs)
        uniform_brier = (1 - 1 / n_out) ** 2 + (n_out - 1) * (1 / n_out) ** 2
        row.update(
            longest_yards=m["longest_td_yards"],
            winners=[names.get(w, w) for w in winners],
            winner_prob=sum(probs[w] for w in winners),
            winner_rank=min(ranked.index(w) + 1 for w in winners),
            top_pick=names.get(ranked[0], ranked[0]), top_prob=probs[ranked[0]],
            top_hit=m["top_choice_hit"], brier=m["brier_score"], log_loss=m["log_loss"],
            uniform_brier=uniform_brier, uniform_log_loss=math.log(n_out), outcomes=n_out,
            unmodeled_winner=m["winner_was_unmodeled"],
            td_rows=m["td_event_rows"],
            no_td_prob=probs["NO_TD"])
    out.append(row)
Path(sys.argv[4]).write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
for r in out:
    if r["status"] != "graded":
        print(r["game"], r["status"], r["reason"], r.get("quarantined"))
        continue
    print(f"{r['game']:<18} longest {r['longest_yards']:>3}y  winner {', '.join(r['winners']):<28} "
          f"p={r['winner_prob']:.3f} rank {r['winner_rank']:>2}/{r['outcomes']}  "
          f"top pick {r['top_pick']} {r['top_prob']:.3f}  brier {r['brier']:.3f} (unif {r['uniform_brier']:.3f})")
