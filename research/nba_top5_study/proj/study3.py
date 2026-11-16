"""Study 3: which source gives the best upper percentiles (75th, 90th)? Played players, lock-time curves."""
import sys as _s, pathlib as _p; _s.path.insert(0, str(_p.Path(__file__).resolve().parents[1])); import _paths
import sys, json
from collections import defaultdict
from pathlib import Path
import numpy as np
from scipy.special import ndtri
TOP5 = _paths.DATA_ROOT
import top5_harness as H
from research.nba_wta_trios import survival_knots
SA = json.loads((_paths.DATA_ROOT / "proj" / "statapi.json").read_text(encoding="utf-8"))
sa = {(r["game_date"][:10], r["player_key"]): r for r in SA}
nights = H.load_nights()
def curve_q(p, level):                      # level = F (e.g. 0.9)
    kn = survival_knots([tuple(c) for c in p["curve"]])
    if kn is None: return None
    s, x = kn
    u = 1 - level
    if u < s[0] or u > s[-1]: return None
    return float(np.interp(u, s, x))
rows = []
for n in nights:
    prior = [m for m in nights if m["date"] < n["date"]]
    g = H.fit_recal(prior) if len(prior) >= 5 else None
    # walk-forward residual quantiles by line band (played, prior nights)
    res = defaultdict(list)
    for m in prior:
        for p in H.pool(m, 60):
            if p.get("played") and p.get("actual") is not None:
                res[min(int(p["line"] // 6), 4)].append((p["actual"] - p["line"]) / (3.9 + 0.18 * p["line"]))
    for p in H.pool(n, 60):
        if not p.get("played") or p.get("actual") is None: continue
        r = dict(date=n["date"], line=p["line"], actual=p["actual"], key=p["key"])
        sd = 3.9 + 0.18 * p["line"]
        for q in (0.75, 0.9):
            t = str(int(q * 100))
            r["ladder" + t] = curve_q(p, q)
            if g is not None:
                lvl = float(np.interp(q, g, H.RECAL_KNOTS))        # stated level whose realized share is q
                r["recal" + t] = curve_q(p, lvl)
            r["normal" + t] = p["line"] + 0.5 + sd * ndtri(q)
            b = min(int(p["line"] // 6), 4)
            if len(res[b]) >= 60: r["empir" + t] = p["line"] + sd * float(np.quantile(res[b], q))
        s = sa.get((n["date"], p["key"]))
        if s and s["pts_p90"] is not None and s["pts_p50"] is not None:
            r["statapi90"] = s["pts_p90"]; r["statapi75"] = s["pts_p50"] + (s["pts_p90"] - s["pts_p50"]) * 0.6745 / 1.2816
            # stat-api spread around the books' line
            r["hybrid90"] = p["line"] + 0.5 + (s["pts_p90"] - s["pts_p50"]); r["hybrid75"] = p["line"] + 0.5 + (s["pts_p90"] - s["pts_p50"]) * 0.6745 / 1.2816
        rows.append(r)
print("played player-games with a lock-time line:", len(rows), "nights", len({r["date"] for r in rows}))
rng = np.random.default_rng(5)
def pin(q, pred, y): return (y - pred) * q if y >= pred else (pred - y) * (1 - q)
for q in (0.75, 0.9):
    t = str(int(q * 100)); srcs = ["ladder", "recal", "normal", "empir", "statapi", "hybrid"]
    common = [r for r in rows if all(r.get(s + t) is not None for s in srcs)]
    print(f"\n  {int(q*100)}th percentile, rows where every source has a value: n={len(common)} nights={len({r['date'] for r in common})}  (target share above = {1-q:.0%})")
    for band, lo, hi in (("all", 0, 99), ("line 18+", 18, 99)):
        cm = [r for r in common if lo <= r["line"] < hi]
        print(f"   [{band}] n={len(cm)}")
        for s in srcs:
            above = np.mean([r["actual"] > r[s + t] for r in cm]); loss = np.mean([pin(q, r[s + t], r["actual"]) for r in cm])
            byd = defaultdict(list)
            for r in cm: byd[r["date"]].append(pin(q, r[s + t], r["actual"]) - pin(q, r["ladder" + t], r["actual"]))
            dd = list(byd); ss = np.array([sum(byd[x]) for x in dd]); cc = np.array([len(byd[x]) for x in dd])
            bi = rng.integers(0, len(dd), (4000, len(dd))); bs = ss[bi].sum(1) / cc[bi].sum(1)
            width = np.mean([r[s + t] - r["line"] for r in cm])
            print(f"     {s:8s} above {above:6.1%}  pinball {loss:.3f}  vs ladder {ss.sum()/cc.sum():+.3f} [{np.percentile(bs,2.5):+.3f}, {np.percentile(bs,97.5):+.3f}]  mean (pctile - line) {width:+.1f}")
