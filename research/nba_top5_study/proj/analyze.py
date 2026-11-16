import sys as _s, pathlib as _p; _s.path.insert(0, str(_p.Path(__file__).resolve().parents[1])); import _paths
import json, math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import numpy as np
from scipy.special import ndtri
P = _paths.DATA_ROOT / "proj"
T = json.loads((P / "table.json").read_text(encoding="utf-8"))
for r in T:
    r["tip_dt"] = datetime.fromisoformat(r["tip"]); r["cap_dt"] = datetime.fromisoformat(r["cap"])
first_tip = {}
for r in T:
    first_tip[r["date"]] = min(first_tip.get(r["date"], r["tip_dt"]), r["tip_dt"])
by = defaultdict(list)
for r in T: by[(r["event"], r["player"])].append(r)
for v in by.values(): v.sort(key=lambda r: r["cap_dt"])
rng = np.random.default_rng(0)
def ci(diffs_by_night):
    """night-clustered bootstrap of the pooled mean difference; input {night: [diffs]}"""
    nights = list(diffs_by_night); sums = np.array([sum(diffs_by_night[n]) for n in nights]); cnt = np.array([len(diffs_by_night[n]) for n in nights])
    idx = rng.integers(0, len(nights), size=(4000, len(nights)))
    m = sums[idx].sum(1) / cnt[idx].sum(1)
    return sums.sum() / cnt.sum(), np.percentile(m, 2.5), np.percentile(m, 97.5)
def cmp(name, rows, fa, fb, loss=abs):
    d = defaultdict(list)
    for r in rows:
        a, b = fa(r), fb(r)
        if a is None or b is None: continue
        d[r["date"]].append(loss(a - r["actual"]) - loss(b - r["actual"]))
    n = sum(len(v) for v in d.values())
    m, lo, hi = ci(d)
    print(f"   {name:58s} n={n:5d}  diff {m:+.3f} [{lo:+.3f}, {hi:+.3f}]")
sq = lambda e: e * e

print("=== STEP 2: line freshness (played player-games; error = |line - actual|) ===")
snap = []
for (ev, pl), v in by.items():
    last = v[-1]
    if not last["played"]: continue
    lock = first_tip[last["date"]]
    atlock = [r for r in v if (lock - r["cap_dt"]).total_seconds() >= 60]
    snap.append(dict(last=last, first=v[0], lock=atlock[-1] if atlock else None, n=len(v), date=last["date"], actual=last["actual"]))
print("player-games played:", len(snap), " with a pre-lock reading:", sum(1 for s in snap if s["lock"]))
for nm, k in (("first reading of the day", "first"), ("reading at the lock", "lock"), ("last reading before own tip", "last")):
    x = [s for s in snap if s[k]]
    e = np.array([abs(s[k]["line"] - s["actual"]) for s in x]); lead = np.median([s[k]["lead_h"] for s in x])
    print(f"   {nm:30s} n={len(x):5d} MAE {e.mean():.3f}  median lead {lead:5.1f} h")
both = [s for s in snap if s["lock"]]
def rowsof(k): return [dict(date=s["date"], actual=s["actual"], a=s[k]["line"], l=s["last"]["line"], f=s["first"]["line"], k=s["lock"]["line"]) for s in both]
R = rowsof("lock")
print(" paired on the same player-games (negative = first-named is better):")
cmp("lock line vs first-of-day line", R, lambda r: r["k"], lambda r: r["f"])
cmp("last-before-tip line vs lock line", R, lambda r: r["l"], lambda r: r["k"])
late = [s for s in both if (s["last"]["tip_dt"] - first_tip[s["date"]]).total_seconds() > 2 * 3600]
R2 = [dict(date=s["date"], actual=s["actual"], l=s["last"]["line"], k=s["lock"]["line"]) for s in late]
cmp("  same, only games tipping 2h+ after the lock", R2, lambda r: r["l"], lambda r: r["k"])
moved = [r for r in R if abs(r["k"] - r["f"]) >= 1.0]
print(f"   line moved 1+ point between first reading and lock: {len(moved)} of {len(R)}; on those:")
cmp("  lock line vs first-of-day line (moved 1+)", moved, lambda r: r["k"], lambda r: r["f"])
mv = np.array([(r["k"] - r["f"], r["actual"] - r["f"]) for r in R if r["k"] != r["f"]])
print(f"   when the line moved, actual-minus-first-line vs the move: slope {np.polyfit(mv[:,0], mv[:,1], 1)[0]:.2f} (1.0 = the move is fully real), n={len(mv)}")
print("   by lead time of the reading used (all readings, played):")
for lo, hi in ((0, 1.5), (1.5, 4), (4, 8), (8, 30)):
    x = [r for r in T if r["played"] and lo <= r["lead_h"] < hi]
    print(f"     {lo:>4}-{hi:<4}h  n={len(x):5d}  MAE {np.mean([abs(r['line']-r['actual']) for r in x]):.3f}")

print("\n=== STEP 3: which books (last reading before tip, played) ===")
L = [s["last"] for s in snap]
books = sorted({b for r in L for b in r["book_lines"]})
print("   per-book MAE on the player-games that book quotes, against the consensus on the SAME rows:")
for b in books:
    x = [r for r in L if b in r["book_lines"]]
    d = defaultdict(list)
    for r in x: d[r["date"]].append(abs(r["book_lines"][b] - r["actual"]) - abs(r["line"] - r["actual"]))
    m, lo, hi = ci(d)
    print(f"     {b:12s} n={len(x):5d}  book MAE {np.mean([abs(r['book_lines'][b]-r['actual']) for r in x]):.3f}  minus consensus {m:+.3f} [{lo:+.3f}, {hi:+.3f}]  agrees with consensus {np.mean([r['book_lines'][b]==r['line'] for r in x]):.0%}")
def mean_line(r): return float(np.mean(list(r["book_lines"].values())))
def med_line(r): return float(np.median(list(r["book_lines"].values())))
def shaded(r):
    """each book's line moved to its own 50/50 point using its over price, then averaged"""
    out = []
    for b, ln in r["book_lines"].items():
        p = r["book_pover"].get(b)
        sd = 3.9 + 0.18 * ln
        out.append(ln + (sd * float(ndtri(min(max(p, 0.02), 0.98))) if p is not None else 0.0))
    return float(np.mean(out))
multi = [r for r in L if r["nbooks"] >= 3]
print(f"   blends, on player-games with 3+ books (n={len(multi)}); negative = better than the consensus line:")
cmp("mean of the books' lines vs consensus", multi, mean_line, lambda r: r["line"])
cmp("median of the books' lines vs consensus", multi, med_line, lambda r: r["line"])
cmp("price-adjusted mean (each line moved by its over price)", multi, shaded, lambda r: r["line"])
print("   by number of books quoting (consensus line MAE):")
for k in (1, 2, 3, 4, 5, 6):
    x = [r for r in L if r["nbooks"] == k]
    if x: print(f"     {k} books n={len(x):5d} MAE {np.mean([abs(r['line']-r['actual']) for r in x]):.3f}")
dis = [r for r in multi if max(r["book_lines"].values()) - min(r["book_lines"].values()) >= 1.0]
print(f"   books disagree by 1+ point on {len(dis)} of {len(multi)}; on those:")
cmp("  mean of lines vs consensus (disagree 1+)", dis, mean_line, lambda r: r["line"])
cmp("  highest book line vs consensus", dis, lambda r: max(r["book_lines"].values()), lambda r: r["line"])
cmp("  lowest book line vs consensus", dis, lambda r: min(r["book_lines"].values()), lambda r: r["line"])

print("\n=== STEP 4: line -> expected points (last reading, played; squared error is the right score for an average) ===")
C = [r for r in L if r["curve_mean"] is not None]
print(f"   player-games with a ladder: {len(C)} of {len(L)}")
def bias(name, f, rows):
    e = np.array([f(r) - r["actual"] for r in rows if f(r) is not None])
    print(f"   {name:44s} bias {e.mean():+.3f}  MAE {np.abs(e).mean():.3f}  RMSE {math.sqrt((e**2).mean()):.3f}")
bias("line", lambda r: r["line"], C)
bias("line + 0.5", lambda r: r["line"] + 0.5, C)
bias("price-adjusted mean of lines", shaded, C)
bias("ladder average (raw)", lambda r: r["curve_mean"], C)
# walk-forward linear recalibration of the ladder average and of the line
dates = sorted({r["date"] for r in C})
def wf(feat):
    out = {}
    for d in dates:
        tr = [r for r in C if r["date"] < d]
        if len({r["date"] for r in tr}) < 5: continue
        X = np.array([[1.0, feat(r)] for r in tr]); y = np.array([r["actual"] for r in tr])
        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        for r in C:
            if r["date"] == d: out[id(r)] = beta[0] + beta[1] * feat(r)
    return out, beta
wl, bl = wf(lambda r: r["line"]); wc, bc = wf(lambda r: r["curve_mean"])
W = [r for r in C if id(r) in wl]
print(f"   walk-forward rows: {len(W)}; last fit: actual = {bl[0]:+.2f} + {bl[1]:.3f} x line ; actual = {bc[0]:+.2f} + {bc[1]:.3f} x ladder average")
bias("line (same rows)", lambda r: r["line"], W)
bias("line, walk-forward straight-line fix", lambda r: wl[id(r)], W)
bias("ladder average (same rows)", lambda r: r["curve_mean"], W)
bias("ladder average, walk-forward straight-line fix", lambda r: wc[id(r)], W)
print("   paired, squared error (negative = first-named is better):")
cmp("ladder average vs line", C, lambda r: r["curve_mean"], lambda r: r["line"], sq)
cmp("line+0.5 vs line", C, lambda r: r["line"] + 0.5, lambda r: r["line"], sq)
cmp("fixed line vs line (walk-forward rows)", W, lambda r: wl[id(r)], lambda r: r["line"], sq)
cmp("fixed ladder average vs line (walk-forward rows)", W, lambda r: wc[id(r)], lambda r: r["line"], sq)
print("   bias of the line by size (actual - line, played):")
for lo, hi in ((0, 10), (10, 15), (15, 20), (20, 25), (25, 60)):
    x = [r for r in L if lo <= r["line"] < hi]; e = np.array([r["actual"] - r["line"] for r in x])
    print(f"     line {lo:>2}-{hi:<2} n={len(x):5d} actual-line {e.mean():+.2f} (se {e.std()/math.sqrt(len(e)):.2f})   ladder avg - line {np.mean([r['curve_mean']-r['line'] for r in x if r['curve_mean'] is not None]):+.2f}")
print("   does it reorder the top? per night, the 20 biggest lines: rank correlation with actual points")
for nm, f in (("line", lambda r: r["line"]), ("ladder average", lambda r: r["curve_mean"] if r["curve_mean"] is not None else r["line"]), ("price-adjusted", shaded)):
    cs, t3 = [], []
    for d in dates:
        x = sorted([r for r in L if r["date"] == d], key=lambda r: -r["line"])[:20]
        if len(x) < 10: continue
        a = np.array([f(r) for r in x]); y = np.array([r["actual"] for r in x])
        ra, ry = a.argsort().argsort(), y.argsort().argsort()
        cs.append(np.corrcoef(ra, ry)[0, 1]); t3.append(y[np.argsort(-a)[:3]].sum())
    print(f"     {nm:16s} mean rank correlation {np.mean(cs):.3f}   actual total of its top three {np.mean(t3):.1f}")
