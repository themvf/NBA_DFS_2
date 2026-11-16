"""Study 5: players beyond the top 20 lines. Field-free."""
import sys as _s, pathlib as _p; _s.path.insert(0, str(_p.Path(__file__).resolve().parents[1])); import _paths
import sys, itertools, json
from collections import defaultdict, Counter
from pathlib import Path
import numpy as np
TOP5 = _paths.DATA_ROOT
import top5_harness as H
import top5_bed as B
real = H.load_nights(); bed = B.bed_nights()
BANDS = ((1, 3), (4, 10), (11, 20), (21, 30), (31, 40), (41, 60), (61, 999))
def band(r):
    if r is None: return "no line"
    return next(f"{lo}-{hi}" if hi < 999 else "61+" for lo, hi in BANDS if lo <= r <= hi)
order = [f"{lo}-{hi}" if hi < 999 else "61+" for lo, hi in BANDS] + ["no line"]
print("=== A. Where the night's three top scorers sat in the line ranking ===")
for name, nights in (("real 24 nights", real), ("last season", bed)):
    c = Counter(); deep_nights = 0; gaps = []; games = defaultdict(list)
    for n in nights:
        rank = {p["key"]: p["lineRank"] for p in n["players"]}
        top = sorted(n["slate"], key=lambda s: -s["points"])[:3]
        rs = [rank.get(s["key"]) for s in top]
        for r in rs: c[band(r)] += 1
        out = sum(1 for r in rs if r is None or r > 20); deep_nights += out > 0
        best20 = H.pool_trio_totals(n, 20)[0]; sb = H.slate_best(n)
        gaps.append(sb - best20); games["2-4" if n["gamesCount"] <= 4 else "5-8" if n["gamesCount"] <= 8 else "9+"].append(out > 0)
    tot = sum(c.values())
    print(f" [{name}] nights {len(nights)}; nights with a top-3 scorer outside the top 20 lines: {deep_nights} ({deep_nights/len(nights):.0%})")
    print("   line rank of the top-3 scorers: " + "  ".join(f"{b}: {c[b]} ({c[b]/tot:.0%})" for b in order if c[b]))
    print(f"   best trio on the whole slate minus best trio inside the top 20: mean {np.mean(gaps):.1f}, zero on {sum(g == 0 for g in gaps)} nights, 5+ on {sum(g >= 5 for g in gaps)}")
    print("   by slate size: " + "  ".join(f"{k} games: {sum(v)}/{len(v)}" for k, v in sorted(games.items())))

print("\n=== B. How often a player in each line band is a top-3 scorer, or scores 30+ (per player-night) ===")
for name, nights in (("real 24 nights", real), ("last season", bed)):
    st = defaultdict(lambda: [0, 0, 0, 0.0, 0.0])
    for n in nights:
        thr = sorted((s["points"] for s in n["slate"]), reverse=True)[2]
        for p in n["players"]:
            if p.get("actual") is None: continue
            b = band(p["lineRank"]); s = st[b]
            s[0] += 1; s[1] += p["actual"] >= thr; s[2] += p["actual"] >= 30; s[3] += p["line"]; s[4] += (p.get("p30") or 0)
    print(f" [{name}]")
    for b in order:
        if st[b][0]:
            n_, t3, p30, ln, sp = st[b]
            print(f"   rank {b:6s} player-nights {n_:5d}  mean line {ln/n_:5.1f}  top-3 scorer {t3:3d} ({t3/n_:5.1%})  30+ {p30:3d} ({p30/n_:5.1%})" + (f"  books said 30+: {sp/n_:5.1%}" if name.startswith("real") else ""))

print("\n=== C. Does a deeper pool help a rule that hunts upside? (simulate top-N, pick five, score against the WHOLE slate) ===")
def run(nights, label, sitkey, pools=(20, 30, 40), sims=2500, every=1):
    res = defaultdict(lambda: defaultdict(list))
    for ni, n in enumerate(nights[::every]):
        v = H.pre_lock(n); sb = H.slate_best(n); a = H._actual(n)
        for N in pools:
            pl = H.pool(v, N)
            if len(pl) < N: continue
            sit = [p.get(sitkey) or 0.0 for p in pl] if sitkey else None
            keys, draws = H.simulate(v, sims, np.random.default_rng(50 * ni + N), N, sit=sit)
            comb = np.array(list(itertools.combinations(range(N), 3)))
            tot = (draws[:, comb[:, 0]] + draws[:, comb[:, 1]] + draws[:, comb[:, 2]]).astype(np.float32)
            act = np.array([a[k] for k in keys]); ta = act[comb].sum(1)
            sets = {"average": np.argsort(-tot.mean(0), kind="stable")[:5], "q90": np.argsort(-np.quantile(tot, .9, axis=0), kind="stable")[:5]}
            mx = tot.max(1); sets["chance best"] = np.argsort(-(tot >= mx[:, None]).mean(0), kind="stable")[:5]
            cur = np.zeros(sims, dtype=np.float32); ch = []
            for _ in range(5):
                g = np.maximum(tot, cur[:, None]).mean(0); g[ch] = -1; j = int(np.argmax(g)); ch.append(j); cur = np.maximum(cur, tot[:, j])
            sets["max best-of-5"] = np.array(ch)
            for nm, idx in sets.items():
                res[(nm, N)]["best"].append(ta[idx].max()); res[(nm, N)]["gap"].append(sb - ta[idx].max())
                res[(nm, N)]["deep"].append(sum(1 for i in {int(x) for j in idx for x in comb[j]} if i >= 20))
                res[(nm, N)]["date"].append(n["date"])
    rng = np.random.default_rng(9)
    print(f" [{label}]")
    base = dict(zip(res[("average", 20)]["date"], res[("average", 20)]["best"]))
    for (nm, N), r in sorted(res.items()):
        d = np.array([b - base[dt] for b, dt in zip(r["best"], r["date"]) if dt in base])
        bs = d[rng.integers(0, len(d), (4000, len(d)))].mean(1)
        print(f"   {nm:14s} pool {N}: nights {len(d):3d}  best of five {np.mean(r['best']):6.1f}  vs average/top-20 {d.mean():+.2f} [{np.percentile(bs,2.5):+.2f}, {np.percentile(bs,97.5):+.2f}]  gap to slate best {np.mean(r['gap']):5.1f}  players from beyond 20: {np.mean(r['deep']):.1f}")
run(real, "real 24 nights", "sitProbInjury")
run(bed, "last season, every 2nd night", None, every=2)
