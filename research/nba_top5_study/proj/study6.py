"""Study 6: how to spread five lineups across the top-20 lines. Field-free."""
import sys as _s, pathlib as _p; _s.path.insert(0, str(_p.Path(__file__).resolve().parents[1])); import _paths
import sys, itertools, json
from collections import defaultdict
from pathlib import Path
import numpy as np
TOP5 = _paths.DATA_ROOT
import top5_harness as H
import top5_bed as B
COMB = np.array(list(itertools.combinations(range(20), 3)))
def capped(order, cap, share):
    cnt = defaultdict(int); out = []
    for j in order:
        t = COMB[j]
        if any(cnt[int(x)] >= cap for x in t): continue
        if any(len(set(t) & set(COMB[k])) > share for k in out): continue
        out.append(int(j))
        for x in t: cnt[int(x)] += 1
        if len(out) == 5: break
    return out
IDX = {tuple(c): i for i, c in enumerate(map(tuple, COMB))}
def fixed(trios): return [IDX[tuple(sorted(x - 1 for x in t))] for t in trios]
STRUCT = {
    "#1,#2 + each of #3-#7": fixed([(1, 2, 3), (1, 2, 4), (1, 2, 5), (1, 2, 6), (1, 2, 7)]),
    "#1 + five pairs from #2-#6": fixed([(1, 2, 3), (1, 4, 5), (1, 2, 6), (1, 3, 4), (1, 5, 6)]),
    "#1 in three, two without": fixed([(1, 2, 3), (1, 4, 5), (1, 6, 7), (2, 4, 6), (3, 5, 7)]),
    "five separate trios (#1-#15)": fixed([(1, 6, 11), (2, 7, 12), (3, 8, 13), (4, 9, 14), (5, 10, 15)]),
    "top-heavy separate (#1-#15)": fixed([(1, 2, 3), (4, 5, 6), (7, 8, 9), (10, 11, 12), (13, 14, 15)]),
}
def night_rules(n, ni, sims=4000, seeds=2, sitkey=None):
    v = H.pre_lock(n); pl = H.pool(v, 20)
    if len(pl) < 20: return None
    lines = np.array([p["line"] for p in pl]); ls = lines[COMB].sum(1)
    order = np.argsort(-ls, kind="stable")
    sets = {"five biggest line sums": [list(order[:5])]}
    for cap in (4, 3, 2, 1):
        sets[f"each player in at most {cap} of 5"] = [capped(order, cap, 2)]
    sets["no two lineups share 2 players"] = [capped(order, 5, 1)]
    for k, vv in STRUCT.items(): sets[k] = [vv]
    sit = [p.get(sitkey) or 0.0 for p in pl] if sitkey else None
    for s in range(seeds):
        keys, draws = H.simulate(v, sims, np.random.default_rng(77 * ni + s), 20, sit=sit)
        tot = (draws[:, COMB[:, 0]] + draws[:, COMB[:, 1]] + draws[:, COMB[:, 2]]).astype(np.float32)
        mean = tot.mean(0); top = mean.max()
        for delta in (3, 6, 10, 99):
            ok = mean >= top - delta
            cur = np.zeros(sims, dtype=np.float32); ch = []
            for _ in range(5):
                g = np.maximum(tot, cur[:, None]).mean(0); g[~ok] = -1; g[ch] = -1
                j = int(np.argmax(g)); ch.append(j); cur = np.maximum(cur, tot[:, j])
            sets.setdefault(f"best-of-five search, each lineup within {delta} of the top" if delta < 99 else "best-of-five search, no limit", []).append(ch)
    a = H._actual(n); act = np.array([a[p["key"]] for p in pl]); ta = act[COMB].sum(1); srt = np.sort(ta)[::-1]
    out = {}
    for nm, lists in sets.items():
        ms = []
        for idx in lists:
            t = ta[idx]; best = t.max(); rank = 1 + int((srt > best).sum())
            ms.append(dict(best=best, rank=rank, top1=rank == 1, top10=rank <= 10, top50=rank <= 57, players=len({int(x) for j in idx for x in COMB[j]}),
                           cost=float(ls[order[0]] - np.mean(ls[idx])), worst=float(t.min()), avg=float(t.mean())))
        out[nm] = {k: float(np.mean([m[k] for m in ms])) for k in ms[0]}
    return out
def report(nights, label, sitkey):
    rows = {}
    for ni, n in enumerate(nights):
        r = night_rules(n, ni, sitkey=sitkey)
        if r: rows[n["date"]] = (r, n["gamesCount"])
    dates = list(rows); rng = np.random.default_rng(11); names = list(rows[dates[0]][0])
    print(f"\n[{label}] nights {len(dates)}")
    print(f"  {'rule':52s} {'players':>7s} {'line cost':>9s} {'best of 5':>9s} {'vs baseline [95%]':>26s} {'med rank':>8s} {'held best':>9s} {'top 10':>6s} {'top 5%':>6s} {'avg lineup':>10s}")
    for nm in names:
        g = lambda k: np.array([rows[d][0][nm][k] for d in dates])
        d = g("best") - np.array([rows[x][0]["five biggest line sums"]["best"] for x in dates])
        bs = d[rng.integers(0, len(d), (5000, len(d)))].mean(1)
        print(f"  {nm:52s} {g('players').mean():7.1f} {g('cost').mean():9.1f} {g('best').mean():9.1f} {d.mean():+7.2f} [{np.percentile(bs,2.5):+6.2f},{np.percentile(bs,97.5):+6.2f}] {np.median(g('rank')):8.0f} {g('top1').sum():9.1f} {g('top10').sum():6.1f} {g('top50').sum():6.1f} {g('avg').mean():10.1f}")
    print("  by slate size, best of five minus baseline:")
    for nm in ("each player in at most 3 of 5", "each player in at most 2 of 5", "#1 + five pairs from #2-#6", "#1 in three, two without", "best-of-five search, no limit"):
        parts = []
        for lab, f in (("2-4 games", lambda c: c <= 4), ("5-8", lambda c: 5 <= c <= 8), ("9+", lambda c: c >= 9)):
            x = [rows[d][0][nm]["best"] - rows[d][0]["five biggest line sums"]["best"] for d in dates if f(rows[d][1])]
            if x: parts.append(f"{lab}: {np.mean(x):+.1f} (n={len(x)})")
        print(f"    {nm:40s} " + "   ".join(parts))
    return {d: rows[d][0] for d in dates}
out = {"real": report(H.load_nights(), "REAL 24 nights", "sitProbInjury"), "bed": report(B.bed_nights(), "LAST SEASON 130 nights", None)}
(_paths.DATA_ROOT / "proj" / "study6.json").write_text(json.dumps(out), encoding="utf-8")
