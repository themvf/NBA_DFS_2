"""Study 1: which statistic of the simulated trio total should rank lineups? Field-free scoring."""
import sys as _s, pathlib as _p; _s.path.insert(0, str(_p.Path(__file__).resolve().parents[1])); import _paths
import sys, itertools, json
from pathlib import Path
import numpy as np
TOP5 = _paths.DATA_ROOT
import top5_harness as H
import top5_bed as B
SIMS = 6000
COMB = np.array(list(itertools.combinations(range(20), 3)))
def pick_sets(draws):
    k = draws.shape[1]
    comb = COMB if k == 20 else np.array(list(itertools.combinations(range(k), 3)))
    tot = (draws[:, comb[:, 0]] + draws[:, comb[:, 1]] + draws[:, comb[:, 2]]).astype(np.float32)   # sims x trios
    out = {}
    out["mean"] = np.argsort(-tot.mean(0), kind="stable")[:5]
    for q in (60, 70, 75, 80, 90, 95):
        out[f"q{q}"] = np.argsort(-np.quantile(tot, q / 100, axis=0), kind="stable")[:5]
    mx = tot.max(1)
    out["p_best"] = np.argsort(-(tot >= mx[:, None]).mean(0), kind="stable")[:5]       # chance to be the pool's best trio
    for lab, t in (("p_top1pct", np.quantile(tot, 0.99, axis=1)),):
        out[lab] = np.argsort(-(tot >= t[:, None]).mean(0), kind="stable")[:5]
    # greedy: five trios that maximise the expected best-of-five total
    cur = np.zeros(tot.shape[0], dtype=np.float32); chosen = []
    for _ in range(5):
        gain = np.maximum(tot, cur[:, None]).mean(0); gain[chosen] = -1
        j = int(np.argmax(gain)); chosen.append(j); cur = np.maximum(cur, tot[:, j])
    out["greedy_emax"] = np.array(chosen)
    # greedy: maximise the chance that one of the five is the pool's best
    hit = tot >= mx[:, None]; cov = np.zeros(tot.shape[0], dtype=bool); chosen = []
    for _ in range(5):
        gain = (hit & ~cov[:, None]).mean(0); gain[chosen] = -1
        j = int(np.argmax(gain)); chosen.append(j); cov |= hit[:, j]
    out["greedy_cover"] = np.array(chosen)
    return comb, out
def run(nights, hist_fn, label, recal, sitkey):
    rows = {}
    for ni, n in enumerate(nights):
        v = H.pre_lock(n); pl = H.pool(v, 20)
        if len(pl) < 20: continue
        g = None
        if recal:
            prior = [m for m in nights if m["date"] < n["date"]]
            g = H.fit_recal(prior) if len(prior) >= 5 else None
        sit = [p.get(sitkey) or 0.0 for p in pl] if sitkey else None
        per = {}
        for seed in range(3):
            rng = np.random.default_rng(1000 * ni + seed)
            keys, draws = H.simulate(v, SIMS, rng, 20, recal_g=g, sit=sit)
            comb, sets = pick_sets(draws)
            a = H._actual(n); act = np.array([a[k] for k in keys])
            trio_act = act[comb].sum(1); order = np.sort(trio_act)[::-1]
            for name, idx in sets.items():
                t = trio_act[idx]
                best = t.max(); rank = 1 + int((order > best).sum())
                per.setdefault(name, []).append(dict(best=best, first=float(t[0]), rank=rank, top1=rank == 1, top10=rank <= 10,
                                                     gap=order[0] - best, distinct=len({int(x) for i in idx for x in comb[i]})))
        rows[n["date"]] = {k: {m: float(np.mean([x[m] for x in v_])) for m in v_[0]} for k, v_ in per.items()}
    names = list(next(iter(rows.values())))
    rng = np.random.default_rng(7); dates = list(rows)
    print(f"\n[{label}] nights {len(dates)}  (each cell = mean over nights, 3 simulation seeds averaged)")
    print(f"  {'rank by':14s} {'best of 5':>9s} {'vs mean [95%]':>24s} {'first':>6s} {'rank':>6s} {'med rank':>8s} {'held best':>9s} {'top10':>6s} {'gap':>5s} {'players':>7s}")
    res = {}
    for nm in names:
        d = np.array([rows[x][nm]["best"] - rows[x]["mean"]["best"] for x in dates])
        bs = d[rng.integers(0, len(d), (4000, len(d)))].mean(1)
        r = {m: float(np.mean([rows[x][nm][m] for x in dates])) for m in ("best", "first", "rank", "top1", "top10", "gap", "distinct")}
        r["med"] = float(np.median([rows[x][nm]["rank"] for x in dates])); r["d"] = (float(d.mean()), float(np.percentile(bs, 2.5)), float(np.percentile(bs, 97.5)))
        res[nm] = r
        print(f"  {nm:14s} {r['best']:9.1f} {r['d'][0]:+7.2f} [{r['d'][1]:+6.2f},{r['d'][2]:+6.2f}] {r['first']:6.1f} {r['rank']:6.1f} {r['med']:8.0f} {r['top1']*len(dates):9.1f} {r['top10']*len(dates):6.1f} {r['gap']:5.1f} {r['distinct']:7.1f}")
    return res
out = {}
real = H.load_nights()
out["real_raw"] = run(real, None, "REAL nights, books' curves as given", False, "sitProbInjury")
out["real_recal"] = run(real, None, "REAL nights, top end corrected walk-forward", True, "sitProbInjury")
bed = B.bed_nights()
out["bed"] = run(bed, None, "LAST SEASON bed (pseudo-lines)", False, None)
(_paths.DATA_ROOT / "proj" / "study1.json").write_text(json.dumps(out), encoding="utf-8")
