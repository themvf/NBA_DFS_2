"""Quick review checks on the study's scoring setup. Read-only on the study data."""
import sys as _s, pathlib as _p; _s.path.insert(0, str(_p.Path(__file__).resolve().parents[1])); import _paths
import itertools, json, math, sys
from collections import Counter, defaultdict
from pathlib import Path
import numpy as np

TOP5 = _paths.DATA_ROOT
import top5_harness as H
import top5_bed as B

real = H.load_nights()
bed = B.bed_nights()
print("real nights", len(real), "bed nights", len(bed))

def bucket(g):
    return "2-4" if g <= 4 else ("5-8" if g <= 8 else "9+")

# 1. slate size mix and baseline results by slate size (field-free, cheap)
for name, nights in (("real", real), ("bed", bed)):
    rows = defaultdict(list)
    for n in nights:
        v = H.pre_lock(n)
        ent = H.line_sum_selector(v)
        tot = [H.trio_total(n, t) for t in ent]
        pt = H.pool_trio_totals(n, 20)
        best = max(tot)
        rank = 1 + int((pt > best).sum())
        p20 = H.pool(n, 20)
        a = H._actual(n)
        keys = [p["key"] for p in p20]
        bt = max(itertools.combinations(keys, 3), key=lambda t: sum(a[k] for k in t))
        top1key = keys[0]
        # quoted players overall and how deep the pool's 20th line is
        rows[bucket(n["gamesCount"])].append(dict(rank=rank, gap=pt[0] - best, top1=rank == 1, top10=rank <= 10,
                                                   n1_in_best=top1key in bt, line20=p20[-1]["line"] if len(p20) >= 20 else None,
                                                   line1=p20[0]["line"], best_from_top3=sum(1 for k in bt if k in keys[:3]),
                                                   best_minrank=max(keys.index(k) + 1 for k in bt)))
    print(f"\n[{name}] baseline (five biggest line sums) by number of games")
    for b in ("2-4", "5-8", "9+"):
        r = rows.get(b, [])
        if not r:
            continue
        print(f"  games {b:>4}: nights {len(r):3d}  mean rank {np.mean([x['rank'] for x in r]):6.1f}  median rank {np.median([x['rank'] for x in r]):5.0f}"
              f"  gap {np.mean([x['gap'] for x in r]):5.1f}  holds best trio {sum(x['top1'] for x in r)}/{len(r)}  top10 {sum(x['top10'] for x in r)}/{len(r)}"
              f"  #1 line in best trio {sum(x['n1_in_best'] for x in r)}/{len(r)}  20th line {np.mean([x['line20'] for x in r if x['line20']]):.1f}"
              f"  deepest rank in best trio (mean) {np.mean([x['best_minrank'] for x in r]):.1f}")

# 2. ruled-out players inside the top-20 on real nights (the harness field can still pick them)
print("\n[real] players listed Out/Doubtful/Questionable at the lock inside the top-20 lines")
for n in real:
    flagged = [(p["lineRank"], p["name"], p["injuryStatus"], p["actual"], p.get("played")) for p in H.pool(n, 20)
               if p.get("injuryStatus") and str(p["injuryStatus"]).lower() in ("out", "doubtful", "questionable")]
    sat = [(p["lineRank"], p["name"], p.get("injuryStatus"), p["line"]) for p in H.pool(n, 20) if p.get("played") is False]
    if flagged or sat:
        print(" ", n["date"], "flagged:", flagged, "| sat:", sat)

# 3. bias of the line by line rank: actual minus line (played only on real; bed has only played)
print("\nmean(actual - line) by line rank (real: played only)")
for name, nights in (("real", real), ("bed", bed)):
    by = defaultdict(list)
    for n in nights:
        for p in H.pool(n, 20):
            if p.get("actual") is None:
                continue
            if name == "real" and not p.get("played"):
                continue
            r = p["lineRank"]
            g = "1" if r == 1 else "2" if r == 2 else "3" if r == 3 else "4-6" if r <= 6 else "7-12" if r <= 12 else "13-20"
            by[g].append(p["actual"] - p["line"])
    print(f"  [{name}] " + "  ".join(f"#{g}: {np.mean(v):+5.2f} (n={len(v)}, se {np.std(v)/math.sqrt(len(v)):.2f})" for g, v in by.items()))

# 4. how noisy is the realized prize metric? per-night share rows from an approach's saved baseline rows
an = json.loads((TOP5 / "approaches" / "anatomy" / "final.json").read_text(encoding="utf-8"))
print("\nper-night realized prize share of the BASELINE (five biggest line sums)")
for key in [k for k in an if k.endswith("baseline_rows")]:
    rows = an[key]
    for s in ("share_pileon", "share_v1", "share_spread"):
        x = np.array([r[s] for r in rows])
        nz = int((x > 0.001).sum())
        print(f"  {key:28s} {s:13s} n={len(x):3d} mean {x.mean():.4f} sd {x.std(ddof=1):.4f} se {x.std(ddof=1)/math.sqrt(len(x)):.4f}"
              f"  nights with any share {nz}  top-2 nights carry {np.sort(x)[-2:].sum()/max(x.sum(),1e-9):.0%}")
        # nights needed to detect a doubling / tripling of the mean at ~2 SE with 80% power (paired noise ~ sd*sqrt(2) worst case)
        if x.mean() > 0:
            for mult in (2, 3):
                d = (mult - 1) * x.mean()
                sd_d = x.std(ddof=1) * math.sqrt(1 + mult)  # variance scales roughly with the mean for rare wins
                need = (2.8 * sd_d / d) ** 2
                print(f"      nights to detect a x{mult} improvement (80% power, unpaired rare-win noise): ~{need:,.0f}")

# 5. fair share reference
print("\nfair share for 5 entries among 300 others:", 5 / 305)
