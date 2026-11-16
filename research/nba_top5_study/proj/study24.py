"""Studies 2 and 4: last season (2025-26), teammate absences from the game logs; points split into minutes x points-per-minute."""
import sys as _s, pathlib as _p; _s.path.insert(0, str(_p.Path(__file__).resolve().parents[1])); import _paths
import json
from collections import defaultdict
from pathlib import Path
import numpy as np
TOP5 = _paths.DATA_ROOT
L = [r for r in json.loads((TOP5 / "data" / "player_logs_2025_26.json").read_text(encoding="utf-8"))
     if r["season_type"] == "Regular Season" and r["minutes"] and float(r["minutes"]) > 0]
for r in L: r["d"] = str(r["game_date"])[:10]
A = 0.1
tg = defaultdict(list)
for r in L: tg[(r["team_abbreviation"], r["d"])].append(r)
tdates = defaultdict(list)
for (t, d) in tg: tdates[t].append(d)
for t in tdates: tdates[t].sort()
pgames = defaultdict(list)                       # (team, pid) -> sorted dates played
for r in L: pgames[(r["team_abbreviation"], r["player_id"])].append(r["d"])
for k in pgames: pgames[k].sort()
state = {}                                       # (team,pid) -> ewma pts, min, n   (walk-forward by date)
rows = []
for d in sorted({r["d"] for r in L}):
    todays = [(t, dd) for (t, dd) in tg if dd == d]
    for (t, _) in todays:
        here = {r["player_id"] for r in tg[(t, d)]}
        prev = [x for x in tdates[t] if x < d]
        absn = []
        for (tt, pid), s in state.items():
            if tt != t or pid in here or s["n"] < 10 or s["pts"] < 12: continue
            ds = pgames[(t, pid)]
            if not any(x > d for x in ds): continue            # never plays for this team again: trade or season over, skip
            last_played = max(x for x in ds if x < d)
            missed = sum(1 for x in prev if x > last_played)    # team games missed before tonight
            absn.append(dict(ppg=s["pts"], mpg=s["min"], missed=missed))
        for r in tg[(t, d)]:
            s = state.get((t, r["player_id"]))
            if not s or s["n"] < 5 or s["pts"] < 5: continue
            rows.append(dict(d=d, line=s["pts"], mpg=s["min"], pts=float(r["points"]), min=float(r["minutes"]), absn=absn,
                             fga=float(r["fga"] or 0)))
    for (t, _) in todays:
        for r in tg[(t, d)]:
            s = state.setdefault((t, r["player_id"]), dict(pts=None, min=None, n=0))
            p, m = float(r["points"]), float(r["minutes"])
            s["pts"] = p if s["pts"] is None else A * p + (1 - A) * s["pts"]
            s["min"] = m if s["min"] is None else A * m + (1 - A) * s["min"]
            s["n"] += 1
print("player-games:", len(rows), "nights:", len({r["d"] for r in rows}))
rng = np.random.default_rng(3)
def grp(r, thr=15):
    a = [x for x in r["absn"] if x["ppg"] >= thr]
    if not a: return "none"
    return "first" if min(x["missed"] for x in a) == 0 else "ongoing"
def contrast(sel_a, sel_b, f, label):
    dates = sorted({r["d"] for r in rows})
    ix = {d: i for i, d in enumerate(dates)}
    sa = np.zeros(len(dates)); ca = np.zeros(len(dates)); sb = np.zeros(len(dates)); cb = np.zeros(len(dates))
    for r in rows:
        v = f(r)
        if v is None: continue
        if sel_a(r): sa[ix[r["d"]]] += v; ca[ix[r["d"]]] += 1
        elif sel_b(r): sb[ix[r["d"]]] += v; cb[ix[r["d"]]] += 1
    pt = sa.sum() / ca.sum() - sb.sum() / cb.sum()
    b = rng.integers(0, len(dates), (3000, len(dates)))
    bs = sa[b].sum(1) / np.maximum(ca[b].sum(1), 1) - sb[b].sum(1) / cb[b].sum(1)
    print(f"   {label:58s} n={int(ca.sum()):5d}  {pt:+.2f} [{np.percentile(bs,2.5):+.2f}, {np.percentile(bs,97.5):+.2f}]")
    return pt
ppm = lambda r: r["pts"] / r["min"] - r["line"] / r["mpg"] if r["min"] >= 5 and r["mpg"] >= 5 else None
print("\n=== STUDY 2: teammate out, remaining players vs their own recent average (exposed minus players with no such teammate out) ===")
for thr in (15, 20):
    for g in ("first", "ongoing"):
        print(f"  {thr}+ ppg teammate out, {'FIRST game missed' if g == 'first' else 'second or later game missed'}")
        for band, lo, hi in (("all", 5, 99), ("avg under 12", 5, 12), ("avg 12-18", 12, 18), ("avg 18+", 18, 99), ("avg 22+", 22, 99)):
            a = lambda r, g=g, lo=lo, hi=hi, thr=thr: grp(r, thr) == g and lo <= r["line"] < hi
            b = lambda r, lo=lo, hi=hi, thr=thr: grp(r, thr) == "none" and lo <= r["line"] < hi
            contrast(a, b, lambda r: r["pts"] - r["line"], f"{band}: points vs own average")
            if band in ("all", "avg 18+"):
                dm = contrast(a, b, lambda r: r["min"] - r["mpg"], f"{band}: minutes vs own average")
                dp = contrast(a, b, ppm, f"{band}: points per minute vs own")
                contrast(a, b, lambda r: r["fga"], f"{band}: shots (raw, exposed minus control)")
print("\n=== STUDY 4: how much of the points change is minutes? points = minutes x points-per-minute ===")
base = [r for r in rows if r["min"] >= 5 and r["mpg"] >= 5]
dm = np.array([r["min"] - r["mpg"] for r in base]); dp = np.array([r["pts"] - r["line"] for r in base]); rate = np.array([r["line"] / r["mpg"] for r in base])
part_min = rate * dm
print(f"   variance of (points - average): {dp.var():.1f};  explained by minutes x usual rate: {1 - (dp - part_min).var() / dp.var():.1%};  corr {np.corrcoef(dp, part_min)[0,1]:.2f}")
print(f"   SD of points miss {dp.std():.2f};  SD of minutes miss {dm.std():.2f};  SD of points miss if minutes were known {(dp - part_min).std():.2f}")
for band, lo, hi in (("avg under 12", 5, 12), ("avg 12-18", 12, 18), ("avg 18+", 18, 99)):
    m = np.array([lo <= r["line"] < hi for r in base])
    print(f"   {band:14s} n={m.sum():5d} SD points miss {dp[m].std():.2f}, SD minutes miss {dm[m].std():.2f}, share from minutes {1 - (dp[m] - part_min[m]).var() / dp[m].var():.1%}, points/min {rate[m].mean():.3f}")
# walk-forward rule: fit the first-game bump per band on the first half of the season, apply on the second half
print("\n=== rule check: add the first-half bump to second-half projections (15+ teammate, first or later game) ===")
dates = sorted({r["d"] for r in rows}); cut = dates[len(dates) // 2]
bands = ((5, 12), (12, 18), (18, 99))
for g in ("first", "ongoing"):
    bump = {}
    for lo, hi in bands:
        tr_e = [r["pts"] - r["line"] for r in rows if r["d"] < cut and grp(r) == g and lo <= r["line"] < hi]
        tr_c = [r["pts"] - r["line"] for r in rows if r["d"] < cut and grp(r) == "none" and lo <= r["line"] < hi]
        bump[(lo, hi)] = (np.mean(tr_e) - np.mean(tr_c), len(tr_e))
    te = [r for r in rows if r["d"] >= cut and grp(r) == g]
    byd = defaultdict(list)
    for r in te:
        b = next(v for (lo, hi), v in bump.items() if lo <= r["line"] < hi)[0]
        byd[r["d"]].append(abs(r["pts"] - r["line"] - b) - abs(r["pts"] - r["line"]))
    dd = list(byd); s = np.array([sum(byd[x]) for x in dd]); c = np.array([len(byd[x]) for x in dd])
    bi = rng.integers(0, len(dd), (3000, len(dd))); bs = s[bi].sum(1) / c[bi].sum(1)
    print(f"   {g:8s} bumps fitted {[(lo, round(v[0], 2), v[1]) for (lo, hi), v in bump.items()]}  second-half n={int(c.sum())}  MAE change {s.sum()/c.sum():+.3f} [{np.percentile(bs,2.5):+.3f}, {np.percentile(bs,97.5):+.3f}]")
print("\n=== rule check, squared error and medians ===")
for g in ("none", "first", "ongoing"):
    for half, sel in (("first half", lambda r: r["d"] < cut), ("second half", lambda r: r["d"] >= cut)):
        x = np.array([r["pts"] - r["line"] for r in rows if grp(r) == g and sel(r)])
        print(f"   {g:8s} {half:11s} n={len(x):5d} mean {x.mean():+.2f} median {np.median(x):+.2f}")
for g in ("first", "ongoing"):
    c1 = np.mean([r["pts"] - r["line"] for r in rows if r["d"] < cut and grp(r) == "none"])
    b = np.mean([r["pts"] - r["line"] for r in rows if r["d"] < cut and grp(r) == g]) - c1
    te = [r for r in rows if r["d"] >= cut and grp(r) == g]
    byd = defaultdict(list)
    for r in te: byd[r["d"]].append((r["pts"] - r["line"] - b) ** 2 - (r["pts"] - r["line"]) ** 2)
    dd = list(byd); s = np.array([sum(byd[x]) for x in dd]); c = np.array([len(byd[x]) for x in dd])
    bi = rng.integers(0, len(dd), (3000, len(dd))); bs = s[bi].sum(1) / c[bi].sum(1)
    print(f"   {g:8s} single bump {b:+.2f}: second-half squared-error change {s.sum()/c.sum():+.2f} [{np.percentile(bs,2.5):+.2f}, {np.percentile(bs,97.5):+.2f}]")
# upper tail: share reaching own average + 10
for g in ("none", "first", "ongoing"):
    x = [r for r in rows if grp(r) == g and r["line"] >= 18]
    print(f"   avg 18+, {g:8s} n={len(x):5d}  reached average+10: {np.mean([r['pts'] >= r['line'] + 10 for r in x]):.1%}   30+ points: {np.mean([r['pts'] >= 30 for r in x]):.1%}")
