"""Steps 5 and 6. Local files only (table.json, box2.json, injuries.json)."""
import sys as _s, pathlib as _p; _s.path.insert(0, str(_p.Path(__file__).resolve().parents[1])); import _paths
import sys, json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
from model.nba_points_ladder import player_key, team_key, PLAYER_ALIASES
P = _paths.DATA_ROOT / "proj"
T = json.loads((P / "table.json").read_text(encoding="utf-8"))
BOX = json.loads((P / "box2.json").read_text(encoding="utf-8"))
INJ = json.loads((P / "injuries.json").read_text(encoding="utf-8"))
def tk(n):
    k = team_key(n or ""); return "losangelesclippers" if k == "laclippers" else k
def pk(n):
    k = player_key(n); return PLAYER_ALIASES.get(k, k)
rng = np.random.default_rng(1)
def ci(by_night):
    nights = [n for n in by_night if by_night[n]]
    if len(nights) < 2: return (float("nan"),) * 3
    s = np.array([sum(by_night[n]) for n in nights]); c = np.array([len(by_night[n]) for n in nights])
    idx = rng.integers(0, len(nights), size=(4000, len(nights)))
    m = s[idx].sum(1) / c[idx].sum(1)
    return s.sum() / c.sum(), np.percentile(m, 2.5), np.percentile(m, 97.5)
def show(name, rows, f):
    d = defaultdict(list)
    for r in rows:
        v = f(r)
        if v is not None: d[r["date"]].append(v)
    n = sum(len(v) for v in d.values()); m, lo, hi = ci(d)
    print(f"   {name:62s} n={n:5d} nights={len(d):2d}  mean {m:+.2f} [{lo:+.2f}, {hi:+.2f}]")

# ---- team-game structure from box scores
for b in BOX:
    b["d"] = b["game_date"][:10]; b["pk"] = pk(b["player"]); b["tkey"] = tk(b["team"])
BOX = [b for b in BOX if b["completed"]]
tg = defaultdict(list)                       # (team, date) -> rows
for b in BOX: tg[(b["tkey"], b["d"])].append(b)
team_dates = defaultdict(list)
for (t, d) in tg: team_dates[t].append(d)
for t in team_dates: team_dates[t].sort()
hist = defaultdict(list)                     # (team, player) -> [(date, played, pts, min)]
for (t, d), rows in tg.items():
    for b in rows: hist[(t, b["pk"])].append((d, bool(b["played"]), float(b["points"] or 0), float(b["minutes"] or 0)))
for k in hist: hist[k].sort()
def prior(t, p, d):
    g = [h for h in hist.get((t, p), []) if h[0] < d and h[1]]
    if len(g) < 3: return None
    return dict(ppg=np.mean([h[2] for h in g]), mpg=np.mean([h[3] for h in g]), n=len(g))
def played_prev(t, p, d):
    ds = [x for x in team_dates[t] if x < d]
    if not ds: return None
    h = [x for x in hist.get((t, p), []) if x[0] == ds[-1]]
    return bool(h and h[0][1])
import os
THR = float(os.environ.get('THR', 15))
absent = {}                                  # (team, date) -> list of material absences
for (t, d), rows in tg.items():
    out = []
    for b in rows:
        if b["played"]: continue
        pr = prior(t, b["pk"], d)
        if pr and pr["ppg"] >= THR:
            out.append(dict(p=b["pk"], ppg=pr["ppg"], new=played_prev(t, b["pk"], d)))
    absent[(t, d)] = out
print("team-games", len(tg), " with a 15+ ppg teammate out:", sum(1 for v in absent.values() if v),
      " of which first game missed:", sum(1 for v in absent.values() if any(a["new"] for a in v)))

# ---- line rows: first reading, lock reading, last reading per player-game
for r in T:
    r["tip_dt"] = datetime.fromisoformat(r["tip"]); r["cap_dt"] = datetime.fromisoformat(r["cap"])
first_tip = {}
for r in T: first_tip[r["date"]] = min(first_tip.get(r["date"], r["tip_dt"]), r["tip_dt"])
by = defaultdict(list)
for r in T: by[(r["event"], r["player"])].append(r)
bidx = defaultdict(list)
for b in BOX: bidx[b["pk"]].append(b)
PG = []
for (ev, pl), v in by.items():
    v.sort(key=lambda r: r["cap_dt"])
    last = v[-1]; d = last["date"]
    cand = [b for b in bidx.get(pl, []) if abs((datetime.fromisoformat(b["d"]) - datetime.fromisoformat(d)).days) <= 1]
    ex = [b for b in cand if b["d"] == d]
    cand = ex if ex else cand
    if len(cand) != 1: continue
    b = cand[0]; t = b["tkey"]; gd = b["d"]
    lock = [r for r in v if (first_tip[d] - r["cap_dt"]).total_seconds() >= 60]
    pr = prior(t, pl, gd)
    PG.append(dict(date=d, gd=gd, team=t, player=pl, played=last["played"], actual=last["actual"], minutes=last["minutes"],
                   first=v[0]["line"], last=last["line"], lock=lock[-1]["line"] if lock else None, first_cap=v[0]["cap_dt"], last_cap=last["cap_dt"],
                   tip=last["tip_dt"], ncap=len(v), ppg=pr["ppg"] if pr else None, mpg=pr["mpg"] if pr else None, absent=absent.get((t, gd), []),
                   nbooks=last["nbooks"], ceiling=last["ceiling"]))
print("player-games with lines matched to a team:", len(PG))

print("\n=== STEP 5: a 15+ ppg teammate sits; what do the remaining players do against their line? (played only) ===")
pl = [r for r in PG if r["played"]]
grp = {"no 15+ teammate out": [r for r in pl if not r["absent"]],
       "15+ teammate out, already missed last game": [r for r in pl if r["absent"] and not any(a["new"] for a in r["absent"])],
       "15+ teammate out, FIRST game missed": [r for r in pl if any(a["new"] for a in r["absent"])]}
for nm, rows in grp.items():
    print(" ", nm)
    show("actual - first line of the day", rows, lambda r: r["actual"] - r["first"])
    show("actual - last line before tip", rows, lambda r: r["actual"] - r["last"])
    show("line move, first to last", rows, lambda r: r["last"] - r["first"])
    show("minutes - own season minutes", rows, lambda r: (r["minutes"] - r["mpg"]) if r["mpg"] else None)
    top = [r for r in rows if r["last"] >= 18]
    show("  18+ lines only: actual - last line", top, lambda r: r["actual"] - r["last"])
    low = [r for r in rows if r["last"] < 12]
    show("  under-12 lines only: actual - last line", low, lambda r: r["actual"] - r["last"])
    if rows:
        ab = np.mean([r["actual"] >= r["ceiling"] for r in rows if r["ceiling"]]) if any(r["ceiling"] for r in rows) else float("nan")
        print(f"   share at or above the books' ceiling: {ab:.1%} (n={sum(1 for r in rows if r['ceiling'])})")
# biggest absences
big = [r for r in pl if any(a["new"] and a["ppg"] >= 20 for a in r["absent"])]
print("  FIRST game missed by a 20+ ppg teammate")
show("actual - first line", big, lambda r: r["actual"] - r["first"]); show("actual - last line", big, lambda r: r["actual"] - r["last"])
show("line move, first to last", big, lambda r: r["last"] - r["first"])
# paired vs control: difference of group means with night bootstrap
def diff(a_rows, b_rows, f):
    nights = sorted({r["date"] for r in a_rows} | {r["date"] for r in b_rows})
    A = {n: [f(r) for r in a_rows if r["date"] == n] for n in nights}; B = {n: [f(r) for r in b_rows if r["date"] == n] for n in nights}
    def m(ix):
        sa = sum(sum(A[nights[i]]) for i in ix); ca = sum(len(A[nights[i]]) for i in ix)
        sb = sum(sum(B[nights[i]]) for i in ix); cb = sum(len(B[nights[i]]) for i in ix)
        return sa / ca - sb / cb if ca and cb else np.nan
    pt = m(range(len(nights))); bs = [m(rng.integers(0, len(nights), len(nights))) for _ in range(3000)]
    return pt, np.nanpercentile(bs, 2.5), np.nanpercentile(bs, 97.5)
for nm in list(grp)[1:]:
    for lab, f in (("first line", lambda r: r["actual"] - r["first"]), ("last line", lambda r: r["actual"] - r["last"])):
        pt, lo, hi = diff(grp[nm], grp["no 15+ teammate out"], f)
        print(f"   [{nm}] minus control, actual - {lab}: {pt:+.2f} [{lo:+.2f}, {hi:+.2f}]")

# ---- injury timing (Nov 2+): was the absence known when the first line was read?
obs = []
for o in INJ:
    st = o["states"] if isinstance(o["states"], list) else json.loads(o["states"])
    obs.append((datetime.fromisoformat(o["observed_at"]), {(s.get("game_date"), s.get("player_key")): s.get("status") for s in st if s.get("player_key")}))
obs.sort(key=lambda x: x[0])
def status_at(gd, p, when):
    cur = "no report"
    for t, m in obs:
        if t > when: break
        if any(k[0] == gd for k in m): cur = m.get((gd, PLAYER_ALIASES.get(p, p)), m.get((gd, p), "not listed"))
    return cur
print("\n  timing, nights with injury reports (Nov 2+): status of the absent 15+ teammate when the remaining player's lines were read")
tim = defaultdict(list)
for r in pl:
    if r["gd"] < "2026-11-02" or not r["absent"]: continue
    a = max(r["absent"], key=lambda a: a["ppg"])
    s1 = status_at(r["gd"], a["p"], r["first_cap"]); s2 = status_at(r["gd"], a["p"], r["last_cap"])
    k1 = "known out" if s1 in ("Out", "Doubtful") else "not yet out"
    tim[(k1, "new" if a["new"] else "ongoing")].append(r)
for k, rows in sorted(tim.items()):
    print("  ", k)
    show("actual - first line", rows, lambda r: r["actual"] - r["first"]); show("actual - last line", rows, lambda r: r["actual"] - r["last"])
    show("line move", rows, lambda r: r["last"] - r["first"])

print("\n=== STEP 6: players listed Questionable (and other tags) on the last report before the lock / before their own tip ===")
q = defaultdict(list)
allq = []
for r in PG:
    if r["gd"] < "2026-11-02": continue
    s_lock = status_at(r["gd"], r["player"], first_tip[r["date"]] - timedelta(minutes=1))
    s_tip = status_at(r["gd"], r["player"], r["tip"] - timedelta(minutes=1))
    r["s_lock"], r["s_tip"] = s_lock, s_tip
    q[s_lock].append(r)
for s, rows in sorted(q.items(), key=lambda kv: -len(kv[1])):
    n = len(rows); pld = [r for r in rows if r["played"]]
    line = np.mean([r["last"] for r in rows])
    e_play = np.mean([r["actual"] for r in pld]) if pld else float("nan")
    e_all = np.mean([r["actual"] if r["played"] else 0 for r in rows])
    res = np.mean([r["actual"] - r["last"] for r in pld]) if pld else float("nan")
    mins = np.mean([r["minutes"] - r["mpg"] for r in pld if r["mpg"]]) if any(r["mpg"] for r in pld) else float("nan")
    print(f"  at lock: {s:12s} n={n:4d} played {len(pld):4d} ({len(pld)/n:5.1%})  mean line {line:5.1f}  when played: actual {e_play:5.1f}, actual-line {res:+.2f}, minutes vs season {mins:+.1f}"
          f"  counting a sit as 0: {e_all:5.1f} ({e_all/line:.0%} of line)")
qq = [r for r in PG if r.get("s_lock") == "Questionable"]
print("\n  Questionable at the lock, each player (line, books, status just before own tip, minutes, points):")
for r in sorted(qq, key=lambda r: (r["date"], -r["last"])):
    print(f"    {r['date']} {r['player']:24s} line {r['last']:5.1f} first {r['first']:5.1f} books {r['nbooks']} tip-status {r['s_tip']:12s} lead {(r['tip']-first_tip[r['date']]).total_seconds()/3600:4.1f}h  "
          + (f"played {r['minutes']:.0f} min, {r['actual']:.0f} pts" if r["played"] else "SAT"))
for lab, f in (("4+ books", lambda r: r["nbooks"] >= 4), ("under 4 books", lambda r: r["nbooks"] < 4),
               ("own tip 1h+ after lock", lambda r: (r["tip"] - first_tip[r["date"]]).total_seconds() >= 3600),
               ("own tip at the lock", lambda r: (r["tip"] - first_tip[r["date"]]).total_seconds() < 3600)):
    x = [r for r in qq if f(r)]
    if x: print(f"    {lab:24s} played {sum(r['played'] for r in x)} of {len(x)}")
# upgraded/downgraded between lock and own tip
print("  Questionable at lock -> status just before own tip:")
c = defaultdict(lambda: [0, 0])
for r in qq: c[r["s_tip"]][0] += 1; c[r["s_tip"]][1] += r["played"]
for k, (n, p) in c.items(): print(f"    {k:12s} n={n} played={p}")

print("\n=== extra: intervals for step 6 (played only, night-clustered) ===")
for lab, f in (("Questionable at lock", lambda r: r.get("s_lock") == "Questionable"), ("Available at lock (listed, cleared)", lambda r: r.get("s_lock") == "Available"),
               ("Questionable or Available", lambda r: r.get("s_lock") in ("Questionable", "Available")), ("Probable", lambda r: r.get("s_lock") == "Probable"),
               ("not listed", lambda r: r.get("s_lock") == "not listed")):
    x = [r for r in PG if f(r) and r["played"]]
    show(lab + ": actual - last line", x, lambda r: r["actual"] - r["last"])
    show(lab + ": minutes - season minutes", x, lambda r: (r["minutes"] - r["mpg"]) if r["mpg"] else None)
    show(lab + ": 18+ lines, actual - last line", [r for r in x if r["last"] >= 18], lambda r: r["actual"] - r["last"])
# power: SD of actual - line, and nights needed
res = np.array([r["actual"] - r["last"] for r in PG if r["played"]])
print("SD of actual - line:", res.std().round(2), " by band:", {b: round(float(np.std([r['actual']-r['last'] for r in PG if r['played'] and lo <= r['last'] < hi])), 2) for b, lo, hi in (("<12", 0, 12), ("12-18", 12, 18), ("18-25", 18, 25), ("25+", 25, 99))})
# intra-night correlation of residuals -> design effect
by_n = defaultdict(list)
for r in PG:
    if r["played"]: by_n[r["date"]].append(r["actual"] - r["last"])
m = np.mean(res); k = np.mean([len(v) for v in by_n.values()])
msb = np.mean([len(v) * (np.mean(v) - m) ** 2 for v in by_n.values()]) * len(by_n) / (len(by_n) - 1)
msw = np.mean(np.concatenate([np.array(v) - np.mean(v) for v in by_n.values()]) ** 2)
icc = (msb - msw) / (msb + (k - 1) * msw)
print(f"nights {len(by_n)}, mean rows/night {k:.0f}, ICC {icc:.4f}, design effect {1 + (k - 1) * icc:.2f}")
