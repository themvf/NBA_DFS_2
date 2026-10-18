"""Weekly recap of the 2026 CFB season at the closing line (`cfb-weekly-recap-v1`).

DESCRIPTIVE ONLY: what happened each CFBD week, with sample sizes visible. Not a
study pattern, not a betting edge, and nothing here feeds a trigger or a grade.

Same closing lines as the market breakdown (`load_base_rate_games`), regular
season only. Four tables, each row carrying a "Week" so the page can filter:

  recap_weeks         per week: favorites straight up against their implied
                      rate, favorites against the spread, overs, upsets, and
                      the biggest upset. Rates use FBS-vs-FBS games only, so
                      they match the existing weekly favorites table; FBS-vs-FCS
                      games are counted in their own columns, not pooled.
  recap_upsets        every outright moneyline-underdog win, cheapest price
                      first within a week ("biggest upsets by price").
  recap_hooks         games whose spread (favorite's cover margin within 0.5
                      of zero, a whole-number push included) or total (missed
                      by 0.5 or less) was decided by the hook.
  recap_total_misses  per week, the 5 games furthest from the closing total,
                      either direction.

The lists include FBS-vs-FCS games, flagged in a "Matchup" column, because an
FCS team beating an FBS favorite is exactly the kind of result a recap reports.

Favorite definitions: straight up and upsets use the shorter closing moneyline
(a game priced level has no favorite); against the spread uses the spread side
(a pick'em spread has no ATS favorite). Ties in the final are left out of
straight-up records; pushes are left out of ATS and over/under rates.

    python -m model.cfb_weekly_recap [--out-dir artifacts/cfb_pattern_watch/charts]
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path

from model import cfb_pattern_watch as pw

RECAP_VERSION = "cfb-weekly-recap-v1"
HOOK = 0.5
TOP_MISSES = 5


# ── helpers ──────────────────────────────────────────────────────────────────
def _f(v) -> float | None:
    return None if v is None else float(v)


def _american(v) -> int:
    return int(round(float(v)))


def _price(v) -> str:
    a = _american(v)
    return f"+{a}" if a > 0 else str(a)


def _line(v: float) -> str:
    """A spread from one side's point of view: -7.5, +3, PK."""
    if v == 0:
        return "PK"
    s = f"{v:+.1f}"
    return s[:-2] if s.endswith(".0") else s


def _num(v: float) -> str:
    s = f"{v:.1f}"
    return s[:-2] if s.endswith(".0") else s


def _date(g: dict) -> str | None:
    ct = g.get("commence_time")
    return None if ct is None else pw._eastern(ct).date().isoformat()


def _matchup(g: dict) -> str:
    return "FBS vs FBS" if g.get("hclass") == "fbs" and g.get("aclass") == "fbs" else "FBS vs FCS"


def _final(g: dict) -> str:
    return f"{g['aname']} {g['away_score']} at {g['hname']} {g['home_score']}" if not g.get("neutral_site") \
        else f"{g['aname']} {g['away_score']} vs {g['hname']} {g['home_score']} (neutral)"


def _regular(games: list[dict]) -> list[dict]:
    out = []
    for g in games:
        if (g.get("season_type") or "regular") != "regular":
            continue
        if g.get("home_score") is None or g.get("away_score") is None or g.get("week") is None:
            continue
        if "fbs" not in (g.get("hclass"), g.get("aclass")):
            continue
        out.append(g)
    return out


def _ml_favorite(g: dict) -> str | None:
    """'home' or 'away' by the shorter moneyline; None when priced level or missing."""
    h, a = g.get("home_ml"), g.get("away_ml")
    if h is None or a is None:
        return None
    ph, pa = 1 / pw.decimal_odds(h), 1 / pw.decimal_odds(a)
    if ph == pa:
        return None
    return "home" if ph > pa else "away"


def _sort_key(r: dict):
    return (r["Week"], r["Date"] or "")


# ── tables ───────────────────────────────────────────────────────────────────
def upsets(games: list[dict]) -> list[dict]:
    """Every outright moneyline-underdog win, cheapest winner first within each week."""
    rows = []
    for g in _regular(games):
        fav = _ml_favorite(g)
        margin = g["home_score"] - g["away_score"]
        if fav is None or margin == 0:
            continue
        winner = "home" if margin > 0 else "away"
        if winner == fav:
            continue
        p_home = pw.vig_free_home(g["home_ml"], g["away_ml"])
        p_win = p_home if winner == "home" else 1 - p_home
        sp = _f(g.get("home_spread"))
        w_sp = None if sp is None else (sp if winner == "home" else -sp)
        w_name, l_name = (g["hname"], g["aname"]) if winner == "home" else (g["aname"], g["hname"])
        w_pts, l_pts = (g["home_score"], g["away_score"]) if winner == "home" else (g["away_score"], g["home_score"])
        rows.append({
            "Week": int(g["week"]),
            "Date": _date(g),
            "Winner": w_name,
            "Loser": l_name,
            "Winner venue": "neutral" if g.get("neutral_site") else winner,
            "Final": f"{w_pts}-{l_pts}",
            "Winner moneyline": _american(g[f"{winner}_ml"]),
            "Winner price": _price(g[f"{winner}_ml"]),
            "Winner implied (%)": round(p_win * 100, 1),
            "Winner spread": w_sp,
            "Matchup": _matchup(g),
            "matchup_id": g.get("matchup_id"),
        })
    rows.sort(key=lambda r: (r["Week"], r["Winner implied (%)"], r["Date"] or ""))
    return rows


def hook_games(games: list[dict]) -> list[dict]:
    """Games whose spread or total landed within the hook (0.5) of the closing number.

    One row per decided bet, so a game whose spread AND total both came down to
    the hook appears twice ("Bet" says which).
    """
    rows = []
    for g in _regular(games):
        margin = g["home_score"] - g["away_score"]
        base = {"Week": int(g["week"]), "Date": _date(g), "Game": _final(g), "Matchup": _matchup(g),
                "matchup_id": g.get("matchup_id")}
        sp = _f(g.get("home_spread"))
        if sp is not None and sp != 0:
            home_fav = sp < 0
            fav, dog = (g["hname"], g["aname"]) if home_fav else (g["aname"], g["hname"])
            fm = margin if home_fav else -margin
            cover = fm - abs(sp)
            if abs(cover) <= HOOK:
                went = "push" if cover == 0 else (f"{fav} covered" if cover > 0 else f"{dog} covered")
                rows.append({**base, "Bet": "Spread", "Line": f"{fav} {_line(-abs(sp))}",
                             "Result": f"{fav} by {fm}" if fm > 0 else (f"{dog} by {-fm}" if fm < 0 else "tie"),
                             "Miss": round(cover, 1), "Went to": went})
        tot_line = _f(g.get("vegas_total"))
        if tot_line is not None:
            tot = g["home_score"] + g["away_score"]
            miss = tot - tot_line
            if abs(miss) <= HOOK:
                went = "push" if miss == 0 else ("Over" if miss > 0 else "Under")
                rows.append({**base, "Bet": "Total", "Line": f"O/U {_num(tot_line)}", "Result": f"{tot} points",
                             "Miss": round(miss, 1), "Went to": went})
    rows.sort(key=lambda r: (*_sort_key(r), r["Bet"]))
    return rows


def total_misses(games: list[dict], top: int = TOP_MISSES) -> list[dict]:
    """Per week, the `top` games whose final total landed furthest from the closing total."""
    by_week: dict[int, list[dict]] = defaultdict(list)
    for g in _regular(games):
        line = _f(g.get("vegas_total"))
        if line is None:
            continue
        tot = g["home_score"] + g["away_score"]
        miss = tot - line
        by_week[int(g["week"])].append({
            "Week": int(g["week"]), "Date": _date(g), "Game": _final(g),
            "Closing total": line, "Actual total": tot, "Miss": round(miss, 1),
            "Direction": "Over" if miss > 0 else ("Under" if miss < 0 else "Push"),
            "Matchup": _matchup(g), "matchup_id": g.get("matchup_id"),
        })
    rows = []
    for week in sorted(by_week):
        ranked = sorted(by_week[week], key=lambda r: (-abs(r["Miss"]), r["Date"] or "", r["Game"]))[:top]
        for i, r in enumerate(ranked, 1):
            rows.append({**r, "Rank": i})
    return rows


def _pct(n: int, d: int) -> float | None:
    return round(100 * n / d, 1) if d else None


def week_summary(games: list[dict]) -> list[dict]:
    """Per week: favorites SU vs implied, favorites ATS, overs and upsets (rates on FBS-vs-FBS)."""
    up = upsets(games)
    up_by_week: dict[int, list[dict]] = defaultdict(list)
    for r in up:
        up_by_week[r["Week"]].append(r)
    by_week: dict[int, list[dict]] = defaultdict(list)
    for g in _regular(games):
        by_week[int(g["week"])].append(g)
    rows = []
    for week in sorted(by_week):
        fbs = [g for g in by_week[week] if _matchup(g) == "FBS vs FBS"]
        fcs = [g for g in by_week[week] if _matchup(g) == "FBS vs FCS"]
        su_w = su_l = 0
        implied: list[float] = []
        ats_w = ats_l = ats_p = 0
        ov = un = pu = 0
        for g in fbs:
            margin = g["home_score"] - g["away_score"]
            fav = _ml_favorite(g)
            if fav is not None:
                p_home = pw.vig_free_home(g["home_ml"], g["away_ml"])
                implied.append(p_home if fav == "home" else 1 - p_home)
                fm = margin if fav == "home" else -margin
                if fm > 0:
                    su_w += 1
                elif fm < 0:
                    su_l += 1
            sp = _f(g.get("home_spread"))
            if sp is not None and sp != 0:
                fm = margin if sp < 0 else -margin
                c = fm - abs(sp)
                ats_w += c > 0
                ats_l += c < 0
                ats_p += c == 0
            tl = _f(g.get("vegas_total"))
            if tl is not None:
                t = g["home_score"] + g["away_score"]
                ov += t > tl
                un += t < tl
                pu += t == tl
        week_up = up_by_week.get(week, [])
        fbs_up = [r for r in week_up if r["Matchup"] == "FBS vs FBS"]
        big = week_up[0] if week_up else None
        rows.append({
            "Week": week,
            "Games": len(fbs),
            "Fav SU record": f"{su_w}-{su_l}",
            "Fav SU (%)": _pct(su_w, su_w + su_l),
            "Implied (%)": round(100 * sum(implied) / len(implied), 1) if implied else None,
            "Fav ATS record": f"{ats_w}-{ats_l}" + (f"-{ats_p}" if ats_p else ""),
            "Fav ATS (%)": _pct(ats_w, ats_w + ats_l),
            "Overs record": f"{ov}-{un}" + (f"-{pu}" if pu else ""),
            "Overs (%)": _pct(ov, ov + un),
            "Upsets": len(fbs_up),
            "FBS vs FCS games": len(fcs),
            "FCS upsets": len(week_up) - len(fbs_up),
            "Priced games": sum(1 for g in by_week[week]
                                if _ml_favorite(g) is not None and g["home_score"] != g["away_score"]),
            "Biggest upset": None if big is None else
                f"{big['Winner']} ({big['Winner price']}) beat {big['Loser']} {big['Final']}"
                + (" [FCS]" if big["Matchup"] == "FBS vs FCS" else ""),
        })
    return rows


def build_tables(db) -> dict[str, list[dict]]:
    from model import cfb_pattern_measures as measures

    games = measures.load_base_rate_games(db)
    return {
        "recap_weeks": week_summary(games),
        "recap_upsets": upsets(games),
        "recap_hooks": hook_games(games),
        "recap_total_misses": total_misses(games),
    }


# ── charts ───────────────────────────────────────────────────────────────────
def upset_chart(upsets_rows: list[dict], week: int, path: Path, top: int = 10) -> bool:
    """The week's biggest upsets as horizontal bars of the winner's implied win chance."""
    from model.cfb_pattern_charts import INK, INK_2, GRID, SERIES, _figure, _save, _style

    rows = [r for r in upsets_rows if r["Week"] == week][:top]
    if not rows:
        return False
    rows = rows[::-1]   # cheapest at the top
    labels = [f"{r['Winner']} ({r['Winner price']}) beat {r['Loser']}, {r['Final']}" for r in rows]
    vals = [r["Winner implied (%)"] for r in rows]
    fig, ax = _figure(max(2.4, 0.42 * len(rows) + 1.4))
    ys = list(range(len(rows)))
    colors = [SERIES[1] if r["Matchup"] == "FBS vs FCS" else SERIES[0] for r in rows]
    ax.barh(ys, vals, color=colors, height=0.6)
    for y, v in zip(ys, vals):
        ax.text(v + 0.8, y, f"{v:.1f}%", va="center", ha="left", fontsize=7.5, color=INK)
    ax.set_yticks(ys, labels, fontsize=7.5, color=INK)
    ax.set_xlim(0, max(55.0, max(vals) + 8))
    ax.set_xlabel("winner's chance of winning implied by the closing moneyline, %", color=INK_2, fontsize=8)
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRID, linewidth=0.6)
    fcs = sum(r["Matchup"] == "FBS vs FCS" for r in rows)
    sub = f"Underdogs that won outright, longest price at the top. {len(rows)} shown"
    sub += f"; orange = FCS team beat FBS." if fcs else "."
    _style(ax, f"Week {week}'s biggest upsets by price", sub)
    _save(fig, path)
    return True


def week_summary_chart(rows: list[dict], path: Path) -> bool:
    """Outright upsets per week, FBS-vs-FBS and FCS-over-FBS stacked."""
    from model.cfb_pattern_charts import INK, INK_2, SERIES, _figure, _save, _style

    if not rows:
        return False
    xs = [r["Week"] for r in rows]
    fbs = [r["Upsets"] for r in rows]
    fcs = [r["FCS upsets"] for r in rows]
    fig, ax = _figure(3.6)
    ax.bar(xs, fbs, color=SERIES[0], width=0.6, label="FBS vs FBS")
    if any(fcs):
        ax.bar(xs, fcs, bottom=fbs, color=SERIES[1], width=0.6, label="FCS beat FBS")
    for r, a, b in zip(rows, fbs, fcs):
        ax.annotate(f"{a + b} of {r.get('Priced games', a + b)}", (r["Week"], a + b), xytext=(0, 3),
                    textcoords="offset points", ha="center", fontsize=7, color=INK)
    ax.set_xticks(xs, [f"Wk {x}" for x in xs])
    top = max(a + b for a, b in zip(fbs, fcs))
    ax.set_ylim(0, max(1, top) * 1.3)
    ax.yaxis.get_major_locator().set_params(integer=True)
    ax.set_ylabel("outright underdog wins", color=INK_2, fontsize=8)
    ax.legend(loc="upper left", fontsize=7.5, frameon=False, labelcolor=INK)
    _style(ax, "Upsets per week", "Moneyline underdogs that won outright, of regular-season games with a priced favorite and no tie.")
    _save(fig, path)
    return True


def main(argv: list[str] | None = None) -> int:
    from config import load_config
    from db.database import DatabaseManager

    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default=str(Path("artifacts") / "cfb_pattern_watch" / "charts"))
    args = ap.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    tables = build_tables(db)
    out = Path(args.out_dir)
    weeks = tables["recap_weeks"]
    latest = weeks[-1]["Week"] if weeks else None
    made = {"recap_upsets_per_week.png": week_summary_chart(weeks, out / "recap_upsets_per_week.png")}
    if latest is not None:
        made[f"recap_upsets_week_{latest}.png"] = upset_chart(tables["recap_upsets"], latest,
                                                             out / f"recap_upsets_week_{latest}.png")
    for name, ok in made.items():
        print(f"{out / name}: {'written' if ok else 'skipped, no data'}")
    for k, v in tables.items():
        print(f"{k}: {len(v)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
