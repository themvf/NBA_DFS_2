"""Where the sportsbooks disagreed, and how the lines moved (2026 CFB).

Descriptive, for the pattern-watch page and newsletter: what happened, with the
sample sizes visible. Nothing here is a betting signal.

Inputs are the captured quotes in `game_odds_history` (sport='cfb') and the
game facts in `cfb_matchups`. Only captures strictly before kickoff count.

  book_gaps    per game, at the last pre-kickoff capture: how far apart the
               books were on the spread, the total and the moneyline (in
               vig-free home win probability points), and which books sat at
               each end. Exchanges are left out: their feed odds exclude the
               commission, so they are not a sportsbook's price.
  line_moves   per game: the consensus open (earliest capture at least 12 hours
               before kickoff) and close (last pre-kickoff capture), the moves,
               and for finished games whether the close or the open sat closer
               to the actual margin and total.
  move_summary season and per-week roll-up of line_moves.

    python -m model.cfb_book_disagreement [--season 2026] [--charts DIR]
"""
from __future__ import annotations

import argparse
import statistics
from datetime import datetime, timedelta
from pathlib import Path

from model import cfb_pattern_watch as pw

SEASON = 2026
OPEN_MIN_HOURS = 12.0       # an "open" must be captured at least this long before kickoff
BIG_MOVE_PTS = 2.0          # a spread or total move this large counts as a big move
# Exchange feed odds exclude the 2-5% commission, so they are not sportsbook prices
# (same list as model/best_price.py, plus the plain key the CFB feed uses).
EXCHANGES = {"matchbook", "smarkets", "betfair", "betfair_ex", "betfair_ex_eu",
             "betfair_ex_uk", "betfair_ex_au", "betfair_sb_uk"}


# ── helpers ──────────────────────────────────────────────────────────────────
def _num(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _home_prob(home_ml, away_ml) -> float | None:
    h, a = _num(home_ml), _num(away_ml)
    if h is None or a is None or h == 0 or a == 0:
        return None
    return pw.vig_free_home(h, a)


def _matchup(g: dict) -> str:
    return f"{g.get('away') or '?'} @ {g.get('home') or '?'}"


def _pre_kickoff(captures: list[dict], games: dict) -> dict[int, list[dict]]:
    """Captures grouped by game, strictly before that game's kickoff, oldest first."""
    out: dict[int, list[dict]] = {}
    for c in captures:
        g = games.get(c["matchup_id"])
        if g is None or g.get("commence_time") is None or c.get("captured_at") is None:
            continue
        if c["captured_at"] >= g["commence_time"]:
            continue
        out.setdefault(c["matchup_id"], []).append(c)
    for rows in out.values():
        rows.sort(key=lambda r: r["captured_at"])
    return out


def _games_by_id(games) -> dict:
    return games if isinstance(games, dict) else {g["matchup_id"]: g for g in games}


def _hours_before(g: dict, ts: datetime) -> float:
    return round((g["commence_time"] - ts).total_seconds() / 3600, 1)


def _extreme(quotes: list[tuple[str, float]]) -> tuple[float, str, float, str] | None:
    if len(quotes) < 2:
        return None
    lo = min(v for _, v in quotes)
    hi = max(v for _, v in quotes)
    lo_b = ", ".join(sorted(b for b, v in quotes if v == lo))
    hi_b = ", ".join(sorted(b for b, v in quotes if v == hi))
    return lo, lo_b, hi, hi_b


def book_quotes(books: dict | None) -> list[dict]:
    """One normalized quote per sportsbook (exchanges dropped)."""
    out = []
    for key, v in (books or {}).items():
        if key in EXCHANGES or not isinstance(v, dict):
            continue
        out.append({
            "book": v.get("title") or key,
            "spread": _num(v.get("spread_home")),
            "total": _num(v.get("total_line")),
            "home_prob": _home_prob(v.get("ml_home"), v.get("ml_away")),
        })
    return out


# ── pure tables ──────────────────────────────────────────────────────────────
def book_gaps(captures: list[dict], games) -> list[dict]:
    """Per game, the spread between the books at the last pre-kickoff capture."""
    games = _games_by_id(games)
    rows = []
    for mid, caps in _pre_kickoff(captures, games).items():
        last = caps[-1]
        quotes = book_quotes(last.get("books"))
        if len(quotes) < 2:
            continue
        g = games[mid]
        sp = _extreme([(q["book"], q["spread"]) for q in quotes if q["spread"] is not None])
        to = _extreme([(q["book"], q["total"]) for q in quotes if q["total"] is not None])
        ml = _extreme([(q["book"], q["home_prob"] * 100) for q in quotes if q["home_prob"] is not None])
        rows.append({
            "Week": g.get("week"),
            "Matchup": _matchup(g),
            "Books": len(quotes),
            "Hours before kickoff": _hours_before(g, last["captured_at"]),
            "Spread range (pts)": round(sp[2] - sp[0], 1) if sp else None,
            "Home spread low": sp[0] if sp else None,
            "Low spread books": sp[1] if sp else None,
            "Home spread high": sp[2] if sp else None,
            "High spread books": sp[3] if sp else None,
            "Total range (pts)": round(to[2] - to[0], 1) if to else None,
            "Total low": to[0] if to else None,
            "Low total books": to[1] if to else None,
            "Total high": to[2] if to else None,
            "High total books": to[3] if to else None,
            "Home win prob range (pts)": round(ml[2] - ml[0], 1) if ml else None,
            "Home win prob low (%)": round(ml[0], 1) if ml else None,
            "Low prob books": ml[1] if ml else None,
            "Home win prob high (%)": round(ml[2], 1) if ml else None,
            "High prob books": ml[3] if ml else None,
        })
    rows.sort(key=lambda r: (-(r["Spread range (pts)"] or 0), -(r["Total range (pts)"] or 0),
                             -(r["Home win prob range (pts)"] or 0), r["Matchup"]))
    return rows


def _consensus_total(c: dict) -> float | None:
    raw = _num(c.get("vegas_total_raw"))
    return raw if raw is not None else _num(c.get("vegas_total"))


def _closer(open_err: float | None, close_err: float | None) -> str | None:
    if open_err is None or close_err is None:
        return None
    if abs(open_err - close_err) < 1e-9:
        return "Tie"
    return "Close" if close_err < open_err else "Open"


def _r(v, d=1):
    return None if v is None else round(v, d)


def line_moves(captures: list[dict], games) -> list[dict]:
    """Per game: consensus open vs close, the moves, and which sat closer to the result."""
    games = _games_by_id(games)
    rows = []
    for mid, caps in _pre_kickoff(captures, games).items():
        g = games[mid]
        cutoff = g["commence_time"] - timedelta(hours=OPEN_MIN_HOURS)
        opens = [c for c in caps if c["captured_at"] <= cutoff]
        op, cl = (opens[0] if opens else None), caps[-1]
        if op is cl:   # a lone capture 12h+ out is a close, not a move
            op = None

        cs, os_ = _num(cl.get("home_spread")), _num(op.get("home_spread")) if op else None
        ct, ot = _consensus_total(cl), (_consensus_total(op) if op else None)
        cp = _home_prob(cl.get("home_ml"), cl.get("away_ml"))
        opp = _home_prob(op.get("home_ml"), op.get("away_ml")) if op else None

        # The favorite is the closing favorite (spread first, moneyline if the spread is a pick'em).
        if cs is not None and cs != 0:
            home_fav = cs < 0
        elif cp is not None:
            home_fav = cp >= 0.5
        else:
            home_fav = None
        fav = None if home_fav is None else (g.get("home") if home_fav else g.get("away"))
        sign = 1 if home_fav else -1
        fav_open_sp = None if (os_ is None or home_fav is None) else sign * os_
        fav_close_sp = None if (cs is None or home_fav is None) else sign * cs
        fav_open_p = None if (opp is None or home_fav is None) else (opp if home_fav else 1 - opp)
        fav_close_p = None if (cp is None or home_fav is None) else (cp if home_fav else 1 - cp)

        spread_move = None if (os_ is None or cs is None) else cs - os_
        total_move = None if (ot is None or ct is None) else ct - ot
        toward = None
        if spread_move:
            toward = g.get("home") if spread_move < 0 else g.get("away")

        hs, as_ = _num(g.get("home_score")), _num(g.get("away_score"))
        done = bool(g.get("completed")) and hs is not None and as_ is not None
        margin = (hs - as_) if done else None
        points = (hs + as_) if done else None
        o_err = None if (margin is None or os_ is None) else abs(margin + os_)
        c_err = None if (margin is None or cs is None) else abs(margin + cs)
        ot_err = None if (points is None or ot is None) else abs(points - ot)
        ct_err = None if (points is None or ct is None) else abs(points - ct)

        rows.append({
            "Week": g.get("week"),
            "Matchup": _matchup(g),
            "Favorite (close)": fav,
            "Open hours before kickoff": _hours_before(g, op["captured_at"]) if op else None,
            "Captures": len(caps),
            "Home spread open": os_,
            "Home spread close": cs,
            "Spread move (pts)": _r(spread_move),
            "Fav spread open": fav_open_sp,
            "Fav spread close": fav_close_sp,
            "Moved toward": toward,
            "Total open": _r(ot),
            "Total close": _r(ct),
            "Total move (pts)": _r(total_move),
            "Fav win prob open (%)": _r(fav_open_p * 100) if fav_open_p is not None else None,
            "Fav win prob close (%)": _r(fav_close_p * 100) if fav_close_p is not None else None,
            "Fav prob move (pts)": (_r((fav_close_p - fav_open_p) * 100)
                                    if fav_open_p is not None and fav_close_p is not None else None),
            "Final": f"{g.get('away')} {int(as_)} - {g.get('home')} {int(hs)}" if done else None,
            "Home margin": margin,
            "Margin error at open": _r(o_err),
            "Margin error at close": _r(c_err),
            "Spread closer": _closer(o_err, c_err),
            "Total points": points,
            "Total error at open": _r(ot_err),
            "Total error at close": _r(ct_err),
            "Total closer": _closer(ot_err, ct_err),
        })
    rows.sort(key=lambda r: (-abs(r["Spread move (pts)"] or 0), r["Week"] or 0, r["Matchup"]))
    return rows


def _summarize(scope: str, rows: list[dict]) -> dict:
    with_open = [r for r in rows if r["Open hours before kickoff"] is not None]
    sp = [abs(r["Spread move (pts)"]) for r in with_open if r["Spread move (pts)"] is not None]
    tm = [abs(r["Total move (pts)"]) for r in with_open if r["Total move (pts)"] is not None]
    sd = [r["Spread closer"] for r in with_open if r["Spread closer"] in ("Close", "Open")]
    td = [r["Total closer"] for r in with_open if r["Total closer"] in ("Close", "Open")]
    s_ties = sum(1 for r in with_open if r["Spread closer"] == "Tie")
    t_ties = sum(1 for r in with_open if r["Total closer"] == "Tie")
    pct = lambda k, n: None if n == 0 else round(100 * k / n, 1)  # noqa: E731
    return {
        "Scope": scope,
        "Games": len(rows),
        "Games with open": len(with_open),
        "Median spread move (pts)": _r(statistics.median(sp)) if sp else None,
        "Spread moved 2+ (%)": pct(sum(1 for v in sp if v >= BIG_MOVE_PTS), len(sp)),
        "Spread decided games": len(sd),
        "Spread ties": s_ties,
        "Close beat open, spread (%)": pct(sd.count("Close"), len(sd)),
        "Median total move (pts)": _r(statistics.median(tm)) if tm else None,
        "Total moved 2+ (%)": pct(sum(1 for v in tm if v >= BIG_MOVE_PTS), len(tm)),
        "Total decided games": len(td),
        "Total ties": t_ties,
        "Close beat open, total (%)": pct(td.count("Close"), len(td)),
    }


def move_summary(moves: list[dict]) -> list[dict]:
    """Season row first, then one row per week (ties excluded from the close-vs-open share)."""
    out = [_summarize("Season", moves)]
    for w in sorted({r["Week"] for r in moves if r["Week"] is not None}):
        s = _summarize(f"Week {w}", [r for r in moves if r["Week"] == w])
        s["Week"] = w
        out.append(s)
    out[0]["Week"] = None
    return out


# ── loader ───────────────────────────────────────────────────────────────────
# books is only shipped for each game's last pre-kickoff capture (the only one
# book_gaps reads); every other row carries the consensus columns alone.
_CAPTURES_SQL = """
SELECT h.matchup_id, h.captured_at, h.home_spread, h.vegas_total, h.vegas_total_raw,
       h.home_ml, h.away_ml,
       CASE WHEN row_number() OVER (PARTITION BY h.matchup_id ORDER BY h.captured_at DESC, h.id DESC) = 1
            THEN h.books END AS books
FROM game_odds_history h
JOIN cfb_matchups m ON m.id = h.matchup_id
WHERE h.sport = 'cfb' AND m.season = %s AND h.captured_at < m.commence_time
ORDER BY h.matchup_id, h.captured_at
"""
_GAMES_SQL = """
SELECT m.id AS matchup_id, m.week, m.commence_time, m.completed, m.home_score, m.away_score,
       ht.name AS home, at.name AS away
FROM cfb_matchups m
JOIN cfb_teams ht ON ht.team_id = m.home_team_id
JOIN cfb_teams at ON at.team_id = m.away_team_id
WHERE m.season = %s AND m.commence_time IS NOT NULL
"""


def build_tables(db, season: int = SEASON) -> dict[str, list[dict]]:
    games = {g["matchup_id"]: dict(g) for g in db.execute(_GAMES_SQL, (season,))}
    captures = [dict(c) for c in db.execute(_CAPTURES_SQL, (season,))]
    moves = line_moves(captures, games)
    return {"book_gaps": book_gaps(captures, games), "line_moves": moves, "move_summary": move_summary(moves)}


# ── charts ───────────────────────────────────────────────────────────────────
def _fav_label(v: float) -> str:
    return "PK" if v == 0 else f"{v:+g}"


def biggest_moves_chart(moves: list[dict], path: Path) -> bool:
    """Top 10 spread moves of the season, open to close, from the closing favorite's side."""
    from model.cfb_pattern_charts import INK, INK_2, SERIES, _figure, _save, _style

    rows = [r for r in moves if r.get("Spread move (pts)") is not None
            and r.get("Fav spread open") is not None and r.get("Fav spread close") is not None]
    rows = sorted(rows, key=lambda r: -abs(r["Spread move (pts)"]))[:10]
    if not rows:
        return False
    rows = rows[::-1]   # biggest at the top
    fig, ax = _figure(height=4.6)
    ys = list(range(len(rows)))
    for y, r in zip(ys, rows):
        o, c = -r["Fav spread open"], -r["Fav spread close"]   # points favored
        ax.plot([o, c], [y, y], color=INK_2, linewidth=1.4, zorder=1)
        ax.scatter([o], [y], color=SERIES[1], s=28, zorder=2)
        ax.scatter([c], [y], color=SERIES[0], s=28, zorder=3)
        ax.annotate(f"{_fav_label(r['Fav spread open'])} → {_fav_label(r['Fav spread close'])}",
                    (max(o, c), y), xytext=(6, -3), textcoords="offset points", fontsize=7.5, color=INK)
    ax.set_yticks(ys)
    ax.set_yticklabels([f"Wk {r['Week']}  {r['Matchup']}" for r in rows], fontsize=7.5, color=INK)
    ax.set_xlabel("Points the closing favorite was laying", fontsize=8, color=INK_2)
    lo = min(min(-r["Fav spread open"], -r["Fav spread close"]) for r in rows)
    hi = max(max(-r["Fav spread open"], -r["Fav spread close"]) for r in rows)
    ax.set_xlim(lo - 1.5, hi + (hi - lo) * 0.25 + 3)
    ax.grid(axis="x", color="#e4e3df", linewidth=0.6)
    ax.grid(axis="y", visible=False)
    ax.scatter([], [], color=SERIES[1], s=28, label="Open (12h+ out)")
    ax.scatter([], [], color=SERIES[0], s=28, label="Close")
    ax.legend(loc="lower right", fontsize=7.5, frameon=False)
    _style(ax, "The biggest spread moves of the 2026 season",
           "Consensus open (12h+ out) and close, closing favorite's side")
    _save(fig, path)
    return True


def close_vs_open_chart(summary: list[dict], path: Path) -> bool:
    """Share of games where the close sat closer to the result than the open, by week."""
    from model.cfb_pattern_charts import INK, INK_2, SERIES, _figure, _save, _style

    weeks = [s for s in summary if s.get("Week") is not None
             and (s.get("Spread decided games") or s.get("Total decided games"))]
    if not weeks:
        return False
    fig, ax = _figure()
    xs = list(range(len(weeks)))
    w = 0.38
    for off, key, n_key, color, label in (
        (-w / 2, "Close beat open, spread (%)", "Spread decided games", SERIES[0], "Spread"),
        (w / 2, "Close beat open, total (%)", "Total decided games", SERIES[1], "Total"),
    ):
        vals = [s.get(key) for s in weeks]
        ax.bar([x + off for x in xs], [v or 0 for v in vals], width=w, color=color, label=label)
        for x, v, s in zip(xs, vals, weeks):
            if v is not None:
                ax.annotate(f"{v:.0f}%\nn={s[n_key]}", (x + off, v), xytext=(0, 2), textcoords="offset points",
                            ha="center", fontsize=6, color=INK_2)
    ax.axhline(50, color=INK, linewidth=0.8, linestyle="--")
    ax.set_xticks(xs)
    ax.set_xticklabels([f"Wk {s['Week']}" for s in weeks], fontsize=8)
    ax.set_ylim(0, 112)
    ax.set_yticks([0, 25, 50, 75, 100])
    ax.set_yticklabels(["0%", "25%", "50%", "75%", "100%"])
    ax.legend(loc="upper right", fontsize=7.5, frameon=False, ncol=2)
    season = summary[0]
    _style(ax, "Did the closing line beat the opener?",
           f"Close nearer the final than the open; ties out. Season: spread "
           f"{season.get('Close beat open, spread (%)')}%, total {season.get('Close beat open, total (%)')}%")
    _save(fig, path)
    return True


def main(argv: list[str] | None = None) -> int:
    from config import load_config
    from db.database import DatabaseManager

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--season", type=int, default=SEASON)
    ap.add_argument("--charts", type=Path, default=None, help="write the two PNGs into this directory")
    args = ap.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    t = build_tables(db, args.season)
    s = t["move_summary"][0]
    print(f"games {s['Games']}, with open {s['Games with open']}, median spread move {s['Median spread move (pts)']}, "
          f"spread moved 2+ {s['Spread moved 2+ (%)']}%, close beat open spread {s['Close beat open, spread (%)']}% "
          f"(n={s['Spread decided games']}, ties {s['Spread ties']}), total {s['Close beat open, total (%)']}% "
          f"(n={s['Total decided games']}, ties {s['Total ties']}), median total move {s['Median total move (pts)']}")
    print(f"book gaps rows {len(t['book_gaps'])}")
    for r in t["book_gaps"][:5]:
        print(" ", r["Week"], r["Matchup"], r["Books"], "books | spread", r["Spread range (pts)"],
              f"({r['Home spread low']} {r['Low spread books']} .. {r['Home spread high']} {r['High spread books']})",
              "| total", r["Total range (pts)"], "| prob", r["Home win prob range (pts)"])
    for r in t["line_moves"][:5]:
        print(" ", r["Week"], r["Matchup"], r["Home spread open"], "->", r["Home spread close"], r["Spread closer"])
    if args.charts:
        print("biggest moves:", biggest_moves_chart(t["line_moves"], args.charts / "biggest_moves.png"))
        print("close vs open:", close_vs_open_chart(t["move_summary"], args.charts / "close_vs_open.png"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
