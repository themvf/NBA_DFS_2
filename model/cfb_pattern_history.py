"""Prior-season comparison for the CFB pattern watch breakdown (`cfb-pattern-history-v1`).

DESCRIPTIVE ONLY. Builds the same moneyline/spread/total breakdown the page
shows for the current season (`cfb_pattern_measures.market_breakdown`) for the
2023-2025 seasons, so every row can read "2026 vs 2023-25". It grades nothing,
changes no trigger and is not evidence of an edge.

Where the prior-season data comes from
--------------------------------------
* Game facts (scores, kickoff, venue, conference game, quarter scores) come from
  `cfb_matchups`. Its own line columns are empty before 2026.
* Lines come from `cfb_historical_game_lines` (CFBD `/lines`, closing lines).
  `game_id` there is `cfb_matchups.id`, NOT `cfbd_game_id` (it is a foreign key
  to `cfb_matchups(id)`; see db/schema.py).
* Only `line_designation = 'historical_reference'` rows are used (the close).
  CFBD reports no opening moneyline, so the line-movement cut is empty for
  prior seasons. Prices are never reported (home_price/away_price are NULL), so
  spreads and totals are graded at -110 exactly as the current season is.

One provider per game
---------------------
`is_canonical_reference` is chosen per (game, market) independently by the
ingest, so a game's canonical moneyline and canonical spread can come from
different books. Mixing books inside one game would let the moneyline favorite
and the spread disagree for no reason, so this module picks ONE provider per
game: the first provider in PROVIDER_PREFERENCE that has a complete moneyline
(both sides) and a spread. The total comes from the same provider when it has
one, otherwise from the next provider in preference order (recorded in
`total_provider`). A game with no complete moneyline anywhere is kept for the
rest/last-result context but carries no lines, so it drops out of the
breakdown exactly as a current-season game without a line does.

Season-correct conference and classification
--------------------------------------------
`cfb_teams.conference` and `classification` are TODAY's values. The line rows
carry each side's conference and classification as CFBD reported them for that
season, so those are used when present (realignment: Texas and Oklahoma were
Big 12 in 2023, SMU was AAC). The tier cut still uses the current definition of
a power conference (`cfb_pattern_watch.P4` plus Notre Dame), so the 2023
Pac-12 counts as Group of Five there. That is a stated limitation, not a fix.
"""
from __future__ import annotations

import statistics
from collections import defaultdict
from pathlib import Path

from model import cfb_pattern_measures as measures

HISTORY_VERSION = "cfb-pattern-history-v1"
PRIOR_SEASONS = (2023, 2024, 2025)
# DraftKings is the only book with a moneyline in every prior season; the rest
# follow by coverage. "draft kings" is a 2025 spelling of the same book.
PROVIDER_PREFERENCE = ("draftkings", "consensus", "espn bet", "bovada", "fanduel", "betmgm",
                       "caesars", "william hill (new jersey)", "caesars sportsbook (colorado)")
PROVIDER_ALIASES = {"draft kings": "draftkings"}

_GAMES_SQL = """
SELECT m.id AS matchup_id, m.season, m.week, m.season_type, m.home_score, m.away_score,
       m.commence_time, m.home_team_id, m.away_team_id, m.conference_game,
       m.home_line_scores, m.away_line_scores, m.neutral_site,
       ht.conference AS hconf, at.conference AS aconf, ht.name AS hname, at.name AS aname,
       ht.classification AS hclass, at.classification AS aclass
FROM cfb_matchups m
JOIN cfb_teams ht ON ht.team_id = m.home_team_id
JOIN cfb_teams at ON at.team_id = m.away_team_id
WHERE m.season = ANY(%s) AND m.completed AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
"""

_LINES_SQL = """
SELECT h.game_id, h.provider, h.market_type, h.home_value, h.away_value, h.id,
       h.home_conference, h.away_conference, h.home_classification, h.away_classification
FROM cfb_historical_game_lines h
JOIN cfb_matchups m ON m.id = h.game_id
WHERE m.season = ANY(%s) AND h.line_designation = 'historical_reference'
"""


# ── pure logic ───────────────────────────────────────────────────────────────
def normalize_provider(name: str | None) -> str:
    p = str(name or "unknown").strip().lower()
    return PROVIDER_ALIASES.get(p, p)


def _rank(provider: str) -> tuple[int, str]:
    try:
        return (PROVIDER_PREFERENCE.index(provider), provider)
    except ValueError:
        return (len(PROVIDER_PREFERENCE), provider)


def _by_provider(rows: list[dict]) -> dict[str, dict[str, dict]]:
    """{provider: {market: row}}; a repeated (provider, market) keeps the newest row id."""
    out: dict[str, dict[str, dict]] = defaultdict(dict)
    for r in rows:
        p, mk = normalize_provider(r.get("provider")), r["market_type"]
        old = out[p].get(mk)
        if old is None or (r.get("id") or 0) > (old.get("id") or 0):
            out[p][mk] = r
    return out


def _complete_ml(row: dict | None) -> bool:
    return row is not None and row.get("home_value") is not None and row.get("away_value") is not None


def select_lines(rows: list[dict]) -> dict:
    """Pick one book's closing lines for one game from its historical line rows.

    Returns home_ml, away_ml, home_spread, vegas_total (None when unavailable),
    plus `line_provider` and `total_provider` for the audit trail.
    """
    books = _by_provider(rows)
    empty = {"home_ml": None, "away_ml": None, "home_spread": None, "vegas_total": None,
             "line_provider": None, "total_provider": None}
    candidates = sorted((p for p, m in books.items() if _complete_ml(m.get("moneyline")) and m.get("spread")
                         and m["spread"].get("home_value") is not None), key=_rank)
    if not candidates:
        return empty
    chosen = candidates[0]
    ml, sp = books[chosen]["moneyline"], books[chosen]["spread"]
    total_provider = None
    total = None
    for p in [chosen] + sorted((p for p in books if p != chosen), key=_rank):
        t = books[p].get("total")
        if t is not None and t.get("home_value") is not None:
            total, total_provider = float(t["home_value"]), p
            break
    return {"home_ml": float(ml["home_value"]), "away_ml": float(ml["away_value"]),
            "home_spread": float(sp["home_value"]), "vegas_total": total,
            "line_provider": chosen, "total_provider": total_provider}


def _season_fact(rows: list[dict], key: str) -> str | None:
    """The most common non-null value of a per-season team attribute across a game's line rows."""
    vals = [r.get(key) for r in rows if r.get(key)]
    if not vals:
        return None
    return statistics.mode(vals)


def build_game(game: dict, line_rows: list[dict]) -> dict:
    """One game dict in the shape cfb_pattern_measures expects (before attach_context)."""
    g = dict(game)
    g.update(select_lines(line_rows))
    # Season-correct conference/classification from the line feed, when it has them.
    for side, src in (("h", "home"), ("a", "away")):
        conf = _season_fact(line_rows, f"{src}_conference")
        cls = _season_fact(line_rows, f"{src}_classification")
        if conf:
            g[f"{side}conf"] = conf
        if cls:
            g[f"{side}class"] = str(cls).lower()
    return g


def group_by_season(games: list[dict]) -> dict[int, list[dict]]:
    out: dict[int, list[dict]] = defaultdict(list)
    for g in games:
        out[int(g["season"])].append(g)
    return dict(out)


def attach_context_by_season(games: list[dict]) -> list[dict]:
    """attach_context one season at a time, so a team's previous game never crosses seasons."""
    out: list[dict] = []
    for _season, gs in sorted(group_by_season(games).items()):
        out.extend(measures.attach_context(gs))
    return out


def build_games(game_rows: list[dict], line_rows: list[dict]) -> list[dict]:
    by_game: dict[int, list[dict]] = defaultdict(list)
    for r in line_rows:
        by_game[int(r["game_id"])].append(r)
    games = [build_game(dict(g), by_game.get(int(g["matchup_id"]), [])) for g in game_rows]
    return attach_context_by_season(games)


def coverage(games: list[dict]) -> list[dict]:
    """Per season: FBS-vs-FBS regular-season completed games, and how many carry lines / reach the breakdown."""
    out = []
    for season, gs in sorted(group_by_season(games).items()):
        fbs = [g for g in gs if g.get("hclass") == "fbs" and g.get("aclass") == "fbs"
               and (g.get("season_type") or "regular") == "regular"]
        with_lines = [g for g in fbs if g.get("home_ml") is not None and g.get("home_spread") is not None]
        out.append({"season": season, "fbs_regular_games": len(fbs), "games_with_lines": len(with_lines),
                    "with_total": sum(1 for g in with_lines if g.get("vegas_total") is not None),
                    "breakdown_games": len(measures._ml_eligible(gs)),
                    "providers": dict(sorted(_count(g.get("line_provider") for g in with_lines).items()))})
    return out


def _count(xs) -> dict:
    c: dict = defaultdict(int)
    for x in xs:
        c[x] += 1
    return c


def seasons_label(seasons) -> str:
    s = sorted(int(x) for x in seasons)
    return str(s[0]) if len(s) == 1 else f"{s[0]}-{str(s[-1])[-2:]}"


def breakdown_rows(games: list[dict], label: str) -> list[dict]:
    """The flat market_breakdown_table for these games, with a leading Seasons column."""
    from model import cfb_pattern_csv as cc

    table = cc.market_breakdown_table(measures.market_breakdown(games))
    return [{"Seasons": label, **r} for r in table]


def summary_row(season_label, games: list[dict]) -> dict:
    """Favorites across every breakdown game of one season (same population as the page's 'All games')."""
    eligible = measures._ml_eligible(games)
    r = measures._ml_row("All games", eligible)
    pct = lambda v: None if v is None else round(v * 100, 1)
    return {
        "Season": season_label, "Games with lines": r["games"],
        "Favorites won (%)": pct(r["win_rate"]), "Price implied (%)": pct(r["implied"]),
        "Difference (pts)": None if r["excess_pp"] is None else round(r["excess_pp"], 1),
        "Favorite ROI (%)": pct(r["roi"]), "Favorites covered (%)": pct(r["cover_rate"]),
        "Overs (%)": pct(r["over_rate"]), "Games with total": r["total_games"],
    }


# ── database ─────────────────────────────────────────────────────────────────
def load_prior_games(db, seasons=PRIOR_SEASONS) -> list[dict]:
    """Completed prior-season games with one book's closing lines, context attached per season. Read-only."""
    seasons = [int(s) for s in seasons]
    game_rows = [dict(r) for r in db.execute(_GAMES_SQL, (seasons,))]
    line_rows = [dict(r) for r in db.execute(_LINES_SQL, (seasons,))]
    return build_games(game_rows, line_rows)


def prior_breakdown_table(db, seasons=PRIOR_SEASONS, per_season: bool = False) -> list[dict]:
    """Pooled prior-season breakdown (Seasons = "2023-25"); with per_season, one block per season follows."""
    games = load_prior_games(db, seasons)
    rows = breakdown_rows(games, seasons_label(seasons))
    if per_season:
        for season, gs in sorted(group_by_season(games).items()):
            rows.extend(breakdown_rows(gs, str(season)))
    return rows


def season_summary_table(db, seasons=PRIOR_SEASONS) -> list[dict]:
    """One row per prior season plus the current season (from load_base_rate_games)."""
    from model import cfb_pattern_watch as pw

    by_season = group_by_season(load_prior_games(db, seasons))
    rows = [summary_row(str(s), by_season.get(int(s), [])) for s in sorted(seasons)]
    rows.append(summary_row(f"{pw.SEASON} (to date)", measures.load_base_rate_games(db)))
    return rows


# ── chart ────────────────────────────────────────────────────────────────────
def season_summary_chart(rows: list[dict], path: Path) -> bool:
    """Favorites won vs implied, and favorites covered, by season."""
    rows = [r for r in rows if r.get("Games with lines") and r.get("Favorites won (%)") is not None]
    if not rows:
        return False
    from model.cfb_pattern_charts import INK, INK_2, SERIES, _figure, _save, _style

    labels = [f"{r['Season']}\n{r['Games with lines']} games" for r in rows]
    series = (("Favorites won", "Favorites won (%)", SERIES[0]),
              ("Price implied", "Price implied (%)", SERIES[1]),
              ("Favorites covered", "Favorites covered (%)", SERIES[2]))
    xs = list(range(len(rows)))
    w = 0.26
    fig, ax = _figure(3.8)
    for i, (name, key, color) in enumerate(series):
        vals = [r.get(key) or 0.0 for r in rows]
        off = (i - 1) * w
        ax.bar([x + off for x in xs], vals, width=w, color=color, label=name)
        for x, v in zip(xs, vals):
            ax.annotate(f"{v:.1f}", (x + off, v), xytext=(0, 3), textcoords="offset points",
                        ha="center", fontsize=6.5, color=INK)
    ax.set_xticks(xs, labels, fontsize=7.5, color=INK)
    ax.set_ylim(0, 100)
    ax.set_ylabel("percent of games", color=INK_2, fontsize=8)
    ax.legend(loc="upper left", fontsize=7.5, frameon=False, labelcolor=INK, ncol=3)
    _style(ax, "Favorites by season",
           "FBS games with a closing line. Implied = moneyline minus the margin; covers at the closing spread.")
    _save(fig, path)
    return True


if __name__ == "__main__":   # read-only report
    from config import load_config
    from db.database import DatabaseManager

    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    games = load_prior_games(db)
    for c in coverage(games):
        print(c)
    for r in season_summary_table(db):
        print(r)
