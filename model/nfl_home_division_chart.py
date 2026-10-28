"""Reader chart: how often home teams won, by the home team's division, against what the market expected.

Descriptive, season to date; eight divisions of a few games each, chosen for display after the results were known.

  nfl_take_home_by_division_<season>.png   one bar per division: the share of its home games it won, a black tick
                                           at the market's expected share (no-vig moneyline, averaged over the same
                                           games), and the division's road record beside it. The two South divisions
                                           are highlighted.

Games from ``nfl_season_games`` (regular season, finished). Neutral-site games are left out: the listed home team
had no home crowd. A tie counts as neither a win nor a loss.

    python -m model.nfl_home_division_chart --season 2026 --out-dir DIR
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import DPI, INK, INK_2, SURFACE  # noqa: E402
from model.story_charts import BAD, NEUTRAL, WIDTH_IN, _axes, _finish  # noqa: E402

DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
HIGHLIGHT = {"NFC South", "AFC South"}
# nflverse lists no venue flag; these 2026 games were played at a neutral site (London, Dublin, Berlin, ...).
NEUTRAL_SITES = {"2026_01_SF_LA", "2026_03_BAL_DAL", "2026_04_IND_WAS", "2026_05_PHI_JAX",
                 "2026_06_HOU_JAX", "2026_07_PIT_NO"}


# ── pure computations ────────────────────────────────────────────────────────
def _implied(ml: float) -> float:
    return -ml / (-ml + 100) if ml < 0 else 100 / (ml + 100)


def market_home(home_ml: float | None, away_ml: float | None) -> float | None:
    """The home team's no-vig win chance from the two moneylines, or None without both."""
    if home_ml is None or away_ml is None:
        return None
    h, a = _implied(home_ml), _implied(away_ml)
    return h / (h + a)


def division_rows(games: list[dict]) -> list[dict]:
    """Per division: home W-L-T, home win share (ties left out), market expectation, road W-L. Sorted by share."""
    acc: dict[str, dict] = {}
    for g in games:
        if g["game_id"] in NEUTRAL_SITES:
            continue
        hs, as_ = g["home_score"], g["away_score"]
        home = acc.setdefault(g["home_div"], {"w": 0, "l": 0, "t": 0, "exp": [], "rw": 0, "rl": 0})
        road = acc.setdefault(g["away_div"], {"w": 0, "l": 0, "t": 0, "exp": [], "rw": 0, "rl": 0})
        if hs == as_:
            home["t"] += 1
        else:
            home["w" if hs > as_ else "l"] += 1
            road["rw" if as_ > hs else "rl"] += 1
        p = market_home(g.get("home_ml"), g.get("away_ml"))
        if p is not None:
            home["exp"].append(p)
    out = []
    for div, a in acc.items():
        decided = a["w"] + a["l"]
        if not decided:
            continue
        exp = sum(a["exp"]) / len(a["exp"]) if a["exp"] else None
        out.append({"division": div, "w": a["w"], "l": a["l"], "t": a["t"], "share": a["w"] / decided,
                    "expected": exp, "expected_wins": exp * decided if exp is not None else None,
                    "road_w": a["rw"], "road_l": a["rl"]})
    return sorted(out, key=lambda r: (-r["share"], r["division"]))


# ── chart ────────────────────────────────────────────────────────────────────
def chart(rows: list[dict], path: Path, season: int, weeks: str) -> bool:
    if not rows:
        return False
    height = 0.42 * len(rows) + 3.0
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    _axes(ax, ygrid=False)
    for y, r in enumerate(rows):
        hot = r["division"] in HIGHLIGHT
        ax.barh(y, r["share"], height=0.62, color=BAD if hot else NEUTRAL, zorder=2)
        if r["expected"] is not None:
            ax.plot([r["expected"]] * 2, [y - 0.38, y + 0.38], color=INK, linewidth=1.8, zorder=3)
        tie = f"-{r['t']}" if r["t"] else ""
        exp = f"   market expected {r['expected_wins']:.0f} wins" if r["expected_wins"] is not None else ""
        ax.text(1.02, y, f"{r['w']}-{r['l']}{tie} at home{exp}   road {r['road_w']}-{r['road_l']}",
                ha="left", va="center", fontsize=7.2, color=INK if hot else INK_2,
                fontweight="bold" if hot else "normal", transform=ax.get_yaxis_transform(), clip_on=False)
        ax.text(0.012, y, f"{r['share']:.0%}", ha="left", va="center", fontsize=7,
                color="white" if hot else INK, fontweight="bold", zorder=4)
    ax.axvline(0.5, color=INK_2, linewidth=0.6, linestyle=":", zorder=1)
    ax.set_yticks(range(len(rows)), [r["division"] for r in rows], fontsize=8)
    for lbl in ax.get_yticklabels():
        if lbl.get_text() in HIGHLIGHT:
            lbl.set_color(BAD)
            lbl.set_fontweight("bold")
    ax.set_ylim(len(rows) - 0.4, -0.6)
    ax.set_xlim(0, 1)
    ax.xaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0, decimals=0))
    ax.tick_params(axis="x", labelsize=7)
    ax.set_xlabel("Share of home games won (ties left out)", fontsize=8, color=INK_2)
    fig.subplots_adjust(left=0.13, right=0.66, bottom=0.6 / height, top=1 - 1.25 / height)
    hot = [r for r in rows if r["division"] in HIGHLIGHT]
    rest = [r for r in rows if r["division"] not in HIGHLIGHT]
    hw, hl = sum(r["w"] for r in hot), sum(r["l"] for r in hot)
    ow, ol = sum(r["w"] for r in rest), sum(r["l"] for r in rest)
    sub = (f"{season} season, {weeks}: every finished game, neutral sites left out. Bar: how often the division's teams "
           f"won at home. Black tick: what the betting market expected\n(moneyline, margin removed, averaged over the same "
           f"games). Right: the record at home, the market's expected home wins, and the same teams' record on the road.\n"
           f"The two South divisions went {hw}-{hl} at home; the other six went {ow}-{ol}. Eight divisions of a dozen "
           f"games each, picked out after the fact:\nthis describes the season so far, it does not predict the rest of it.")
    return _finish(fig, path, "Home field has vanished in the South", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def load_games(season: int) -> list[dict]:
    import psycopg2
    from dotenv import load_dotenv

    load_dotenv(".env")
    sql = """
        SELECT g.nflverse_game_id, g.week, g.home_score, g.away_score, g.market_home_ml, g.market_away_ml,
               h.conference || ' ' || h.division, a.conference || ' ' || a.division
        FROM nfl_season_games g
        JOIN nfl_teams h ON h.team_id = g.home_team_id
        JOIN nfl_teams a ON a.team_id = g.away_team_id
        WHERE g.season = %s AND g.game_type = 'REG' AND g.completed
          AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL
    """
    with psycopg2.connect(os.environ["DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute(sql, (season,))
        rows = cur.fetchall()
    return [{"game_id": gid, "week": int(wk), "home_score": int(hs), "away_score": int(as_),
             "home_ml": float(hm) if hm is not None else None, "away_ml": float(am) if am is not None else None,
             "home_div": hd, "away_div": ad}
            for gid, wk, hs, as_, hm, am, hd, ad in rows]


def build_all(out_dir: Path, season: int) -> list[tuple[str, bool]]:
    games = load_games(season)
    ws = sorted({g["week"] for g in games})
    weeks = f"weeks {ws[0]}-{ws[-1]}" if ws else "no weeks"
    name = f"nfl_take_home_by_division_{season}.png"
    return [(name, chart(division_rows(games), Path(out_dir) / name, season, weeks))]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--season", type=int, default=2026)
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    for name, ok in build_all(Path(args.out_dir), args.season):
        print(f"{name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
