"""Reader chart: each NFL game on a date, with each team's chance to win from the betting market.

Descriptive; it is the sportsbooks' number, not a prediction of ours.

  nfl_take_win_prob_<date>.png   one row per game, a bar split between the away team (left) and the home team
                                 (right) at their win chances, with the kickoff time, the spread and how far the
                                 home team's chance moved since the earliest pregame price we captured that week

Win chance is the consensus moneyline from the latest pregame capture in ``game_odds_history`` (the median across
the books we capture), with the bookmaker margin removed proportionally: p = r_home / (r_home + r_away), where r is
each side's implied probability. Ties (about 0.2% in the NFL) are ignored.

  nfl_take_model_win_prob_<date>.png   the same layout from our pick'em matchup model
                                       (``pickem-matchup-residual-shadow-v1``): the market's no-vig chance plus a
                                       small ridge adjustment from six matchup features (pass rush, yards before and
                                       after contact). A black tick marks the market's split; the right column says
                                       which team the model likes more than the market and by how much. It is a
                                       shadow model anchored to the market, not an independent or validated forecast.

    python -m model.nfl_win_prob_chart --date 2026-10-25 --out-dir DIR
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import DPI, INK, INK_2, SURFACE  # noqa: E402
from model.story_charts import NEUTRAL, WIDTH_IN, _axes, _finish  # noqa: E402

DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
ET = ZoneInfo("America/New_York")
FAV_COLOUR = "#2a78d6"
DOG_COLOUR = NEUTRAL
TEAM_ALIAS = {"LA": "LAR"}


# ── pure computations ────────────────────────────────────────────────────────
def implied(american: float) -> float:
    a = float(american)
    return -a / (-a + 100) if a < 0 else 100 / (a + 100)


def home_win(home_ml: float, away_ml: float) -> float:
    """Home team's no-vig win chance from a two-way moneyline."""
    rh, ra = implied(home_ml), implied(away_ml)
    return rh / (rh + ra)


def game_rows(raw: list[dict]) -> list[dict]:
    """Attach win chances and the move since the opening capture to each game."""
    out = []
    for g in raw:
        if g.get("home_ml") is None or g.get("away_ml") is None:
            continue
        p = home_win(g["home_ml"], g["away_ml"])
        p0 = (home_win(g["open_home_ml"], g["open_away_ml"])
              if g.get("open_home_ml") is not None and g.get("open_away_ml") is not None else None)
        out.append({**g, "p_home": p, "p_away": 1 - p, "move": None if p0 is None else p - p0})
    return out


# ── chart ────────────────────────────────────────────────────────────────────
def win_prob_chart(games: list[dict], path: Path, day_label: str, captured: str, opened: str = "") -> bool:
    if not games:
        return False
    height = 0.5 * len(games) + 2.6
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    _axes(ax, xgrid=False, ygrid=False)
    for y, g in enumerate(games):
        pa, ph = g["p_away"], g["p_home"]
        away_fav = pa > ph
        ax.barh(y, pa, left=0, height=0.62, color=FAV_COLOUR if away_fav else DOG_COLOUR)
        ax.barh(y, ph, left=pa, height=0.62, color=DOG_COLOUR if away_fav else FAV_COLOUR)
        ax.text(0.015, y, f"{g['away']}  {pa:.0%}", ha="left", va="center", fontsize=8,
                color="white" if away_fav else INK, fontweight="bold")
        ax.text(0.985, y, f"{ph:.0%}  {g['home']}", ha="right", va="center", fontsize=8,
                color=INK if away_fav else "white", fontweight="bold")
        ax.text(-0.02, y, f"{g['kickoff_label']}", ha="right", va="center", fontsize=7, color=INK_2)
        fav, dog = (g["away"], g["home"]) if away_fav else (g["home"], g["away"])
        spread = abs(g["spread"]) if g.get("spread") is not None else None
        line = f"{fav} -{spread:g}" if spread else f"{fav} fav"
        move = g.get("move")
        pts = 0 if move is None else round(move * 100)
        # Name the team whose chance rose, so the reader never has to work out a sign.
        riser = g["home"] if pts > 0 else g["away"]
        move_txt = (f"  {riser} up {abs(pts)} pt{'s' if abs(pts) > 1 else ''}" if abs(pts) >= 1 else "  no change")
        if move is None:
            move_txt = ""
        big = abs(pts) >= 4
        ax.text(1.02, y, f"{line}{move_txt}", ha="left", va="center", fontsize=6.8,
                color=INK if big else INK_2, fontweight="bold" if big else "normal")
    ax.axvline(0.5, color=SURFACE, linewidth=1.2, zorder=3)
    ax.set_xlim(0, 1)
    ax.set_ylim(len(games) - 0.5, -0.5)
    ax.set_xticks([])
    ax.set_yticks([])
    for side in ax.spines.values():
        side.set_visible(False)
    fig.tight_layout(rect=(0.09, 0, 0.74, 1))
    fav_top = max(games, key=lambda g: max(g["p_home"], g["p_away"]))
    tight = min(games, key=lambda g: abs(g["p_home"] - 0.5))
    tf = fav_top["home"] if fav_top["p_home"] > 0.5 else fav_top["away"]
    sub = (f"{day_label}, {len(games)} games. Away team on the left, home team on the right; blue is the favorite. "
           f"White line: 50%.\nBiggest favorite: {tf} ({max(fav_top['p_home'], fav_top['p_away']):.0%}). Closest: "
           f"{tight['away']} @ {tight['home']} ({max(tight['p_home'], tight['p_away']):.0%} / "
           f"{min(tight['p_home'], tight['p_away']):.0%}).\nWin chance = consensus moneyline across 6 sportsbooks with "
           f"their margin removed, captured {captured}. Right: the spread, and which team's\nchance has risen since our first "
           f"price{opened} (bold when 4+ points). The sportsbooks' number, not a prediction of ours.")
    return _finish(fig, path, "Who's favored to win tomorrow", sub, height)


def model_rows(games: list[dict], forecasts: dict[str, dict]) -> list[dict]:
    """Attach the model's and the market's home win chance (ties removed) to each game it covers."""
    out = []
    for g in games:
        f = forecasts.get(f"{g['away']}@{g['home']}")
        if not f:
            continue
        b, c = f["baseline"], f["candidate"]
        pm = c.get("homeConditional") or c["home"] / (c["home"] + c["away"])
        pk = b["home"] / (b["home"] + b["away"])
        out.append({**g, "p_home": pm, "p_away": 1 - pm, "market_home": pk, "edge": pm - pk})
    return out


def model_chart(games: list[dict], path: Path, day_label: str, built: str) -> bool:
    if not games:
        return False
    height = 0.5 * len(games) + 2.8
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    _axes(ax, xgrid=False, ygrid=False)
    for y, g in enumerate(games):
        pa, ph = g["p_away"], g["p_home"]
        away_fav = pa > ph
        ax.barh(y, pa, left=0, height=0.62, color=FAV_COLOUR if away_fav else DOG_COLOUR)
        ax.barh(y, ph, left=pa, height=0.62, color=DOG_COLOUR if away_fav else FAV_COLOUR)
        ax.text(0.015, y, f"{g['away']}  {pa:.0%}", ha="left", va="center", fontsize=8,
                color="white" if away_fav else INK, fontweight="bold")
        ax.text(0.985, y, f"{ph:.0%}  {g['home']}", ha="right", va="center", fontsize=8,
                color=INK if away_fav else "white", fontweight="bold")
        ax.text(-0.02, y, g["kickoff_label"], ha="right", va="center", fontsize=7, color=INK_2)
        # Where the market splits the same game: the away team's market chance from the left.
        ax.plot([1 - g["market_home"]] * 2, [y - 0.36, y + 0.36], color=INK, linewidth=1.6, zorder=4)
        pts = round(g["edge"] * 100)
        likes = g["home"] if pts > 0 else g["away"]
        txt = f"model likes {likes} +{abs(pts)} pt{'s' if abs(pts) > 1 else ''}" if abs(pts) >= 1 else "same as market"
        big = abs(pts) >= 4
        ax.text(1.02, y, f"market {1 - g['market_home']:.0%} / {g['market_home']:.0%}   {txt}", ha="left",
                va="center", fontsize=6.8, color=INK if big else INK_2, fontweight="bold" if big else "normal")
    ax.axvline(0.5, color=SURFACE, linewidth=1.2, zorder=3)
    ax.set_xlim(0, 1)
    ax.set_ylim(len(games) - 0.5, -0.5)
    ax.set_xticks([])
    ax.set_yticks([])
    for side in ax.spines.values():
        side.set_visible(False)
    fig.tight_layout(rect=(0.09, 0, 0.68, 1))
    top = max(games, key=lambda g: abs(g["edge"]))
    top_team = top["home"] if top["edge"] > 0 else top["away"]
    sub = (f"{day_label}, {len(games)} games. Bars: our model's win chance. Away team left, home right; blue is the "
           f"model's favorite.\nBlack tick: where the betting market splits the same game. Biggest disagreement: "
           f"{top_team}, +{abs(top['edge']) * 100:.0f} points over the market.\nModel = our pick'em matchup model "
           f"(shadow): the market's no-vig chance, nudged by pass rush and yards before and after contact.\nIt starts "
           f"from the market, so it rarely moves more than a few points, and it has not been shown to beat the market. "
           f"Built {built}.")
    return _finish(fig, path, "Who our model favors tomorrow", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def load_games(day: dt.date) -> list[dict]:
    import psycopg2
    from dotenv import load_dotenv

    load_dotenv(".env")
    start = dt.datetime.combine(day, dt.time(4), tzinfo=dt.timezone.utc)
    end = start + dt.timedelta(days=1, hours=4)
    sql = """
        SELECT s.kickoff, ht.abbreviation, at.abbreviation,
               c.home_ml, c.away_ml, c.home_spread, c.captured_at, c.bookmaker_count,
               o.home_ml, o.away_ml, o.captured_at
        FROM nfl_season_games s
        JOIN nfl_teams ht ON ht.team_id = s.home_team_id
        JOIN nfl_teams at ON at.team_id = s.away_team_id
        LEFT JOIN LATERAL (SELECT home_ml, away_ml, home_spread, captured_at, bookmaker_count
                           FROM game_odds_history h
                           WHERE h.sport = 'nfl' AND h.matchup_id = s.matchup_id AND h.captured_at < s.kickoff
                             AND h.home_ml IS NOT NULL AND h.away_ml IS NOT NULL
                           ORDER BY h.captured_at DESC LIMIT 1) c ON true
        LEFT JOIN LATERAL (SELECT home_ml, away_ml, captured_at FROM game_odds_history h
                           WHERE h.sport = 'nfl' AND h.matchup_id = s.matchup_id AND h.captured_at < s.kickoff
                             AND h.captured_at > s.kickoff - interval '7 days'
                             AND h.home_ml IS NOT NULL AND h.away_ml IS NOT NULL
                           ORDER BY h.captured_at ASC LIMIT 1) o ON true
        WHERE s.kickoff >= %s AND s.kickoff < %s
        ORDER BY s.kickoff, ht.abbreviation
    """
    with psycopg2.connect(os.environ["DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute(sql, (start, end))
        rows = cur.fetchall()
    out = []
    for ko, home, away, hml, aml, spread, cap, books, ohml, oaml, ocap in rows:
        local = ko.astimezone(ET)
        out.append({"kickoff": ko, "kickoff_label": local.strftime("%I:%M %p ET").lstrip("0"),
                    "home": TEAM_ALIAS.get(home, home), "away": TEAM_ALIAS.get(away, away),
                    "home_ml": hml, "away_ml": aml, "spread": spread, "captured_at": cap, "books": books,
                    "open_home_ml": ohml, "open_away_ml": oaml, "opened_at": ocap})
    return out


def load_forecasts(day: dt.date) -> tuple[dict[str, dict], str]:
    """Latest shadow pick'em forecast per game for the NFL week containing ``day``, keyed 'AWAY@HOME'."""
    import psycopg2
    from dotenv import load_dotenv

    load_dotenv(".env")
    sql = """
        SELECT DISTINCT ON (f.game_id) f.game_id, f.available_at, f.payload->'baseline', f.payload->'candidate'
        FROM nfl_pickem_matchup_forecasts f
        JOIN nfl_season_games s ON s.nflverse_game_id = f.game_id
        WHERE f.model_version = 'pickem-matchup-residual-shadow-v1' AND (f.payload->>'covered')::boolean
          AND (f.payload->>'selectedForDefault')::boolean
          AND s.kickoff >= %s AND s.kickoff < %s AND f.available_at < s.kickoff
        ORDER BY f.game_id, f.available_at DESC
    """
    start = dt.datetime.combine(day, dt.time(4), tzinfo=dt.timezone.utc)
    with psycopg2.connect(os.environ["DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute(sql, (start, start + dt.timedelta(days=1)))
        rows = cur.fetchall()
    out, built = {}, []
    for game_id, at, base, cand in rows:
        _, _, away, home = game_id.split("_")
        out[f"{TEAM_ALIAS.get(away, away)}@{TEAM_ALIAS.get(home, home)}"] = {"baseline": base, "candidate": cand}
        built.append(at)
    label = max(built).astimezone(ET).strftime("%b %d %I:%M %p ET").replace(" 0", " ") if built else "unknown"
    return out, label


def build_all(out_dir: Path, day: dt.date) -> list[tuple[str, bool]]:
    games = game_rows(load_games(day))
    caps = [g["captured_at"] for g in games if g.get("captured_at")]
    captured = max(caps).astimezone(ET).strftime("%b %d %I:%M %p ET").replace(" 0", " ") if caps else "unknown"
    opens = sorted({g["opened_at"].astimezone(ET).strftime("%b %d").replace(" 0", " ")
                    for g in games if g.get("opened_at")})
    opened = "" if not opens else f" ({opens[0]})" if len(opens) == 1 else f" ({opens[0]}-{opens[-1].split()[-1]})"
    name = f"nfl_take_win_prob_{day.isoformat()}.png"
    label = day.strftime("%A, %B %d").replace(" 0", " ")
    forecasts, built = load_forecasts(day)
    mname = f"nfl_take_model_win_prob_{day.isoformat()}.png"
    return [(name, win_prob_chart(games, Path(out_dir) / name, label, captured, opened)),
            (mname, model_chart(model_rows(games, forecasts), Path(out_dir) / mname, label, built))]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--date", default=(dt.datetime.now(ET).date() + dt.timedelta(days=1)).isoformat())
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    for name, ok in build_all(Path(args.out_dir), dt.date.fromisoformat(args.date)):
        print(f"{name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
