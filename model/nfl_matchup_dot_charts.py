"""Reader charts: two teams before they meet, drawn as dots on one league-rank scale. Simpler successor to the tale
of the tape and its two deeper charts; same numbers, one encoding.

Descriptive, no edge claimed: none of these numbers has been shown to predict better than the betting line.

  nfl_take_dots_<away>_<home>.png         Season profile: every stat as one row, each team a dot placed by its rank
                                          among 32 (further right = better, whatever the stat), the real values in
                                          a column on the right; each game's final margin underneath
  nfl_take_dots_matchup_<away>_<home>.png Matchup: each offense against the other defense on the same play type
                                          (the dot further right has the edge on that row), the two quarterbacks,
                                          and players listed out, doubtful or questionable

Ranks are among all 32 teams, 1st = best for that side of the ball, so a row where lower is better (points allowed,
sacks taken) still reads "right = better". Rank hides size: two teams 0.01 EPA apart can sit several ranks apart, so
the values column stays. Definitions are those of model.nfl_matchup_tape_chart and model.nfl_matchup_deep_charts.

    python -m model.nfl_matchup_dot_charts --away DAL --home PHI --out-dir DIR
"""

from __future__ import annotations

import argparse
import collections
import textwrap
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model import nfl_matchup_deep_charts as deep  # noqa: E402
from model import nfl_matchup_tape_chart as tape  # noqa: E402
from model.cfb_pattern_charts import DPI, INK, INK_2, SURFACE  # noqa: E402
from model.nfl_matchup_tape_chart import AWAY_COLOUR, HOME_COLOUR, _name, _ord, margin_panel  # noqa: E402
from model.story_charts import WIDTH_IN, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
# metric -> (higher is better, value format); the union of both source modules' metrics.
METRICS = {**deep.METRICS, **{k: (h, f) for _, f, rows in tape.PANELS for k, _, h in rows}}

# Season profile: (group title, [(row label, metric)]).
PROFILE = (
    ("Scoring", (("Points scored a game", "pf"), ("Points allowed a game", "pa"),
                 ("Offensive TDs scored a game", "off_td"), ("Offensive TDs allowed a game", "def_td"),
                 ("Red-zone TD rate", "rz_td"), ("Red-zone TD rate allowed", "def_rz_td"))),
    ("Moving the ball", (("Yards gained a game", "off_yds"), ("Yards allowed a game", "def_yds"),
                         ("EPA a play, offense", "off_epa"), ("EPA a play allowed", "def_epa"))),
    ("Turnovers and luck", (("Turnovers forced a game", "takeaways"), ("Turnovers committed a game", "giveaways"),
                            ("Share of fumbles recovered", "fum_rec"), ("Win rate in one-score games", "one_score_pct"),
                            ("Schedule faced (right = tougher)", "sos"))),
)


def _cross(off: str, dfn: str):
    return (("Passing, EPA a dropback", (off, "pass_off"), (dfn, "pass_def")),
            ("Rushing, EPA a carry", (off, "rush_off"), (dfn, "rush_def")),
            ("Explosive plays (20+ yards)", (off, "expl_off"), (dfn, "expl_def")),
            ("Third-down conversion rate", (off, "third_off"), (dfn, "third_def")),
            ("Sacks, share of dropbacks", (off, "sack_off"), (dfn, "sack_def")))


QB_ROWS = (("EPA a dropback", "qb_epa"), ("Completion % above expected", "qb_cpoe"),
           ("Interceptions, share of dropbacks", "qb_int"), ("Sacked, share of dropbacks", "qb_sack"))


# ── pure computations ────────────────────────────────────────────────────────
def merge(tape_table: dict, deep_table: dict) -> dict[str, dict]:
    """One row of metrics per team from both source tables (they agree on the keys they share)."""
    return {t: {**deep_table.get(t, {}), **tape_table[t]} for t in tape_table}


def ranks(table: dict[str, dict]) -> dict[str, dict[str, int]]:
    """team -> {metric: league rank}, 1 = best (toughest for schedule strength), ties share the better rank."""
    out: dict[str, dict[str, int]] = collections.defaultdict(dict)
    for key, (higher, _) in METRICS.items():
        vals = [(t, v[key]) for t, v in table.items() if v.get(key) is not None]
        for t, v in vals:
            out[t][key] = 1 + sum((o > v) if higher else (o < v) for _, o in vals)
    return out


def position(rank: int, n: int) -> float:
    """x on the dot axis: worst rank at 1, best at n, so further right is always better."""
    return n + 1 - rank


# ── chart ────────────────────────────────────────────────────────────────────
def _dots(ax, title: str, rows, table: dict, rk: dict, n: int, away: str, columns: tuple[str, str]) -> None:
    """``rows``: (label, (team, metric), (team, metric)). One line per row, a dot per item, values on the right."""
    ax.set_facecolor(SURFACE)
    ax.set_xlim(0.2, n + 0.8)
    ax.set_ylim(len(rows) - 0.4, -0.75)
    mid = (n + 1) / 2
    ax.axvline(mid, color="#d8d6d0", linewidth=0.8, linestyle=(0, (2, 2)), zorder=0)
    for c, (text, (team, _)) in enumerate(zip(columns, rows[0][1:])):
        colour = AWAY_COLOUR if team == away else HOME_COLOUR
        ax.text(1.03 + 0.13 * c, -0.75, text, transform=ax.get_yaxis_transform(), ha="left", va="bottom",
                fontsize=7.0, color=colour, fontweight="bold")
    for y, (_, *items) in enumerate(rows):
        ax.plot([1, n], [y, y], color="#ebe9e4", linewidth=5, solid_capstyle="round", zorder=1)
        xs = []
        for c, (team, key) in enumerate(items):
            v, r = table[team].get(key), rk[team].get(key)
            colour = AWAY_COLOUR if team == away else HOME_COLOUR
            ax.text(1.03 + 0.13 * c, y, "n/a" if v is None else METRICS[key][1].format(v),
                    transform=ax.get_yaxis_transform(), ha="left", va="center", fontsize=7.0, color=INK)
            if r is None:
                xs.append(None)
                continue
            xs.append((position(r, n), r, colour))
        placed = [p for p in xs if p is not None]
        if len(placed) == 2:
            ax.plot([placed[0][0], placed[1][0]], [y, y], color=INK_2, linewidth=1.2, zorder=2)
        close = len(placed) == 2 and abs(placed[0][0] - placed[1][0]) < 1.5
        for i, (x, r, colour) in enumerate(placed):
            dy = (-0.13 if i == 0 else 0.13) if close else 0.0
            ax.scatter([x], [y + dy], s=58, color=colour, edgecolor=SURFACE, linewidth=1.0, zorder=4)
            ax.text(x, y + dy + (-0.24 if i == 0 else 0.24), _ord(r), ha="center",
                    va="bottom" if i == 0 else "top", fontsize=6.2, color=colour, zorder=5)
    ax.set_yticks(range(len(rows)), [lab for lab, *_ in rows], fontsize=7.2, color=INK)
    ax.set_xticks([1, mid, n], [f"{_ord(n)} (worst)", "middle", "1st (best)"], fontsize=6.4, color=INK_2)
    ax.tick_params(length=0)
    for side in ax.spines.values():
        side.set_visible(False)
    ax.set_title(title, loc="left", fontsize=8.6, color=INK, fontweight="bold")


def _rec(t: dict) -> str:
    return f"{t['wins']}-{t['losses']}" + (f"-{t['ties']}" if t["ties"] else "")


def profile_chart(away: str, home: str, table: dict, rk: dict, path: Path, context: str) -> bool:
    if away not in table or home not in table:
        return False
    n = len(table)
    a, h = table[away], table[home]
    sizes = [len(rows) for _, rows in PROFILE]
    row_in, gap_in, top_in, margin_in = 0.33, 0.62, 1.15, 2.6
    height = top_in + sum(sizes) * row_in + len(sizes) * gap_in + margin_in
    fig = plt.figure(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    y_in = height - top_in
    for title, rows in PROFILE:
        h_in = len(rows) * row_in
        y_in -= h_in + gap_in * 0.55
        ax = fig.add_axes((0.30, y_in / height, 0.48, h_in / height))
        _dots(ax, title, [(lab, (away, k), (home, k)) for lab, k in rows], table, rk, n, away, (away, home))
        y_in -= gap_in * 0.45
    for i, (team, colour) in enumerate(((away, AWAY_COLOUR), (home, HOME_COLOUR))):
        margin_panel(fig.add_axes((0.07 + i * 0.48, 0.35 / height, 0.42, (margin_in - 0.95) / height)),
                     table[team]["log"], f"{_name(team)} ({_rec(table[team])})", colour)
    exp_a, exp_h = a["pyth_pct"] * a["games"], h["pyth_pct"] * h["games"]
    sub = "\n".join(textwrap.wrap(
        f"Each dot is a team's rank among {n}: further right is better on every row. Values on the right. "
        f"From points scored and allowed, {_name(away)} look like a {exp_a:.1f}-win team "
        f"(actual {a['wins']}) and {_name(home)} a {exp_h:.1f}-win team (actual {h['wins']}). {context}", 120))
    return _finish(fig, path, f"{_name(away)} ({_rec(a)}) at {_name(home)} ({_rec(h)}): the tale of the tape",
                   sub, height)


def matchup_chart(away: str, home: str, table: dict, rk: dict, path: Path, injuries: list[str],
                  line_text: str) -> bool:
    if away not in table or home not in table:
        return False
    n = len(table)
    row_in, gap_in, top_in = 0.36, 0.7, 1.0
    blocks = [(f"{_name(away)} offense vs {_name(home)} defense", _cross(away, home), (f"{away} off", f"{home} def")),
              (f"{_name(home)} offense vs {_name(away)} defense", _cross(home, away), (f"{home} off", f"{away} def"))]
    qa, qh = table[away].get("qb_name") or "?", table[home].get("qb_name") or "?"
    da, dh = table[away].get("qb_dropbacks") or 0, table[home].get("qb_dropbacks") or 0
    blocks.append((f"Quarterbacks: {qa} ({da} dropbacks) vs {qh} ({dh})",
                   [(lab, (away, k), (home, k)) for lab, k in QB_ROWS], (away, home)))
    inj = injuries or ["None listed with 10%+ of their team's targets or carries."]
    inj_in = 0.45 + 0.17 * min(len(inj), 8)
    height = top_in + sum(len(b[1]) for b in blocks) * row_in + len(blocks) * gap_in + inj_in
    fig = plt.figure(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    y_in = height - top_in
    for title, rows, cols in blocks:
        h_in = len(rows) * row_in
        y_in -= h_in + gap_in * 0.55
        ax = fig.add_axes((0.30, y_in / height, 0.48, h_in / height))
        _dots(ax, title, rows, table, rk, n, away, cols)
        y_in -= gap_in * 0.45
    fig.text(0.04, (inj_in - 0.1) / height, "Out, doubtful or questionable (10%+ of their team's targets or carries)",
             fontsize=8.0, color=INK, fontweight="bold", va="top")
    fig.text(0.04, (inj_in - 0.32) / height, "\n".join(inj[:8]), fontsize=7.0, color=INK, va="top", linespacing=1.5)
    sub = "\n".join(textwrap.wrap(
        f"On each row the dot further right has the edge: an offense ranked 5th facing a defense ranked 28th favors "
        f"the offense. Quarterback = most dropbacks in his team's latest game; his whole season with the team. "
        f"{line_text} Descriptive only: none of this beats the betting line.", 120))
    return _finish(fig, path, f"{_name(away)} at {_name(home)}: where each side has the edge", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def load_pbp(season: int) -> list[dict]:
    import pandas as pd

    from model.nfl_market_scenarios import pbp_url

    cols = set(tape.PBP_COLS) | set(deep.PBP_COLS)
    frame = pd.read_csv(pbp_url(season), usecols=lambda c: c in cols, compression="gzip", low_memory=False)
    missing = cols - set(frame.columns)
    if missing:
        raise RuntimeError(f"play-by-play {season} is missing columns: {sorted(missing)}")
    return frame.to_dict("records")


def line_text(away: str, home: str, line: list[tuple]) -> str:
    """'Line: DAL -4.5 (opened -4.5).' from (captured_at, home favored by) captures."""
    if not line:
        return "No line captured this week."
    first, last = line[0][1], line[-1][1]
    fav = home if last >= 0 else away
    sign = 1 if fav == home else -1
    return f"Line: {fav} -{abs(last):g} (opened {fav} {-sign * first:+g})."


def build_all(out_dir: Path, away: str, home: str, season: int = SEASON) -> list[tuple[str, bool]]:
    schedule = tape.load_schedule(season)
    game = next((g for g in schedule if (g["away_team"], g["home_team"]) == (away, home)), None)
    if game is None:
        raise SystemExit(f"no {season} regular-season game {away} at {home}")
    pbp = load_pbp(season)
    table = merge(tape.team_table(pbp, schedule, game["week"]), deep.team_stats(pbp, schedule, game["week"]))
    rk = ranks(table)
    stem = f"{away.lower()}_{home.lower()}"
    out_dir = Path(out_dir)
    ctx = tape.context_line(away, home, game, schedule, season)
    lt = line_text(away, home, deep.load_line(game["game_id"]))
    inj = deep.load_injuries(away, home, season)
    return [(f"nfl_take_dots_{stem}.png", profile_chart(away, home, table, rk, out_dir / f"nfl_take_dots_{stem}.png", ctx)),
            (f"nfl_take_dots_matchup_{stem}.png",
             matchup_chart(away, home, table, rk, out_dir / f"nfl_take_dots_matchup_{stem}.png", inj, lt))]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--away", required=True)
    ap.add_argument("--home", required=True)
    ap.add_argument("--season", type=int, default=SEASON)
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    for name, ok in build_all(Path(args.out_dir), args.away.upper(), args.home.upper(), args.season):
        print(f"{name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
