"""Drive and play stories from CFBD play-by-play, for the CFB pattern-watch page.

Four descriptive stories, evergreen across 2022-2025 FBS play-by-play
(`cfb_drives`, `cfb_plays`), with the 2026 season to date shown as its own row
where a table is per season:

  drive_field_position  what a drive is worth by where it starts
  red_zone              red-zone trips and how they end, per season
  fourth_down           go-for-it attempts and conversions by distance and field zone
  fourth_down_trend     how often teams went for it, per season
  comebacks             how often a team trailing entering the 4th quarter won

Nothing here is a signal or a betting edge; each row shows its sample size.

The tables are large and the database bills by egress, so every query
aggregates in Postgres (GROUP BY) and returns a few thousand rows at most. The
bucketing and percentages live in pure functions below, which take aggregated
rows carrying an optional ``n`` weight (a plain per-drive or per-play row is
``n = 1``), so tests exercise them with synthetic rows and no database.

Definitions worth knowing before quoting a number:

* A drive is scored by the offense's actual score change over the drive
  (``end_offense_score - start_offense_score``), so a touchdown includes the
  try after it rather than an assumed 7. "Net points" also subtracts what the
  defense scored on the drive (pick-sixes, fumble returns).
* Overtime drives (start period 5+), drives that ran out the half or game
  (``END OF HALF``, ``END OF GAME``, ``END OF 4TH QUARTER`` and their TD
  variants), CFBD's ``Uncategorized``/``KICKOFF`` rows, and drives without a
  start spot of 1-99 yards to goal are left out of the drive tables.
* A red-zone trip is a drive with at least one scrimmage snap at the opponent's
  20 or closer. A long touchdown from outside the 20 is not a trip.
* ``down`` being non-null does not mean a play is a scrimmage play: kickoffs,
  PATs and period markers carry ``down = 0`` or a stale down. Fourth-down plays
  are therefore chosen by play type: a run or pass on 4th down (fakes included)
  is a go; punts and field-goal attempts are the alternatives; penalties and
  timeouts are not decisions and are left out. Regulation only.
* A go converts when it gains the distance or scores an offensive touchdown.
* Comebacks use quarter line scores from ``cfb_matchups`` (the game the drives
  link to through ``game_id``): the margin after three quarters, and the final.

    python -m model.cfb_drive_stories [--out-dir artifacts/cfb_pattern_watch/charts]
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Iterable

from model.cfb_pattern_charts import GRID, INK, INK_2, SERIES, _figure, _save, _style

POOLED_FIRST, POOLED_LAST = 2022, 2025
POOLED_LABEL = "2022-25"
CURRENT_SEASON = 2026

# Field-position buckets by drive start, yards to the opponent's goal line.
FIELD_BUCKETS: list[tuple[str, int, int]] = [
    ("Own 1-20", 80, 99),
    ("Own 21-40", 60, 79),
    ("Own 41-midfield", 50, 59),
    ("Opp 49-30", 30, 49),
    ("Opp 29-21", 21, 29),
    ("Red zone (20-1)", 1, 20),
]

EXCLUDED_RESULTS = frozenset({
    "END OF HALF", "END OF GAME", "END OF 4TH QUARTER", "END OF HALF TD", "END OF GAME TD",
    "Uncategorized", "KICKOFF",
})
TURNOVER_RESULTS = frozenset({"INT", "FUMBLE", "INT TD", "FUMBLE RETURN TD", "FUMBLE TD"})

# Fourth-down decisions, by CFBD play type.
GO_TYPES = frozenset({
    "Rush", "Pass Reception", "Pass Incompletion", "Pass Completion", "Sack",
    "Rushing Touchdown", "Passing Touchdown",
    "Pass Interception Return", "Interception", "Interception Return Touchdown",
    "Fumble Recovery (Own)", "Fumble Recovery (Opponent)", "Fumble", "Fumble Return Touchdown", "Safety",
})
PUNT_TYPES = frozenset({"Punt", "Punt Return", "Blocked Punt", "Blocked Punt Touchdown", "Punt Return Touchdown"})
FG_TYPES = frozenset({
    "Field Goal Good", "Field Goal Missed", "Blocked Field Goal", "Blocked Field Goal Touchdown",
    "Missed Field Goal Return",
})
OFFENSIVE_TD_TYPES = frozenset({"Rushing Touchdown", "Passing Touchdown"})
TURNOVER_GO_TYPES = frozenset({
    "Pass Interception Return", "Interception", "Interception Return Touchdown",
    "Fumble Recovery (Opponent)", "Fumble Return Touchdown", "Safety",
})
# Scrimmage snaps used to decide whether a drive reached the red zone.
SCRIMMAGE_TYPES = GO_TYPES | PUNT_TYPES | FG_TYPES

DISTANCE_BUCKETS: list[tuple[str, int, int]] = [("4th & 1", 1, 1), ("4th & 2-3", 2, 3), ("4th & 4-6", 4, 6), ("4th & 7+", 7, 99)]
FIELD_ZONES: list[tuple[str, int, int]] = [
    ("Own territory", 51, 99),
    ("Opp 50-36", 36, 50),
    ("Opp 35-21", 21, 35),
    ("Red zone (20-1)", 1, 20),
]
ALL_ZONES = "All field"

DEFICIT_BUCKETS: list[tuple[str, int, int]] = [("1-3", 1, 3), ("4-8", 4, 8), ("9-16", 9, 16), ("17+", 17, 999)]


# ---------------------------------------------------------------- helpers

def _n(row: dict) -> int:
    return int(row.get("n", 1) or 0)


def _pct(num: float, den: float) -> float | None:
    return round(100.0 * num / den, 1) if den else None


def _bucket(value: int | None, buckets: list[tuple[str, int, int]]) -> str | None:
    if value is None:
        return None
    for name, lo, hi in buckets:
        if lo <= value <= hi:
            return name
    return None


def season_label(season: int) -> str:
    return f"{season} (to date)" if season >= CURRENT_SEASON else str(season)


def _pooled(rows: Iterable[dict]) -> list[dict]:
    return [r for r in rows if POOLED_FIRST <= int(r["season"]) <= POOLED_LAST]


def drive_included(row: dict) -> bool:
    """A regulation drive with a real start spot and a result that is an offensive outcome."""
    start = row.get("start_yards_to_goal")
    if start is None or not 1 <= int(start) <= 99:
        return False
    if row.get("start_period") is not None and int(row["start_period"]) > 4:
        return False
    return (row.get("drive_result") or "Uncategorized") not in EXCLUDED_RESULTS


def classify_fourth_down(play_type: str | None) -> str | None:
    """'go', 'punt', 'fg', or None when the play is not a fourth-down decision."""
    if play_type in GO_TYPES:
        return "go"
    if play_type in PUNT_TYPES:
        return "punt"
    if play_type in FG_TYPES:
        return "fg"
    return None


def go_converted(play_type: str | None, gained_distance: bool) -> bool:
    if play_type in OFFENSIVE_TD_TYPES:
        return True
    if play_type in TURNOVER_GO_TYPES:
        return False
    return bool(gained_distance)


# ---------------------------------------------------------------- stories

def points_by_field_position(drives: Iterable[dict], seasons_label: str = POOLED_LABEL) -> list[dict]:
    """Drive value by starting spot. Rows: season, start_yards_to_goal, drive_result,
    offense_points (sum over the row's drives), defense_points, n; start_period optional."""
    acc = {name: defaultdict(float) for name, _, _ in FIELD_BUCKETS}
    for d in drives:
        if not drive_included(d):
            continue
        name = _bucket(int(d["start_yards_to_goal"]), FIELD_BUCKETS)
        if name is None:
            continue
        a, n, result = acc[name], _n(d), d.get("drive_result")
        a["drives"] += n
        a["td"] += n if result == "TD" else 0
        a["fg"] += n if result == "FG" else 0
        a["punt"] += n if result == "PUNT" else 0
        a["to"] += n if result in TURNOVER_RESULTS else 0
        a["pts"] += float(d.get("offense_points") or 0)
        a["dpts"] += float(d.get("defense_points") or 0)
    out = []
    for name, _, _ in FIELD_BUCKETS:
        a = acc[name]
        drives = a["drives"]
        out.append({
            "Seasons": seasons_label,
            "Start": name,
            "Drives": int(drives),
            "TD (%)": _pct(a["td"], drives),
            "FG (%)": _pct(a["fg"], drives),
            "Punt (%)": _pct(a["punt"], drives),
            "Turnover (%)": _pct(a["to"], drives),
            "Points per drive": round(a["pts"] / drives, 2) if drives else None,
            "Net points per drive": round((a["pts"] - a["dpts"]) / drives, 2) if drives else None,
        })
    return out


def red_zone(drives: Iterable[dict]) -> list[dict]:
    """Red-zone trips per season. Rows as for points_by_field_position plus reached_red_zone."""
    acc: dict[int, defaultdict] = defaultdict(lambda: defaultdict(float))
    for d in drives:
        if not drive_included(d) or not d.get("reached_red_zone"):
            continue
        a, n, result = acc[int(d["season"])], _n(d), d.get("drive_result")
        a["trips"] += n
        a["td"] += n if result == "TD" else 0
        a["fg"] += n if result == "FG" else 0
        a["pts"] += float(d.get("offense_points") or 0)
    out = []
    for season in sorted(acc):
        a = acc[season]
        trips = a["trips"]
        out.append({
            "Season": season_label(season),
            "Trips": int(trips),
            "TD (%)": _pct(a["td"], trips),
            "FG (%)": _pct(a["fg"], trips),
            "No points (%)": _pct(trips - a["td"] - a["fg"], trips),
            "Points per trip": round(a["pts"] / trips, 2) if trips else None,
        })
    return out


def _fourth_rows(plays: Iterable[dict]):
    """(season, distance bucket, zone, decision, converted, n) for each fourth-down decision row."""
    for p in plays:
        if p.get("period") is not None and int(p["period"]) > 4:
            continue
        decision = classify_fourth_down(p.get("play_type"))
        dist = _bucket(p.get("distance"), DISTANCE_BUCKETS)
        zone = _bucket(p.get("yards_to_goal"), FIELD_ZONES)
        if decision is None or dist is None or zone is None:
            continue
        conv = decision == "go" and go_converted(p.get("play_type"), bool(p.get("gained_distance")))
        yield int(p["season"]), dist, zone, decision, conv, _n(p)


def fourth_down(plays: Iterable[dict], seasons_label: str = POOLED_LABEL) -> list[dict]:
    """Fourth-down decisions by distance and field zone, plus an 'All field' row per distance.
    Rows: season, play_type, distance, yards_to_goal, gained_distance (bool), n; period optional."""
    acc: dict[tuple[str, str], defaultdict] = defaultdict(lambda: defaultdict(float))
    for _season, dist, zone, decision, conv, n in _fourth_rows(plays):
        for z in (zone, ALL_ZONES):
            a = acc[(dist, z)]
            a["decisions"] += n
            if decision == "go":
                a["go"] += n
                a["conv"] += n if conv else 0
    out = []
    for dist, _, _ in DISTANCE_BUCKETS:
        for zone in [ALL_ZONES] + [z for z, _, _ in FIELD_ZONES]:
            a = acc.get((dist, zone))
            if not a:
                continue
            out.append({
                "Seasons": seasons_label,
                "Distance": dist,
                "Field zone": zone,
                "Fourth downs": int(a["decisions"]),
                "Went for it": int(a["go"]),
                "Go rate (%)": _pct(a["go"], a["decisions"]),
                "Converted": int(a["conv"]),
                "Conversion (%)": _pct(a["conv"], a["go"]),
            })
    return out


def fourth_down_trend(plays: Iterable[dict]) -> list[dict]:
    """How often teams went for it, per season (the analytics-era question)."""
    acc: dict[int, defaultdict] = defaultdict(lambda: defaultdict(float))
    for season, dist, _zone, decision, conv, n in _fourth_rows(plays):
        a = acc[season]
        short = dist in ("4th & 1", "4th & 2-3")
        a["decisions"] += n
        a["short"] += n if short else 0
        if decision == "go":
            a["go"] += n
            a["short_go"] += n if short else 0
            a["conv"] += n if conv else 0
    return [{
        "Season": season_label(s),
        "Fourth downs": int(acc[s]["decisions"]),
        "Went for it": int(acc[s]["go"]),
        "Go rate (%)": _pct(acc[s]["go"], acc[s]["decisions"]),
        "Short go rate (%)": _pct(acc[s]["short_go"], acc[s]["short"]),
        "Conversion (%)": _pct(acc[s]["conv"], acc[s]["go"]),
    } for s in sorted(acc)]


def comebacks(games: Iterable[dict], seasons_label: str = POOLED_LABEL) -> list[dict]:
    """Win rate of the team trailing entering the 4th quarter, by deficit.
    Rows: margin_after_3 (home minus away through three quarters), home_won (bool),
    went_to_overtime (bool, optional), n. Games tied after three quarters are left out."""
    acc = {name: defaultdict(float) for name, _, _ in DEFICIT_BUCKETS}
    for g in games:
        margin = g.get("margin_after_3")
        if margin is None or int(margin) == 0 or g.get("home_won") is None:
            continue
        margin = int(margin)
        name = _bucket(abs(margin), DEFICIT_BUCKETS)
        trailing_won = bool(g["home_won"]) if margin < 0 else not bool(g["home_won"])
        a, n = acc[name], _n(g)
        a["games"] += n
        a["won"] += n if trailing_won else 0
        a["ot"] += n if g.get("went_to_overtime") else 0
    out = []
    for name, _, _ in DEFICIT_BUCKETS:
        a = acc[name]
        out.append({
            "Seasons": seasons_label,
            "Deficit entering 4th": name,
            "Games": int(a["games"]),
            "Trailing team won": int(a["won"]),
            "Comeback (%)": _pct(a["won"], a["games"]),
            "Went to overtime (%)": _pct(a["ot"], a["games"]),
        })
    return out


# ---------------------------------------------------------------- loader

_DRIVES_SQL = """
WITH rz AS (
    SELECT cfbd_drive_id, MIN(yards_to_goal) AS min_ytg
    FROM cfb_plays
    WHERE play_type = ANY(%(scrimmage)s) AND yards_to_goal BETWEEN 1 AND 99
    GROUP BY cfbd_drive_id
)
SELECT d.season, d.start_yards_to_goal, d.drive_result,
       (COALESCE(rz.min_ytg, 100) <= 20) AS reached_red_zone,
       COUNT(*) AS n,
       SUM(GREATEST(COALESCE(d.end_offense_score - d.start_offense_score, 0), 0)) AS offense_points,
       SUM(GREATEST(COALESCE(d.end_defense_score - d.start_defense_score, 0), 0)) AS defense_points
FROM cfb_drives d
LEFT JOIN rz ON rz.cfbd_drive_id = d.cfbd_drive_id
WHERE d.start_period BETWEEN 1 AND 4
  AND d.start_yards_to_goal BETWEEN 1 AND 99
  AND NOT (d.drive_result = ANY(%(excluded)s))
GROUP BY 1, 2, 3, 4
"""

# yards_to_goal is sent as the low edge of its 5-yard band (1, 6, 11, ...): every
# field-zone edge (20, 35, 50) is a band edge, and it cuts the rows returned.
_FOURTH_SQL = """
SELECT season, play_type, LEAST(distance, 7) AS distance,
       ((yards_to_goal - 1) / 5) * 5 + 1 AS yards_to_goal,
       (yards_gained >= distance) AS gained_distance,
       COUNT(*) AS n
FROM cfb_plays
WHERE down = 4 AND period BETWEEN 1 AND 4
  AND distance >= 1 AND yards_to_goal BETWEEN 1 AND 99
  AND play_type = ANY(%(types)s)
GROUP BY 1, 2, 3, 4, 5
"""

# Quarter line scores of every game the play-by-play covers.
_GAMES_SQL = """
SELECT m.season,
       ((m.home_line_scores->>0)::int + (m.home_line_scores->>1)::int + (m.home_line_scores->>2)::int)
     - ((m.away_line_scores->>0)::int + (m.away_line_scores->>1)::int + (m.away_line_scores->>2)::int) AS margin_after_3,
       (m.home_score > m.away_score) AS home_won,
       COALESCE(m.went_to_overtime, false) AS went_to_overtime,
       COUNT(*) AS n
FROM cfb_matchups m
WHERE m.id IN (SELECT DISTINCT game_id FROM cfb_drives WHERE season BETWEEN %(first)s AND %(last)s)
  AND m.completed AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
  AND m.home_score <> m.away_score
  AND jsonb_array_length(m.home_line_scores) >= 3 AND jsonb_array_length(m.away_line_scores) >= 3
GROUP BY 1, 2, 3, 4
"""


def _plain(rows) -> list[dict]:
    out = []
    for r in rows:
        d = dict(r)
        for k, v in d.items():
            if v is not None and not isinstance(v, (bool, int, float, str)):
                d[k] = float(v)   # Decimal sums
        out.append(d)
    return out


def load_drive_rows(db) -> list[dict]:
    rows = _plain(db.execute(_DRIVES_SQL, {"scrimmage": sorted(SCRIMMAGE_TYPES), "excluded": sorted(EXCLUDED_RESULTS)}))
    for r in rows:
        r["start_period"] = 1   # the query already restricted to regulation
    return rows


def load_fourth_down_rows(db) -> list[dict]:
    return _plain(db.execute(_FOURTH_SQL, {"types": sorted(GO_TYPES | PUNT_TYPES | FG_TYPES)}))


def load_game_rows(db, first: int = POOLED_FIRST, last: int = POOLED_LAST) -> list[dict]:
    return _plain(db.execute(_GAMES_SQL, {"first": first, "last": last}))


def build_tables(db) -> dict[str, list[dict]]:
    """Every drive-story table, 2022-25 pooled (per-season tables also carry 2026 to date)."""
    drives = load_drive_rows(db)
    fourth = load_fourth_down_rows(db)
    games = load_game_rows(db)
    return {
        "drive_field_position": points_by_field_position(_pooled(drives)),
        "red_zone": red_zone(drives),
        "fourth_down": fourth_down(_pooled(fourth)),
        "fourth_down_trend": fourth_down_trend(fourth),
        "comebacks": comebacks(games),
    }


# ---------------------------------------------------------------- charts

def _bar_labels(ax, xs, values, fmt, offset=3):
    for x, v in zip(xs, values):
        if v is not None:
            ax.annotate(fmt(v), (x, v), xytext=(0, offset), textcoords="offset points",
                        ha="center", fontsize=7.5, color=INK)


def field_position_chart(rows: list[dict], path: Path) -> bool:
    rows = [r for r in rows if r["Drives"]]
    if not rows:
        return False
    xs = list(range(len(rows)))
    pts = [r["Points per drive"] for r in rows]
    fig, ax = _figure(3.9)
    ax.bar(xs, pts, color=SERIES[0], width=0.6)
    _bar_labels(ax, xs, pts, lambda v: f"{v:.2f}")
    ax.set_xticks(xs, [f"{r['Start']}\nTD {r['TD (%)']:.0f}% · {r['Drives']:,}" for r in rows], fontsize=7, color=INK)
    ax.margins(y=0.15)
    ax.set_ylabel("points per drive", color=INK_2, fontsize=8)
    total = sum(r["Drives"] for r in rows)
    _style(ax, "Where a drive starts decides most of what it is worth",
           f"FBS regulation drives, {rows[0]['Seasons']} ({total:,} drives). Points include the try after a TD.")
    _save(fig, path)
    return True


def comeback_chart(rows: list[dict], path: Path) -> bool:
    rows = [r for r in rows if r["Games"]]
    if not rows:
        return False
    xs = list(range(len(rows)))
    won = [r["Comeback (%)"] for r in rows]
    fig, ax = _figure(3.8)
    ax.bar(xs, won, color=SERIES[1], width=0.6)
    _bar_labels(ax, xs, won, lambda v: f"{v:.1f}%")
    ax.set_xticks(xs, [f"Down {r['Deficit entering 4th']}\n{r['Trailing team won']} of {r['Games']:,}" for r in rows],
                  fontsize=7.5, color=INK)
    ax.margins(y=0.15)
    ax.set_ylabel("trailing team won, %", color=INK_2, fontsize=8)
    total = sum(r["Games"] for r in rows)
    _style(ax, "How often the team trailing after three quarters wins",
           f"FBS games, {rows[0]['Seasons']} ({total:,} games not tied entering the 4th). Overtime wins count.")
    _save(fig, path)
    return True


def fourth_down_chart(rows: list[dict], path: Path) -> bool:
    """Go rate and conversion rate by distance, whole field."""
    rows = [r for r in rows if r["Field zone"] == ALL_ZONES and r["Fourth downs"]]
    if not rows:
        return False
    xs = list(range(len(rows)))
    go = [r["Go rate (%)"] or 0.0 for r in rows]
    conv = [r["Conversion (%)"] or 0.0 for r in rows]
    w = 0.38
    fig, ax = _figure(3.8)
    ax.bar([x - w / 2 for x in xs], go, width=w, color=SERIES[0], label="Went for it")
    ax.bar([x + w / 2 for x in xs], conv, width=w, color=SERIES[2], label="Converted when they went")
    _bar_labels(ax, [x - w / 2 for x in xs], go, lambda v: f"{v:.0f}%")
    _bar_labels(ax, [x + w / 2 for x in xs], conv, lambda v: f"{v:.0f}%")
    ax.set_xticks(xs, [f"{r['Distance']}\n{r['Went for it']:,} tries" for r in rows], fontsize=7.5, color=INK)
    ax.set_ylim(0, 100)
    ax.set_ylabel("percent", color=INK_2, fontsize=8)
    ax.legend(loc="upper right", fontsize=7.5, frameon=False, labelcolor=INK)
    total = sum(r["Fourth downs"] for r in rows)
    _style(ax, "Fourth down: how often teams go, and how often it works",
           f"FBS regulation 4th downs, {rows[0]['Seasons']} ({total:,} decisions: go, punt or field goal).")
    _save(fig, path)
    return True


def fourth_down_trend_chart(rows: list[dict], path: Path) -> bool:
    rows = [r for r in rows if r["Fourth downs"]]
    if not rows:
        return False
    xs = list(range(len(rows)))
    go = [r["Go rate (%)"] for r in rows]
    short = [r["Short go rate (%)"] for r in rows]
    fig, ax = _figure(3.6)
    ax.plot(xs, short, color=SERIES[1], linewidth=2, marker="o", markersize=4, label="4th & 1-3")
    ax.plot(xs, go, color=SERIES[0], linewidth=2, marker="o", markersize=4, label="All 4th downs")
    _bar_labels(ax, xs, short, lambda v: f"{v:.1f}%", 5)
    _bar_labels(ax, xs, go, lambda v: f"{v:.1f}%", 5)
    ax.set_xticks(xs, [f"{r['Season']}\n{r['Fourth downs']:,}" for r in rows], fontsize=7.5, color=INK)
    ax.set_ylim(0, max(v for v in short + go if v is not None) * 1.25)
    ax.set_ylabel("went for it, %", color=INK_2, fontsize=8)
    ax.legend(loc="lower right", fontsize=7.5, frameon=False, labelcolor=INK)
    ax.grid(axis="y", color=GRID, linewidth=0.6)
    _style(ax, "Are college coaches going for it more?",
           "Share of FBS regulation 4th downs where the offense ran or passed instead of kicking. "
           "Count of 4th downs under each season; a current season is to date.")
    _save(fig, path)
    return True


def write_charts(tables: dict[str, list[dict]], out_dir: Path) -> list[Path]:
    made = []
    for name, fn, key in (
        ("drive_field_position.png", field_position_chart, "drive_field_position"),
        ("fourth_down.png", fourth_down_chart, "fourth_down"),
        ("fourth_down_trend.png", fourth_down_trend_chart, "fourth_down_trend"),
        ("comebacks.png", comeback_chart, "comebacks"),
    ):
        if fn(tables.get(key, []), out_dir / name):
            made.append(out_dir / name)
    return made


def main() -> None:
    from config import load_config
    from db.database import DatabaseManager

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out-dir", type=Path, default=Path("artifacts") / "cfb_pattern_watch" / "charts")
    args = ap.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    tables = build_tables(db)
    for key, rows in tables.items():
        print(f"\n{key} ({len(rows)} rows)")
        for r in rows:
            print("  ", r)
    for p in write_charts(tables, args.out_dir):
        print("wrote", p)


if __name__ == "__main__":
    main()
