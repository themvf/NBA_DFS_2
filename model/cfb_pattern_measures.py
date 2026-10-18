"""Instrument measurements for the CFB pattern watch (`cfb-pattern-measures-v1`).

All four are DESCRIPTIVE. None gates a verdict, none changes a trigger, and
none touches how a ledger row is graded. They exist so the study can see its
own instrument:

1. weekly_base_rates   the effect the favourite triggers rest on, measured
                       across EVERY FBS game, week by week: favourite
                       straight-up rate minus the vig-free implied rate,
                       favourite ATS, overs, and the share of games decided by
                       7 or fewer. If the excess sits near zero, T1/T5/T6
                       should be expected to drift toward their kill criteria.
2. game_counts         independent games behind the rows. One game can feed
                       up to three triggers; rows overstate the evidence.
3. freeze_vs_close     the publication record (frozen selection) against the
                       verified close: points and price moved toward or away
                       from the pick, and whether grading at the frozen line
                       would have changed the result.
4. board events        stale refusals and late-move flags, appended once per
                       (trigger, game, status), plus one line per board run so
                       a run that flagged nothing is recorded as zero rather
                       than as silence.
"""

from __future__ import annotations

import json
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from model import cfb_pattern_watch as pw

MEASURES_VERSION = "cfb-pattern-measures-v1"
BASE = Path("artifacts") / "cfb_pattern_watch"
EVENTS_PATH = BASE / "events.jsonl"
RUNS_PATH = BASE / "board_runs.jsonl"
BASE_RATES_PATH = BASE / "base_rates.json"

_BASE_SQL = """
SELECT m.id AS matchup_id, m.week, m.season_type, m.home_score, m.away_score,
       COALESCE(h.home_spread, m.home_spread) AS home_spread,
       COALESCE(h.home_ml, m.home_ml) AS home_ml,
       COALESCE(h.away_ml, m.away_ml) AS away_ml,
       COALESCE(h.vegas_total, m.vegas_total) AS vegas_total,
       ht.classification AS hclass, at.classification AS aclass
FROM cfb_matchups m
JOIN cfb_teams ht ON ht.team_id = m.home_team_id
JOIN cfb_teams at ON at.team_id = m.away_team_id
LEFT JOIN event_closing_lines e ON e.sport = 'cfb' AND e.matchup_id = m.id AND e.quality IN ('A', 'B')
LEFT JOIN game_odds_history h ON h.id = e.history_id
WHERE m.season = %s AND m.completed AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
"""


# ── 1. weekly base rates ─────────────────────────────────────────────────────
def weekly_base_rates(games: list[dict]) -> list[dict]:
    """Per CFBD week, FBS-vs-FBS games with a spread and both moneylines."""
    by_week: dict[int, list[dict]] = defaultdict(list)
    for g in games:
        if g.get("hclass") != "fbs" or g.get("aclass") != "fbs":
            continue
        if g.get("home_spread") is None or g.get("home_ml") is None or g.get("away_ml") is None:
            continue
        if (g.get("season_type") or "regular") != "regular":
            continue
        by_week[int(g["week"])].append(g)
    out = []
    for week in sorted(by_week):
        gs = by_week[week]
        su, implied, ats, over = [], [], [], []
        close = 0
        for g in gs:
            sp = g["home_spread"]
            home_fav = sp < 0 if sp != 0 else g["home_ml"] < g["away_ml"]
            margin = g["home_score"] - g["away_score"]
            fm = margin if home_fav else -margin
            p = pw.vig_free_home(g["home_ml"], g["away_ml"])
            implied.append(p if home_fav else 1 - p)
            if fm != 0:
                su.append(fm > 0)
            cover = fm - abs(sp)
            if cover != 0:
                ats.append(cover > 0)
            if g.get("vegas_total") is not None:
                tot = g["home_score"] + g["away_score"]
                if tot != g["vegas_total"]:
                    over.append(tot > g["vegas_total"])
            close += abs(margin) <= 7
        fav_su = sum(su) / len(su) if su else None
        imp = statistics.mean(implied)
        out.append({
            "week": week, "games": len(gs),
            "fav_su": fav_su, "implied": imp,
            "excess_pp": None if fav_su is None else (fav_su - imp) * 100,
            "fav_ats": sum(ats) / len(ats) if ats else None,
            "over": sum(over) / len(over) if over else None,
            "within_7": close / len(gs),
        })
    return out


# Moneyline-favorite splits: descriptive, whole-market, not study triggers.
# Each split is (label, rule over a game dict). "Total" is the closing O/U.
MARKET_SPLITS = (
    ("All moneyline favorites", lambda g: True),
    ("Total 45 or higher", lambda g: g.get("vegas_total") is not None and float(g["vegas_total"]) >= 45.0),
    ("Spread 2.5 or less", lambda g: abs(float(g["home_spread"])) <= 2.5),
    ("Spread 7 to 13.5", lambda g: 7.0 <= abs(float(g["home_spread"])) <= 13.5),
    ("Spread 14 or more", lambda g: abs(float(g["home_spread"])) >= 14.0),
)


def _ml_eligible(games: list[dict]) -> list[tuple]:
    """(game, favorite won, favorite decimal, favorite vig-free prob, underdog decimal).

    FBS-vs-FBS regular season with a spread and both moneylines. The favorite
    is the side with the shorter moneyline; a game priced level on both sides
    has no favorite and is skipped, as is a tied final.
    """
    eligible = []
    for g in games:
        if g.get("hclass") != "fbs" or g.get("aclass") != "fbs":
            continue
        if g.get("home_spread") is None or g.get("home_ml") is None or g.get("away_ml") is None:
            continue
        if (g.get("season_type") or "regular") != "regular":
            continue
        dh, da = pw.decimal_odds(g["home_ml"]), pw.decimal_odds(g["away_ml"])
        if dh == da:
            continue
        margin = g["home_score"] - g["away_score"]
        if margin == 0:
            continue
        home_fav = dh < da
        p_home = pw.vig_free_home(g["home_ml"], g["away_ml"])
        won = (margin > 0) == home_fav
        price = dh if home_fav else da
        dog_price = da if home_fav else dh
        eligible.append((g, won, price, p_home if home_fav else 1 - p_home, dog_price))
    return eligible


def _ml_row(label: str, picked: list[tuple]) -> dict:
    """One unit on the favorite and one on the underdog in the same games, at the close."""
    n = len(picked)
    wins = sum(1 for _, won, *_ in picked if won)
    units = sum((price - 1) if won else -1.0 for _, won, price, _, _ in picked)
    dog_units = sum(-1.0 if won else (dog - 1) for _, won, _, _, dog in picked)
    imp = statistics.mean(i for *_, i, _ in picked) if picked else None
    win_rate = wins / n if n else None
    return {
        "split": label, "games": n, "wins": wins, "losses": n - wins,
        "win_rate": win_rate, "implied": imp,
        "excess_pp": None if win_rate is None else (win_rate - imp) * 100,
        "units": units, "roi": units / n if n else None,
        "median_price": statistics.median(p for _, _, p, _, _ in picked) if picked else None,
        "dog_units": dog_units, "dog_roi": dog_units / n if n else None,
        "dog_median_price": statistics.median(d for *_, d in picked) if picked else None,
    }


def market_splits(games: list[dict], splits=MARKET_SPLITS) -> list[dict]:
    """Moneyline favorites' and underdogs' record and flat-unit ROI at the closing moneyline.

    Same population as weekly_base_rates. One unit on the favorite: a win pays
    its closing decimal price minus 1, a loss costs 1; the underdog the reverse.
    """
    eligible = _ml_eligible(games)
    return [_ml_row(label, [e for e in eligible if rule(e[0])]) for label, rule in splits]


# Bands partition every eligible game: each one lands in exactly one spread band
# and, when it has a total, exactly one total band. CFB lines are in half points.
SPREAD_BANDS = (
    ("Spread 0 to 2.5", lambda s: s <= 2.5),
    ("Spread 3 to 6.5", lambda s: 2.5 < s <= 6.5),
    ("Spread 7 to 13.5", lambda s: 6.5 < s <= 13.5),
    ("Spread 14 or more", lambda s: s > 13.5),
)
TOTAL_BANDS = (
    ("Total under 45", lambda t: t < 45.0),
    ("Total 45 to 54.5", lambda t: 45.0 <= t < 55.0),
    ("Total 55 or more", lambda t: t >= 55.0),
)


def _band(value: float | None, bands) -> str | None:
    if value is None:
        return None
    return next((label for label, rule in bands if rule(float(value))), None)


def market_breakdown(games: list[dict]) -> dict:
    """Favorites and underdogs at the closing moneyline, from simplest cut to the most detailed.

    overall          every eligible game
    by_spread        one row per spread band (the bands add up to overall)
    by_total         one row per total band (games without a total are left out and counted)
    by_spread_total  every spread band crossed with every total band
    """
    eligible = _ml_eligible(games)
    tagged = [(_band(abs(float(e[0]["home_spread"])), SPREAD_BANDS), _band(e[0].get("vegas_total"), TOTAL_BANDS), e)
              for e in eligible]
    cross = []
    for s_label, _ in SPREAD_BANDS:
        for t_label, _ in TOTAL_BANDS:
            row = _ml_row(f"{s_label}, {t_label.lower()}", [e for s, t, e in tagged if s == s_label and t == t_label])
            cross.append({**row, "spread_band": s_label, "total_band": t_label})
    return {
        "overall": _ml_row("All games", eligible),
        "by_spread": [{**_ml_row(lbl, [e for s, _, e in tagged if s == lbl]), "spread_band": lbl} for lbl, _ in SPREAD_BANDS],
        "by_total": [{**_ml_row(lbl, [e for _, t, e in tagged if t == lbl]), "total_band": lbl} for lbl, _ in TOTAL_BANDS],
        "by_spread_total": cross,
        "missing_total": sum(1 for _, t, _ in tagged if t is None),
    }


def load_base_rate_games(db) -> list[dict]:
    return [dict(r) for r in db.execute(_BASE_SQL, (pw.SEASON,))]


def write_base_rates(rates: list[dict], path: Path = BASE_RATES_PATH, now: datetime | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"measures_version": MEASURES_VERSION,
                                "generated_at": now.isoformat() if now else None,
                                "weeks": rates}, indent=2), encoding="utf-8")


def read_base_rates(path: Path = BASE_RATES_PATH) -> list[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8")).get("weeks", [])
    except (OSError, ValueError):
        return []


def format_base_rates(rates: list[dict]) -> list[str]:
    lines = ["  week games  fav SU  implied  excess  fav ATS  over  within 7"]
    for r in rates:
        f = lambda v: "   -  " if v is None else f"{v:6.1%}"
        ex = "   -  " if r["excess_pp"] is None else f"{r['excess_pp']:+6.1f}"
        lines.append(f"  {r['week']:>4} {r['games']:>5}  {f(r['fav_su'])}  {f(r['implied'])}  {ex}  {f(r['fav_ats'])}  {f(r['over'])}  {f(r['within_7'])}")
    return lines


# ── 2. independent games ─────────────────────────────────────────────────────
def game_counts(rows: list[dict]) -> dict:
    by_game: dict[int, set] = defaultdict(set)
    for r in rows:
        by_game[int(r["matchup_id"])].add(r["trigger"])
    per_trigger = defaultdict(set)
    for r in rows:
        per_trigger[r["trigger"]].add(int(r["matchup_id"]))
    multi = sum(1 for t in by_game.values() if len(t) > 1)
    return {
        "rows": len(rows),
        "games": len(by_game),
        "games_with_multiple_triggers": multi,
        "max_triggers_per_game": max((len(t) for t in by_game.values()), default=0),
        "per_trigger_games": {t: len(g) for t, g in per_trigger.items()},
    }


# ── 3. frozen selection vs close ─────────────────────────────────────────────
def _points_clv(trigger: str, freeze: dict, close: dict) -> float | None:
    """Points gained (+) or given up (-) by the frozen line relative to the close, from the pick's side."""
    if trigger in ("T2_fcs_fav_spread", "T6_mid_fav_spread"):
        if freeze.get("home_spread") is None or close.get("home_spread") is None:
            return None
        return abs(close["home_spread"]) - abs(freeze["home_spread"])  # laid fewer points than the close: +
    if trigger == "T3_small_fav_dog_spread":
        if freeze.get("home_spread") is None or close.get("home_spread") is None:
            return None
        return abs(freeze["home_spread"]) - abs(close["home_spread"])  # took more points than the close: +
    if trigger == "T4_big_fav_over":
        if freeze.get("vegas_total") is None or close.get("vegas_total") is None:
            return None
        return close["vegas_total"] - freeze["vegas_total"]  # bought the over lower than the close: +
    return None  # moneyline: price only


def freeze_vs_close(rows: list[dict], selections: list[dict]) -> dict:
    graded = {(r["trigger"], int(r["matchup_id"])): r for r in rows}
    items = []
    for s in selections:
        r = graded.get((s["trigger"], int(s["matchup_id"])))
        if r is None:
            continue
        frozen_game = dict(r, home_spread=s["home_spread"], vegas_total=s["vegas_total"],
                           home_ml=s["home_ml"], away_ml=s["away_ml"])
        fg = pw.grade(s["trigger"], frozen_game)
        br = s.get("best_retail") or {}
        br_pnl = None
        if fg["result"] == "won" and br.get("price") is not None:
            br_pnl = round(pw.decimal_odds(br["price"]) - 1, 4)
        elif fg["result"] == "lost":
            br_pnl = -1.0
        elif fg["result"] == "push":
            br_pnl = 0.0
        lead_min = None
        try:
            lead_min = round((datetime.fromisoformat(s["commence_time"]) - datetime.fromisoformat(s["frozen_at"])).total_seconds() / 60)
        except (KeyError, TypeError, ValueError):
            pass
        items.append({
            "trigger": s["trigger"], "matchup_id": int(s["matchup_id"]), "away": s["away"], "home": s["home"],
            "selection": s["selection"], "lead_minutes": lead_min,
            "freeze_spread": s["home_spread"], "close_spread": r["home_spread"],
            "freeze_total": s["vegas_total"], "close_total": r["vegas_total"],
            "points_clv": _points_clv(s["trigger"], s, r),
            "price_clv": round(s["consensus_decimal"] / r["price"] - 1, 4) if s.get("consensus_decimal") and r.get("price") else None,
            "close_result": r["result"], "close_pnl": r["pnl_units"],
            "freeze_result": fg["result"], "freeze_pnl": fg["pnl_units"],
            "best_retail_book": br.get("book"), "best_retail_pnl": br_pnl,
        })
    pts = [i["points_clv"] for i in items if i["points_clv"] is not None]
    prc = [i["price_clv"] for i in items if i["price_clv"] is not None]
    leads = [i["lead_minutes"] for i in items if i["lead_minutes"] is not None]
    summary = {
        "n": len(items),
        "result_changed": sum(i["freeze_result"] != i["close_result"] for i in items),
        "mean_points_clv": statistics.mean(pts) if pts else None,
        "points_better": sum(p > 0 for p in pts), "points_worse": sum(p < 0 for p in pts),
        "mean_price_clv": statistics.mean(prc) if prc else None,
        "median_lead_minutes": statistics.median(leads) if leads else None,
        "close_units": sum(i["close_pnl"] for i in items),
        "freeze_units": sum(i["freeze_pnl"] for i in items),
        "best_retail_units": sum(i["best_retail_pnl"] for i in items if i["best_retail_pnl"] is not None),
    }
    return {"summary": summary, "items": items}


# ── 4. board events ──────────────────────────────────────────────────────────
EVENT_STATUSES = ("stale", "watch", "dropped")


def read_event_keys(path: Path = EVENTS_PATH) -> set[tuple[str, int, str]]:
    keys = set()
    if path.exists():
        for line in path.open(encoding="utf-8"):
            if line.strip():
                e = json.loads(line)
                keys.add((e["trigger"], int(e["matchup_id"]), e["status"]))
    return keys


def new_events(cards: list[dict], existing: set[tuple[str, int, str]], now: datetime) -> list[dict]:
    out = []
    for c in cards:
        if c["status"] not in EVENT_STATUSES:
            continue
        key = (c["trigger"], int(c["matchup_id"]), c["status"])
        if key in existing:
            continue
        existing.add(key)
        out.append({
            "measures_version": MEASURES_VERSION, "at": now.isoformat(), "status": c["status"],
            "trigger": c["trigger"], "matchup_id": int(c["matchup_id"]), "away": c["away"], "home": c["home"],
            "commence_time": c["commence_time"].isoformat() if hasattr(c["commence_time"], "isoformat") else c["commence_time"],
            "minutes_since_capture": c.get("minutes_since_capture"),
            "late_reasons": c.get("late_reasons") or [],
            "had_frozen_selection": c.get("frozen") is not None,
        })
    return out


def run_record(cards: list[dict], new_selections: int, new_event_count: int, now: datetime) -> dict:
    counts: dict[str, int] = {}
    for c in cards:
        counts[c["status"]] = counts.get(c["status"], 0) + 1
    for s in ("current", "final", "watch", "dropped", "stale", "started"):
        counts.setdefault(s, 0)
    return {"measures_version": MEASURES_VERSION, "run_at": now.isoformat(), "cards": len(cards),
            "status_counts": counts, "new_selections": new_selections, "new_events": new_event_count}


def append_jsonl(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, default=str, sort_keys=True) + "\n")


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(l) for l in path.open(encoding="utf-8") if l.strip()]


def event_summary(events: list[dict], runs: list[dict]) -> dict:
    by_status: dict[str, int] = {}
    for e in events:
        by_status[e["status"]] = by_status.get(e["status"], 0) + 1
    return {"runs": len(runs), "last_run": runs[-1]["run_at"] if runs else None,
            "events": len(events), "by_status": by_status}
