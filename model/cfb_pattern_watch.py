"""CFB early-season pattern watch — pre-registered study (registered 2026-10-09).

Five patterns found in a descriptive sweep of the first six weeks of the 2026
season (259 FBS-vs-FBS games, 70 FBS-vs-FCS games). Each is frozen here as a
trigger, a bet, a price basis and a kill criterion BEFORE any week-7+ game is
graded. The discovery sample can never confirm them; it is recorded only so
drift is visible.

Registration document: docs/cfb-early-season-patterns-study.md

The scan appends one row per (trigger, game) to a local JSON-lines ledger and
never rewrites an existing row. Verdicts are withheld until each trigger's
sample floor is reached; before that every summary line is descriptive-only.

Usage:
    python -m model.cfb_pattern_watch               # scan + grade prospective games
    python -m model.cfb_pattern_watch --discovery   # replay the frozen discovery window
    python -m model.cfb_pattern_watch --ledger PATH # alternate ledger location
"""

from __future__ import annotations

import argparse
import collections
import json
import logging
import os
import random
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

# ── Frozen registration ──────────────────────────────────────────────────────
STUDY_VERSION = "cfb-pattern-watch-v1"
REGISTERED_AT = datetime(2026, 10, 9, 22, 0, tzinfo=timezone.utc)
DISCOVERY_START = datetime(2026, 8, 1, tzinfo=timezone.utc)
SEASON = 2026
VERDICT_NOT_BEFORE = datetime(2026, 12, 7, tzinfo=timezone.utc)  # after conference title games
BOOTSTRAP_ITERS = 5000
_SEED = 20261009
SPREAD_TOTAL_PRICE = -110          # consensus has no spread/total price; stated assumption
P4 = frozenset({"SEC", "Big Ten", "Big 12", "ACC"})
G5_TARGET_DOGS = frozenset({"Conference USA", "Sun Belt", "Mid-American"})
INDEPENDENTS = "FBS Independents"

# trigger -> (bet market, sample floor in games)
TRIGGERS = {
    "T1_g5_mid_fav_ml": ("favorite moneyline", 40),
    "T2_fcs_fav_spread": ("favorite spread", 25),
    "T3_small_fav_dog_spread": ("underdog spread", 80),
    "T4_big_fav_over": ("over", 50),
    "T5_sat_evening_g5_fav_ml": ("favorite moneyline", 60),
}
FAMILY_SIZE = len(TRIGGERS)

DEFAULT_LEDGER = Path("artifacts") / "cfb_pattern_watch" / "ledger.jsonl"


# ── Odds helpers ─────────────────────────────────────────────────────────────
def decimal_odds(american: int | float) -> float:
    american = float(american)
    return 1 + 100 / abs(american) if american < 0 else 1 + american / 100


def vig_free_home(home_ml: int, away_ml: int) -> float:
    ph, pa = 1 / decimal_odds(home_ml), 1 / decimal_odds(away_ml)
    return ph / (ph + pa)


def _eastern(ts: datetime) -> datetime:
    # Fixed -4h: the season to the verdict date is entirely inside US daylight time
    # except the final week; a one-hour shift cannot move a 5:00-8:30pm window
    # across a slot boundary for any real kickoff time (kickoffs are on :00/:30).
    return ts - timedelta(hours=4)


def is_g5(conference: str | None) -> bool:
    return conference not in P4 and conference != INDEPENDENTS and conference is not None


# ── Trigger classification (pure) ────────────────────────────────────────────
def classify(game: dict) -> list[str]:
    """Return every trigger the game qualifies for. Pure function of frozen rules.

    `game` needs: home_spread, home_ml, away_ml, hclass, aclass, hconf, aconf,
    commence_time (tz-aware).
    """
    sp = game["home_spread"]
    if sp is None or game["home_ml"] is None or game["away_ml"] is None:
        return []
    home_fav = sp < 0 if sp != 0 else game["home_ml"] < game["away_ml"]
    fav_sp = abs(sp)
    fbs_both = game["hclass"] == "fbs" and game["aclass"] == "fbs"
    fav_conf = game["hconf"] if home_fav else game["aconf"]
    dog_conf = game["aconf"] if home_fav else game["hconf"]
    fav_class = game["hclass"] if home_fav else game["aclass"]
    out = []
    if fbs_both and is_g5(fav_conf) and is_g5(dog_conf) and 7 <= fav_sp <= 13.5 and dog_conf in G5_TARGET_DOGS:
        out.append("T1_g5_mid_fav_ml")
    if not fbs_both and fav_class == "fbs":
        out.append("T2_fcs_fav_spread")
    if fbs_both and 3 <= fav_sp <= 6.5:
        out.append("T3_small_fav_dog_spread")
    if fbs_both and 14 <= fav_sp <= 20.5 and game.get("vegas_total") is not None:
        out.append("T4_big_fav_over")
    if fbs_both and (is_g5(game["hconf"]) or is_g5(game["aconf"])):
        et = _eastern(game["commence_time"])
        if et.strftime("%a") == "Sat" and 17 * 60 <= et.hour * 60 + et.minute < 20 * 60 + 30:
            out.append("T5_sat_evening_g5_fav_ml")
    return out


def grade(trigger: str, game: dict) -> dict:
    """Grade one trigger on a completed game at the frozen price basis.

    Returns result ('won'|'lost'|'push'), price (decimal), pnl_units (flat 1u),
    and the selection text.
    """
    sp = game["home_spread"]
    home_fav = sp < 0 if sp != 0 else game["home_ml"] < game["away_ml"]
    margin = game["home_score"] - game["away_score"]
    fav_margin = margin if home_fav else -margin
    fav_sp = abs(sp)
    fav_name = game["home"] if home_fav else game["away"]
    dog_name = game["away"] if home_fav else game["home"]
    fav_ml = game["home_ml"] if home_fav else game["away_ml"]
    p_home = vig_free_home(game["home_ml"], game["away_ml"])
    fav_p = p_home if home_fav else 1 - p_home

    if trigger in ("T1_g5_mid_fav_ml", "T5_sat_evening_g5_fav_ml"):
        price = decimal_odds(fav_ml)
        if fav_margin > 0: result = "won"
        elif fav_margin < 0: result = "lost"
        else: result = "push"
        sel = f"{fav_name} ML {fav_ml:+d}"
        implied = fav_p
    elif trigger == "T2_fcs_fav_spread":
        price = decimal_odds(SPREAD_TOTAL_PRICE)
        c = fav_margin - fav_sp
        result = "won" if c > 0 else "lost" if c < 0 else "push"
        sel = f"{fav_name} -{fav_sp:g}"
        implied = 0.5
    elif trigger == "T3_small_fav_dog_spread":
        price = decimal_odds(SPREAD_TOTAL_PRICE)
        c = fav_margin - fav_sp
        result = "won" if c < 0 else "lost" if c > 0 else "push"
        sel = f"{dog_name} +{fav_sp:g}"
        implied = 0.5
    elif trigger == "T4_big_fav_over":
        price = decimal_odds(SPREAD_TOTAL_PRICE)
        tot = game["home_score"] + game["away_score"]
        line = game["vegas_total"]
        result = "won" if tot > line else "lost" if tot < line else "push"
        sel = f"Over {line:g}"
        implied = 0.5
    else:
        raise ValueError(trigger)
    pnl = (price - 1) if result == "won" else (-1.0 if result == "lost" else 0.0)
    return {"result": result, "price": round(price, 4), "pnl_units": round(pnl, 4),
            "selection": sel, "implied_prob": round(implied, 4)}


# ── Data ─────────────────────────────────────────────────────────────────────
_GAME_SQL = """
SELECT m.id AS matchup_id, m.week, m.game_date, m.commence_time, m.home_score, m.away_score,
       m.conference_game, m.went_to_overtime,
       COALESCE(h.home_spread, m.home_spread) AS home_spread,
       COALESCE(h.vegas_total, m.vegas_total) AS vegas_total,
       COALESCE(h.home_ml, m.home_ml) AS home_ml,
       COALESCE(h.away_ml, m.away_ml) AS away_ml,
       CASE WHEN h.id IS NOT NULL THEN 'verified_close:' || e.quality ELSE 'last_capture' END AS price_source,
       ht.name AS home, at.name AS away, ht.conference AS hconf, at.conference AS aconf,
       ht.classification AS hclass, at.classification AS aclass
FROM cfb_matchups m
JOIN cfb_teams ht ON ht.team_id = m.home_team_id
JOIN cfb_teams at ON at.team_id = m.away_team_id
LEFT JOIN event_closing_lines e ON e.sport = 'cfb' AND e.matchup_id = m.id AND e.quality IN ('A', 'B')
LEFT JOIN game_odds_history h ON h.id = e.history_id
WHERE m.season = %s AND m.completed AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
  AND m.commence_time >= %s AND m.commence_time < %s
ORDER BY m.commence_time, m.id
"""


def load_games(db: DatabaseManager, start: datetime, end: datetime) -> list[dict]:
    rows = db.execute(_GAME_SQL, (SEASON, start, end))
    return [dict(r) for r in rows if r["home_spread"] is not None and r["home_ml"] is not None and r["away_ml"] is not None]


# ── Ledger (append-only JSON lines) ─────────────────────────────────────────
def read_ledger(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def append_rows(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, default=str, sort_keys=True) + "\n")


def build_rows(games: list[dict], origin: str, existing_keys: set[tuple[str, int]]) -> list[dict]:
    now = datetime.now(timezone.utc).isoformat()
    out = []
    for g in games:
        for trig in classify(g):
            key = (trig, int(g["matchup_id"]))
            if key in existing_keys:
                continue
            gr = grade(trig, g)
            out.append({
                "study_version": STUDY_VERSION, "origin": origin, "trigger": trig,
                "matchup_id": int(g["matchup_id"]), "week": g["week"], "game_date": str(g["game_date"]),
                "commence_time": g["commence_time"].isoformat(), "home": g["home"], "away": g["away"],
                "home_spread": g["home_spread"], "vegas_total": g["vegas_total"],
                "home_ml": g["home_ml"], "away_ml": g["away_ml"], "price_source": g["price_source"],
                "home_score": g["home_score"], "away_score": g["away_score"],
                **gr, "graded_at": now,
            })
    return out


# ── Summary ──────────────────────────────────────────────────────────────────
def _bootstrap_roi(rows: list[dict], iters: int = BOOTSTRAP_ITERS, seed: int = _SEED) -> tuple[float, float]:
    by_date = collections.defaultdict(list)
    for r in rows:
        by_date[r["game_date"]].append(r["pnl_units"])
    dates = list(by_date)
    rng = random.Random(seed)
    vals = []
    for _ in range(iters):
        s = []
        for d in rng.choices(dates, k=len(dates)):
            s.extend(by_date[d])
        vals.append(sum(s) / len(s))
    vals.sort()
    return vals[int(0.025 * iters)], vals[int(0.975 * iters) - 1]


def summarize(rows: list[dict], label: str, as_of: datetime | None = None) -> list[str]:
    lines = [f"== {label}: {STUDY_VERSION} =="]
    as_of = as_of or datetime.now(timezone.utc)
    for trig, (bet, floor) in TRIGGERS.items():
        tr = [r for r in rows if r["trigger"] == trig]
        n = len(tr)
        if n == 0:
            lines.append(f"  {trig:28s} {bet:20s} n=0")
            continue
        w = sum(r["result"] == "won" for r in tr); l = sum(r["result"] == "lost" for r in tr); p = n - w - l
        units = sum(r["pnl_units"] for r in tr)
        roi = units / n
        exp = statistics.mean(r["implied_prob"] for r in tr)
        dates = len({r["game_date"] for r in tr})
        tag = "descriptive-only" if n < floor else ("floor reached" if as_of < VERDICT_NOT_BEFORE else "verdict eligible")
        lo, hi = _bootstrap_roi(tr) if dates >= 3 else (float("nan"), float("nan"))
        lines.append(
            f"  {trig:28s} {bet:20s} n={n:3d}/{floor} {w}-{l}-{p} win {w / max(1, w + l):.1%} (implied {exp:.1%}) "
            f"units {units:+.2f} ROI {roi:+.1%} CI[{lo:+.1%},{hi:+.1%}] dates={dates} [{tag}]"
        )
    overlap = len({r["matchup_id"] for r in rows if r["trigger"] == "T1_g5_mid_fav_ml"} &
                  {r["matchup_id"] for r in rows if r["trigger"] == "T5_sat_evening_g5_fav_ml"})
    lines.append(f"  T1/T5 overlapping games: {overlap}  (family size {FAMILY_SIZE}; Bonferroni CI needs ~99% per test)")
    return lines


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--discovery", action="store_true", help="replay the frozen discovery window, no ledger write")
    ap.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cfg = load_config()
    db = DatabaseManager(cfg.database_url, initialize_schema=False)

    if args.discovery:
        games = load_games(db, DISCOVERY_START, REGISTERED_AT)
        rows = build_rows(games, "discovery", set())
        for line in summarize(rows, f"DISCOVERY {DISCOVERY_START.date()} -> {REGISTERED_AT.isoformat()} ({len(games)} games) — cannot confirm anything"):
            print(line)
        return 0

    path = Path(args.ledger)
    existing = read_ledger(path)
    keys = {(r["trigger"], int(r["matchup_id"])) for r in existing}
    games = load_games(db, REGISTERED_AT, datetime(SEASON + 1, 2, 1, tzinfo=timezone.utc))
    new_rows = build_rows(games, "prospective", keys)
    append_rows(path, new_rows)
    rows = existing + new_rows
    print(f"scanned {len(games)} completed prospective games; appended {len(new_rows)} rows; ledger {path} now {len(rows)} rows")
    for line in summarize(rows, "PROSPECTIVE (commence >= registration)"):
        print(line)
    summary_path = path.with_name("summary.md")
    summary_path.write_text(
        f"# {STUDY_VERSION} — generated {datetime.now(timezone.utc).isoformat()}\n\n```\n"
        + "\n".join(summarize(rows, "PROSPECTIVE")) + "\n```\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
