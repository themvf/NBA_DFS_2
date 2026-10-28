"""Reverse-engineering the books' NFL player lines, steps 0 and 1.

Step 0 stores each player's opening and closing pregame line per market and
week in `nfl_prop_implied_lines`, read back as an expected stat
(`model/nfl_prop_implied.py`), and checks each implied stat against what the
player actually did. Step 1 reads the book's own math: yards per catch it
assumes, how much of the quarterback's yardage its receiver lines account for,
and, per stat, how our projection's stat line compares with the book's.

Exploratory: nothing here is a pre-registered test, and the skew/margin it
reports are measured on the same weeks it describes. The leave-one-week-out
columns are the honest ones.

    python -m research.nfl_prop_implied build --season 2026 --weeks 1-4
    python -m research.nfl_prop_implied study --season 2026 --weeks 1-4
"""

from __future__ import annotations

import argparse
import collections
from zoneinfo import ZoneInfo
import json
import statistics as st

from psycopg2.extras import execute_values

from config import load_config
from db.database import DatabaseManager
from model import nfl_prop_implied as pim
from model.nfl_dfs_dk_pool_match import normalize_name

TWO_WAY = tuple(pim.MARKETS)
ALL_MARKETS = TWO_WAY + ("player_anytime_td",)
BOOK_ORDER = ("draftkings", "fanduel", "betmgm", "caesars", "fanatics", "espnbet", "betrivers")

# Stat a market prices, and the nflverse field it settles on.
ACTUAL_FIELD = {
    "player_pass_yds": "passing_yards", "player_pass_tds": "passing_tds",
    "player_pass_interceptions": "passing_interceptions", "player_rush_yds": "rushing_yards",
    "player_receptions": "receptions", "player_reception_yds": "receiving_yards",
}


def parse_weeks(text: str) -> list[int]:
    if "-" in text:
        a, b = text.split("-")
        return list(range(int(a), int(b) + 1))
    return [int(x) for x in text.split(",")]


def _pick_book(books) -> tuple[str, dict] | None:
    if not isinstance(books, dict):
        return None
    for name in BOOK_ORDER:
        if books.get(name):
            return name, books[name]
    for name, quote in sorted(books.items()):
        if quote:
            return name, quote
    return None


def week_of(db, season: int):
    kicks = db.execute("SELECT week, kickoff FROM nfl_season_games WHERE season = %s", (season,))

    def find(t):
        for r in kicks:
            if abs((r["kickoff"] - t).total_seconds()) < 7200:
                return r["week"]
        return None
    return find


def player_meta(db, season: int, week: int) -> dict[str, dict]:
    """normalized name -> team/position/actual stats for a week (nflverse), else the roster."""
    out = {}
    for r in db.execute(
            """SELECT f.canonical_name, s.team, s.source_row FROM ff_player_week_stats s JOIN ff_players f ON f.id = s.player_id
               WHERE s.season = %s AND s.week = %s""", (season, week)):
        row = r["source_row"] if isinstance(r["source_row"], dict) else json.loads(r["source_row"] or "{}")
        out.setdefault(normalize_name(r["canonical_name"]), {"team": r["team"], "position": row.get("position"), "stats": row})
    for r in db.execute("SELECT canonical_name, team_abbrev, position FROM ff_players WHERE season = %s", (season,)):
        out.setdefault(normalize_name(r["canonical_name"]), {"team": r["team_abbrev"], "position": r["position"], "stats": None})
    return out


TEAM_ALIAS = {"LA": "LAR", "WAS": "WSH", "JAC": "JAX", "AZ": "ARI"}


def _team(t):
    return TEAM_ALIAS.get(t, t) if t else t


def team_history(db, season: int) -> dict[str, list[tuple[int, str]]]:
    """normalized name -> [(week, team)] from every weekly stat row this season."""
    out = collections.defaultdict(list)
    for r in db.execute(
            """SELECT f.canonical_name, s.week, s.team FROM ff_player_week_stats s JOIN ff_players f ON f.id = s.player_id
               WHERE s.season = %s""", (season,)):
        out[normalize_name(r["canonical_name"])].append((r["week"], _team(r["team"])))
    return out


def resolve_team(meta_team, history: list[tuple[int, str]], week: int, event_teams: set[str]) -> str | None:
    """The player's team in THIS game, which must be one of the game's two teams.

    The name lookup can return a stale roster team (a trade the roster feed
    has recorded, a player with no stat row that week) or another player who
    shares the name. A line listed in a game belongs to one of its two teams,
    so: the week's own team if it is one of them, else the nearest week this
    season he played for one of them, else unknown (None), never a third team.
    """
    t = _team(meta_team)
    if not event_teams or t in event_teams:
        return t
    on_event = sorted((abs(w - week), w, tm) for w, tm in history if tm in event_teams)
    return on_event[0][2] if on_event else None


EASTERN = ZoneInfo("America/New_York")
WEEKDAY_ROLES = {2: "wed", 4: "fri", 5: "sat"}


def capture_roles(caps) -> list[tuple[str, dict]]:
    """Which stored roles a player-market's pregame captures fill.

    `open` is the first capture and `close` the last (both unchanged since the
    table was created). `wed`, `fri` and `sat` are the LAST capture made on that
    Eastern weekday (nfl-prop-field-lag-v1): the Wednesday and Friday 22:07 UTC
    runs and the Saturday day-ahead run for Sunday games. For a Thursday game the
    Wednesday day-ahead capture is also the `wed` row; for a Monday game the
    Saturday row is a midweek read and the Sunday day-ahead capture has no
    weekday role. Captures are already pregame and ordered by time.
    """
    roles = [("open", caps[0]), ("close", caps[-1])]
    by_day = {}
    for cap in caps:
        day = cap["captured_at"].astimezone(EASTERN).weekday()
        if day in WEEKDAY_ROLES:
            by_day[WEEKDAY_ROLES[day]] = cap
    roles.extend(sorted(by_day.items()))
    return roles


def build(db, season: int, weeks: list[int]) -> int:
    find_week = week_of(db, season)
    abbrev = {r["name"]: r["abbreviation"] for r in db.execute(
        "SELECT abbreviation, COALESCE(odds_api_name, name) AS name FROM nfl_teams")}
    rows = db.execute(
        """SELECT event_id, player, market, commence_time, captured_at, books, home_team_name, away_team_name
           FROM prop_odds_history
           WHERE sport = 'nfl' AND captured_at < commence_time AND market = ANY(%s)
           ORDER BY event_id, player, market, captured_at""", (list(ALL_MARKETS),))
    grouped = collections.defaultdict(list)
    for r in rows:
        w = find_week(r["commence_time"])
        if w in weeks:
            grouped[(w, r["event_id"], r["player"], r["market"])].append(r)
    metas = {w: player_meta(db, season, w) for w in weeks}
    history = team_history(db, season)
    out = []
    for (w, event, player, market), caps in grouped.items():
        key = normalize_name(player)
        meta = metas[w].get(key, {})
        event_teams = {abbrev.get(caps[0]["home_team_name"]), abbrev.get(caps[0]["away_team_name"])} - {None}
        team = resolve_team(meta.get("team"), history.get(key, []), w, event_teams)
        for role, cap in capture_roles(caps):
            picked = _pick_book(cap["books"])
            if not picked:
                continue
            book, q = picked
            line, over, under, yes = q.get("line"), q.get("over"), q.get("under"), q.get("yes")
            mean = (pim.anytime_td_rate(yes) if market == "player_anytime_td"
                    else pim.market_mean(market, line, over, under))
            out.append((season, w, key, player, team, meta.get("position"), market, role, book,
                        line, over, under, yes, mean, event, cap["commence_time"], cap["captured_at"], pim.VERSION))
    count_sql = "SELECT COUNT(*) AS n FROM nfl_prop_implied_lines WHERE season = %s AND conversion_version = %s"
    before = db.execute(count_sql, (season, pim.VERSION))[0]["n"]
    with db.connect() as conn, conn.cursor() as cur:
        execute_values(cur, """INSERT INTO nfl_prop_implied_lines (season, week, player_key, player_name, team, position,
            market, capture_role, book, line, over_price, under_price, yes_price, implied_mean, event_id, commence_time,
            captured_at, conversion_version) VALUES %s
            ON CONFLICT (season, week, player_key, market, capture_role, conversion_version)
            DO UPDATE SET team = EXCLUDED.team
            WHERE nfl_prop_implied_lines.team IS DISTINCT FROM EXCLUDED.team""", out)
        conn.commit()
    written = db.execute(count_sql, (season, pim.VERSION))[0]["n"] - before
    print(f"{len(out)} line rows prepared for weeks {weeks[0]}-{weeks[-1]}, {written} new "
          f"(an existing row's team is corrected in place; prices are never rewritten)")
    return written


def load(db, season: int, weeks: list[int], role: str = "close") -> dict:
    """(week, player_key) -> {"position","team","lines":{market: quote}}."""
    out = {}
    for r in db.execute(
            """SELECT week, player_key, team, position, market, line, over_price, under_price, yes_price
               FROM nfl_prop_implied_lines WHERE season = %s AND week = ANY(%s) AND capture_role = %s
               AND conversion_version = %s""", (season, weeks, role, pim.VERSION)):
        d = out.setdefault((r["week"], r["player_key"]), {"position": r["position"], "team": r["team"], "lines": {}})
        d["lines"][r["market"]] = {"line": r["line"], "over": r["over_price"], "under": r["under_price"], "yes": r["yes_price"]}
    return out


def our_stat_means(db, season: int, weeks: list[int]) -> dict:
    """(week, player_key) -> our projection's stat means, from the week's main Classic slate."""
    out = {}
    for r in db.execute(
            """SELECT DISTINCT ON (c.week) c.week, u.projection_run_id FROM nfl_dfs_slate_report_cards c
               JOIN nfl_dfs_slate_uploads u ON u.upload_id::text = c.upload_id
               WHERE c.season = %s AND c.format = 'classic' AND c.week = ANY(%s)
               ORDER BY c.week, jsonb_array_length(c.payload->'rows') DESC, c.created_at DESC""", (season, weeks)):
        for p in db.execute("SELECT normalized_name, player_name, stat_means FROM nfl_dfs_player_projections WHERE run_id = %s",
                            (r["projection_run_id"],)):
            sm = p["stat_means"] if isinstance(p["stat_means"], dict) else json.loads(p["stat_means"] or "{}")
            out.setdefault((r["week"], normalize_name(p["player_name"])), sm)
    return out


STAT_OF = {m: md.stat for m, md in pim.MARKETS.items()} | {"player_anytime_td": "scrimmage_tds"}


def _actual(stats: dict, stat: str):
    if stat == "scrimmage_tds":
        return (stats.get("rushing_tds") or 0) + (stats.get("receiving_tds") or 0)
    return stats.get(stat)


def _ours(mine: dict | None, stat: str):
    if mine is None:
        return None
    if stat == "scrimmage_tds":
        return (mine.get("rushing_tds") or 0) + (mine.get("receiving_tds") or 0)
    return mine.get(stat)


def _raw(market: str, q: dict):
    if market == "player_anytime_td":
        return pim.anytime_td_rate(q.get("yes"))
    return pim.market_mean(market, q.get("line"), q.get("over"), q.get("under"))


def report_card_rows(db, season: int, weeks: list[int]) -> list[dict]:
    """Our projected and the actual DK points per player-week, from the week's main Classic slate card."""
    out = []
    for r in db.execute(
            """SELECT DISTINCT ON (week) week, payload FROM nfl_dfs_slate_report_cards
               WHERE season = %s AND format = 'classic' AND week = ANY(%s)
               ORDER BY week, jsonb_array_length(payload->'rows') DESC, created_at DESC""", (season, weeks)):
        payload = r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"])
        for row in payload["rows"]:
            if row.get("status") == "scored" and row.get("position") in pim.REQUIRED \
                    and row.get("projected") is not None and row.get("actual") is not None:
                out.append({"w": r["week"], "key": normalize_name(row["name"]), "pos": row["position"], "game": row["game_key"],
                            "ours": row["projected"], "act": row["actual"]})
    return out


def _boot(groups: dict, fn, n: int = 2000, seed: int = 7):
    import random
    rng = random.Random(seed); keys = list(groups); out = []
    for _ in range(n):
        sample = [x for k in rng.choices(keys, k=len(keys)) for x in groups[k]]
        out.append(fn(sample))
    out.sort()
    return out[int(0.025 * n)], out[int(0.975 * n)]


def study(db, season: int, weeks: list[int]) -> None:
    lines = load(db, season, weeks)
    ours = our_stat_means(db, season, weeks)
    metas = {w: player_meta(db, season, w) for w in weeks}

    # Per market, every (week, player) with a closing line and a stat row.
    obs = collections.defaultdict(list)
    for (w, key), d in lines.items():
        stats = (metas[w].get(key) or {}).get("stats")
        if not stats:
            continue
        for market, q in d["lines"].items():
            stat = STAT_OF.get(market)
            raw, act = (_raw(market, q), _actual(stats, stat)) if stat else (None, None)
            if raw is not None and act is not None:
                obs[market].append({"w": w, "key": key, "raw": raw, "act": act, "ours": _ours(ours.get((w, key)), stat)})

    def factor(market: str, held_out: int) -> float:
        """Mean actual / mean raw implied on the OTHER weeks: the median-to-mean correction."""
        train = [o for o in obs[market] if o["w"] != held_out]
        den = sum(o["raw"] for o in train)
        return sum(o["act"] for o in train) / den if den else 1.0

    factors = {(m, w): factor(m, w) for m in obs for w in weeks}
    full = {m: (sum(o["act"] for o in obs[m]) / sum(o["raw"] for o in obs[m])) for m in obs}

    print("STEP 0 - each implied stat vs actual (closing lines; 'cal' = corrected on the other weeks only)")
    print(f"  {'market':26}{'n':>5}{'factor':>8}{'raw bias':>10}{'cal bias':>10}  |{'same n':>7}{'book MAE':>10}{'ours MAE':>10}{'ours bias':>10}")
    for market in ALL_MARKETS:
        rows = obs.get(market) or []
        if not rows:
            continue
        cal = [o["raw"] * factors[(market, o["w"])] for o in rows]
        both = [(c, o) for c, o in zip(cal, rows) if o["ours"] is not None]
        print(f"  {market:26}{len(rows):>5}{full[market]:>8.3f}{st.mean(o['raw'] - o['act'] for o in rows):>+10.2f}"
              f"{st.mean(c - o['act'] for c, o in zip(cal, rows)):>+10.2f}  |{len(both):>7}"
              f"{st.mean(abs(c - o['act']) for c, o in both):>10.2f}{st.mean(abs(o['ours'] - o['act']) for c, o in both):>10.2f}"
              f"{st.mean(o['ours'] - o['act'] for c, o in both):>+10.2f}")

    # ---- Step 0b: DK points from the calibrated stat line vs our projection ----
    card = report_card_rows(db, season, weeks)
    scored = []
    for r in card:
        d = lines.get((r["w"], r["key"]))
        s = pim.stat_line(r["pos"], d["lines"]) if d else None
        if s is None:
            continue
        for market, stat in STAT_OF.items():
            if (market, r["w"]) in factors and s.get(stat):
                s[stat] *= factors[(market, r["w"])]
        scored.append({**r, "vegas": pim.dk_points(r["pos"], s)})
    groups = collections.defaultdict(list)
    for x in scored:
        groups[(x["w"], x["game"])].append(x)
    mae = lambda xs, k: st.mean(abs(x[k] - x["act"]) for x in xs)
    print(f"\nSTEP 0b - DK points: calibrated book stat line vs our projection ({len(scored)} player-games with full lines, {len(groups)} games)")
    print(f"  {'pos':5}{'n':>5}{'book MAE':>10}{'ours MAE':>10}{'book bias':>11}{'ours bias':>11}")
    for pos in ("ALL", "QB", "RB", "WR", "TE"):
        xs = scored if pos == "ALL" else [x for x in scored if x["pos"] == pos]
        if xs:
            print(f"  {pos:5}{len(xs):>5}{mae(xs, 'vegas'):>10.2f}{mae(xs, 'ours'):>10.2f}"
                  f"{st.mean(x['vegas'] - x['act'] for x in xs):>+11.2f}{st.mean(x['ours'] - x['act'] for x in xs):>+11.2f}")
    if scored:
        lo, hi = _boot(groups, lambda s: mae(s, "ours") - mae(s, "vegas"))
        print(f"  ours minus book MAE {mae(scored, 'ours') - mae(scored, 'vegas'):+.2f}, game-clustered 95% CI [{lo:+.2f}, {hi:+.2f}]")
        for label, keep in (("ours 2+ above the book", lambda x: x["ours"] - x["vegas"] >= 2),
                            ("within 2", lambda x: abs(x["ours"] - x["vegas"]) < 2),
                            ("ours 2+ below the book", lambda x: x["ours"] - x["vegas"] <= -2)):
            xs = [x for x in scored if keep(x)]
            if xs:
                closer = sum(abs(x["vegas"] - x["act"]) < abs(x["ours"] - x["act"]) for x in xs)
                print(f"    {label:24} n={len(xs):>4}  ours {st.mean(x['ours'] for x in xs):5.1f}  book {st.mean(x['vegas'] for x in xs):5.1f}"
                      f"  actual {st.mean(x['act'] for x in xs):5.1f}  book closer {closer}/{len(xs)}")

    # ---- Step 1a: receiving volume vs efficiency, ours vs the calibrated book ----
    print("\nSTEP 1a - receiving volume vs efficiency (players with receptions + yards lines; book calibrated as above)")
    print(f"  {'pos':4}{'n':>5}{'rec ours':>10}{'rec book':>10}{'rec act':>9}{'yds ours':>10}{'yds book':>10}{'yds act':>9}"
          f"{'Y/R ours':>10}{'Y/R book':>10}{'Y/R act':>9}{'TD ours':>9}{'TD book':>9}{'TD act':>8}")
    for pos in ("WR", "TE", "RB"):
        acc = collections.defaultdict(list)
        for (w, key), d in lines.items():
            stats = (metas[w].get(key) or {}).get("stats")
            mine = ours.get((w, key))
            q = d["lines"]
            if d["position"] != pos or not stats or not mine or not q.get("player_receptions") or not q.get("player_reception_yds"):
                continue
            rec, yds = _raw("player_receptions", q["player_receptions"]), _raw("player_reception_yds", q["player_reception_yds"])
            if rec is None or yds is None:
                continue
            acc["rec_o"].append(mine.get("receptions") or 0)
            acc["rec_b"].append(rec * factors[("player_receptions", w)])
            acc["rec_a"].append(stats.get("receptions") or 0)
            acc["yds_o"].append(mine.get("receiving_yards") or 0)
            acc["yds_b"].append(yds * factors[("player_reception_yds", w)])
            acc["yds_a"].append(stats.get("receiving_yards") or 0)
            td = _raw("player_anytime_td", q["player_anytime_td"]) if q.get("player_anytime_td") else None
            if td is not None:
                acc["td_o"].append(_ours(mine, "scrimmage_tds")); acc["td_b"].append(td * factors[("player_anytime_td", w)])
                acc["td_a"].append(_actual(stats, "scrimmage_tds"))
        if acc["rec_o"]:
            m = {k: st.mean(v) for k, v in acc.items()}
            print(f"  {pos:4}{len(acc['rec_o']):>5}{m['rec_o']:>10.2f}{m['rec_b']:>10.2f}{m['rec_a']:>9.2f}{m['yds_o']:>10.1f}{m['yds_b']:>10.1f}"
                  f"{m['yds_a']:>9.1f}{m['yds_o'] / m['rec_o']:>10.2f}{m['yds_b'] / m['rec_b']:>10.2f}{m['yds_a'] / m['rec_a']:>9.2f}"
                  f"{m.get('td_o', 0):>9.3f}{m.get('td_b', 0):>9.3f}{m.get('td_a', 0):>8.3f}")

    # ---- Step 1b: the team pie ----
    teams = collections.defaultdict(lambda: {"qb": None, "qb_act": None, "rec": 0.0, "rec_act": 0.0, "n": 0})
    for (w, key), d in lines.items():
        if not d["team"]:
            continue
        stats = (metas[w].get(key) or {}).get("stats") or {}
        t = teams[(w, d["team"])]
        q = d["lines"]
        if d["position"] == "QB" and q.get("player_pass_yds"):
            v = _raw("player_pass_yds", q["player_pass_yds"])
            if v is not None and (t["qb"] is None or v > t["qb"]):  # the starter is the bigger line
                t["qb"] = v * factors[("player_pass_yds", w)]; t["qb_act"] = stats.get("passing_yards")
        elif d["position"] in ("WR", "TE", "RB") and q.get("player_reception_yds"):
            v = _raw("player_reception_yds", q["player_reception_yds"])
            if v is not None:
                t["rec"] += v * factors[("player_reception_yds", w)]; t["rec_act"] += stats.get("receiving_yards") or 0; t["n"] += 1
    kept = [t for t in teams.values() if t["qb"] and t["n"] and t["qb_act"]]
    if kept:
        book = sum(t["rec"] for t in kept) / sum(t["qb"] for t in kept)
        real = sum(t["rec_act"] for t in kept) / sum(t["qb_act"] for t in kept)
        print(f"\nSTEP 1b - the team pie ({len(kept)} team-weeks, {st.mean(t['n'] for t in kept):.1f} receivers with a yards line each)")
        print(f"  book: those receivers' implied yards = {book:.0%} of the QB's implied passing yards")
        print(f"  actual: the same receivers caught {real:.0%} of their QB's actual passing yards")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("build", "study"))
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--weeks", default="1-4")
    args = parser.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=args.command == "build")
    weeks = parse_weeks(args.weeks)
    if args.command == "build":
        build(db, args.season, weeks)
    else:
        study(db, args.season, weeks)


if __name__ == "__main__":
    main()
