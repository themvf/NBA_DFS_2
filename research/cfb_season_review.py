"""Read-only season-to-date CFB review: results & performance vs ML / spread / total.

Two independent views:
  A. GAME RESULTS vs REFERENCE LINES — every completed game graded straight-up,
     against the spread, and over/under using the canonical reference line
     (cfb_matchups embedded columns, falling back to cfb_historical_game_lines).
  B. PROSPECTIVE PAPER PICKS — the settled line_alerts record (W/L/push/void,
     P&L units, ROI, CLV) grouped by market.

Nothing here is a betting recommendation; it is a descriptive audit of what the
market/model has done so far this season.
"""

from __future__ import annotations

import argparse
from collections import defaultdict

from config import load_config
from db.database import DatabaseManager
from model.cfb_historical_signals import grade_home, summarize_outcomes


def _american_to_decimal(american):
    try:
        price = float(american)
    except (TypeError, ValueError):
        return None
    if price == 0:
        return None
    return 1 + (100 / abs(price) if price < 0 else price / 100)


def _pct(n, d):
    return f"{n / d * 100:.1f}%" if d else "n/a"


def _season(db, season):
    if season is not None:
        return season
    row = db.execute_one("SELECT MAX(season) AS s FROM cfb_matchups")
    return row["s"] if row else None


def coverage(db, season):
    print("=" * 78)
    print(f"CFB SEASON REVIEW — season {season}")
    print("=" * 78)
    rows = db.execute(
        """
        SELECT season,
               COUNT(*) AS games,
               COUNT(*) FILTER (WHERE completed) AS completed,
               MIN(game_date) AS first_game,
               MAX(game_date) FILTER (WHERE completed) AS last_completed,
               MAX(week) FILTER (WHERE completed) AS last_week
        FROM cfb_matchups
        GROUP BY season ORDER BY season
        """
    )
    print("\nCoverage by season:")
    for r in rows:
        marker = "  <-- reviewing" if r["season"] == season else ""
        print(f"  {r['season']}: {r['completed']}/{r['games']} completed, "
              f"{r['first_game']} .. {r['last_completed']} (through wk {r['last_week']}){marker}")


def game_results_view(db, season):
    """Track A: grade every completed game vs the reference ML/spread/total."""
    rows = db.execute(
        """
        SELECT m.id, m.week, m.neutral_site, m.conference_game,
               m.home_score, m.away_score,
               m.home_ml, m.away_ml, m.home_spread, m.vegas_total,
               ht.name AS home, at.name AS away
        FROM cfb_matchups m
        JOIN cfb_teams ht ON ht.team_id = m.home_team_id
        JOIN cfb_teams at ON at.team_id = m.away_team_id
        WHERE m.season = %s AND m.completed = TRUE
          AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
        ORDER BY m.week, m.id
        """,
        (season,),
    )

    n_games = len(rows)

    # --- Moneyline: did the favorite win? (chalk record) ---
    fav_ml_out = []          # favorite straight-up win/loss
    ml_priced = 0
    dog_ml_pnl = 0.0         # flat 1u on every underdog moneyline
    dog_ml_settled = 0
    fav_ml_pnl = 0.0
    fav_ml_settled = 0

    # --- Spread: home cover, and favorite cover ---
    home_ats = []
    fav_ats = []             # favorite-side ATS outcome
    dog_ats = []
    home_su = []

    # --- Total: over/under ---
    over_under = []          # 'win' == over hit
    total_priced = 0

    for r in rows:
        hs, as_ = int(r["home_score"]), int(r["away_score"])

        # Straight-up + ATS from home perspective (needs home_spread)
        if r["home_spread"] is not None:
            su, ats = grade_home(hs, as_, float(r["home_spread"]))
            home_su.append(su)
            home_ats.append(ats)
            # favorite side = negative home_spread means home favored
            home_is_fav = float(r["home_spread"]) < 0
            if ats != "push":
                fav_ats.append(ats if home_is_fav else ("win" if ats == "loss" else "loss"))
                dog_ats.append(ats if not home_is_fav else ("win" if ats == "loss" else "loss"))

        # Moneyline chalk: which side was favored by price
        h_ml, a_ml = r["home_ml"], r["away_ml"]
        if h_ml is not None and a_ml is not None:
            ml_priced += 1
            home_fav = int(h_ml) < int(a_ml)  # more negative / smaller = favorite
            winner = "home" if hs > as_ else "away" if as_ > hs else "push"
            if winner != "push":
                fav_won = (winner == "home") == home_fav
                fav_ml_out.append("win" if fav_won else "loss")
                # flat-stake P&L on dog and on fav
                dog_ml = a_ml if home_fav else h_ml
                fav_ml = h_ml if home_fav else a_ml
                dog_dec = _american_to_decimal(dog_ml)
                fav_dec = _american_to_decimal(fav_ml)
                dog_won = not fav_won
                if dog_dec is not None:
                    dog_ml_settled += 1
                    dog_ml_pnl += (dog_dec - 1) if dog_won else -1
                if fav_dec is not None:
                    fav_ml_settled += 1
                    fav_ml_pnl += (fav_dec - 1) if fav_won else -1

        # Total
        if r["vegas_total"] is not None:
            total_priced += 1
            diff = (hs + as_) - float(r["vegas_total"])
            if abs(diff) > 1e-9:
                over_under.append("win" if diff > 0 else "loss")

    print("\n" + "-" * 78)
    print("A. GAME RESULTS vs REFERENCE LINES (all completed games)")
    print("-" * 78)
    print(f"Completed games: {n_games}")

    # Moneyline
    fav_ml = summarize_outcomes(fav_ml_out)
    print("\nMONEYLINE (straight-up vs the price)")
    print(f"  Games with ML priced: {ml_priced}")
    print(f"  Favorites went: {fav_ml.wins}-{fav_ml.losses} "
          f"({_pct(fav_ml.wins, fav_ml.wins + fav_ml.losses)} of decisions)")
    print(f"  Flat 1u on every FAVORITE ML: {fav_ml_pnl:+.2f}u over {fav_ml_settled} bets "
          f"(ROI {_pct(fav_ml_pnl, fav_ml_settled)})")
    print(f"  Flat 1u on every UNDERDOG ML: {dog_ml_pnl:+.2f}u over {dog_ml_settled} bets "
          f"(ROI {_pct(dog_ml_pnl, dog_ml_settled)})")

    # Spread
    home = summarize_outcomes(home_ats)
    fav = summarize_outcomes(fav_ats)
    dog = summarize_outcomes(dog_ats)
    hsu = summarize_outcomes(home_su)
    print("\nSPREAD (against the reference home_spread)")
    print(f"  Games with spread: {home.n}")
    print(f"  Home teams SU:  {hsu.wins}-{hsu.losses}-{hsu.pushes} ({_pct(hsu.wins, hsu.wins + hsu.losses)})")
    print(f"  Home teams ATS: {home.wins}-{home.losses}-{home.pushes} "
          f"({_pct(home.wins, home.wins + home.losses)} cover)")
    print(f"  Favorites ATS:  {fav.wins}-{fav.losses}-{fav.pushes} "
          f"({_pct(fav.wins, fav.wins + fav.losses)} cover)")
    print(f"  Underdogs ATS:  {dog.wins}-{dog.losses}-{dog.pushes} "
          f"({_pct(dog.wins, dog.wins + dog.losses)} cover)")

    # Total
    tot = summarize_outcomes(over_under)
    print("\nTOTAL (over/under vs vegas_total)")
    print(f"  Games with total: {total_priced}")
    print(f"  Overs hit: {tot.wins}-{tot.losses} ({_pct(tot.wins, tot.wins + tot.losses)} went OVER)")

    # Weekly ATS/OU drift
    print("\n  Home-favorite bias check — home ATS cover by week:")
    by_week = defaultdict(list)
    for r in rows:
        if r["home_spread"] is not None:
            _, ats = grade_home(int(r["home_score"]), int(r["away_score"]), float(r["home_spread"]))
            by_week[r["week"]].append(ats)
    for wk in sorted(by_week):
        s = summarize_outcomes(by_week[wk])
        print(f"    wk {wk:>2}: home ATS {s.wins}-{s.losses}-{s.pushes} "
              f"({_pct(s.wins, s.wins + s.losses)})")


def paper_pick_view(db, season):
    """Track B: settled prospective line_alerts, grouped by market."""
    rows = db.execute(
        """
        SELECT a.alert_type, a.side, a.outcome, a.pnl_units, a.clv_pp, a.dk_clv_pct,
               a.settled_at, a.details_json,
               COALESCE(a.details_json->>'market',
                        CASE WHEN a.alert_type LIKE 'spread%%' THEN 'spread'
                             WHEN a.alert_type LIKE 'total%%' THEN 'total'
                             ELSE 'moneyline' END) AS market
        FROM line_alerts a
        JOIN cfb_matchups m ON m.id = a.matchup_id
        WHERE a.sport = 'cfb' AND a.origin = 'prospective' AND m.season = %s
        ORDER BY a.settled_at NULLS LAST
        """,
        (season,),
    )

    print("\n" + "-" * 78)
    print("B. PROSPECTIVE PAPER PICKS (settled line_alerts record)")
    print("-" * 78)
    if not rows:
        print("  No prospective CFB alerts recorded for this season yet.")
        return

    by_market = defaultdict(list)
    for r in rows:
        by_market[r["market"]].append(r)

    total_settled = 0
    for market in ("moneyline", "spread", "total"):
        mrows = by_market.get(market, [])
        if not mrows:
            continue
        settled = [r for r in mrows if r["outcome"] in ("won", "lost", "push", "void")]
        wl = [r for r in settled if r["outcome"] in ("won", "lost")]
        wins = sum(1 for r in wl if r["outcome"] == "won")
        losses = sum(1 for r in wl if r["outcome"] == "lost")
        pushes = sum(1 for r in settled if r["outcome"] == "push")
        pnl = sum(float(r["pnl_units"]) for r in settled if r["pnl_units"] is not None)
        stake = sum(1 for r in settled if r["outcome"] in ("won", "lost", "push"))
        clvs = [float(r["clv_pp"]) for r in mrows if r["clv_pp"] is not None]
        dkclvs = [float(r["dk_clv_pct"]) for r in mrows if r["dk_clv_pct"] is not None]
        total_settled += len(settled)
        print(f"\n{market.upper()}  ({len(mrows)} picks, {len(settled)} settled)")
        print(f"  Record: {wins}-{losses}-{pushes}  ({_pct(wins, wins + losses)} win)")
        print(f"  P&L: {pnl:+.2f}u  ROI: {_pct(pnl, stake)}  (flat 1u/settled decision)")
        if clvs:
            print(f"  Reference CLV: {sum(clvs) / len(clvs):+.2f} pp avg, "
                  f"beat close {_pct(sum(1 for c in clvs if c > 0), len(clvs))}")
        if dkclvs:
            print(f"  Execution (DK) CLV: {sum(dkclvs) / len(dkclvs):+.2f}% avg")
        # per alert_type breakdown
        by_type = defaultdict(lambda: [0, 0, 0, 0.0])  # w, l, push, pnl
        for r in settled:
            b = by_type[r["alert_type"]]
            if r["outcome"] == "won":
                b[0] += 1
            elif r["outcome"] == "lost":
                b[1] += 1
            elif r["outcome"] == "push":
                b[2] += 1
            if r["pnl_units"] is not None:
                b[3] += float(r["pnl_units"])
        for atype, (w, l, p, pl) in sorted(by_type.items()):
            print(f"    {atype:<20} {w}-{l}-{p}  {pl:+.2f}u")

    unsettled = [r for r in rows if r["outcome"] not in ("won", "lost", "push", "void")]
    if unsettled:
        print(f"\n  ({len(unsettled)} picks still open / awaiting verified close)")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, default=None, help="season year (default: latest in DB)")
    args = ap.parse_args()

    db = DatabaseManager(load_config().database_url or "", initialize_schema=False)
    season = _season(db, args.season)
    if season is None:
        print("No CFB data found.")
        return
    coverage(db, season)
    game_results_view(db, season)
    paper_pick_view(db, season)


if __name__ == "__main__":
    main()
