"""Futures model (futures-v1) — outright winner + group winner, Monte Carlo.

Simulates the tournament from Elo ratings and rates futures bets:

  * **Outright winner** — random-bracket single-elimination Monte Carlo over the
    actual field (teams appearing in soccer_matchups), padded to a power of two
    with byes assigned to the top Elo seeds.  Compared to the
    soccer_fifa_world_cup_winner outright market → edge/EV/stars.
  * **Group winner** — for each group cleanly derived from the loaded group-stage
    fixtures, a round-robin Monte Carlo → P(finish 1st).  No API market exists,
    so stars come from edge over the 1/N baseline (settled from final standings).

Match outcomes use an Elo W/D/L model (draw probability peaks for even games);
knockout ties resolve via Elo-weighted shootout.  The bracket pairing is a
documented simplification — championship probability is dominated by strength.

Usage:
    python -m model.soccer_futures                  # derive groups, sim, rate
    python -m model.soccer_futures --sims 30000
"""

from __future__ import annotations

import argparse
import logging
import math
import random
import unicodedata

import requests

from config import load_config
from db.database import DatabaseManager
from model.soccer_bet_rating import american_to_prob, new_capture_key, record_bet

logger = logging.getLogger(__name__)

MODEL_VERSION = "futures-v1"
WINNER_SPORT_KEY = "soccer_fifa_world_cup_winner"
REGIONS = "us,uk,eu"
ODDS_BASE = "https://api.the-odds-api.com/v4"

DEFAULT_SIMS = 20000
_DRAW_BASE = 0.32   # max draw probability at an even matchup
# The single-elim bracket is an approximation that over-concentrates probability
# on top seeds; anchor the outright to the market so we don't claim wild favorite
# edges (same market-anchoring philosophy as the match model).
_W_OUTRIGHT_MODEL = 0.35

_TEAM_ALIASES = {  # outright-market name → our soccer_teams name
    "United States": "USA",
    "Bosnia and Herzegovina": "Bosnia & Herzegovina",
}


def _norm(name: str) -> str:
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in text.lower() if ch.isalnum())


# ── Elo match model ───────────────────────────────────────────────────────────

def _expected(elo_a: float, elo_b: float) -> float:
    return 1.0 / (1.0 + 10 ** ((elo_b - elo_a) / 400.0))


def wdl_probs(elo_a: float, elo_b: float) -> tuple[float, float, float]:
    """(P(a win), P(draw), P(b win)) — draw peaks for even matchups."""
    exp_a = _expected(elo_a, elo_b)
    p_draw = _DRAW_BASE * (1.0 - abs(2 * exp_a - 1.0))
    p_a = max(0.0, exp_a - p_draw / 2.0)
    p_b = max(0.0, 1.0 - exp_a - p_draw / 2.0)
    s = p_a + p_draw + p_b
    return p_a / s, p_draw / s, p_b / s


def knockout_win_prob(elo_a: float, elo_b: float) -> float:
    """P(a advances) — draws resolve via Elo-weighted shootout."""
    p_a, p_draw, _ = wdl_probs(elo_a, elo_b)
    return p_a + p_draw * _expected(elo_a, elo_b)


# ── Field + groups ────────────────────────────────────────────────────────────

def _load_field(db: DatabaseManager) -> list[dict]:
    """Teams actually in the tournament (appear in soccer_matchups), with Elo."""
    return db.execute(
        """
        SELECT DISTINCT t.team_id, t.name, COALESCE(r.elo, 1500) AS elo
        FROM soccer_teams t
        JOIN soccer_team_ratings r ON r.team_id = t.team_id
        WHERE t.team_id IN (
            SELECT home_team_id FROM soccer_matchups
            UNION SELECT away_team_id FROM soccer_matchups
        )
        """,
    )


def derive_groups(db: DatabaseManager) -> dict[str, list[int]]:
    """Derive clean 4-team groups from group-stage fixtures and store them.

    A group = a connected component of exactly 4 teams that forms a complete
    round-robin (all 6 pairings present) within the fixtures.  Only such clean
    groups are stored — no fabricated groups.  Returns {label: [team_id,...]}.
    """
    edges = db.execute(
        "SELECT home_team_id AS a, away_team_id AS b FROM soccer_matchups "
        "WHERE home_team_id IS NOT NULL AND away_team_id IS NOT NULL"
    )
    adj: dict[int, set[int]] = {}
    pairset: set[frozenset] = set()
    for e in edges:
        a, b = e["a"], e["b"]
        adj.setdefault(a, set()).add(b)
        adj.setdefault(b, set()).add(a)
        pairset.add(frozenset((a, b)))

    # Connected components.
    seen: set[int] = set()
    groups: dict[str, list[int]] = {}
    label_ord = 0
    for start in adj:
        if start in seen:
            continue
        stack, comp = [start], []
        while stack:
            n = stack.pop()
            if n in seen:
                continue
            seen.add(n)
            comp.append(n)
            stack.extend(adj[n] - seen)
        # Clean group: exactly 4 teams, all 6 pairings present.
        if len(comp) == 4:
            complete = all(frozenset((comp[i], comp[j])) in pairset
                           for i in range(4) for j in range(i + 1, 4))
            if complete:
                label = chr(ord("A") + label_ord)
                label_ord += 1
                groups[label] = comp

    # Persist.
    db.execute("DELETE FROM soccer_groups")
    for label, members in groups.items():
        for tid in members:
            db.execute(
                "INSERT INTO soccer_groups (team_id, group_label, derived_at) VALUES (%s, %s, NOW()) "
                "ON CONFLICT (team_id) DO UPDATE SET group_label = EXCLUDED.group_label, derived_at = NOW()",
                (tid, label),
            )
    return groups


# ── Simulations ───────────────────────────────────────────────────────────────

def simulate_outright(field: list[dict], sims: int) -> dict[int, float]:
    """Random-bracket single-elim Monte Carlo → {team_id: P(champion)}."""
    n = len(field)
    if n < 2:
        return {}
    size = 1
    while size < n:
        size *= 2
    # Seed by Elo so byes (the size-n empty slots) fall to the strongest teams.
    ordered = sorted(field, key=lambda t: t["elo"], reverse=True)
    elo = {t["team_id"]: float(t["elo"]) for t in field}
    ids = [t["team_id"] for t in ordered]
    n_byes = size - n

    wins: dict[int, int] = {tid: 0 for tid in elo}
    for _ in range(sims):
        # Top n_byes seeds get a bye into round 2; the rest are randomly bracketed.
        bye = ids[:n_byes]
        rest = ids[n_byes:]
        random.shuffle(rest)
        # Round 1 among 'rest' (even count by construction).
        advanced = list(bye)
        for i in range(0, len(rest), 2):
            a, b = rest[i], rest[i + 1]
            advanced.append(a if random.random() < knockout_win_prob(elo[a], elo[b]) else b)
        # Remaining rounds.
        random.shuffle(advanced)
        while len(advanced) > 1:
            nxt = []
            for i in range(0, len(advanced), 2):
                a, b = advanced[i], advanced[i + 1]
                nxt.append(a if random.random() < knockout_win_prob(elo[a], elo[b]) else b)
            advanced = nxt
        wins[advanced[0]] += 1
    return {tid: w / sims for tid, w in wins.items()}


def simulate_group(member_elos: list[tuple[int, float]], sims: int) -> dict[int, float]:
    """Round-robin Monte Carlo → {team_id: P(finish 1st)}."""
    first: dict[int, int] = {tid: 0 for tid, _ in member_elos}
    pairs = [(i, j) for i in range(len(member_elos)) for j in range(i + 1, len(member_elos))]
    for _ in range(sims):
        pts = {tid: 0 for tid, _ in member_elos}
        for i, j in pairs:
            (ti, ei), (tj, ej) = member_elos[i], member_elos[j]
            p_a, p_draw, _ = wdl_probs(ei, ej)
            r = random.random()
            if r < p_a:
                pts[ti] += 3
            elif r < p_a + p_draw:
                pts[ti] += 1
                pts[tj] += 1
            else:
                pts[tj] += 3
        top = max(pts.values())
        leaders = [tid for tid, p in pts.items() if p == top]
        # Split a tie randomly (proxy for tiebreakers).
        first[random.choice(leaders)] += 1
    return {tid: c / sims for tid, c in first.items()}


# ── Outright market ───────────────────────────────────────────────────────────

def _fetch_outright_market(api_key: str, name_to_id: dict[str, int]) -> dict[int, dict]:
    """Return {team_id: {"prob_vigfree", "best_odds", "best_book"}} from the winner market."""
    try:
        r = requests.get(
            f"{ODDS_BASE}/sports/{WINNER_SPORT_KEY}/odds",
            params={"apiKey": api_key, "regions": REGIONS, "markets": "outrights", "oddsFormat": "american"},
            timeout=25,
        )
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        logger.warning("Outright market fetch failed: %s", e)
        return {}

    # Average implied prob across books per team; track best price.
    acc: dict[int, dict] = {}
    norm_to_id = {_norm(name): tid for name, tid in name_to_id.items()}
    for ev in data if isinstance(data, list) else []:
        for bm in ev.get("bookmakers", []):
            for mk in bm.get("markets", []):
                if mk.get("key") != "outrights":
                    continue
                for o in mk.get("outcomes", []):
                    team_name = o.get("name", "")
                    aliased = _TEAM_ALIASES.get(team_name, team_name)
                    tid = norm_to_id.get(_norm(aliased))
                    if tid is None:
                        continue
                    price = o.get("price")
                    if price is None:
                        continue
                    prob = american_to_prob(price)
                    e = acc.setdefault(tid, {"prob_sum": 0.0, "n": 0, "best_decimal": 0.0,
                                             "best_odds": None, "best_book": None})
                    e["prob_sum"] += prob
                    e["n"] += 1
                    dec = 1.0 + (price / 100.0 if price > 0 else 100.0 / abs(price))
                    if dec > e["best_decimal"]:
                        e["best_decimal"] = dec
                        e["best_odds"] = price
                        e["best_book"] = bm.get("key")

    # Vig-free: normalize average implied by the overround across the field.
    avg = {tid: e["prob_sum"] / e["n"] for tid, e in acc.items() if e["n"] > 0}
    overround = sum(avg.values())
    out = {}
    for tid, e in acc.items():
        if e["n"] == 0:
            continue
        out[tid] = {
            "prob_vigfree": (avg[tid] / overround) if overround > 0 else avg[tid],
            "best_odds": e["best_odds"],
            "best_book": e["best_book"],
        }
    return out


def run(db: DatabaseManager, api_key: str, sims: int = DEFAULT_SIMS) -> int:
    """Derive groups, simulate, fetch outright market, and rate futures bets."""
    field = _load_field(db)
    if len(field) < 2:
        print("Futures: no rated tournament field yet")
        return 0
    name_to_id = {t["name"]: t["team_id"] for t in field}
    capture_key = new_capture_key()
    written = 0

    # ── Outright winner ── (one connection for the batch)
    champ = simulate_outright(field, sims)
    market = _fetch_outright_market(api_key, name_to_id)
    with db.connect() as conn:
        for t in field:
            tid = t["team_id"]
            sim_prob = champ.get(tid, 0.0)
            mkt = market.get(tid)
            # Anchor the approximate sim to the market when a line exists.
            if mkt is not None:
                our_prob = _W_OUTRIGHT_MODEL * sim_prob + (1 - _W_OUTRIGHT_MODEL) * mkt["prob_vigfree"]
            else:
                our_prob = sim_prob
            record_bet(
                db,
                model_version=MODEL_VERSION,
                bet_type="outright_winner",
                scope="tournament",
                selection_label=t["name"],
                our_prob=our_prob,
                capture_key=capture_key,
                market_odds=mkt["best_odds"] if mkt else None,
                market_prob=mkt["prob_vigfree"] if mkt else None,
                book=mkt["best_book"] if mkt else None,
                subject_team_id=tid,
                baseline_prob=1.0 / len(field),
                conn=conn,
                inputs={"elo": round(float(t["elo"]), 1), "sims": sims,
                        "sim_prob": round(sim_prob, 4),
                        "market_vigfree": round(mkt["prob_vigfree"], 4) if mkt else None,
                        "anchor_w_model": _W_OUTRIGHT_MODEL,
                        "has_market": mkt is not None},
            )
            written += 1

    # ── Group winners (clean groups only) ──
    # Group labels are derived from fixtures and can shift as more games load, so
    # clear PENDING group bets first to avoid orphan rows under reused labels.
    # Settled bets (status != 'pending') are preserved for the backtest.
    db.execute(
        "DELETE FROM soccer_bets WHERE bet_type = 'group_winner' "
        "AND model_version = %s AND status = 'pending'",
        (MODEL_VERSION,),
    )
    groups = derive_groups(db)
    elo_by_id = {t["team_id"]: float(t["elo"]) for t in field}
    name_by_id = {t["team_id"]: t["name"] for t in field}
    with db.connect() as conn:
        for label, members in groups.items():
            member_elos = [(tid, elo_by_id.get(tid, 1500.0)) for tid in members]
            probs = simulate_group(member_elos, sims)
            for tid in members:
                if tid not in name_by_id:
                    continue
                record_bet(
                    db,
                    model_version=MODEL_VERSION,
                    bet_type="group_winner",
                    scope=f"Group {label}",
                    selection_label=name_by_id[tid],
                    our_prob=probs.get(tid, 0.0),
                    capture_key=capture_key,
                    subject_team_id=tid,
                    baseline_prob=1.0 / len(members),
                    conn=conn,
                    inputs={"elo": round(elo_by_id.get(tid, 1500.0), 1),
                            "group": label, "sims": sims},
                )
                written += 1

    print(f"Futures: {written} bets rated "
          f"({len(field)} teams outright, {len(groups)} clean groups derived)")
    return written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Soccer futures model (outright + group winner)")
    parser.add_argument("--sims", type=int, default=DEFAULT_SIMS, help="Monte Carlo simulations")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    run(db, config.odds_api.api_key, args.sims)
