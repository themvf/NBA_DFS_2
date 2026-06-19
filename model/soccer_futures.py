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

# Pinnacle public guest API — no auth required for odds reads.
_PINNACLE_BASE = "https://guest.api.arcadia.pinnacle.com/0.1"
_PINNACLE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "x-api-key": "CmX2KcMrXuFmNg6YFbmTxE0y9CblMOzm",
}
_PINNACLE_WC_LEAGUE = 2686

DEFAULT_SIMS = 20000
_DRAW_BASE = 0.32   # max draw probability at an even matchup
# The single-elim bracket is an approximation that over-concentrates probability
# on top seeds; anchor the outright to the market so we don't claim wild favorite
# edges (same market-anchoring philosophy as the match model).
_W_OUTRIGHT_MODEL = 0.35
# Group winner markets are 4-team and more efficient — lean on the market more.
_W_GROUP_MODEL = 0.30

_TEAM_ALIASES = {  # outright-market name → our soccer_teams name
    "United States": "USA",
    "Bosnia and Herzegovina": "Bosnia & Herzegovina",
}

# Pinnacle team names that differ from our soccer_teams.name
_PINNACLE_ALIASES = {
    "Bosnia and Herzegovina": "Bosnia & Herzegovina",
    "Czechia": "Czech Republic",
    "Turkiye": "Turkey",
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
    round-robin (all 6 pairings present).  Derivation is **additive**: once a
    group is found it is kept, even after knockout fixtures later merge the graph
    into bigger components (which would otherwise make the clean K4 disappear and
    break group settlement).  Labels are stable — a found group never changes
    letter.  Returns ALL stored groups {label: [team_id,...]}.
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

    # Already-known assignments (kept; never relabeled).
    existing = {r["team_id"]: r["group_label"] for r in db.execute(
        "SELECT team_id, group_label FROM soccer_groups")}
    used_labels = set(existing.values())

    def next_label() -> str:
        for i in range(26):
            lab = chr(ord("A") + i)
            if lab not in used_labels:
                used_labels.add(lab)
                return lab
        return f"G{len(used_labels)}"

    seen: set[int] = set()
    for start in list(adj):
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
        if len(comp) != 4:
            continue
        if not all(frozenset((comp[i], comp[j])) in pairset
                   for i in range(4) for j in range(i + 1, 4)):
            continue
        # Skip if any member is already assigned (a team belongs to one group, so
        # this is either an already-known group or a transient artifact).
        if any(tid in existing for tid in comp):
            continue
        label = next_label()
        for tid in comp:
            existing[tid] = label
            db.execute(
                "INSERT INTO soccer_groups (team_id, group_label, derived_at) VALUES (%s, %s, NOW()) "
                "ON CONFLICT (team_id) DO UPDATE SET group_label = EXCLUDED.group_label, derived_at = NOW()",
                (tid, label),
            )

    # Return ALL stored groups (label → members).
    groups: dict[str, list[int]] = {}
    for tid, lab in existing.items():
        groups.setdefault(lab, []).append(tid)
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


# Group-stage scoreline model (for proper FIFA tiebreakers): map the Elo win-prob
# gap to a goal supremacy around an international group-stage average total, then
# sample a Poisson scoreline.  Real goals → real GD + goals-for, and a known
# head-to-head result per pair, so ties resolve the way FIFA actually breaks them
# instead of by a coin flip.
_GROUP_SIM_TOTAL = 2.6        # avg goals/game, group stage
_GROUP_SIM_SUP_SCALE = 2.2    # win-prob gap → goal supremacy
_GROUP_SIM_MAX_SUP = 2.4


def _elo_lambdas(elo_a: float, elo_b: float) -> tuple[float, float]:
    """Poisson goal rates (λ_a, λ_b) implied by the Elo win-prob gap."""
    p_a, _, p_b = wdl_probs(elo_a, elo_b)
    sup = max(-_GROUP_SIM_MAX_SUP, min(_GROUP_SIM_MAX_SUP, (p_a - p_b) * _GROUP_SIM_SUP_SCALE))
    return max(0.2, (_GROUP_SIM_TOTAL + sup) / 2.0), max(0.2, (_GROUP_SIM_TOTAL - sup) / 2.0)


def _sample_poisson(lam: float) -> int:
    """Knuth sampler — fine for the small λ (~1–2) of a football scoreline."""
    target = math.exp(-lam)
    k, p = 0, 1.0
    while True:
        p *= random.random()
        if p <= target:
            return k
        k += 1


def _group_winner(ids: list[int], pts: dict, gd: dict, gf: dict,
                  scorelines: dict[tuple[int, int], tuple[int, int]]) -> int:
    """Group winner by FIFA order: pts → GD → goals-for → head-to-head → random."""
    best = max((pts[t], gd[t], gf[t]) for t in ids)
    tied = [t for t in ids if (pts[t], gd[t], gf[t]) == best]
    if len(tied) == 1:
        return tied[0]
    # Head-to-head mini-table among the tied teams (pts → GD → GF → random).
    hp = {t: 0 for t in tied}
    hgd = {t: 0 for t in tied}
    hgf = {t: 0 for t in tied}
    for i in range(len(tied)):
        for j in range(i + 1, len(tied)):
            x, y = tied[i], tied[j]
            sc = scorelines.get((x, y))
            if sc is not None:
                gx, gy = sc
            else:
                sc = scorelines.get((y, x))
                if sc is None:
                    continue
                gy, gx = sc
            hgf[x] += gx
            hgf[y] += gy
            hgd[x] += gx - gy
            hgd[y] += gy - gx
            if gx > gy:
                hp[x] += 3
            elif gx < gy:
                hp[y] += 3
            else:
                hp[x] += 1
                hp[y] += 1
    return max(tied, key=lambda t: (hp[t], hgd[t], hgf[t], random.random()))


def simulate_group(
    member_elos: list[tuple[int, float]],
    sims: int,
    results_by_pair: dict[tuple[int, int], tuple[int, int]] | None = None,
) -> dict[int, float]:
    """Round-robin Monte Carlo → {team_id: P(finish 1st)}.

    Results-aware: games already played — passed in ``results_by_pair`` keyed by
    the ordered ``(home_team_id, away_team_id)`` with value ``(home_goals,
    away_goals)`` — are banked once and only the unplayed pairings are simulated.
    Remaining games sample a Poisson scoreline from the Elo-implied goal rates, so
    each sim has real points, goal difference, goals-for, and head-to-head
    results — and the group winner is decided by the full FIFA tiebreaker order
    (pts → GD → goals-for → head-to-head → random) rather than pts → GD → coin flip.
    """
    results_by_pair = results_by_pair or {}
    ids = [tid for tid, _ in member_elos]
    elo = dict(member_elos)
    first: dict[int, int] = {tid: 0 for tid in ids}
    unordered = [(ids[i], ids[j]) for i in range(len(ids)) for j in range(i + 1, len(ids))]

    # Bank completed results once (constant across sims): points, GD, goals-for,
    # and the per-pair scoreline (for head-to-head).
    base_pts: dict[int, int] = {tid: 0 for tid in ids}
    base_gd: dict[int, int] = {tid: 0 for tid in ids}
    base_gf: dict[int, int] = {tid: 0 for tid in ids}
    base_scores: dict[tuple[int, int], tuple[int, int]] = {}
    remaining: list[tuple[int, int]] = []
    for a, b in unordered:
        if (a, b) in results_by_pair:
            (ga, gb) = results_by_pair[(a, b)]
        elif (b, a) in results_by_pair:
            (gb, ga) = results_by_pair[(b, a)]
        else:
            remaining.append((a, b))
            continue
        base_scores[(a, b)] = (ga, gb)
        base_gf[a] += ga
        base_gf[b] += gb
        base_gd[a] += ga - gb
        base_gd[b] += gb - ga
        if ga > gb:
            base_pts[a] += 3
        elif ga < gb:
            base_pts[b] += 3
        else:
            base_pts[a] += 1
            base_pts[b] += 1

    lambdas = {(a, b): _elo_lambdas(elo[a], elo[b]) for a, b in remaining}

    for _ in range(sims):
        pts = dict(base_pts)
        gd = dict(base_gd)
        gf = dict(base_gf)
        scores = dict(base_scores)
        for a, b in remaining:
            lam_a, lam_b = lambdas[(a, b)]
            ga, gb = _sample_poisson(lam_a), _sample_poisson(lam_b)
            scores[(a, b)] = (ga, gb)
            gf[a] += ga
            gf[b] += gb
            gd[a] += ga - gb
            gd[b] += gb - ga
            if ga > gb:
                pts[a] += 3
            elif ga < gb:
                pts[b] += 3
            else:
                pts[a] += 1
                pts[b] += 1
        first[_group_winner(ids, pts, gd, gf, scores)] += 1
    return {tid: c / sims for tid, c in first.items()}


# ── Pinnacle group winner market ──────────────────────────────────────────────

def _pinnacle_get(url: str, retries: int = 3, base_delay: float = 0.6):
    """GET from Pinnacle's guest API with retry-on-rate-limit.

    The guest endpoint rate-limits bursts with 403/429, so a single group
    market can transiently fail and leave that whole group without market data
    (e.g. Group H dropping while the other 11 succeed). Retry with exponential
    backoff so a throttle doesn't become a permanent gap. Returns the parsed
    JSON, or None after exhausting retries.
    """
    import time

    for attempt in range(retries):
        try:
            r = requests.get(url, headers=_PINNACLE_HEADERS, timeout=12)
            if r.status_code in (403, 429) and attempt < retries - 1:
                time.sleep(base_delay * (2 ** attempt))
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(base_delay * (2 ** attempt))
                continue
            logger.warning("Pinnacle GET failed after %d tries (%s): %s", retries, url, e)
            return None
    return None


def _fetch_pinnacle_group_winner_odds(name_to_id: dict[str, int]) -> dict[int, dict]:
    """Fetch WC group winner odds from Pinnacle's public API.

    Returns {team_id: {"prob_vigfree": float, "american_odds": int}}.
    Pinnacle applies ~3% vig on 4-team futures; we remove it with the
    multiplicative method (divide each implied prob by the group overround).
    Group matching is by team-set intersection, not by label, because our
    derived group labels may differ from Pinnacle's FIFA labels.
    """
    import time

    norm_to_id = {_norm(alias if (alias := _PINNACLE_ALIASES.get(name)) else name): tid
                  for name, tid in name_to_id.items()}
    # Also index by original name and alias
    for orig, aliased in _PINNACLE_ALIASES.items():
        tid = next((v for k, v in name_to_id.items() if _norm(k) == _norm(aliased)), None)
        if tid:
            norm_to_id[_norm(orig)] = tid

    matchups = _pinnacle_get(f"{_PINNACLE_BASE}/leagues/{_PINNACLE_WC_LEAGUE}/matchups")
    if not matchups:
        logger.warning("Pinnacle matchup list unavailable")
        return {}

    gw_matchups = [
        m for m in matchups
        if m.get("special")
        and "group" in m["special"].get("description", "").lower()
        and "winner" in m["special"].get("description", "").lower()
    ]
    logger.info("Pinnacle: %d group winner matchups found", len(gw_matchups))

    result: dict[int, dict] = {}
    for m in gw_matchups:
        pid_to_name = {p["id"]: p["name"] for p in m.get("participants", [])}
        markets = _pinnacle_get(f"{_PINNACLE_BASE}/matchups/{m['id']}/markets/straight")
        if not markets:
            logger.warning("Pinnacle group market unavailable (%s)", m["id"])
            time.sleep(0.1)
            continue

        prices: list[tuple[int, int]] = []  # (team_id, american_odds)
        for mkt in markets:
            if mkt.get("type") != "moneyline":
                continue
            for price in mkt.get("prices", []):
                raw_name = pid_to_name.get(price["participantId"], "")
                tid = norm_to_id.get(_norm(raw_name))
                if tid is not None and price.get("price") is not None:
                    prices.append((tid, int(price["price"])))

        if not prices:
            time.sleep(0.1)
            continue

        # De-vig multiplicatively
        impl = [(tid, american_to_prob(odds)) for tid, odds in prices]
        total = sum(p for _, p in impl)
        if total <= 0:
            time.sleep(0.1)
            continue

        for (tid, impl_prob), (_, american_odds) in zip(impl, prices):
            result[tid] = {
                "prob_vigfree": impl_prob / total,
                "american_odds": american_odds,
            }
        time.sleep(0.1)

    logger.info("Pinnacle group winner odds: %d teams mapped", len(result))
    return result


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
    # Completed group-stage results → bank points/GD so the sim conditions on
    # games already played rather than re-rolling the whole group from Elo.
    played = db.execute(
        "SELECT home_team_id AS h, away_team_id AS a, home_score AS hs, away_score AS as_ "
        "FROM soccer_matchups WHERE home_score IS NOT NULL AND away_score IS NOT NULL"
    )
    results_by_pair = {(r["h"], r["a"]): (r["hs"], r["as_"]) for r in played}
    # Fetch Pinnacle group winner market — degrades gracefully to no-market if unavailable.
    gw_market = _fetch_pinnacle_group_winner_odds(name_to_id)
    with db.connect() as conn:
        for label, members in groups.items():
            if len(members) != 4:
                continue  # only rate/settle complete 4-team groups
            member_elos = [(tid, elo_by_id.get(tid, 1500.0)) for tid in members]
            probs = simulate_group(member_elos, sims, results_by_pair)
            for tid in members:
                if tid not in name_by_id:
                    continue
                mkt = gw_market.get(tid)
                # Anchor to Pinnacle market (sharper than a 4-team single-elim sim).
                if mkt is not None:
                    our_prob = (_W_GROUP_MODEL * probs.get(tid, 0.0)
                                + (1.0 - _W_GROUP_MODEL) * mkt["prob_vigfree"])
                else:
                    our_prob = probs.get(tid, 0.0)
                record_bet(
                    db,
                    model_version=MODEL_VERSION,
                    bet_type="group_winner",
                    scope=f"Group {label}",
                    selection_label=name_by_id[tid],
                    our_prob=our_prob,
                    capture_key=capture_key,
                    subject_team_id=tid,
                    baseline_prob=1.0 / len(members),
                    market_odds=mkt["american_odds"] if mkt else None,
                    market_prob=mkt["prob_vigfree"] if mkt else None,
                    conn=conn,
                    inputs={"elo": round(elo_by_id.get(tid, 1500.0), 1),
                            "group": label, "sims": sims,
                            "sim_prob": round(probs.get(tid, 0.0), 4),
                            "market_vigfree": round(mkt["prob_vigfree"], 4) if mkt else None,
                            "anchor_w_model": _W_GROUP_MODEL,
                            "pinnacle_odds": mkt["american_odds"] if mkt else None},
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
