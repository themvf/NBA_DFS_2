"""Exploratory scrimmage-TD model. Pure functions; no production writes.

TD hazards use ALL eligible carries/targets, not only scoring plays. A scored
play's distance equals its simulated starting distance to the goal line.
Possessions update score, clock, down and field position after every snap.
All priors and approximations are explicit; outputs are not calibrated odds.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
from typing import Any

import numpy as np

VERSION = "nfl-longest-scrimmage-td-research-v1"
IMPLEMENTATION_SHA256 = sha256(Path(__file__).read_bytes()).hexdigest()
FIELD_BOUNDS = (5, 10, 20, 39, 59, 99)


def timestamp(value: Any) -> datetime:
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("Timestamps must include a timezone")
    return dt.astimezone(timezone.utc)


def canonical(team: str) -> str:
    return {"LA": "LAR", "WAS": "WSH"}.get(team, team)


def field_bin(distance: float) -> int:
    if not 0 < distance <= 99:
        raise ValueError("Scrimmage field position must be in (0,99]")
    return next(i for i, upper in enumerate(FIELD_BOUNDS) if distance <= upper)


def score_state(diff: int) -> str:
    return "leading" if diff > 7 else "trailing" if diff < -7 else "close"


@dataclass(frozen=True)
class Settings:
    seed: int = 20261006
    draws: int = 2000
    prior_season_weight: float = .35
    peer_prior_opportunities: float = 80.
    defense_prior_opportunities: float = 150.
    team_prior_snaps: float = 35.
    role_prior_opportunities: float = 20.
    half_life_weeks: float = 6.
    max_snaps: int = 220
    # v1 hands every team opportunity to a named supported player, so a scorer
    # outside that list (newcomer, depth player, unsupported roster name) has a
    # structural zero. When on, OTHER:TEAM receives the share of opportunities
    # that historically went to players absent from the team's prior three games.
    newcomer_reserve: bool = False

    def __post_init__(self):
        if self.draws < 1 or self.max_snaps < 1:
            raise ValueError("Positive draw and snap counts required")
        if not 0 < self.prior_season_weight <= 1:
            raise ValueError("Prior-season weight must be in (0,1]")
        if min(self.peer_prior_opportunities, self.defense_prior_opportunities,
               self.team_prior_snaps, self.role_prior_opportunities, self.half_life_weeks) <= 0:
            raise ValueError("Smoothing settings must be positive")


def prepare(snapshot: dict, decision_at: str, *, retrospective: bool = False) -> tuple[list[dict], dict]:
    """Gate schedule/labels and conservatively verify offensive TD descriptions.

    No name-only player joins, duplicate role actors, overturned TDs, returns,
    lateral/fumble scoring attribution, kneels, spikes or two-point attempts.
    Unresolvable scoring plays are quarantined, never silently counted as zeros.
    """
    cutoff = timestamp(decision_at)
    rows, audit, seen = [], Counter(), set()
    for raw in snapshot["plays"]:
        key = (raw["game_id"], int(raw["play_id"]))
        if key in seen:
            raise ValueError(f"Duplicate source play: {key}")
        seen.add(key)
        if timestamp(raw["kickoff"]) >= cutoff:
            audit["future_game_excluded"] += 1
            continue
        if timestamp(raw["labelled_at"]) > cutoff and not retrospective:
            audit["later_label_excluded"] += 1
            continue
        if raw.get("season_type") != "REG" or raw.get("game_type") != "REG":
            continue
        if (canonical(raw["home_team"]) != raw["canonical_home"]
                or canonical(raw["away_team"]) != raw["canonical_away"]
                or int(raw["season"]) != int(raw["canonical_season"])
                or int(raw["week"]) != int(raw["canonical_week"])):
            raise ValueError(f"Canonical schedule mismatch: {key}")
        text = (raw.get("description") or "").upper()
        action = raw.get("play_type")
        if action not in ("run", "pass", "punt", "field_goal"):
            continue
        if any(s in text for s in ("NO PLAY", "NULLIFIED", "OVERTURNED", "REVERSED", "TWO-POINT", "TWO POINT", "KNEEL", "SPIKE")):
            audit["administrative_or_ambiguous_excluded"] += 1
            continue
        if any(raw.get(k) is None for k in ("yardline_100", "down", "ydstogo", "game_seconds_remaining", "score_differential")):
            audit["missing_context_excluded"] += 1
            continue
        field = float(raw["yardline_100"])
        if not 0 < field <= 99 or not 1 <= int(raw["down"]) <= 4:
            audit["invalid_context_excluded"] += 1
            continue
        td_mention = "TOUCHDOWN" in text
        turnover = bool(raw.get("turnover_type"))
        actor_role = "rusher" if action == "run" else "receiver"
        actors = {(a.get("player_id"), a.get("position")) for a in raw.get("actors", []) if a["role"] == actor_role and a.get("player_id")}
        # Missing actor on an incomplete pass/sack is a team event; on a score
        # it must never be assigned to a guessed receiver or rusher.
        if len(actors) > 1:
            audit["ambiguous_actor_excluded"] += 1
            continue
        actor, position = next(iter(actors)) if actors else (None, None)
        if td_mention and (action not in ("run", "pass") or turnover or "FUMBLE" in text or "LATERAL" in text):
            audit["return_or_complex_score_excluded"] += 1
            continue
        if td_mention and (not actor or raw.get("yards_gained") is None
                           or abs(float(raw["yards_gained"]) - field) > 1):
            audit["unverified_offensive_td_excluded"] += 1
            continue
        if action in ("run", "pass") and not text:
            audit["missing_description_excluded"] += 1
            continue
        row = dict(raw)
        row.update(team=canonical(raw["posteam"]), defense=canonical(raw["defteam"]),
                   actor=actor, position=position or "UNKNOWN",
                   field=field, field_bin=field_bin(field), action=action,
                   td=td_mention, turnover=turnover,
                   state=score_state(int(raw["score_differential"])),
                   late=int(raw["game_seconds_remaining"]) <= 900)
        rows.append(row)
        audit["verified_td" if td_mention else "non_scoring_play"] += 1
        if actor and not position:
            audit["position_fallback"] += 1
    if not rows:
        raise ValueError("No eligible pre-decision plays; use explicit retrospective mode for later labels")
    return rows, dict(audit)


def newcomer_share(rows: list[dict]) -> dict:
    """Per action, the share of team opportunities taken by players who had no
    rushing/receiving opportunity in that team's previous three games.

    Computed only from the supplied (pre-decision) training rows, pooled across
    teams, within each team-season (offseason churn is not an in-season
    newcomer), so it is walk-forward safe. Games without three prior same-season
    team games are skipped.
    """
    by_team = defaultdict(lambda: defaultdict(list))
    kickoff = {}
    for r in rows:
        if r["action"] in ("run", "pass") and r["actor"]:
            by_team[r["team"], int(r["season"])][r["game_id"]].append(r)
            kickoff[r["game_id"]] = timestamp(r["kickoff"])
    counts = {"run": [0, 0], "pass": [0, 0]}
    for games in by_team.values():
        ordered = sorted(games, key=lambda g: kickoff[g])
        for i in range(3, len(ordered)):
            seen = {r["actor"] for g in ordered[i-3:i] for r in games[g]}
            for r in games[ordered[i]]:
                c = counts[r["action"]]
                c[0] += r["actor"] not in seen
                c[1] += 1
    return {a: (c[0]/c[1] if c[1] else 0.) for a, c in counts.items()}


class TouchdownModel:
    def __init__(self, rows: list[dict], season: int, week: int, settings: Settings):
        self.rows, self.season, self.week, self.cfg = rows, season, week, settings
        self.action_cells = defaultdict(list)
        self.roles = defaultdict(list)
        self.events = defaultdict(list)
        self.gain_rows = defaultdict(list)
        self.league_hazards = defaultdict(lambda: [0., 0.])
        self.hazards = defaultdict(lambda: [0., 0.])
        self.current = defaultdict(list)
        self.cache = {}
        self.weights = {}
        self.starts = []
        self.reserve = newcomer_share(rows) if settings.newcomer_reserve else {"run": 0., "pass": 0.}
        for r in rows:
            age = max(0, week - int(r["week"])) if int(r["season"]) == season else 0
            w = .5 ** (age / settings.half_life_weeks) if int(r["season"]) == season else settings.prior_season_weight ** (season - int(r["season"]))
            self.weights[r["game_id"], int(r["play_id"])] = w
            r["_weight"] = w
            cell = (int(r["down"]), r["field_bin"], r["state"], r["late"], int(float(r["ydstogo"]) > 5))
            for scope in ("league", r["team"]):
                self.action_cells[scope, cell].append(r)
                self.action_cells[scope, (cell[0], cell[1])].append(r)
            if int(r["down"]) == 1 and int(r.get("drive_plays") or 0) > 0:
                self.starts.append(r["field"])
            if r["action"] in ("run", "pass"):
                self.events[r["action"], r["field_bin"]].append(r)
                if not r["td"]:
                    for scope in (("player", r["actor"]), ("peer", r["position"]), ("league", "all")):
                        self.gain_rows[scope, r["action"], r["field_bin"]].append(r)
                if r["actor"]:
                    h = self.league_hazards[r["action"], r["field_bin"]]
                    h[0] += w*r["td"]; h[1] += w
                    for scope in ("all", r["state"]):
                        self.roles[r["team"], r["action"], r["field_bin"], scope].append(r)
                        self.roles[r["team"], r["action"], "all", scope].append(r)
                    for scope in (("peer", r["position"]), ("player", r["actor"])):
                        h = self.hazards[scope, r["action"], r["field_bin"]]
                        h[0] += w * r["td"]; h[1] += w
                if int(r["season"]) == season:
                    self.current[r["action"], r["field_bin"]].append(r)

    def weight(self, row):
        return row["_weight"]

    def choices(self, key, rows, weights):
        if key not in self.cache:
            if not rows:
                raise ValueError(f"No empirical support for {key}")
            p = np.asarray(weights, float)
            p /= p.sum()
            self.cache[key] = (rows, np.cumsum(p))
        return self.cache[key]

    @staticmethod
    def pick(rng, pool):
        rows, cdf = pool
        return rows[min(len(rows)-1, int(np.searchsorted(cdf, rng.random())))]

    def action_pool(self, team, down, field, diff, clock, to_go):
        cell = (down, field_bin(field), score_state(diff), clock <= 900, int(to_go > 5))
        key = ("action", team, cell)
        if key in self.cache:
            return self.cache[key]
        league = self.action_cells["league", cell] or self.action_cells["league", cell[:2]]
        own = self.action_cells[team, cell] or self.action_cells[team, cell[:2]]
        if not league:
            league = [r for r in self.rows if int(r["down"]) == down]
        weights = [self.weight(r) for r in own]
        lw = [self.weight(r) for r in league]
        scale = self.cfg.team_prior_snaps / sum(lw)
        return self.choices(key, own + league, weights + [w*scale for w in lw])

    def role_pool(self, team, action, field, diff, active):
        b, state = field_bin(field), score_state(diff)
        key = ("role", team, action, b, state, tuple(sorted(active)))
        if key in self.cache:
            return self.cache[key]
        # Current-role history is preferred; old-season teammate roles are not
        # inherited automatically. Caller explicitly supplies the eligible roster.
        base = [r for r in self.roles[team, action, "all", "all"] if r["actor"] in active]
        current = [r for r in base if int(r["season"]) == self.season]
        base = current or base
        local = [r for r in base if r["field_bin"] == b and r["state"] == state]
        if not base:
            return None
        weights = [self.weight(r) for r in local]
        bw = [self.weight(r) for r in base]
        scale = self.cfg.role_prior_opportunities / sum(bw)
        return self.choices(key, local + base, weights + [w*scale for w in bw])

    def defense_factor(self, defense, action, b):
        key = ("defense", defense, action, b)
        if key in self.cache:
            return self.cache[key]
        rows = [r for r in self.current[action, b] if r["actor"]]
        allowed = [r for r in rows if r["defense"] == defense]
        expected, observed, exposure = 0., 0., 0
        for offense in {r["team"] for r in allowed}:
            against = [r for r in allowed if r["team"] == offense]
            other = [r for r in rows if r["team"] == offense and r["defense"] != defense]
            if not other:
                continue
            expected += len(against)*sum(r["td"] for r in other)/len(other)
            observed += sum(r["td"] for r in against)
            exposure += len(against)
        baseline = sum(r["td"] for r in rows)/max(1, len(rows))
        prior = self.cfg.defense_prior_opportunities * max(baseline, .001)
        factor = float(np.clip((observed+prior)/(expected+prior), .5, 2.))
        result = {"factor": factor, "comparable_opportunities": exposure,
                  "observed_td": observed, "expected_td_from_opponents_other_games": expected}
        self.cache[key] = result
        return result

    def td_probability(self, actor, position, action, field, defense):
        b = field_bin(field)
        key = ("td", actor, position, action, b, defense)
        if key in self.cache:
            return self.cache[key]
        own_td, own_n = self.hazards[("player", actor), action, b]
        peer_td, peer_n = self.hazards[("peer", position), action, b]
        league_td, league_n = self.league_hazards[action, b]
        if not league_n:
            raise ValueError(f"No scoring-opportunity coverage for {action}/{b}")
        peer_rate = (peer_td + 20*league_td/league_n)/(peer_n+20)
        p = (own_td+self.cfg.peer_prior_opportunities*peer_rate)/(own_n+self.cfg.peer_prior_opportunities)
        factor = self.defense_factor(defense, action, b)["factor"]
        adjusted = p*factor/(1-p+p*factor)
        result = float(np.clip(adjusted, 0., .95)), {"weighted_player_td": own_td,
               "weighted_player_opportunities": own_n, "peer_rate": peer_rate,
               "defense": self.defense_factor(defense, action, b)}
        self.cache[key] = result
        return result

    def gain_pool(self, actor, position, action, field, state):
        b = field_bin(field)
        key = ("gain", actor, position, action, b, state)
        if key in self.cache:
            return self.cache[key]
        own = self.gain_rows[("player", actor), action, b]
        local = [r for r in own if r["state"] == state]
        peers = self.gain_rows[("peer", position), action, b]
        peers = peers or self.gain_rows[("league", "all"), action, b]
        recent = local or own
        pw = [self.weight(r) for r in peers]
        if not pw:
            raise ValueError(f"No non-scoring yardage coverage for {action}/{b}")
        scale = 20/sum(pw)
        return self.choices(key, recent+peers, [self.weight(r) for r in recent]+[w*scale for w in pw])


def simulate(snapshot: dict, request: dict, settings: Settings = Settings()) -> dict:
    cutoff = request["decision_at"]
    target = request["game"]
    if snapshot.get("games") is not None:
        matches = [g for g in snapshot["games"] if g["game_id"] == target["game_id"]]
        if len(matches) != 1 or any(matches[0][k] != target[k] for k in ("season", "week", "away", "home")) or timestamp(matches[0]["kickoff"]) != timestamp(target["kickoff"]):
            raise ValueError("Target does not match the frozen canonical schedule")
    if timestamp(cutoff) >= timestamp(target["kickoff"]):
        raise ValueError("Decision must precede target kickoff, including retrospective replays")
    rows, coverage = prepare(snapshot, cutoff, retrospective=request.get("retrospective", False))
    if any(r["game_id"] == target["game_id"] for r in rows):
        raise ValueError("Target game leaked into training")
    teams = (canonical(target["away"]), canonical(target["home"]))
    if teams[0] == teams[1]:
        raise ValueError("Two distinct teams required")
    roster = request["players"]
    if len({p["identity"] for p in roster}) != len(roster):
        raise ValueError("Duplicate roster identities")
    if any(p["team"] not in teams or p.get("status") not in ("active", "out") for p in roster):
        raise ValueError("Explicit team and active/out status required for every player")
    active = {t: {p["identity"] for p in roster if p["team"] == t and p["status"] == "active"} for t in teams}
    if not all(active.values()):
        raise ValueError("Both teams require an eligible roster")
    model = TouchdownModel(rows, int(target["season"]), int(target["week"]), settings)
    rng = np.random.default_rng(settings.seed)
    supported = {r["actor"] for r in rows if r["actor"] and r["team"] in teams and r["actor"] in active[r["team"]]}
    identities = sorted(supported | {"OTHER:"+t for t in teams})
    maxima = np.zeros((settings.draws, len(identities)), int)
    opportunities = np.zeros_like(maxima)
    scores = np.zeros((settings.draws, 2), int)
    id_index = {i: j for j, i in enumerate(identities)}
    diagnostics, sample_ledgers = Counter(), []
    starts = []
    by_drive = defaultdict(list)
    for r in rows:
        if r.get("drive") is not None:
            by_drive[r["game_id"], r["team"], r["drive"]].append(r)
    for drive in by_drive.values():
        starts.append(min(drive, key=lambda r: int(r["play_id"]))["field"])
    if not starts:
        raise ValueError("Drive-start coverage required")
    for draw in range(settings.draws):
        score, clock, side = [0, 0], 3600, int(rng.integers(2))
        field, down, to_go, ledger = 75., 1, 10., []
        for snap in range(settings.max_snaps):
            if clock <= 0:
                break
            team, defense = teams[side], teams[1-side]
            diff = score[side]-score[1-side]
            if clock <= 120 and diff > 0 and side == int(np.argmax(score)):
                diagnostics["kneel_clock_end"] += 1
                break
            template = model.pick(rng, model.action_pool(team, down, field, diff, clock, to_go))
            action = template["action"]
            actor, distance, change, result = None, 0, False, "gain"
            prefield, before = field, clock
            if action == "punt":
                change, result = True, "punt"
                next_field = float(rng.choice(starts))
            elif action == "field_goal":
                made = "GOOD" in (template.get("description") or "").upper() and "NO GOOD" not in (template.get("description") or "").upper()
                score[side] += 3*int(made)
                change, result = True, "field_goal" if made else "missed_field_goal"
                next_field = 75. if made else float(np.clip(100-field-7, 1, 99))
            else:
                roles = model.role_pool(team, action, field, diff, active[team])
                if action == "pass" and template.get("had_sack"):
                    event = template
                elif roles and not (model.reserve[action] and rng.random() < model.reserve[action]):
                    participant = model.pick(rng, roles)
                    actor, position = participant["actor"], participant["position"]
                    opportunities[draw, id_index[actor]] += 1
                    p, _ = model.td_probability(actor, position, action, field, defense)
                    if rng.random() < p:
                        distance = int(round(field))
                        maxima[draw, id_index[actor]] = max(maxima[draw, id_index[actor]], distance)
                        score[side] += 7
                        change, result, next_field = True, "touchdown", 75.
                        event = None
                    else:
                        event = model.pick(rng, model.gain_pool(actor, position, action, field, score_state(diff)))
                else:
                    actor = "OTHER:"+team
                    opportunities[draw, id_index[actor]] += 1
                    # Keep unmodeled team work visible and include residual
                    # scorers in the winning field rather than renormalize it away.
                    eligible = [r for r in model.events[action, field_bin(field)] if r["actor"]]
                    reference = eligible[int(rng.integers(len(eligible)))]
                    p, _ = model.td_probability(actor, reference["position"], action, field, defense)
                    if rng.random() < p:
                        distance = int(round(field)); maxima[draw, id_index[actor]] = max(maxima[draw, id_index[actor]], distance)
                        score[side] += 7; change, result, next_field = True, "touchdown", 75.; event = None
                    else:
                        event = model.pick(rng, model.gain_pool(actor, reference["position"], action, field, score_state(diff)))
                if event is not None:
                    gain = float(event.get("yards_gained") or 0)
                    # A non-scoring empirical gain never manufactures a TD.
                    gain = min(gain, field-1)
                    field = float(np.clip(field-gain, 1, 99))
                    if event["turnover"]:
                        change, result, next_field = True, "turnover", 100-field
                    elif gain >= to_go:
                        down, to_go = 1, min(10., field)
                    elif down == 4:
                        change, result, next_field = True, "downs", 100-field
                    else:
                        down += 1; to_go = min(field, max(1., to_go-gain))
            # Empirical between-snap duration where available, otherwise an
            # explicit 25-second approximation. Late trailing snaps run faster
            # only when their source tempo supports it.
            seconds = int(template.get("elapsed_seconds") or 25)
            clock -= int(np.clip(seconds, 5, 60))
            diagnostics[team+":"+score_state(diff)+":"+action] += 1
            entry = {"team": team, "clock_before": before, "field_before": prefield,
                     "state": score_state(diff), "action": action, "actor": actor,
                     "result": result, "td_distance": distance, "score_after": list(score)}
            if draw < 3:
                ledger.append(entry)
            # Halftime crossing must be tracked independently of stored examples.
            if before > 1800 >= clock:
                side = 1-side; field, down, to_go = 75., 1, 10.
            elif change:
                side = 1-side; field = float(np.clip(next_field, 1, 99)); down, to_go = 1, min(10., field)
        else:
            diagnostics["snap_cap_reached"] += 1
        scores[draw] = score
        if draw < 3:
            sample_ledgers.append(ledger)
    best = maxima.max(axis=1)
    any_td = best > 0
    winners = (maxima == best[:, None]) & any_td[:, None]
    ties = winners.sum(axis=1)
    shares = np.divide(winners, ties[:, None], out=np.zeros_like(maxima, dtype=float), where=ties[:, None] > 0).mean(axis=0)
    names = {p["identity"]: p["name"] for p in roster}
    predictions = []
    evidence = {}
    for identity in identities:
        j = id_index[identity]
        predictions.append({"identity": identity, "name": names.get(identity, identity),
            "any_td_probability": float(np.mean(maxima[:, j] > 0)),
            "td_20_plus_probability": float(np.mean(maxima[:, j] >= 20)),
            "td_40_plus_probability": float(np.mean(maxima[:, j] >= 40)),
            "longest_td_win_share": float(shares[j]),
            "sole_longest_probability": float(np.mean(winners[:, j] & (ties == 1))),
            "longest_or_tied_probability": float(winners[:, j].mean()),
            "mean_opportunities": float(opportunities[:, j].mean())})
        evidence[identity] = [{"action": action, "field_range": [1 if b == 0 else FIELD_BOUNDS[b-1]+1, upper],
                              "weighted_td": h[0], "weighted_opportunities": h[1]}
            for ((scope, actor), action, b), h in model.hazards.items() if scope == "player" and actor == identity
            for upper in (FIELD_BOUNDS[b],)]
    assert abs(float(shares.sum()) + float(np.mean(~any_td)) - 1) < 1e-8
    return {"version": VERSION, "authority": "unvalidated_exploratory", "production_changed": False,
        "decision_at": cutoff, "game": target, "settings": asdict(settings),
        "label_policy": "retrospective_corrected_sources" if request.get("retrospective") else "labelled_at_lte_decision",
        "source_sha256": sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest(),
        "request_sha256": sha256(json.dumps(request, sort_keys=True).encode()).hexdigest(),
        "implementation_sha256": IMPLEMENTATION_SHA256,
        "unresolved_players": [p for p in roster if p["status"] == "active" and p["identity"] not in supported],
        "coverage": coverage, "training_games": len({r["game_id"] for r in rows}),
        "training_seasons": sorted({int(r["season"]) for r in rows}), "player_evidence": evidence,
        "players": sorted(predictions, key=lambda p: -p["longest_td_win_share"]),
        "no_scrimmage_td_probability": float(np.mean(~any_td)),
        "mean_team_scores": dict(zip(teams, scores.mean(axis=0).tolist())),
        "diagnostics": dict(diagnostics), "sample_ledgers": sample_ledgers,
        "newcomer_reserve": model.reserve,
        "market_inputs_used": False, "roster_evidence": request.get("roster_evidence", "caller_supplied_unverified"),
        "limitations": ["Scrimmage rushing/receiving TDs only; excludes defensive and return scores, laterals, safeties, overtime and two-point plays.",
            "Possession mechanics approximate kicks, halftime receiving order, clock, penalties, turnovers and extra points; not a full football rules engine.",
            "Field bins and smoothing parameters are research assumptions, not calibrated coefficients.",
            "Active roster is caller-supplied; active players without historical team-role support fall into OTHER. No automatic injury or replacement-role forecast.",
            "Defensive TD-risk factors compare current opponents with their other games; no direct injury or weather effect.",
            "Current PBP and participant identities may include later corrections. Retrospective evaluation is not a frozen historical forecast.",
            "No demonstrated calibration or profitable edge; Monte Carlo sample size does not establish predictive accuracy."]}


def evaluate(prediction: dict, actual_rows: list[dict]) -> dict:
    """Grade all identities, with fractional tie credit and no-TD outcome."""
    if not any(row["game_id"] == prediction["game"]["game_id"] for row in actual_rows):
        raise ValueError("Missing actual PBP is unknown, not a no-touchdown game")
    longest = defaultdict(int)
    eligible_ids = {p["identity"] for p in prediction["players"]}
    for row in actual_rows:
        if row["game_id"] != prediction["game"]["game_id"]:
            continue
        if row["td"]:
            identity = row["actor"] if row["actor"] in eligible_ids else "OTHER:"+row["team"]
            longest[identity] = max(longest[identity], int(round(row["field"])))
    best = max(longest.values(), default=0)
    winners = {i for i, d in longest.items() if d == best} if best else set()
    probabilities = {p["identity"]: p["longest_td_win_share"] for p in prediction["players"]}
    probabilities["NO_TD"] = prediction["no_scrimmage_td_probability"]
    observed = {i: (1/len(winners) if i in winners else 0) for i in probabilities}
    if not winners:
        observed["NO_TD"] = 1.
    return {"game_id": prediction["game"]["game_id"], "longest_td_yards": best,
            "winners": sorted(winners), "brier_score": sum((probabilities[i]-observed[i])**2 for i in probabilities),
            "log_loss": -sum(observed[i]*math.log(max(probabilities[i], 1e-12)) for i in probabilities),
            "top_choice_hit": prediction["players"][0]["identity"] in winners,
            "calibration_rows": [{"identity": i, "probability": probabilities[i], "observed_share": observed[i]} for i in probabilities],
            # Per-player TD events carry far more information than one winner per
            # game; they test whether the simulated TD rates themselves are right.
            "td_event_rows": [{"identity": p["identity"], "any_td_probability": p["any_td_probability"],
                               "td_40_plus_probability": p["td_40_plus_probability"],
                               "scored": longest.get(p["identity"], 0) > 0,
                               "scored_40_plus": longest.get(p["identity"], 0) >= 40}
                              for p in prediction["players"] if "any_td_probability" in p]}
