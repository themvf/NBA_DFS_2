"""Auditable offensive-drive features; no market or future outcome is a feature."""
from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
import math
import re
import numpy as np

KNEEL = re.compile(r"kneel|kneels|kneeling|takes? a knee", re.I)
OFFENSIVE_TD = {"Rushing Touchdown", "Passing Touchdown", "Fumble Recovery (Own)"}
SCRIMMAGE = {"Rush", "Pass Reception", "Pass Incompletion", "Sack", "Rushing Touchdown",
             "Passing Touchdown", "Interception", "Pass Interception Return", "Interception Return Touchdown",
             "Fumble", "Fumble Recovery (Own)", "Fumble Recovery (Opponent)", "Fumble Return Touchdown", "Safety"}
DEFAULT = {"ppd": 2.0, "ppa": 0.0, "success": .4, "explosive": .1,
           "field": 70.0, "seconds": 27.0, "drives": 11.0, "other": 1.0}
FEATURES = ["off_ppd", "opponent_def_ppd", "adjusted_off_ppd", "adjusted_opponent_def_ppd",
            "off_ppa", "opponent_def_ppa", "off_success", "opponent_def_success",
            "off_explosive", "opponent_def_explosive", "off_field", "opponent_def_field", "home"]


def is_snap(play):
    return play.get("play_type") in SCRIMMAGE and not KNEEL.search(play.get("play_text") or "")


def offensive_points(drive, plays):
    """Only credit a source-identified offensive scoring drive, including its PAT delta."""
    result = (drive.get("drive_result") or "").upper()
    start, end = drive.get("start_offense_score"), drive.get("end_offense_score")
    if start is None or end is None:
        return None
    delta = end - start
    if result == "FG":
        return 3 if delta == 3 or any(p.get("play_type")=="Field Goal Good" for p in plays) else None
    if result in {"TD", "FUMBLE TD", "END OF HALF TD"}:
        offensive=[p for p in plays if p.get("play_type") in OFFENSIVE_TD]
        if not offensive:
            if delta==0 and any(p.get("play_type") in {"Fumble Return Touchdown","Interception Return Touchdown"} for p in plays):
                return 0
            return None
        if 6<=delta<=8:
            return delta
        # Some CFBD drive score fields are post-event or stale. Recover only when
        # the linked scoring text explicitly states the conversion, never assume seven.
        text=" ".join(p.get("play_text") or "" for p in offensive).lower()
        if re.search(r"(?:kick|pat|extra point).*(?:fail|miss|block)|(?:two|2)[ -]?point.*(?:fail|no good)",text):
            return 6
        if re.search(r"(?:two|2)[ -]?point.*(?:good|success)|conversion.*(?:good|success)",text):
            return 8
        if re.search(r"\([^)]*\bkick\)|\([^)]*\bkick good\)|\([^)]*\bkick\s*\)",text):
            return 7
        return None
    # A defensive/return touchdown must never become offensive points per drive.
    if delta != 0:
        return None
    return 0


def audit_games(games, drives, plays):
    by_drive, by_game = defaultdict(list), defaultdict(list)
    for p in plays:
        by_drive[(p["game_id"], p.get("cfbd_drive_id"))].append(p)
    for d in drives:
        by_game[d["game_id"]].append(d)
    audits, stats = [], {}
    for game in games:
        if not game["completed"] or game["home_score"] is None or game["away_score"] is None:
            continue
        team_stats = {}
        issues = []
        for team, opponent, score in [(game["home_team_id"], game["away_team_id"], game["home_score"]),
                                      (game["away_team_id"], game["home_team_id"], game["away_score"])]:
            own = [d for d in by_game[game["id"]] if d.get("offense_team_id") == team]
            meaningful, efficiency, snapshots, field, seconds = [], [], [], [], []
            offensive, ot, excluded_points, excluded = 0, 0, 0, 0
            turnovers, nonoff_events = 0, 0
            for d in own:
                ps = by_drive[(game["id"], d["cfbd_drive_id"])]
                points = offensive_points(d, ps)
                if points is None:
                    issues.append({"team": team, "drive_id": d["cfbd_drive_id"], "reason": "unresolved offensive scoring delta"})
                    continue
                if (d.get("start_period") or 0) > 4:
                    ot += points
                    continue
                offensive += points
                snaps = [p for p in ps if is_snap(p) and (p.get("period") or 0) <= 4]
                if not snaps:
                    excluded += 1
                    excluded_points += points
                    continue
                meaningful.append(d)
                good = [p for p in snaps if not (p.get("period") == 4 and p.get("offense_score") is not None
                        and p.get("defense_score") is not None and abs(p["offense_score"] - p["defense_score"]) >= 28)]
                efficiency.extend(good)
                snapshots.extend(snaps)
                if d.get("start_yards_to_goal") is not None:
                    field.append(d["start_yards_to_goal"])
                if d.get("start_period") == d.get("end_period") and d.get("start_seconds_remaining") is not None and d.get("end_seconds_remaining") is not None:
                    duration = d["start_seconds_remaining"] - d["end_seconds_remaining"]
                    if 0 < duration <= 900:
                        seconds.append((duration, len(snaps)))
                result = d.get("drive_result") or ""
                turnovers += result in {"INT", "INT TD", "FUMBLE", "FUMBLE RETURN TD"} or (result=="FUMBLE TD" and points==0)
                nonoff_events += result in {"INT TD", "FUMBLE RETURN TD", "PUNT TD", "KICKOFF RETURN TD", "MISSED FG TD", "SF"}
            ppa = [float(p["ppa"]) for p in efficiency if p.get("ppa") is not None and math.isfinite(float(p["ppa"]))]
            other = score - offensive - ot
            if other < 0 or not meaningful or len(ppa) < 30:
                issues.append({"team": team, "reason": "score reconciliation or usable drive/PPA coverage"})
            n = len(meaningful)
            team_stats[team] = {
                "off": {"ppd": (offensive-excluded_points)/n if n else None,
                        "ppa": float(np.mean(ppa)) if ppa else None,
                        "success": float(np.mean([p > 0 for p in ppa])) if ppa else None,
                        "explosive": sum((p.get("yards_gained") or 0) >= (20 if "Pass" in (p.get("play_type") or "") else 10) for p in efficiency) / len(efficiency) if efficiency else None,
                        "field": float(np.mean(field)) if field else 70.,
                        "seconds": sum(a for a,b in seconds)/sum(b for a,b in seconds) if seconds else 27.,
                        "drives": n, "other": other+ot},
                "audit": {"final_points": score, "source_drives":len(own),
                          "overtime_drives":sum((d.get("start_period") or 0)>4 for d in own),
                          "regulation_offensive_points": offensive,
                          "overtime_offensive_points": ot, "nonoffensive_or_unattributed_points": other,
                          "meaningful_drives": n, "excluded_drives": excluded, "excluded_drive_points": excluded_points,
                          "scrimmage_snaps": len(snapshots), "efficiency_snaps": len(efficiency),
                          "garbage_snaps_excluded": len(snapshots)-len(efficiency), "ppa_plays": len(ppa),
                          "turnover_drives": turnovers, "opponent_return_or_safety_drives": nonoff_events,
                          "score_reconciled": other >= 0,
                          "attribution_limit": "Remainder is not automatically classified as defense/special teams; source omissions remain possible."},
                "game_id": game["id"], "team": team, "opponent": opponent,
                "kickoff": game["commence_time"], "season": game["season"],
            }
        for team, value in team_stats.items():
            other = team_stats[value["opponent"]]
            value["def"] = other["off"]
            value["usable"] = not issues
            stats[(game["id"], team)] = value
        audits.append({"game_id": game["id"], "home": game["home_name"], "away": game["away_name"],
                       "usable": not issues, "issues": issues,
                       "teams": {str(t): s["audit"] for t,s in team_stats.items()}})
    return stats, audits


def profile(entries, season, prior, half_life):
    current = [s for s in entries if s["season"] == season]
    previous = [s for s in entries if s["season"] == season-1]
    def average(rows, section, key, fallback):
        values = [(r[section].get(key), .5**((len(rows)-1-i)/half_life)) for i,r in enumerate(rows)]
        values = [(v,w) for v,w in values if v is not None and math.isfinite(v)]
        return sum(v*w for v,w in values)/sum(w for v,w in values) if values else fallback
    weight = len(current)/(len(current)+prior) if current else 0.
    result = {section: {} for section in ("off","def")}
    for section in result:
        for key, fallback in DEFAULT.items():
            old = average(previous,section,key,fallback)
            result[section][key] = weight*average(current,section,key,old)+(1-weight)*old
    result.update(current_games=len(current), current_weight=weight,
                  source_games=[s["game_id"] for s in current+previous],
                  ppa_plays=sum(s["audit"]["ppa_plays"] for s in current))
    return result


def replay(games, stats, prior, half_life):
    history, rows, cache = defaultdict(list), [], {}
    for game in sorted(games, key=lambda g:(g["commence_time"],g["id"])):
        at = game["commence_time"]
        # Do not treat a merely earlier kickoff on the same day as an available final.
        boundary = at.replace(hour=0,minute=0,second=0,microsecond=0)
        ids = (game["home_team_id"],game["away_team_id"])
        def get_profile(t):
            key=(boundary,t)
            if key not in cache:
                cache[key]=profile([s for s in history[t] if s["kickoff"]+timedelta(hours=6)<boundary],game["season"],prior,half_life)
            return cache[key]
        profiles = {t:get_profile(t) for t in ids}
        if min(p["current_games"] for p in profiles.values()) >= 2:
            side_features=[]
            for index,team in enumerate(ids):
                opponent=ids[1-index];own,opp=profiles[team],profiles[opponent]
                def adjust(team_id,section):
                    es=[s for s in history[team_id] if s["kickoff"]+timedelta(hours=6)<boundary and s["season"] in (game["season"],game["season"]-1)]
                    corrections=[]
                    for e in es:
                        op=get_profile(e["opponent"])
                        corrections.append(op["def" if section=="off" else "off"]["ppd"]-2.)
                    return profiles[team_id][section]["ppd"]-(float(np.mean(corrections)) if corrections else 0.)
                side_features.append([own["off"]["ppd"],opp["def"]["ppd"],adjust(team,"off"),adjust(opponent,"def"),
                                      own["off"]["ppa"],opp["def"]["ppa"],own["off"]["success"],opp["def"]["success"],
                                      own["off"]["explosive"],opp["def"]["explosive"],own["off"]["field"],opp["def"]["field"],float(index==0 and not game.get("neutral_site"))])
            pace_features=[[profiles[t]["off"]["drives"],profiles[ids[1-i]]["def"]["drives"],
                            profiles[t]["off"]["seconds"],profiles[ids[1-i]]["off"]["seconds"],
                            profiles[t]["off"]["success"],profiles[ids[1-i]]["off"]["success"]] for i,t in enumerate(ids)]
            expected=[(profiles[t]["off"]["drives"]+profiles[ids[1-i]]["def"]["drives"])/2 for i,t in enumerate(ids)]
            row={**game,"x_home":side_features[0],"x_away":side_features[1],"expected_drives":expected,"pace_features":pace_features,
                 "profiles":profiles,"home_current_games":profiles[ids[0]]["current_games"],
                 "away_current_games":profiles[ids[1]]["current_games"],
                 "home_ppa_plays":profiles[ids[0]]["ppa_plays"],"away_ppa_plays":profiles[ids[1]]["ppa_plays"]}
            actual=[stats.get((game["id"],t)) for t in ids]
            row["actual"]=actual if all(a and a["usable"] for a in actual) else None
            rows.append(row)
        for team in ids:
            s=stats.get((game["id"],team))
            if s and s["usable"]:
                history[team].append(s)
    return rows
