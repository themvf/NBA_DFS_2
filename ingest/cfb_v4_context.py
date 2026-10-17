"""Capture forecast weather and import sourced CFB availability observations.

Team input is a JSON list of game_id, team_id, source_url, source_published_at,
qb (name, source_player_id, status, changed), availability_reviewed, ol_reviewed,
coach_changed and roster_continuity. Unknown fields stay null. Published sources
are downloaded and hashed; roster listings alone do not confirm a starter.
"""
from __future__ import annotations

import argparse
from datetime import datetime,timezone,timedelta
import hashlib
import json
import math
import os
from pathlib import Path
from urllib.parse import urlparse
import requests

from config import load_config
from db.database import DatabaseManager
from research.cfb_v4_store import ensure_schema,save_context


def refresh_venue_coordinates(db,api_key):
    """Join source venue IDs to canonical venues; never match on stadium name."""
    if not api_key:return {"status":"unavailable","reason":"CFBD_API_KEY_missing"}
    url="https://api.collegefootballdata.com/venues"
    try:
        response=requests.get(url,headers={"Authorization":"Bearer "+api_key},timeout=30)
        response.raise_for_status();rows=response.json()
    except (requests.RequestException,ValueError) as exc:
        return {"status":"unavailable","source_url":url,"reason":type(exc).__name__}
    if not isinstance(rows,list):return {"status":"unavailable","source_url":url,"reason":"invalid_venue_response"}
    values=[]
    for row in rows:
        lat,lon=row.get("latitude"),row.get("longitude")
        if (row.get("id") is not None and isinstance(lat,(float,int)) and isinstance(lon,(float,int))
                and math.isfinite(lat) and math.isfinite(lon) and -90<=lat<=90 and -180<=lon<=180):
            values.append((lat,lon,row["id"]))
    db.execute_many("UPDATE cfb_venues SET latitude=%s,longitude=%s,updated_at=NOW() WHERE cfbd_venue_id=%s",values)
    return {"status":"captured","source_url":url,"captured_at":datetime.now(timezone.utc).isoformat(),
            "response_sha256":hashlib.sha256(response.content).hexdigest(),"valid_source_venues":len(values)}


def validate_team(payload,game,now):
    if payload.get("team_id") not in (game["home_team_id"],game["away_team_id"]):
        raise ValueError("Team does not belong to canonical game")
    if now>=game["commence_time"]:
        raise ValueError("Post-kickoff observation cannot be imported as pregame")
    url=payload.get("source_url","")
    if urlparse(url).scheme!="https" or not urlparse(url).hostname:
        raise ValueError("HTTPS published source is required")
    published=datetime.fromisoformat(payload["source_published_at"].replace("Z","+00:00"))
    if published.tzinfo is None or published>now:
        raise ValueError("Invalid source publication time")
    qb=payload.get("qb") or {}
    if qb.get("status")=="confirmed" and (not qb.get("name") or not qb.get("source_player_id")):
        raise ValueError("Confirmed starter requires name and canonical source identity")
    if payload.get("availability_reviewed") is True:
        players=payload.get("unavailable_players")
        if not isinstance(players,list) or any(not p.get("source_player_id") or not p.get("position") or not p.get("status") for p in players):
            raise ValueError("Reviewed availability needs an explicit list with player identities, positions and statuses (empty if none)")
    continuity=payload.get("roster_continuity")
    if continuity is not None and not 0<=continuity<=1:
        raise ValueError("Roster continuity must be a fraction")
    return published


def context_status(captures,game,at):
    valid=[c for c in captures if c["game_id"]==game["id"] and c["captured_at"]<=at<game["commence_time"]
           and c["captured_at"]<game["commence_time"]
           and (c.get("source_published_at") is None or c["source_published_at"]<=c["captured_at"])]
    reasons=[];selected={}
    weather=[c for c in valid if c["kind"]=="weather" and at-c["captured_at"]<=timedelta(hours=6)]
    if weather:
        w=max(weather,key=lambda c:c["captured_at"]);selected["weather"]=w
        if w["payload_json"].get("status")!="captured":reasons.append("weather_unavailable")
    else:reasons.append("weather_missing_or_stale")
    for side in ("home","away"):
        ts=[c for c in valid if c["kind"]=="team" and c["team_id"]==game[side+"_team_id"]
            and at-c["captured_at"]<=timedelta(hours=24)
            and c.get("source_published_at") is not None and at-c["source_published_at"]<=timedelta(days=7)]
        if not ts:
            reasons.append(side+"_availability_missing_or_stale");continue
        team=max(ts,key=lambda c:c["captured_at"]);selected[side]=team
        p=team["payload_json"];qb=p.get("qb") or {}
        for key in ("availability_reviewed","ol_reviewed"):
            if p.get(key) is not True:reasons.append(side+"_"+key+"_missing")
        if not isinstance(p.get("unavailable_players"),list):reasons.append(side+"_unavailable_players_missing")
        if qb.get("status")!="confirmed":reasons.append(side+"_qb_unconfirmed")
        if qb.get("changed") is not False:reasons.append(side+"_qb_change_or_unknown_impact")
        if p.get("coach_changed") is not False:reasons.append(side+"_coach_change_or_unknown_impact")
        if p.get("roster_continuity") is None:reasons.append(side+"_continuity_missing")
    # Even complete captures are not a learned numerical context adjustment.
    return {"complete":not reasons,"reasons":reasons,"captures":selected,
            "numeric_adjustment_status":"unfitted_context_effects","action_allowed":False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--team-input",type=Path)
    parser.add_argument("--weather",action="store_true")
    parser.add_argument("--refresh-venues",action="store_true")
    parser.add_argument("--within-hours",type=int,default=24)
    args=parser.parse_args();db=DatabaseManager(load_config().database_url or "",initialize_schema=False);ensure_schema(db)
    venue_reference=refresh_venue_coordinates(db,os.getenv("CFBD_API_KEY","")) if args.refresh_venues else {"status":"stored_coordinates_source_capture_unverified"}
    now=datetime.now(timezone.utc)
    games=[dict(r) for r in db.execute("""SELECT m.*,v.latitude,v.longitude,v.cfbd_venue_id FROM cfb_matchups m
        LEFT JOIN cfb_venues v ON v.venue_id=m.venue_id WHERE m.commence_time>%s AND m.commence_time<=%s AND NOT m.completed""",
        (now,now+timedelta(hours=args.within_hours)))]
    if args.team_input:
        for p in json.loads(args.team_input.read_text(encoding="utf-8")):
            game=next((g for g in games if g["id"]==p["game_id"]),None)
            if game is None:raise ValueError("Game outside upcoming capture window")
            published=validate_team(p,game,now)
            response=requests.get(p["source_url"],timeout=30);response.raise_for_status()
            p={**p,"source_content_sha256":hashlib.sha256(response.content).hexdigest(),"source_content":response.text[:500000],"review_type":"operator_transcription_requires_source_audit"}
            print("team capture",save_context(db,game["id"],"team",p["team_id"],p["source_url"],p,published))
    if args.weather:
        for game in games:
            url="https://api.open-meteo.com/v1/forecast"
            payload={"status":"unavailable","reason":"venue_coordinates_missing"}
            if game.get("latitude") is not None and game.get("longitude") is not None:
                try:
                    response=requests.get(url,params={"latitude":game["latitude"],"longitude":game["longitude"],
                        "hourly":"temperature_2m,precipitation,wind_speed_10m,wind_gusts_10m","timezone":"UTC","forecast_days":3},timeout=30)
                    response.raise_for_status();data=response.json();hourly=data["hourly"]
                    samples=[{k:v[i] for k,v in hourly.items()} for i,t in enumerate(hourly["time"])
                             if abs((datetime.fromisoformat(t).replace(tzinfo=timezone.utc)-game["commence_time"]).total_seconds())<=7200]
                    payload={"status":"captured" if samples else "unavailable","samples":samples,
                             "hourly_units":data.get("hourly_units"),"latitude":game["latitude"],"longitude":game["longitude"],
                             "source_response_sha256":hashlib.sha256(response.content).hexdigest(),"roof_status":"unknown",
                             "kind":"forecast_not_actual_weather"}
                except (requests.RequestException,KeyError,ValueError) as exc:
                    payload={"status":"unavailable","reason":type(exc).__name__}
            payload["venue_reference"]={**venue_reference,"cfbd_venue_id":game.get("cfbd_venue_id"),"canonical_venue_id":game.get("venue_id")}
            print("weather capture",game["id"],save_context(db,game["id"],"weather",None,url,payload))


if __name__=="__main__":main()
