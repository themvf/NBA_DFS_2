"""Diagnostic: does TheSportsDB (premium key) carry useful ATP/WTA tennis data —
results AND per-match serve stats (aces, DFs, serve %), or just scores?

Runs in CI where THESPORTSDB_API_KEY lives. Prints league coverage, event
counts, and a full single-event field dump so we can see if stats exist. Never
prints the key.

    python scripts/tennis_tsdb_probe.py
"""

from __future__ import annotations

import os
import requests

KEY = os.getenv("THESPORTSDB_API_KEY", "")
V1 = "https://www.thesportsdb.com/api/v1/json"
V2 = "https://www.thesportsdb.com/api/v2/json"
H = {"X-API-KEY": KEY}
STAT_HINTS = ("ace", "serve", "fault", "doublefault", "break", "winner", "stat", "set", "tiebreak")


def _get(label, url, **kw):
    try:
        r = requests.get(url, timeout=25, **kw)
        ok = r.headers.get("content-type", "").startswith("application/json")
        print(f"[{label}] HTTP {r.status_code} json={ok}")
        return r.json() if ok else None
    except Exception as e:  # noqa: BLE001
        print(f"[{label}] ERROR {type(e).__name__}: {e}")
        return None


def main() -> int:
    print(f"key present: {bool(KEY)} len={len(KEY)}\n")

    # 1. Tennis leagues
    print("== tennis leagues ==")
    body = _get("v1 search_all_leagues Tennis",
                f"{V1}/{KEY}/search_all_leagues.php", params={"s": "Tennis"})
    leagues = (body or {}).get("countries") or (body or {}).get("leagues") or []
    tennis = [(l.get("idLeague"), l.get("strLeague")) for l in leagues]
    for lid, name in tennis[:30]:
        print(f"    {lid}  {name}")

    # 2. Pick an ATP + WTA + Wimbledon league, pull a season schedule
    def find(*subs):
        for lid, name in tennis:
            n = (name or "").lower()
            if all(s in n for s in subs):
                return lid, name
        return None, None

    for tag, subs in [("ATP", ("atp",)), ("WTA", ("wta",)), ("Wimbledon", ("wimbledon",))]:
        lid, name = find(*subs)
        print(f"\n== {tag}: league {lid} ({name}) ==")
        if not lid:
            print("    not found in league list")
            continue
        for season in ("2026", "2025"):
            sched = _get(f"v2 schedule {tag} {season}",
                         f"{V2}/schedule/league/{lid}/{season}", headers=H)
            evs = (sched or {}).get("schedule") or (sched or {}).get("events") or []
            print(f"    season {season}: {len(evs)} events")
            if evs:
                e = evs[0]
                eid = e.get("idEvent")
                print(f"    sample: {e.get('strEvent')} score={e.get('intHomeScore')}-{e.get('intAwayScore')} status={e.get('strStatus')}")
                # Full single-event dump → look for stat fields
                full = _get(f"v2 lookup/event {eid}", f"{V2}/lookup/event/{eid}", headers=H)
                fev = ((full or {}).get("events") or [{}])[0] if isinstance(full, dict) else {}
                stat_fields = [k for k in fev.keys() if any(h in k.lower() for h in STAT_HINTS)]
                nonnull_stats = {k: fev[k] for k in stat_fields if fev.get(k) not in (None, "")}
                print(f"    stat-like fields present: {stat_fields}")
                print(f"    stat-like NON-NULL: {nonnull_stats}")
                # Is there a dedicated event-stats endpoint?
                stats = _get(f"v2 lookup/event_stats {eid}", f"{V2}/lookup/event_stats/{eid}", headers=H)
                print(f"    event_stats endpoint: {type(stats).__name__} "
                      f"{list(stats.keys())[:5] if isinstance(stats, dict) else ''}")
                break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
