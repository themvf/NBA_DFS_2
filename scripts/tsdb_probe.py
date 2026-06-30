"""One-off diagnostic: determine which TheSportsDB API style the configured key
supports (v1 path vs v2 header), and whether the WC schedule + penalty-shootout
fields are reachable. Runs in CI where THESPORTSDB_API_KEY is available; prints
ONLY status codes / counts / field names — never the key (also masked by GH).

    python -m scripts.tsdb_probe
"""

from __future__ import annotations

import os
import requests

KEY = os.getenv("THESPORTSDB_API_KEY", "")
WC_LEAGUE = 4429
SEASON = "2026"
V1 = "https://www.thesportsdb.com/api/v1/json"
V2 = "https://www.thesportsdb.com/api/v2/json"
H = {"X-API-KEY": KEY}


def _try(label: str, method):
    try:
        r = method()
        ct = r.headers.get("content-type", "")
        ok_json = ct.startswith("application/json")
        body = r.json() if ok_json else None
        print(f"[{label}] HTTP {r.status_code} json={ok_json}")
        return r, body
    except Exception as e:  # noqa: BLE001
        print(f"[{label}] ERROR {type(e).__name__}: {e}")
        return None, None


def _summarize_events(body: dict | None, key: str = "events"):
    if not isinstance(body, dict):
        print("    (no dict body)")
        return
    evs = body.get(key) or body.get("schedule") or []
    if not isinstance(evs, list):
        print(f"    keys: {list(body.keys())[:8]}")
        return
    print(f"    events: {len(evs)}")
    if evs:
        sample = evs[0]
        sckeys = [k for k in sample.keys() if "Shootout" in k or k in
                  ("strHomeTeam", "strAwayTeam", "intHomeScore", "intAwayScore", "intRound", "strStatus", "dateEvent")]
        print(f"    sample fields: {sckeys}")
        # Find a penalty-decided knockout tie (Germany/Paraguay/Netherlands/Morocco)
        for e in evs:
            h, a = e.get("strHomeTeam", ""), e.get("strAwayTeam", "")
            if any(t in (h + a) for t in ("Paraguay", "Morocco", "Germany", "Netherlands")):
                print(f"    KO sample: {h} {e.get('intHomeScore')}-{e.get('intAwayScore')} {a} "
                      f"pH={e.get('intScoreHomeShootout')} pA={e.get('intScoreAwayShootout')} "
                      f"round={e.get('intRound')} status={e.get('strStatus')}")


def main() -> int:
    print(f"key present: {bool(KEY)} | length: {len(KEY)}")

    print("\n== V1 path (legacy) ==")
    _try("v1 eventsround r=4", lambda: requests.get(
        f"{V1}/{KEY}/eventsround.php", params={"id": WC_LEAGUE, "r": 4, "s": SEASON}, timeout=20))
    _, b = _try("v1 eventsday 2026-06-29", lambda: requests.get(
        f"{V1}/{KEY}/eventsday.php", params={"d": "2026-06-29", "s": "Soccer"}, timeout=20))
    _summarize_events(b)

    print("\n== V2 header (premium) ==")
    _, b = _try("v2 schedule/league", lambda: requests.get(
        f"{V2}/schedule/league/{WC_LEAGUE}/{SEASON}", headers=H, timeout=25))
    _summarize_events(b)

    _try("v2 livescore/soccer", lambda: requests.get(f"{V2}/livescore/soccer", headers=H, timeout=20))

    # Find a penalty-decided tie's idEvent from the v2 schedule, then dump the
    # single-event lookup (both APIs) to locate where the winner is encoded.
    print("\n== single-event inspection (penalty tie) ==")
    _, sched = _try("v2 schedule (for id)", lambda: requests.get(
        f"{V2}/schedule/league/{WC_LEAGUE}/{SEASON}", headers=H, timeout=25))
    target_id = None
    evs = (sched or {}).get("schedule") or (sched or {}).get("events") or []
    for e in evs:
        if "Germany" in e.get("strHomeTeam", "") and "Paraguay" in e.get("strAwayTeam", "") \
           and str(e.get("intRound")) == "32":
            target_id = e.get("idEvent")
            break
    print(f"    target idEvent: {target_id}")
    if target_id:
        def _dump(label, body):
            ev = None
            if isinstance(body, dict):
                arr = body.get("events") or body.get("lookup") or []
                ev = arr[0] if arr else (body if "strHomeTeam" in body else None)
            if not ev:
                print(f"    [{label}] no event body; keys={list(body.keys())[:6] if isinstance(body,dict) else None}")
                return
            hits = {k: v for k, v in ev.items()
                    if any(t in k.lower() for t in ("shoot", "penal", "winner", "result", "score", "status", "round"))}
            print(f"    [{label}] {ev.get('strHomeTeam')} v {ev.get('strAwayTeam')}: {hits}")
        _, b1 = _try("v1 lookupevent", lambda: requests.get(
            f"{V1}/{KEY}/lookupevent.php", params={"id": target_id}, timeout=20))
        _dump("v1", b1)
        _, b2 = _try("v2 lookup/event", lambda: requests.get(
            f"{V2}/lookup/event/{target_id}", headers=H, timeout=20))
        _dump("v2", b2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
