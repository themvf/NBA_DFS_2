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

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
