"""LineStar API client — fetch NBA projections + ownership without manual CSV download.

Identical to the CBB version except:
  sport=5  (NBA, was sport=4 for CBB)

LineStar is built on DotNetNuke CMS. The relevant endpoints are:

  GET /DesktopModules/DailyFantasyApi/API/Fantasy/GetPeriodInformation
      → no auth required; returns {PeriodId, Name, ...} for today/current slate

  GET /DesktopModules/DailyFantasyApi/API/Fantasy/GetSalariesV5
      ?periodId={periodId}&site=1&sport=5
      → full projection + ownership payload (requires .DOTNETNUKE session cookie)
      → response body: JSON with {SalaryContainerJson (embedded JSON string),
                                   Ownership.Projected {contestTypeId: [{SalaryId, Owned}]},
                                   Periods [...], Slates [...]}

Parameters (site / sport):
  site=1  → DraftKings
  sport=5 → NBA

SalaryContainerJson.Salaries[] fields used:
  Id      → SalaryId (foreign key into Ownership.Projected)
  Name    → player display name (matches DK salary CSV)
  SAL     → DK salary in dollars
  POS     → position ("PG", "SG", "SF", "PF", "C", "G", "F")
  PP      → LineStar projection (FPTS)
  PTEAM   → player's team abbreviation
  IS      → injury status (1=injured, 3=GTD, 0=healthy)
  STAT    → player status (4=out, 0=active)

Usage:
    from ingest.linestar_fetch import fetch_linestar_for_draft_group

    linestar_map = fetch_linestar_for_draft_group(dk_draft_group_id=144324)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import requests

logger = logging.getLogger(__name__)

_BASE    = "https://www.linestarapp.com"
_TIMEOUT = 20
_SITE    = 1   # DraftKings
_SPORT   = 5   # NBA (CBB was 4)

_OUT_STATS = {4}
_OUT_IS    = {1}


# ── Public API ─────────────────────────────────────────────────────────────────


def fetch_linestar_for_draft_group(
    dk_draft_group_id: int,
    dnn_cookie: str | None = None,
) -> dict[tuple, dict]:
    """Fetch LineStar projections + ownership for a DK NBA draft group.

    Returns:
        Dict keyed by (player_name_lower, salary_int) →
        {linestar_proj, proj_own_pct, is_out}

    Returns an empty dict (rather than raising) if the DNN_COOKIE is missing,
    expired, or rejected (HTTP 401/403). The caller handles an empty map by
    writing NULL for linestar_proj and proj_own_pct on all players.
    """
    try:
        cookie = dnn_cookie or os.environ.get("DNN_COOKIE", "")
        period_id = _get_period_id_for_draft_group(dk_draft_group_id, cookie)
        logger.info("Resolved draftGroupId %d → LineStar periodId %d", dk_draft_group_id, period_id)

        data      = _fetch_salaries_v5(period_id, cookie)
        players   = _parse_salaries(data)
        ownership = _parse_ownership(data)
        linestar_map = _build_linestar_map(players, ownership)

        logger.info("LineStar: %d players for periodId %d", len(linestar_map), period_id)
        return linestar_map

    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        if status in (401, 403):
            logger.warning(
                "LineStar auth failed (HTTP %s) — DNN_COOKIE likely expired or missing. "
                "Continuing without LineStar projections.",
                status,
            )
            return {}
        raise

    except ValueError as exc:
        # Period ID discovery failed (no matching slate found after all probes).
        # Treat as a soft failure — cookie is likely stale or slate not yet listed.
        logger.warning(
            "LineStar period ID lookup failed: %s — continuing without LineStar projections.",
            exc,
        )
        return {}


def fetch_live_ownership(
    dk_draft_group_id: int,
    dnn_cookie: str | None = None,
) -> dict[int, float]:
    """Fetch live pre-lock ownership % from GetFastUpdateV2.

    Returns {salary_id → owned_pct}. Call 30–60 s before slate lock.
    """
    cookie    = dnn_cookie or os.environ.get("DNN_COOKIE", "")
    period_id = _get_period_id_for_draft_group(dk_draft_group_id, cookie)

    url    = f"{_BASE}/DesktopModules/DailyFantasyApi/API/Fantasy/GetFastUpdateV2"
    params = {"periodId": period_id, "site": _SITE, "sport": _SPORT}
    resp   = _get(url, params, cookie)

    ownership_raw = resp.get("Ownership", {}).get("Projected", {})
    return _average_ownership_by_salary_id(ownership_raw)


# ── Internal helpers ───────────────────────────────────────────────────────────


def _get_period_id_for_draft_group(dk_draft_group_id: int, cookie: str) -> int:
    """Discover LineStar periodId that maps to the given DK draftGroupId.

    Probes GetPeriodInformation first (works for upcoming slates), then falls
    back to a probe scan around LINESTAR_PERIOD_ID env var (handles in-progress
    slates where GetPeriodInformation returns empty).
    """
    url    = f"{_BASE}/DesktopModules/DailyFantasyApi/API/Fantasy/GetPeriodInformation"
    params = {"site": _SITE, "sport": _SPORT}

    def probe(pid: int) -> int | None:
        try:
            data   = _fetch_salaries_v5(pid, cookie)
            slates = data.get("Slates", [])
            return int(pid) if any(s.get("DfsSlateId") == dk_draft_group_id for s in slates) else None
        except requests.HTTPError:
            return None

    # 1. Primary: GetPeriodInformation with auth
    try:
        resp    = _get(url, params, cookie)
        periods = resp if isinstance(resp, list) else resp.get("Periods", [])
        for period in periods[:10]:
            pid = period.get("PeriodId") or period.get("Id")
            if not pid:
                continue
            match = probe(pid)
            if match:
                return match
    except (requests.HTTPError, requests.RequestException):
        pass

    # 2. Fallback: probe scan ±5 around LINESTAR_PERIOD_ID env var anchor
    env_hint_str = os.environ.get("LINESTAR_PERIOD_ID", "")
    if env_hint_str.isdigit():
        env_hint = int(env_hint_str)
        for offset in range(-3, 6):
            match = probe(env_hint + offset)
            if match:
                return match

    raise ValueError(
        f"LineStar: could not locate periodId for DK draftGroupId {dk_draft_group_id}. "
        "Set LINESTAR_PERIOD_ID in .env to any nearby known period ID."
    )


def _fetch_salaries_v5(period_id: int, cookie: str) -> dict[str, Any]:
    url    = f"{_BASE}/DesktopModules/DailyFantasyApi/API/Fantasy/GetSalariesV5"
    params = {"periodId": period_id, "site": _SITE, "sport": _SPORT}
    return _get(url, params, cookie)


def _parse_salaries(data: dict[str, Any]) -> list[dict]:
    scj = data.get("SalaryContainerJson", "{}")
    if not scj:
        return []
    try:
        container = json.loads(scj)
    except json.JSONDecodeError as exc:
        logger.warning("Failed to parse SalaryContainerJson: %s", exc)
        return []

    players = []
    for p in container.get("Salaries", []):
        stat   = p.get("STAT", 0)
        is_val = p.get("IS", 0)
        is_out = (stat in _OUT_STATS) or (is_val in _OUT_IS)
        proj   = _safe_float(p.get("PP")) or 0.0
        players.append({
            "id":     p["Id"],
            "name":   p.get("Name", "").strip(),
            "salary": int(p.get("SAL", 0)),
            "proj":   proj,
            "is_out": is_out,
        })
    return players


def _parse_ownership(data: dict[str, Any]) -> dict[int, float]:
    ownership_raw = data.get("Ownership", {}).get("Projected", {})
    return _average_ownership_by_salary_id(ownership_raw)


def _average_ownership_by_salary_id(ownership_raw: dict) -> dict[int, float]:
    totals: dict[int, float] = {}
    counts: dict[int, int]   = {}
    for _contest_type, entries in ownership_raw.items():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            sid   = entry.get("SalaryId")
            owned = _safe_float(entry.get("Owned"))
            if sid is not None and owned is not None:
                totals[sid] = totals.get(sid, 0.0) + owned
                counts[sid] = counts.get(sid, 0) + 1
    return {sid: totals[sid] / counts[sid] for sid in totals}


def _build_linestar_map(players: list[dict], ownership: dict[int, float]) -> dict[tuple, dict]:
    linestar_map: dict[tuple, dict] = {}
    for p in players:
        own_pct = ownership.get(p["id"], 0.0)
        key = (p["name"].lower(), p["salary"])
        linestar_map[key] = {
            "linestar_proj": p["proj"],
            "proj_own_pct":  own_pct,
            "is_out":        p["is_out"],
        }
    return linestar_map


def _get(url: str, params: dict, cookie: str) -> Any:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept":  "application/json",
        "Referer": "https://www.linestarapp.com/DesktopModules/DailyFantasyApi/",
    }
    if cookie:
        headers["Cookie"] = f".DOTNETNUKE={cookie}"
    resp = requests.get(url, params=params, headers=headers, timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Fetch LineStar NBA projections + ownership")
    parser.add_argument("--draft-group-id", type=int, required=True)
    parser.add_argument("--cookie", default=os.environ.get("DNN_COOKIE", ""))
    args = parser.parse_args()

    result = fetch_linestar_for_draft_group(args.draft_group_id, dnn_cookie=args.cookie)
    print(f"Fetched {len(result)} players from LineStar")
    top = sorted(result.items(), key=lambda x: x[1]["linestar_proj"], reverse=True)[:10]
    print(f"\n{'Name':<30} {'Salary':>7}  {'Proj':>6}  {'Own%':>6}")
    print("-" * 56)
    for (name, salary), vals in top:
        print(f"{name:<30} ${salary:>6}  {vals['linestar_proj']:>6.2f}  {vals['proj_own_pct']:>5.1f}%")
