"""Read-only probe for the FantasyPros `nfl/players` universe endpoint.

Why this exists: the endpoint answers 200 with the correct shape and zero
rows, so a failure is indistinguishable from an empty period and there is
nothing in the response to diagnose from. Pinning `week: 0` -- the difference
between it and every sibling contract that works -- did NOT fix it, so the
remaining hypotheses have to be tested against the live vendor rather than
reasoned about.

Prints only counts and scalar echoes. No player rows are emitted, so the
output is safe to keep as a build artifact.
"""
from __future__ import annotations

import argparse
import json
import os
from typing import Any

import requests

from ingest.ff_fantasypros import FantasyProsClient

_BASE = {"external_ids": "yahoo:espn:cbs:nfl:mfl:draftkings"}

# Each case isolates ONE difference from the failing production request, so a
# pass identifies the responsible parameter rather than a working blob.
CASES: list[tuple[str, dict[str, Any]]] = [
    ("production (week 0)", {**_BASE, "ecr": "included", "show": "pos_rank", "week": 0}),
    ("no week at all", {**_BASE, "ecr": "included", "show": "pos_rank"}),
    ("without ecr=included", {**_BASE, "show": "pos_rank", "week": 0}),
    ("without show=pos_rank", {**_BASE, "ecr": "included", "week": 0}),
    ("bare external_ids only", dict(_BASE)),
    ("no params at all", {}),
    ("with scoring=PPR", {**_BASE, "ecr": "included", "show": "pos_rank", "week": 0, "scoring": "PPR"}),
    ("with position=ALL", {**_BASE, "ecr": "included", "show": "pos_rank", "week": 0, "position": "ALL"}),
    ("with explicit season", {**_BASE, "ecr": "included", "show": "pos_rank", "week": 0, "season": 2026}),
    ("with explicit year", {**_BASE, "ecr": "included", "show": "pos_rank", "week": 0, "year": 2026}),
]


def probe(season: int) -> dict[str, Any]:
    client = FantasyProsClient(os.environ.get("FANTASYPROS_API_KEY", ""))
    results: list[dict[str, Any]] = []
    for label, params in CASES:
        record: dict[str, Any] = {"case": label, "params": params}
        try:
            payload = client.get("nfl/players", params)
            rows = payload.get("players")
            record["rows"] = len(rows) if isinstance(rows, list) else 0
            record["scalars"] = {
                str(k): v for k, v in payload.items()
                if isinstance(v, (str, int, float, bool)) or v is None
            }
            record["status"] = "pass" if record["rows"] > 0 else "empty"
        except requests.RequestException as exc:
            response = getattr(exc, "response", None)
            record["status"] = "error"
            record["error_type"] = type(exc).__name__
            record["http_status"] = getattr(response, "status_code", None)
            record["rows"] = 0
        results.append(record)
    return {"season": season, "path": "nfl/players", "cases": results,
            "any_pass": any(r["status"] == "pass" for r in results)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--output")
    args = parser.parse_args()
    report = probe(args.season)
    text = json.dumps(report, indent=2, sort_keys=True)
    print(text)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(text)
