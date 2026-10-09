"""PFR boxscore supplement CLI. See docs/nfl-pfr-supplement.md."""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import load_config
from db.database import DatabaseManager
from db.nfl_pfr_schema import DDL, save_snapshot
from ingest.nfl_season_schedule import fetch_schedule
from model.nfl_pfr_supplement import boxscore_id, parse_boxscore


class AccessBlocked(RuntimeError):
    pass


def proxy_settings() -> dict:
    explicit = os.getenv("PFR_PROXY_URL")
    if explicit:
        return {"http": explicit, "https": explicit}
    user, password = os.getenv("WEBSHARE_PROXY_USERNAME"), os.getenv("WEBSHARE_PROXY_PASSWORD")
    if user and password:
        # Match the existing YouTube Webshare account configuration.
        username = user if user.endswith("-rotate") else user + "-rotate"
        url = f"http://{quote(username, safe='')}:{quote(password, safe='')}@p.webshare.io:80"
        return {"http": url, "https": url}
    raise ValueError("Set PFR_PROXY_URL or WEBSHARE_PROXY_USERNAME / WEBSHARE_PROXY_PASSWORD")


class Fetcher:
    def __init__(self, *, direct=False, interval=6.0):
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.proxies.update({} if direct else proxy_settings())
        self.session.headers["User-Agent"] = "NFLGameSupplement/1.0"
        self.interval = max(6.0, interval)
        self.last_request = None

    def fetch(self, pfr_id: str) -> str:
        pfr_id = boxscore_id(pfr_id)
        if self.last_request is not None:
            time.sleep(max(0.0, self.interval - (time.monotonic() - self.last_request)))
        self.last_request = time.monotonic()
        try:
            response = self.session.get(f"https://www.pro-football-reference.com/boxscores/{pfr_id}.htm",
                                        timeout=60, allow_redirects=False)
        except requests.RequestException:
            # Exception strings may contain authenticated proxy URLs.
            raise RuntimeError("PFR transport failed; proxy credentials omitted") from None
        if response.headers.get("cf-mitigated") == "challenge":
            raise AccessBlocked(
                f"PFR Cloudflare browser challenge (HTTP {response.status_code}); "
                "the HTTP collector cannot complete the JavaScript/cookie challenge; stopping the batch")
        if response.status_code == 407:
            raise AccessBlocked("Proxy authentication rejected (HTTP 407); stopping the batch")
        if response.status_code in {401, 403, 407, 429}:
            raise AccessBlocked(f"PFR/proxy returned HTTP {response.status_code}; stopping the batch")
        if response.status_code != 200:
            raise RuntimeError(f"PFR returned HTTP {response.status_code}")
        title = BeautifulSoup(response.text, "html.parser").title
        if title and any(term in title.get_text().lower() for term in
                         ("just a moment", "access denied", "captcha", "attention required")):
            raise AccessBlocked("PFR returned an access challenge; stopping the batch")
        return response.text


def select_games(frame, season, week=None, game_ids=None, limit=None):
    required = {"game_id", "pfr", "season", "week", "home_team", "away_team", "home_score", "away_score"}
    if not required.issubset(frame.columns):
        raise ValueError("Schedule lacks required identity/results columns")
    games = frame[(frame.season == season) & frame.home_score.notna() & frame.away_score.notna()].copy()
    if week is not None:
        games = games[games.week == week]
    if game_ids:
        games = games[games.game_id.isin(game_ids)]
        if set(game_ids) != set(games.game_id):
            raise ValueError("Requested games are missing, unfinished, or outside season/week")
    if games.empty:
        raise ValueError("No completed games match this selection")
    if games.game_id.duplicated().any() or games.pfr.dropna().duplicated().any():
        raise ValueError("Ambiguous schedule game identity")
    if not games.game_id.str.fullmatch(r"\d{4}_\d{2}_[A-Z]{2,3}_[A-Z]{2,3}").all():
        raise ValueError("Invalid canonical game ID")
    games = games.sort_values(["week", "game_id"])
    return games.head(limit).to_dict("records") if limit else games.to_dict("records")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--week", type=int)
    parser.add_argument("--game", action="append", help="Exact nflverse game ID; repeatable")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--schedule-csv", type=Path, help="Optional local nflverse games.csv")
    parser.add_argument("--html-dir", type=Path, help="Offline mode: <pfr_id>.htm files with canonical URL")
    parser.add_argument("--cache-dir", type=Path, default=Path("data/pfr/cache"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/pfr/output"))
    parser.add_argument("--refresh", action="store_true", help="Refetch cached games for corrections")
    parser.add_argument("--direct", action="store_true", help="Explicitly bypass configured proxy")
    parser.add_argument("--write-db", action="store_true", help="Persist validated snapshots; default is JSON only")
    args = parser.parse_args(argv)
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be positive")
    config = load_config()
    frame = pd.read_csv(args.schedule_csv) if args.schedule_csv else fetch_schedule()
    games = select_games(frame, args.season, args.week, args.game, args.limit)
    db = DatabaseManager(config.database_url, initialize_schema=False) if args.write_db else None
    if db:
        db.execute(DDL)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    fetcher, report = None, []
    for game in games:
        try:
            pfr_id = boxscore_id(str(game["pfr"]))
            cache = args.cache_dir / f"{pfr_id}.json"
            if args.html_dir:
                html = (args.html_dir / f"{pfr_id}.htm").read_text(encoding="utf-8")
                captured = datetime.now(timezone.utc).isoformat()
            elif cache.exists() and not args.refresh:
                cached = json.loads(cache.read_text(encoding="utf-8"))
                html, captured = cached["html"], cached["captured_at"]
            else:
                fetcher = fetcher or Fetcher(direct=args.direct)
                html = fetcher.fetch(pfr_id)
                captured = datetime.now(timezone.utc).isoformat()
            payload = parse_boxscore(html, game, captured_at=captured)
            # Cache only validated pages, including their original capture time.
            cache.write_text(json.dumps({"html": html, "captured_at": captured}), encoding="utf-8")
            (args.output_dir / f"{game['game_id']}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
            if db:
                save_snapshot(db, payload)
            report.append({"game_id": game["game_id"], "status": payload["status"],
                           "rows": len(payload["rows"]), "coverage": payload["coverage"]})
        except (AccessBlocked, OSError, ValueError, RuntimeError) as exc:
            report.append({"game_id": game["game_id"], "status": "failed", "error": str(exc)})
            if isinstance(exc, AccessBlocked):
                break
    attempted = {r["game_id"] for r in report}
    report.extend({"game_id": g["game_id"], "status": "not_attempted"} for g in games if g["game_id"] not in attempted)
    (args.output_dir / "run-report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    counts = {status: sum(r["status"] == status for r in report)
              for status in ("complete", "partial", "failed", "not_attempted")}
    print(json.dumps(counts))
    return 1 if counts["failed"] or counts["not_attempted"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
