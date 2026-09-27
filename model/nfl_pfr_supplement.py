"""Parse PFR game-level evidence without inventing play-level pressure labels."""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

from bs4 import BeautifulSoup, Comment

VERSION = "pfr-boxscore-v1"
SECTIONS = (
    "passing_advanced", "rushing_advanced", "receiving_advanced", "defense_advanced",
    "home_starters", "vis_starters", "home_snap_counts", "vis_snap_counts",
)
ALIASES = {"LAR": "LA", "WSH": "WAS", "JAC": "JAX", "AZ": "ARI"}


def team_code(value: str) -> str:
    value = value.strip().upper()
    return ALIASES.get(value, value)


def boxscore_id(value: str) -> str:
    """Accept only PFR game IDs or HTTPS URLs on the expected host."""
    if "://" in value:
        parsed = urlparse(value)
        if (parsed.scheme != "https" or parsed.netloc != "www.pro-football-reference.com"
                or parsed.query or parsed.fragment):
            raise ValueError("Expected a PFR HTTPS boxscore URL")
        match = re.fullmatch(r"/boxscores/(\d{9}[a-z]{3})\.htm", parsed.path)
    else:
        match = re.fullmatch(r"(\d{9}[a-z]{3})", value)
    if not match:
        raise ValueError("Invalid PFR boxscore ID")
    return match[1]


def scalar(text: str):
    """Blank means unknown; percentages remain percentage points, not fractions."""
    text = text.strip()
    if not text or text in {"—", "--"}:
        return None
    number = text.rstrip("%").replace(",", "")
    if re.fullmatch(r"-?\d+(?:\.\d+)?", number):
        return float(number) if "." in number else int(number)
    return text


def parse_boxscore(html: str, game: dict, *, captured_at: str | None = None) -> dict:
    expected = boxscore_id(str(game["pfr"]))
    soup = BeautifulSoup(html, "html.parser")
    canonical = soup.select_one('link[rel="canonical"]')
    if not canonical or boxscore_id(str(canonical.get("href", ""))) != expected:
        raise ValueError("Page identity does not match the scheduled PFR game")
    for comment in list(soup.find_all(string=lambda s: isinstance(s, Comment))):
        if "<table" in comment:
            comment.replace_with(BeautifulSoup(str(comment), "html.parser"))
    rows, coverage = [], {}
    teams = {team_code(game["home_team"]), team_code(game["away_team"])}
    for section in SECTIONS:
        tables = soup.find_all("table", id=section)
        if len(tables) > 1:
            raise ValueError(f"Duplicate table: {section}")
        if not tables:
            coverage[section] = {"status": "missing", "rows": 0}
            continue
        table = tables[0]
        headers = {cell.get("data-stat"): cell.get("data-tip") or cell.get_text(" ", strip=True)
                   for cell in table.select("thead [data-stat]")}
        count, seen = 0, set()
        for tr in table.select("tbody tr"):
            if "thead" in tr.get("class", []):
                continue
            cells = tr.select("th[data-stat], td[data-stat]")
            raw = {cell["data-stat"]: cell.get_text(" ", strip=True) for cell in cells}
            player = tr.select_one('[data-stat="player"]')
            if not player or not player.get_text(strip=True):
                continue
            player_id = player.get("data-append-csv")
            if not player_id:
                link = player.select_one('a[href^="/players/"]')
                match = re.fullmatch(r"/players/[A-Za-z]/([A-Za-z0-9]+)\.htm", link.get("href", "")) if link else None
                player_id = match[1] if match else None
            if not player_id:
                # Aggregate rows are not player evidence.
                if player.get_text(strip=True).lower() in {"team totals", "total", "totals"}:
                    continue
                raise ValueError(f"Missing player identity in {section}")
            side = "home" if section.startswith("home_") else "away" if section.startswith("vis_") else None
            team = team_code(game[f"{side}_team"]) if side else team_code(raw.get("team", ""))
            if team not in teams:
                raise ValueError(f"Unknown player team in {section}")
            key = (player_id, team)
            if key in seen:
                raise ValueError(f"Duplicate player in {section}")
            seen.add(key)
            rows.append({"section": section, "pfr_player_id": player_id,
                         "player_name": player.get_text(" ", strip=True), "team": team,
                         "stats": {k: scalar(v) for k, v in raw.items() if k not in {"player", "team"}},
                         "raw": raw})
            count += 1
        coverage[section] = {"status": "available" if count else "empty", "rows": count, "headers": headers}
    if not rows:
        raise ValueError("No supported player tables; blocked, unfinished, or changed page")
    return {"game_id": game["game_id"], "season": int(game["season"]), "week": int(game["week"]),
            "home_team": team_code(game["home_team"]), "away_team": team_code(game["away_team"]),
            "pfr_game_id": expected, "source_url": f"https://www.pro-football-reference.com/boxscores/{expected}.htm",
            "captured_at": captured_at or datetime.now(timezone.utc).isoformat(),
            "parser_version": VERSION, "source_sha256": hashlib.sha256(html.encode()).hexdigest(),
            "status": "complete" if all(c["status"] == "available" for c in coverage.values()) else "partial",
            "coverage": coverage, "rows": rows}
