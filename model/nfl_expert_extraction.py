"""Structured extraction for NFL DFS expert articles (docs/nfl-expert-coverage-study.md).

DeepSeek reads a stored article revision and returns, as JSON: whether it is
an NFL DFS slate article, the slate(s) it covers, every player it discusses
with the role the article gives that player, any ownership percentage it
PRINTS, and any sample lineup it prints. The model never estimates ownership
or judges whether a play is good; it copies what the writer published.

Grounding (same rule as model/mlb_beat_extraction.py): every player record
needs a quote that is a substring of the stored text (compared after
collapsing whitespace and straight-ening quote characters); a printed
ownership number must appear in that quote; every name in a sample lineup must
appear in the text. Anything else is discarded and counted by reason.

Then, deterministically (no LLM): the slate is resolved against
`nfl_season_games` (Showdown = one game; Classic = the Sunday 1 PM / 4 PM ET
main slate of that week), timing is classed against the slate's first kickoff
(eligible <= T-20 < late <= kickoff < retrospective), and players are matched
to the slate's DraftKings pool snapshot.

Usage:
    python -m model.nfl_expert_extraction                # every revision not yet extracted
    python -m model.nfl_expert_extraction --limit 5
    python -m model.nfl_expert_extraction --report
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

EXTRACTION_VERSION = "nfl-expert-extract-deepseek-v1"
ROLES = ("captain", "core", "value", "contrarian", "fade", "mention")
FROZEN_CUTOFF_MINUTES = 20
MAX_ARTICLE_CHARS = 60_000
EASTERN = ZoneInfo("America/New_York")

DK_TEAMS = {
    "ARI": ["arizona", "cardinals", "ari"], "ATL": ["atlanta", "falcons", "atl"],
    "BAL": ["baltimore", "ravens", "bal"], "BUF": ["buffalo", "bills", "buf"],
    "CAR": ["carolina", "panthers", "car"], "CHI": ["chicago", "bears", "chi"],
    "CIN": ["cincinnati", "bengals", "cin"], "CLE": ["cleveland", "browns", "cle"],
    "DAL": ["dallas", "cowboys", "dal"], "DEN": ["denver", "broncos", "den"],
    "DET": ["detroit", "lions", "det"], "GB": ["green bay", "packers", "gb", "gnb"],
    "HOU": ["houston", "texans", "hou"], "IND": ["indianapolis", "colts", "ind"],
    "JAX": ["jacksonville", "jaguars", "jax", "jac"], "KC": ["kansas city", "chiefs", "kc", "kan"],
    "LAC": ["los angeles chargers", "la chargers", "chargers", "lac"],
    "LAR": ["los angeles rams", "la rams", "rams", "lar", "la"],
    "LV": ["las vegas", "raiders", "lv", "lvr"], "MIA": ["miami", "dolphins", "mia"],
    "MIN": ["minnesota", "vikings", "min"], "NE": ["new england", "patriots", "ne", "nwe"],
    "NO": ["new orleans", "saints", "no", "nor"], "NYG": ["new york giants", "ny giants", "giants", "nyg"],
    "NYJ": ["new york jets", "ny jets", "jets", "nyj"], "PHI": ["philadelphia", "eagles", "phi"],
    "PIT": ["pittsburgh", "steelers", "pit"], "SEA": ["seattle", "seahawks", "sea"],
    "SF": ["san francisco", "49ers", "niners", "sf", "sfo"], "TB": ["tampa bay", "buccaneers", "bucs", "tb", "tam"],
    "TEN": ["tennessee", "titans", "ten"], "WAS": ["washington", "commanders", "was", "wsh"],
}
_TEAM_ALIAS = {alias: code for code, aliases in DK_TEAMS.items() for alias in aliases + [code.lower()]}


def team_code(value: str | None) -> str | None:
    """DraftKings team code for an abbreviation, city or nickname; None if unknown."""
    if not value:
        return None
    key = re.sub(r"[^a-z0-9 ]", "", str(value).lower()).strip()
    if key in _TEAM_ALIAS:
        return _TEAM_ALIAS[key]
    for alias, code in sorted(_TEAM_ALIAS.items(), key=lambda kv: -len(kv[0])):
        if len(alias) > 3 and alias in key:
            return code
    return None


# --------------------------------------------------------------------------- grounding

_QUOTE_CHARS = str.maketrans({"‘": "'", "’": "'", "“": '"', "”": '"', "–": "-",
                              "—": "-", "−": "-", " ": " "})


def normalize_for_match(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").translate(_QUOTE_CHARS)).strip()


def is_grounded(quote: str, text: str) -> bool:
    q = normalize_for_match(quote)
    return len(q) >= 8 and q in normalize_for_match(text)


def ownership_in_quote(pct: float, quote: str) -> bool:
    """The printed number must be in the quote: 19.3 matches '19.3%' or '19.30'; 19 matches '19%'."""
    numbers = [float(n) for n in re.findall(r"\d+(?:\.\d+)?", quote)]
    return any(abs(n - pct) < 0.051 for n in numbers)


def name_in_text(name: str, text: str) -> bool:
    n = normalize_for_match(name).lower()
    return bool(n) and n in normalize_for_match(text).lower()


@dataclass
class ValidationResult:
    mentions: list[dict]
    lineups: list[dict]
    discard_reasons: dict[str, int]
    mentions_raw: int
    lineups_raw: int


def _as_pct(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        pct = float(str(value).replace("%", "").strip())
    except ValueError:
        return None
    return pct if 0 <= pct <= 100 else None


def validate(response: dict, text: str) -> ValidationResult:
    reasons: dict[str, int] = {}

    def drop(reason: str) -> None:
        reasons[reason] = reasons.get(reason, 0) + 1

    players = response.get("players") if isinstance(response.get("players"), list) else []
    lineups = response.get("lineups") if isinstance(response.get("lineups"), list) else []
    mentions, seen = [], set()
    for p in players:
        if not isinstance(p, dict) or not str(p.get("name") or "").strip():
            drop("no_name")
            continue
        role = str(p.get("role") or "").strip().lower()
        if role not in ROLES:
            drop("bad_role")
            continue
        quote = str(p.get("quote") or "")
        if not is_grounded(quote, text):
            drop("ungrounded_quote")
            continue
        pct = _as_pct(p.get("ownership_pct"))
        if pct is not None and not ownership_in_quote(pct, quote):
            drop("ungrounded_ownership")  # the record stays; the number does not
            pct = None
        name = str(p["name"]).strip()
        key = (name.lower(), role, (p.get("slot") or "").upper())
        if key in seen:
            drop("duplicate")
            continue
        seen.add(key)
        slot = str(p.get("slot") or "").strip().upper() or None
        mentions.append({"name": name, "team": team_code(p.get("team")), "team_raw": p.get("team"), "role": role,
                         "slot": slot, "ownership_pct": pct,
                         "ownership_definition": (str(p.get("ownership_definition")).strip()
                                                  if pct is not None and p.get("ownership_definition") else None),
                         "quote": quote})
    kept_lineups = []
    for idx, lu in enumerate(lineups):
        members = lu.get("players") if isinstance(lu, dict) else None
        if not isinstance(members, list) or len(members) < 2:
            drop("lineup_malformed")
            continue
        names = [str(m.get("name") or "").strip() for m in members if isinstance(m, dict)]
        if len(names) != len(members) or not all(names):
            drop("lineup_malformed")
            continue
        if not all(name_in_text(n, text) for n in names):
            drop("ungrounded_lineup")
            continue
        kept_lineups.append({"index": idx, "label": (lu.get("label") or None),
                             "players": [{"name": str(m.get("name")).strip(),
                                          "slot": (str(m.get("slot") or "").strip().upper() or None),
                                          "team": team_code(m.get("team"))} for m in members]})
    return ValidationResult(mentions, kept_lineups, reasons, len(players), len(lineups))


# --------------------------------------------------------------------------- slates

@dataclass
class Game:
    season: int
    week: int
    away: str
    home: str
    kickoff: datetime

    @property
    def key(self) -> str:
        return f"{self.away}@{self.home}"


@dataclass
class Slate:
    season: int
    week: int
    format: str
    games: list[Game]
    resolution: str

    @property
    def first_kickoff(self) -> datetime:
        return min(g.kickoff for g in self.games)

    @property
    def key(self) -> str:
        names = "+".join(sorted(g.key for g in self.games))
        return f"{self.format}|{names}|{self.first_kickoff.astimezone(timezone.utc).isoformat()}"

    @property
    def teams(self) -> set[str]:
        return {t for g in self.games for t in (g.away, g.home)}


def is_main_slate_game(g: Game) -> bool:
    local = g.kickoff.astimezone(EASTERN)
    return local.weekday() == 6 and 12 <= local.hour < 17


def resolve_slates(raw_slates: Iterable[dict], schedule: list[Game], reference: datetime) -> list[Slate]:
    """Map the LLM's slate description to scheduled games. `reference` is the
    article's published time (else capture time): the slate is the nearest
    matching game(s) from a day before it to nine days after."""
    window = [g for g in schedule if reference - timedelta(days=1) <= g.kickoff <= reference + timedelta(days=9)]
    out: dict[str, Slate] = {}
    for s in raw_slates or []:
        if not isinstance(s, dict):
            continue
        fmt = str(s.get("format") or "").lower()
        pairs = []
        for gm in s.get("games") or []:
            if isinstance(gm, dict):
                a, h = team_code(gm.get("away")), team_code(gm.get("home"))
            elif isinstance(gm, str) and re.search(r"@|\bvs\.?\b|\bat\b", gm):
                parts = re.split(r"\s*(?:@|\bvs\.?|\bat\b)\s*", gm, maxsplit=1)
                a, h = team_code(parts[0]), team_code(parts[1] if len(parts) > 1 else None)
            else:
                continue
            if a and h and a != h:
                pairs.append({a, h})
        matched = []
        for pair in pairs:
            cands = [g for g in window if {g.away, g.home} == pair]
            if cands:
                matched.append(min(cands, key=lambda g: abs((g.kickoff - reference).total_seconds())))
        if fmt == "showdown":
            for g in matched:
                sl = Slate(g.season, g.week, "showdown", [g], "showdown_game")
                out[sl.key] = sl
        elif fmt == "classic":
            if matched:
                season, week = matched[0].season, matched[0].week
                how = "classic_main_by_week_of_named_game"
            else:
                upcoming = sorted((g for g in window if is_main_slate_game(g) and g.kickoff >= reference
                                   - timedelta(hours=12)), key=lambda g: g.kickoff)
                if not upcoming:
                    continue
                season, week = upcoming[0].season, upcoming[0].week
                how = "classic_main_next_sunday"
            main = [g for g in schedule if g.season == season and g.week == week and is_main_slate_game(g)]
            if main:
                sl = Slate(season, week, "classic", main, how)
                out[sl.key] = sl
    return list(out.values())


def slate_reference(article: dict) -> datetime:
    """When the article was written, for finding its slate: published time; a
    day-precision date is read as noon Eastern; no date falls back to capture."""
    published = article.get("published_at")
    if published is None or article.get("published_precision") == "none":
        return article["captured_at"]
    if article.get("published_precision") == "day":
        return published + timedelta(hours=12)
    return published


def timing_class(captured_at: datetime, first_kickoff: datetime) -> str:
    if captured_at <= first_kickoff - timedelta(minutes=FROZEN_CUTOFF_MINUTES):
        return "eligible"
    if captured_at <= first_kickoff:
        return "late"
    return "retrospective"


# --------------------------------------------------------------------------- identity

def _normalize_name(value: str) -> str:
    from ingest.ff_fantasypros import normalize_name
    return normalize_name(value)


_DST = re.compile(r"\b(d/?st|dst|defen[cs]e|def)\b", re.I)


def match_player(name: str, team: str | None, pool: list[dict], slate_teams: set[str]) -> tuple[str, str | None, dict | None]:
    """(status, method, pool row). Pool rows carry name, normalized_name, team, position."""
    if not pool:
        return "no_pool", None, None
    candidates = [r for r in pool if r.get("team") in slate_teams] or pool
    if team:
        candidates_team = [r for r in candidates if r.get("team") == team]
    else:
        candidates_team = candidates
    if _DST.search(name) or (team and name.strip().lower() in DK_TEAMS.get(team, [])):
        code = team or team_code(_DST.sub("", name))
        hits = [r for r in candidates if r.get("position") == "DST" and r.get("team") == code]
        return ("matched", "team_dst", hits[0]) if len(hits) == 1 else ("unmatched", None, None)
    norm = _normalize_name(name)
    hits = [r for r in candidates_team if r.get("normalized_name") == norm]
    if len(hits) == 1:
        return "matched", ("name_team" if team else "name"), hits[0]
    if len(hits) > 1:
        return "ambiguous", "name", None
    if team:
        hits = [r for r in candidates if r.get("normalized_name") == norm]
        if len(hits) == 1:
            return "matched", "name_other_team", hits[0]
    surname = _normalize_name(name.split()[-1]) if name.split() else ""
    if surname:
        def last(r):
            return _normalize_name(str(r.get("name") or "").split()[-1]) if r.get("name") else ""
        hits = [r for r in candidates_team if last(r) == surname]
        if len(hits) == 1:
            return "matched", ("surname_team" if team else "surname"), hits[0]
        if len(hits) > 1:
            return "ambiguous", "surname", None
    return "unmatched", None, None


# --------------------------------------------------------------------------- DeepSeek

SYSTEM_PROMPT = """You are a data-extraction tool. You read one published NFL daily-fantasy (DFS) \
article and copy out what it says. You do NOT give opinions, rate players, estimate ownership, \
or judge whether any play is good.

Return ONLY a JSON object with this shape:
{"is_nfl_dfs": true|false,
 "slates": [{"format": "showdown"|"classic", "games": [{"away": "WAS", "home": "SF"}], "label": "text"}],
 "players": [{"name": "Full Name", "team": "SF", "role": "captain|core|value|contrarian|fade|mention",
              "slot": "CPT|FLEX|QB|RB|WR|TE|DST|K|null", "ownership_pct": number|null,
              "ownership_definition": "text|null", "quote": "verbatim text"}],
 "lineups": [{"label": "text|null", "players": [{"name": "Full Name", "slot": "CPT|FLEX|QB|...", "team": "SF"}]}]}

Definitions:
- is_nfl_dfs: true only if the article gives NFL daily fantasy advice for a specific slate (a single-game \
  Showdown or a multi-game Classic slate such as the Sunday main slate). Season-long start/sit, betting picks, \
  pick'em, and post-game recaps are false; then return empty lists.
- slates: "showdown" for a single-game slate (Captain mode); "classic" for a multi-game slate. Name the games \
  the article covers with team abbreviations (away and home).
- players: EVERY player or team defense the article discusses as a DFS option, once per role. role is the \
  article's own framing: "captain" = recommended at Captain/CPT; "core" = top play, lock, cash-game core, \
  must-play; "value" = cheap/salary-saving/punt/value play; "contrarian" = low-owned pivot, leverage, GPP-only \
  dart; "fade" = the article says to avoid or underweight; "mention" = discussed without one of those framings.
- slot: only if the article states it (e.g. a captain or flex recommendation); otherwise null.
- ownership_pct: ONLY if the article prints a projected or expected ownership/roster percentage for that \
  player; copy the number exactly. Never estimate one. ownership_definition: what the article says that number \
  refers to (e.g. "CPT", "FLEX", "projected ownership in large-field GPPs"), else null.
- quote: an EXACT, verbatim substring of the article that supports the record (one sentence or phrase, copied \
  character for character). Records whose quote is not found in the article are discarded.
- lineups: only lineups the article itself prints as a full or partial roster, with the slot of each player.
Read the whole article. Do not invent players, teams, numbers or lineups that are not in the text."""


def call_deepseek(cfg, text: str, title: str | None) -> dict:
    import httpx
    from model.deepseek_account import account_error_for

    body = text[:MAX_ARTICLE_CHARS]
    payload = {
        "model": cfg.model,
        "messages": [{"role": "system", "content": SYSTEM_PROMPT},
                     {"role": "user", "content": f"Title: {title or ''}\n\nArticle text:\n\n{body}"}],
        "temperature": 0.1,
        "max_tokens": 8000,
        "response_format": {"type": "json_object"},
    }
    headers = {"Authorization": f"Bearer {cfg.api_key}", "Content-Type": "application/json"}
    delay, last = cfg.retry_backoff_seconds, None
    for attempt in range(1, cfg.max_retries + 1):
        try:
            with httpx.Client(timeout=max(cfg.timeout_seconds, 240)) as client:
                resp = client.post(cfg.base_url, headers=headers, json=payload)
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"]
                return json.loads(content)
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            last = exc
        except httpx.HTTPStatusError as exc:
            last = exc
            err = account_error_for(exc.response.status_code)
            if err is not None:
                raise err from None
            if exc.response.status_code not in (429, 500, 502, 503, 504):
                raise
        except (KeyError, json.JSONDecodeError) as exc:
            logger.warning("DeepSeek: malformed response (%s)", exc)
            return {"_malformed": str(exc)}
        logger.warning("DeepSeek attempt %d failed (%s); retrying in %.0fs", attempt, last, delay)
        time.sleep(delay)
        delay *= 2
    raise RuntimeError(f"DeepSeek failed after {cfg.max_retries} attempts") from last


# --------------------------------------------------------------------------- database

def load_schedule(db, around: datetime) -> list[Game]:
    rows = db.execute(
        """SELECT g.season, g.week, a.abbreviation AS away, h.abbreviation AS home, g.kickoff
           FROM nfl_season_games g JOIN nfl_teams h ON h.team_id = g.home_team_id
           JOIN nfl_teams a ON a.team_id = g.away_team_id
           WHERE g.kickoff BETWEEN %s AND %s""", (around - timedelta(days=30), around + timedelta(days=30)))
    return [Game(r["season"], r["week"], team_code(r["away"]) or r["away"], team_code(r["home"]) or r["home"],
                 r["kickoff"]) for r in rows]


def load_pool(db, slate: Slate) -> tuple[str | None, list[dict]]:
    """The newest DraftKings pool snapshot for this slate captured before its first kickoff
    (else the newest at all), matched on format, first kickoff and team set."""
    snaps = db.execute(
        """SELECT snapshot_id, teams, captured_at FROM nfl_dfs_dk_pool_snapshots
           WHERE format = %s AND start_date = %s ORDER BY captured_at DESC""",
        (slate.format, slate.first_kickoff))
    exact = [s for s in snaps if set(s["teams"] or []) == slate.teams]
    usable = exact or [s for s in snaps if slate.teams <= set(s["teams"] or [])]
    if not usable:
        return None, []
    before = [s for s in usable if s["captured_at"] <= slate.first_kickoff]
    snap = (before or usable)[0]
    rows = db.execute("""SELECT dk_pid, name, normalized_name, team, position, salary
                         FROM nfl_dfs_dk_pool_player_status WHERE snapshot_id = %s""", (snap["snapshot_id"],))
    return str(snap["snapshot_id"]), [dict(r) for r in rows]


def pending_articles(db, limit: int | None) -> list[dict]:
    rows = db.execute(
        """SELECT a.* FROM nfl_expert_articles a
           WHERE NOT EXISTS (SELECT 1 FROM nfl_expert_extractions e
                             WHERE e.article_id = a.id AND e.extraction_version = %s)
           ORDER BY a.captured_at""", (EXTRACTION_VERSION,))
    return rows[:limit] if limit else rows


def extract_one(db, cfg, article: dict, *, response: dict | None = None) -> dict:
    from psycopg2.extras import Json

    text = article["raw_text"]
    if response is None:
        response = call_deepseek(cfg, text, article["title"])
    if "_malformed" in response:
        status, result = "malformed", ValidationResult([], [], {"malformed": 1}, 0, 0)
    elif not response.get("is_nfl_dfs"):
        status, result = "not_nfl_dfs", ValidationResult([], [], {}, len(response.get("players") or []),
                                                         len(response.get("lineups") or []))
    else:
        result = validate(response, text)
        status = "ok" if result.mentions or result.lineups else "empty"
    discarded_m = result.mentions_raw - len(result.mentions) if status in ("ok", "empty") else 0
    discarded_l = result.lineups_raw - len(result.lineups) if status in ("ok", "empty") else 0
    ext = db.execute_one(
        """INSERT INTO nfl_expert_extractions (article_id, extraction_version, llm_model, status, slate_raw,
               mentions_raw, mentions_grounded, mentions_discarded, lineups_raw, lineups_grounded, lineups_discarded,
               discard_reasons, response)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (article_id, extraction_version) DO NOTHING RETURNING id""",
        (article["id"], EXTRACTION_VERSION, cfg.model, status, Json({"slates": response.get("slates")}),
         result.mentions_raw, len(result.mentions), discarded_m, result.lineups_raw, len(result.lineups), discarded_l,
         Json(result.discard_reasons), Json(response)))
    if ext is None:
        return {"status": "already_extracted"}
    ext_id = ext["id"]
    summary = {"status": status, "mentions": len(result.mentions), "discarded": discarded_m,
               "lineups": len(result.lineups), "lineups_discarded": discarded_l, "matched": 0, "unmatched": 0,
               "slates": []}
    if status != "ok":
        return summary

    reference = slate_reference(article)
    slates = resolve_slates(response.get("slates") or [], load_schedule(db, reference), reference)
    pools = {}
    for sl in slates:
        tc = timing_class(article["captured_at"], sl.first_kickoff)
        db.execute(
            """INSERT INTO nfl_expert_article_slates (extraction_id, article_id, season, week, format, slate_key,
                   games, first_kickoff, captured_at, minutes_before_kickoff, timing_class, resolution)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (ext_id, article["id"], sl.season, sl.week, sl.format, sl.key, Json(sorted(g.key for g in sl.games)),
             sl.first_kickoff, article["captured_at"],
             (sl.first_kickoff - article["captured_at"]).total_seconds() / 60, tc, sl.resolution))
        pools[sl.key] = (sl, *load_pool(db, sl))
        summary["slates"].append({"slate_key": sl.key, "timing_class": tc})

    def slate_for(team: str | None):
        if len(pools) == 1:
            return next(iter(pools.values()))
        for sl, snap, pool in pools.values():
            if team and team in sl.teams:
                return sl, snap, pool
        return None

    for m in result.mentions:
        hit = slate_for(m["team"])
        if hit is None:
            status_m, method, row, sl_key, snap = "no_pool", None, None, None, None
        else:
            sl, snap, pool = hit
            sl_key = sl.key
            status_m, method, row = match_player(m["name"], m["team"], pool, sl.teams)
        summary["matched" if status_m == "matched" else "unmatched"] += 1
        db.execute(
            """INSERT INTO nfl_expert_player_mentions (extraction_id, article_id, source, slate_key, player_name, team,
                   role, slot, published_own_pct, own_definition, quote, match_status, match_method, dk_pid, dk_name,
                   dk_team, dk_position, dk_salary, pool_snapshot_id)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (ext_id, article["id"], article["source"], sl_key, m["name"], m["team"], m["role"], m["slot"],
             m["ownership_pct"], m["ownership_definition"], m["quote"], status_m, method,
             row["dk_pid"] if row else None, row["name"] if row else None, row["team"] if row else None,
             row["position"] if row else None, row["salary"] if row else None, snap if row else None))
    for lu in result.lineups:
        teams = {p["team"] for p in lu["players"] if p["team"]}
        hit = slate_for(next(iter(teams), None)) if teams else slate_for(None)
        players = []
        for p in lu["players"]:
            st, method, row = ("no_pool", None, None) if hit is None else match_player(p["name"], p["team"], hit[2], hit[0].teams)
            players.append({**p, "match_status": st, "dk_pid": row["dk_pid"] if row else None})
        db.execute(
            """INSERT INTO nfl_expert_sample_lineups (extraction_id, article_id, source, slate_key, lineup_index, label,
                   players) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (ext_id, article["id"], article["source"], hit[0].key if hit else None, lu["index"], lu["label"],
             Json(players)))
    return summary


def run(db, limit: int | None = None) -> dict:
    from config import load_config
    from ingest.nfl_expert_coverage import ensure_tables

    ensure_tables(db)
    cfg = load_config().deepseek_api
    if not cfg.api_key:
        raise RuntimeError("DEEPSEEK_API_KEY is not set")
    totals = {"articles": 0, "ok": 0, "not_nfl_dfs": 0, "empty": 0, "malformed": 0, "mentions": 0, "discarded": 0,
              "lineups": 0, "lineups_discarded": 0, "matched": 0, "unmatched": 0}
    for article in pending_articles(db, limit):
        s = extract_one(db, cfg, article)
        totals["articles"] += 1
        if s["status"] in totals:
            totals[s["status"]] += 1
        for k in ("mentions", "discarded", "lineups", "lineups_discarded", "matched", "unmatched"):
            totals[k] += s.get(k, 0)
        logger.info("article %s (%s): %s", article["id"], article["source"], s)
    print(f"NFL expert extraction ({EXTRACTION_VERSION}): {json.dumps(totals)}")
    return totals


def report(db) -> None:
    for r in db.execute(
            """SELECT a.source, a.title, s.slate_key, s.timing_class, round(s.minutes_before_kickoff) AS mins,
                      e.status, e.mentions_grounded, e.mentions_discarded
               FROM nfl_expert_extractions e JOIN nfl_expert_articles a ON a.id = e.article_id
               LEFT JOIN nfl_expert_article_slates s ON s.extraction_id = e.id
               WHERE e.extraction_version = %s ORDER BY a.captured_at""", (EXTRACTION_VERSION,)):
        print(dict(r))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="NFL DFS expert article extraction")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()
    from config import load_config
    from db.database import DatabaseManager
    from model.deepseek_account import DeepSeekAccountError, exit_on_account_error

    database = DatabaseManager(load_config().database_url, initialize_schema=False)
    if args.report:
        report(database)
    else:
        try:
            run(database, args.limit)
        except DeepSeekAccountError as exc:
            exit_on_account_error(exc, job="NFL expert extraction")
