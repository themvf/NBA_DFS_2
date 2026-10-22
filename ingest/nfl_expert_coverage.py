"""Collect NFL DFS expert articles before lock (docs/nfl-expert-coverage-study.md).

What it does, per run:
  1. For each source in SOURCES, fetch robots.txt and evaluate it for our
     user-agent token (RFC 9309: grouped rules, longest match, `*` and `$`).
     The body and the decision are stored in `nfl_expert_robots_checks`. A
     source whose robots.txt cannot be read (5xx, network) is skipped for the
     run; a 404 means "no restrictions" and is recorded as such.
  2. Discover candidate articles from the source's own sitemap / RSS / listing.
     No article URL is hard-coded.
  3. Keep NFL DFS slate articles (Showdown and Classic) by title/URL, skip
     known premium paths, fetch at most one page per second, never log in.
  4. Store the visible text append-only. A page whose text is unchanged since
     its last stored revision writes only a fetch-log row; a changed page
     appends a new revision. Nothing is ever updated or deleted.

Timing fields: `published_at` is parsed from the page (JSON-LD, Open Graph,
<time>, the site's printed date) with its method and precision; `captured_at`
is our fetch time and is the time the study binds on.

Extraction (players, roles, ownership, lineups) is a separate step:
model/nfl_expert_extraction.py.

Usage:
    python -m ingest.nfl_expert_coverage                  # one collection pass
    python -m ingest.nfl_expert_coverage --pass-label manual --lookback-days 4
    python -m ingest.nfl_expert_coverage --sources dknetwork,rotowire --max-articles 10
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from html import unescape
from typing import Callable, Iterable
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

COLLECTOR_VERSION = "nfl-expert-coverage-v1"
ROBOTS_TOKEN = "NBADFSv2-ExpertCoverage"
USER_AGENT = (f"{ROBOTS_TOKEN}/1.0 (private NFL DFS research collector; reads robots.txt; "
              "at most 1 request per second; never logs in)")
MIN_REQUEST_INTERVAL_SECONDS = 1.0
TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
MIN_TEXT_CHARS = 800
EASTERN = ZoneInfo("America/New_York")

# Pass windows, minutes before a kickoff group (docs section 2). The dispatch
# job fires on the one 15-minute tick inside each window.
PASS_WINDOWS = {"T-48h": 48 * 60, "T-24h": 24 * 60, "T-6h": 6 * 60, "T-90m": 90}
PASS_WINDOW_TOLERANCE_MINUTES = 30


# --------------------------------------------------------------------------- robots

@dataclass
class RobotsGroup:
    agents: list[str]
    rules: list[tuple[bool, str]] = field(default_factory=list)  # (allow, pattern)


def parse_robots(text: str) -> list[RobotsGroup]:
    """Group records as RFC 9309 section 2.1: consecutive user-agent lines open
    a group, rules attach to the open group, a user-agent after rules opens a
    new one. Other records (Sitemap, LLM-policy, comments) are ignored."""
    groups: list[RobotsGroup] = []
    current: RobotsGroup | None = None
    last_was_agent = False
    for raw in (text or "").splitlines():
        line = raw.split("#", 1)[0].strip()
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key, value = key.strip().lower(), value.strip()
        if key == "user-agent":
            if current is None or not last_was_agent:
                current = RobotsGroup(agents=[])
                groups.append(current)
            current.agents.append(value.lower())
            last_was_agent = True
        elif key in ("allow", "disallow"):
            last_was_agent = False
            if current is None:
                continue
            if key == "disallow" and value == "":
                continue  # an empty Disallow allows everything; no rule
            current.rules.append((key == "allow", value))
        else:
            continue
    return groups


def _rules_for(groups: list[RobotsGroup], token: str) -> list[tuple[bool, str]]:
    """Merge every group naming our token; else every `*` group (RFC 9309 2.2.1)."""
    token = token.lower()
    # RFC 9309 2.2.1: the product token is matched to the user-agent line
    # case-insensitively; a substring rule would let `User-agent: doc` capture us.
    specific = [g for g in groups if any(a != "*" and a == token for a in g.agents)]
    chosen = specific or [g for g in groups if "*" in g.agents]
    rules: list[tuple[bool, str]] = []
    for g in chosen:
        rules.extend(g.rules)
    return rules


def _pattern_regex(pattern: str) -> re.Pattern:
    anchored = pattern.endswith("$")
    body = pattern[:-1] if anchored else pattern
    rx = "".join(".*" if ch == "*" else re.escape(ch) for ch in body)
    return re.compile("^" + rx + ("$" if anchored else ""))


def robots_allows(robots_text: str | None, path: str, token: str = ROBOTS_TOKEN) -> bool:
    """Longest matching rule wins; on a tie Allow wins; no match allows.
    `path` is the URL path plus query. None (no robots file) allows."""
    if robots_text is None:
        return True
    rules = _rules_for(parse_robots(robots_text), token.lower())
    best_len, best_allow = -1, True
    for allow, pattern in rules:
        if _pattern_regex(pattern).match(path):
            length = len(pattern)
            if length > best_len or (length == best_len and allow):
                best_len, best_allow = length, allow
    return best_allow


def url_path(url: str) -> str:
    parts = urlsplit(url)
    return (parts.path or "/") + (("?" + parts.query) if parts.query else "")


@dataclass
class RobotsDecision:
    source: str
    robots_url: str
    fetched_at: datetime
    http_status: int | None
    body: str | None
    decision: str  # allowed | no_robots_file | disallowed | unreachable
    detail: str = ""

    def allows(self, url: str) -> bool:
        if self.decision == "unreachable":
            return False
        if self.decision == "no_robots_file":
            return True
        return robots_allows(self.body, url_path(url))


def classify_robots_response(status: int | None, body: str | None, probe_path: str) -> tuple[str, str | None]:
    """(decision, body to evaluate). 4xx = no restrictions (RFC 9309 2.3.1.3);
    5xx/network = treat as complete disallow (2.3.1.4)."""
    if status is None or status >= 500:
        return "unreachable", None
    if 400 <= status < 500:
        return "no_robots_file", None
    if not robots_allows(body, probe_path):
        return "disallowed", body
    return "allowed", body


# --------------------------------------------------------------------------- polite fetching

class PoliteSession:
    """One requests session; at most one request per MIN_REQUEST_INTERVAL_SECONDS
    across all hosts, exponential backoff on network errors / 429 / 5xx."""

    def __init__(self, interval: float = MIN_REQUEST_INTERVAL_SECONDS, sleep: Callable[[float], None] = time.sleep):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.8"})
        self.interval = interval
        self._sleep = sleep
        self._last = 0.0
        self.requests_made = 0

    def _wait(self) -> None:
        gap = time.monotonic() - self._last
        if gap < self.interval:
            self._sleep(self.interval - gap)
        self._last = time.monotonic()

    def get(self, url: str) -> requests.Response:
        delay = 2.0
        for attempt in range(1, MAX_RETRIES + 1):
            self._wait()
            self.requests_made += 1
            try:
                resp = self.session.get(url, timeout=TIMEOUT_SECONDS, allow_redirects=True)
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                if attempt == MAX_RETRIES:
                    raise
                logger.warning("%s: network error %s (attempt %d), retrying in %.0fs", url, exc, attempt, delay)
                self._sleep(delay)
                delay *= 2
                continue
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                logger.warning("%s: HTTP %s (attempt %d), retrying in %.0fs", url, resp.status_code, attempt, delay)
                self._sleep(delay)
                delay *= 2
                continue
            return resp
        raise RuntimeError(f"unreachable: {url}")


# --------------------------------------------------------------------------- discovery

@dataclass
class Candidate:
    url: str
    title: str | None = None
    listed_at: datetime | None = None
    via: str = ""


def parse_dt(value: str | None) -> datetime | None:
    """ISO 8601 (with Z or offset) or RFC 822 (RSS). Naive values are UTC."""
    if not value:
        return None
    value = unescape(value).strip()
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _decode_maybe_gzip(content: bytes) -> str:
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    return content.decode("utf-8", "replace")


def parse_sitemap(xml: str, via: str) -> list[Candidate]:
    """<url> entries of a sitemap or Google News sitemap."""
    out = []
    for block in re.findall(r"<url>(.*?)</url>", xml, re.S):
        loc = re.search(r"<loc>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</loc>", block, re.S)
        if not loc:
            continue
        title = re.search(r"<news:title>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</news:title>", block, re.S)
        when = (re.search(r"<news:publication_date>(.*?)</news:publication_date>", block, re.S)
                or re.search(r"<lastmod>(.*?)</lastmod>", block, re.S))
        out.append(Candidate(url=unescape(loc.group(1).strip()),
                             title=unescape(title.group(1).strip()) if title else None,
                             listed_at=parse_dt(when.group(1)) if when else None, via=via))
    return out


def sitemap_index_locs(xml: str) -> list[str]:
    return [unescape(u.strip()) for u in re.findall(r"<sitemap>\s*<loc>(.*?)</loc>", xml, re.S)]


def parse_rss(xml: str, via: str) -> list[Candidate]:
    out = []
    for block in re.findall(r"<item>(.*?)</item>", xml, re.S):
        link = re.search(r"<link>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</link>", block, re.S)
        if not link:
            continue
        title = re.search(r"<title>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</title>", block, re.S)
        when = re.search(r"<pubDate>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</pubDate>", block, re.S)
        out.append(Candidate(url=unescape(link.group(1).strip()),
                             title=unescape(title.group(1).strip()) if title else None,
                             listed_at=parse_dt(when.group(1)) if when else None, via=via))
    return out


def parse_listing_links(html: str, pattern: str, base: str, via: str) -> list[Candidate]:
    out, seen = [], set()
    for href in re.findall(r'href="([^"#]+)"', html):
        url = href if href.startswith("http") else base.rstrip("/") + "/" + href.lstrip("/")
        if re.search(pattern, url) and url not in seen:
            seen.add(url)
            out.append(Candidate(url=url, via=via))
    return out


# --------------------------------------------------------------------------- NFL DFS filtering

_OTHER_SPORTS = re.compile(r"\b(nba|wnba|mlb|nhl|pga|golf|cfb|college|ncaa|ufc|mma|nascar|soccer|tennis|"
                           r"basketball|baseball|hockey|f1|lol|cs2|esports|cbb)\b|cfb-|college-football|ncaaf",
                           re.I)
_NFL = re.compile(r"\b(nfl|football|showdown|thursday night|sunday night|monday night|tnf|snf|mnf|main slate)\b", re.I)
_DFS = re.compile(r"\b(dfs|showdown|draftkings|dk|gpp|cash game|lineup picks|value plays?|captains?|ownership|"
                  r"stacks?|core plays|chalk|contrarian|punt)\b", re.I)
# Season-long and betting formats that share DFS vocabulary ("DraftKings Pick6",
# "start 'em, sit 'em cheat sheet", Footballguys' season-long "High-Leverage
# Opportunities"): not slate DFS coverage.
_NOT_DFS = re.compile(r"\b(start em|sit em|starts and sits|pick6|pick 6|end zone|best bets|predictions|props?|"
                      r"sleepers|busts|waiver|rankings|high leverage opportunities|trade)\b", re.I)
_POST_HOC = re.compile(r"\b(winning lineup|recap|review|audit|tout corrector|sim desk|results|lessons)\b", re.I)
_FANDUEL_ONLY = re.compile(r"fanduel", re.I)
_DRAFTKINGS = re.compile(r"draftkings|\bdk\b", re.I)


def _words(url: str, title: str | None) -> str:
    slug = urlsplit(url).path.replace("-", " ").replace("_", " ").replace("/", " ")
    return f"{title or ''} {slug}"


def is_nfl_dfs(url: str, title: str | None, extra_include: str | None = None, implied_nfl: bool = False) -> bool:
    """Pre-game NFL DFS slate article for DraftKings (or generic DFS).
    `implied_nfl`: the source only covers the NFL, so the NFL keyword is not required."""
    text = _words(url, title)
    if extra_include and re.search(extra_include, url, re.I):
        return not _POST_HOC.search(text)
    if _OTHER_SPORTS.search(text) or _POST_HOC.search(text):
        return False
    if _NOT_DFS.search(text.replace("’", "").replace("'", "")):
        return False
    if _FANDUEL_ONLY.search(text) and not _DRAFTKINGS.search(text):
        return False
    return bool((implied_nfl or _NFL.search(text)) and _DFS.search(text))


# --------------------------------------------------------------------------- sources

@dataclass
class Source:
    key: str
    name: str
    base: str
    discover: Callable[["PoliteSession", "DiscoveryContext"], list[Candidate]]
    skip_path: str | None = None          # regex: URL never fetched (premium paths)
    extra_include: str | None = None      # regex: URL counts as NFL DFS regardless of keywords
    content_selectors: tuple[str, ...] = ("article", "main")
    printed_date: str | None = None       # regex with one group, MM/DD/YYYY, for day-precision dates
    implied_nfl: bool = False             # the site covers only the NFL


@dataclass
class DiscoveryContext:
    now: datetime
    lookback: timedelta
    target_weeks: set[int]
    robots: RobotsDecision

    def recent(self, c: Candidate) -> bool:
        return c.listed_at is None or c.listed_at >= self.now - self.lookback


def _fetch_text(sess: PoliteSession, ctx: DiscoveryContext, url: str) -> str | None:
    if not ctx.robots.allows(url):
        logger.info("robots disallows discovery URL %s", url)
        return None
    resp = sess.get(url)
    if resp.status_code != 200:
        logger.warning("discovery %s: HTTP %s", url, resp.status_code)
        return None
    return _decode_maybe_gzip(resp.content)


def _discover_dknetwork(sess, ctx):
    out = []
    xml = _fetch_text(sess, ctx, "https://dknetwork.draftkings.com/news-sitemap.xml")
    if xml:
        out += parse_sitemap(xml, "news-sitemap")
    idx = _fetch_text(sess, ctx, "https://dknetwork.draftkings.com/sitemap_index.xml")
    if idx:
        posts = [u for u in sitemap_index_locs(idx) if re.search(r"/post-sitemap\d*\.xml$", u)]
        posts.sort(key=lambda u: int(re.search(r"post-sitemap(\d*)", u).group(1) or 1))
        if posts:
            xml = _fetch_text(sess, ctx, posts[-1])
            if xml:
                out += parse_sitemap(xml, posts[-1].rsplit("/", 1)[-1])
    return out


def _discover_rotoballer(sess, ctx):
    out = []
    xml = _fetch_text(sess, ctx, "https://www.rotoballer.com/google-news-sitemap.xml")
    if xml:
        out += parse_sitemap(xml, "google-news-sitemap")
    xml = _fetch_text(sess, ctx, "https://www.rotoballer.com/sitemap_post_1.xml.gz")
    if xml:
        out += parse_sitemap(xml, "sitemap_post_1")
    return out


def _discover_rotowire(sess, ctx):
    xml = _fetch_text(sess, ctx, "https://www.rotowire.com/news-sitemap.php")
    return parse_sitemap(xml, "news-sitemap") if xml else []


def _discover_fantasylife(sess, ctx):
    xml = _fetch_text(sess, ctx, "https://www.fantasylife.com/news-sitemap.xml")
    return parse_sitemap(xml, "news-sitemap") if xml else []


def _discover_dailyambush(sess, ctx):
    xml = _fetch_text(sess, ctx, "https://www.dailyambush.com/sitemap.rss")
    return parse_rss(xml, "sitemap.rss") if xml else []


def footballguys_week(url: str) -> int | None:
    m = re.search(r"week-?0?(\d{1,2})(?:\b|$|[^0-9])", url, re.I)
    return int(m.group(1)) if m else None


def _discover_footballguys(sess, ctx):
    html = _fetch_text(sess, ctx, "https://www.footballguys.com/articles?category=dfs")
    if not html:
        return []
    year = ctx.now.year
    cands = parse_listing_links(html, rf"footballguys\.com/article/{year}-", "https://www.footballguys.com",
                                "articles?category=dfs")
    # The listing carries no dates; the week in the slug stands in for recency.
    return [c for c in cands if footballguys_week(c.url) in ctx.target_weeks]


SOURCES: dict[str, Source] = {s.key: s for s in [
    Source("dknetwork", "DraftKings Network", "https://dknetwork.draftkings.com", _discover_dknetwork,
           content_selectors=("div.entry-content", "article", "main")),
    Source("rotoballer", "RotoBaller", "https://www.rotoballer.com", _discover_rotoballer,
           skip_path=r"/premium/", content_selectors=("div.entry-content", "article", "main")),
    Source("rotowire", "RotoWire", "https://www.rotowire.com", _discover_rotowire,
           content_selectors=("article", "main")),
    Source("footballguys", "Footballguys", "https://www.footballguys.com", _discover_footballguys,
           content_selectors=("div.article-content", "article", "main"),
           printed_date=r"Published\s+(\d{1,2}/\d{1,2}/\d{4})", implied_nfl=True),
    Source("dailyambush", "DailyAmbush", "https://www.dailyambush.com", _discover_dailyambush,
           extra_include=r"/(nfl-ambush-|[a-z]+-night-showdown|mnf-|snf-|tnf-)",
           content_selectors=("div.entry-content", "article", "main")),
    Source("fantasylife", "Fantasy Life", "https://www.fantasylife.com", _discover_fantasylife,
           content_selectors=("article", "main")),
]}

# Checked live 2026-10-21 and dropped; kept here so the decision is in the code.
DROPPED_SOURCES = {
    "lineups": "www.lineups.com: every page, sitemap included, answers a Cloudflare challenge (HTTP 403). Not bypassed.",
    "draftwithdoug": ("www.draftwithdoug.com: the HTML carries only title and deck; the article body is loaded "
                      "client-side from a data API and robots.txt disallows /api/."),
}


# --------------------------------------------------------------------------- article parsing

@dataclass
class ParsedArticle:
    title: str | None
    author: str | None
    published_at: datetime | None
    published_at_raw: str | None
    published_at_method: str
    published_precision: str
    modified_at: datetime | None
    text: str
    accessible_for_free: bool | None


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _jsonld_objects(soup: BeautifulSoup) -> list[dict]:
    out: list[dict] = []

    def walk(node):
        if isinstance(node, dict):
            out.append(node)
            for key in ("@graph", "mainEntity"):
                if key in node:
                    walk(node[key])
        elif isinstance(node, list):
            for item in node:
                walk(item)

    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            walk(json.loads(tag.string or tag.get_text() or "null"))
        except (json.JSONDecodeError, TypeError):
            continue
    return out


_ARTICLE_TYPES = {"article", "newsarticle", "blogposting", "reportagenewsarticle", "analysisnewsarticle"}


def _type_names(obj: dict) -> set[str]:
    t = obj.get("@type")
    names = t if isinstance(t, list) else [t]
    return {str(n).lower() for n in names if n}


def parse_published(soup: BeautifulSoup, printed_date: str | None, text: str) -> tuple:
    """(published_at, raw, method, precision, modified_at, accessible_for_free)."""
    objs = _jsonld_objects(soup)
    articles = [o for o in objs if _type_names(o) & _ARTICLE_TYPES] or [o for o in objs if "datePublished" in o]
    free = None
    for o in objs:
        if "isAccessibleForFree" in o:
            v = o["isAccessibleForFree"]
            free = v if isinstance(v, bool) else str(v).strip().lower() == "true"
            if free is False:
                break
    modified = None
    for o in articles:
        if o.get("datePublished"):
            dt = parse_dt(str(o["datePublished"]))
            if dt:
                modified = parse_dt(str(o.get("dateModified") or "")) if o.get("dateModified") else None
                return dt, str(o["datePublished"]), "jsonld_datePublished", "second", modified, free
    meta = soup.find("meta", attrs={"property": "article:published_time"})
    if meta and meta.get("content"):
        dt = parse_dt(meta["content"])
        if dt:
            mod = soup.find("meta", attrs={"property": "article:modified_time"})
            modified = parse_dt(mod["content"]) if mod and mod.get("content") else None
            return dt, meta["content"], "meta_article_published_time", "second", modified, free
    tm = soup.find("time", attrs={"datetime": True})
    if tm:
        dt = parse_dt(tm["datetime"])
        if dt:
            return dt, tm["datetime"], "html_time_datetime", "second", None, free
    if printed_date:
        m = re.search(printed_date, text)
        if m:
            raw = m.group(1)
            d = datetime.strptime(raw, "%m/%d/%Y").date()
            # Day precision: stored as local midnight Eastern; never used for timing.
            return (datetime(d.year, d.month, d.day, tzinfo=EASTERN).astimezone(timezone.utc), raw,
                    "printed_date_mmddyyyy", "day", None, free)
    return None, None, "none", "none", None, free


def parse_article(html: str, source: Source) -> ParsedArticle:
    soup = BeautifulSoup(html, "html.parser")
    title = None
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        title = normalize_space(unescape(og["content"]))
    elif soup.title:
        title = normalize_space(soup.title.get_text())
    author = None
    meta_author = soup.find("meta", attrs={"name": "author"})
    if meta_author and meta_author.get("content"):
        author = normalize_space(meta_author["content"])

    for tag in soup(["script", "style", "noscript", "svg", "form", "iframe"]):
        tag.decompose()
    container = None
    for selector in source.content_selectors:
        found = soup.select(selector)
        if found:
            container = max(found, key=lambda el: len(el.get_text(" ", strip=True)))
            if len(container.get_text(" ", strip=True)) >= MIN_TEXT_CHARS:
                break
    container = container or soup.body or soup
    for tag in container.find_all(["nav", "footer", "aside"]):
        tag.decompose()
    text = normalize_space(container.get_text(" ", strip=True))

    # Parse dates from the full page (JSON-LD sits outside the content container).
    full = BeautifulSoup(html, "html.parser")
    full_text = normalize_space(full.get_text(" ", strip=True))
    published, raw, method, precision, modified, free = parse_published(full, source.printed_date, full_text)
    return ParsedArticle(title, author, published, raw, method, precision, modified, text, free)


def content_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- pass labels

def pass_label_for(now: datetime, kickoffs: Iterable[datetime]) -> str:
    """The scheduled window this run stands for, else 'adhoc'."""
    best = None
    for k in kickoffs:
        lead = (k - now).total_seconds() / 60
        if lead <= 0:
            continue
        for label, minutes in PASS_WINDOWS.items():
            gap = abs(lead - minutes)
            if gap <= PASS_WINDOW_TOLERANCE_MINUTES and (best is None or gap < best[0]):
                best = (gap, label)
    return best[1] if best else "adhoc"


# --------------------------------------------------------------------------- database

def ensure_tables(db) -> None:
    """Apply only this collection's DDL (the full schema pass is not run here)."""
    from db.nfl_expert_coverage_schema import STATEMENTS
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("SET LOCAL lock_timeout = '30s'")
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('nfl_expert_coverage_schema'))")
        for statement in STATEMENTS:
            cur.execute(statement)


def _upcoming_kickoffs(db, now: datetime) -> list[datetime]:
    rows = db.execute("SELECT DISTINCT kickoff FROM nfl_season_games WHERE kickoff > %s AND kickoff <= %s",
                      (now, now + timedelta(hours=72)))
    return [r["kickoff"] for r in rows]


def target_weeks(db, now: datetime, lookback: timedelta) -> set[int]:
    rows = db.execute("SELECT DISTINCT week FROM nfl_season_games WHERE season = %s AND kickoff BETWEEN %s AND %s",
                      (now.year if now.month >= 3 else now.year - 1, now - lookback, now + timedelta(days=8)))
    return {int(r["week"]) for r in rows}


def _record_robots(db, run_uuid: str, d: RobotsDecision) -> None:
    probe = {"/": robots_allows(d.body, "/") if d.body is not None else d.decision != "unreachable"}
    db.execute(
        """INSERT INTO nfl_expert_robots_checks (run_uuid, source, robots_url, fetched_at, http_status, body_sha256,
               body, user_agent, decision, evaluated_paths)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (run_uuid, d.source, d.robots_url, d.fetched_at, d.http_status,
         content_sha256(d.body) if d.body is not None else None, d.body, USER_AGENT, d.decision,
         json.dumps({**probe, "detail": d.detail})))


def _log_fetch(db, run_uuid, source, url, outcome, status=None, article_id=None, detail=None):
    db.execute("""INSERT INTO nfl_expert_fetch_log (run_uuid, source, url, outcome, http_status, article_id, detail)
                  VALUES (%s,%s,%s,%s,%s,%s,%s)""", (run_uuid, source, url, outcome, status, article_id, detail))


def store_article(db, run_uuid: str, pass_label: str, source: Source, cand: Candidate, status: int,
                  parsed: ParsedArticle, captured_at: datetime) -> tuple[str, int]:
    """Append a revision unless the text is unchanged. Returns (outcome, article_id)."""
    digest = content_sha256(parsed.text)
    existing = db.execute("SELECT id, revision, content_sha256 FROM nfl_expert_articles WHERE url = %s "
                          "ORDER BY revision DESC", (cand.url,))
    for row in existing:
        if row["content_sha256"] == digest:
            return "unchanged", row["id"]
    revision = (existing[0]["revision"] + 1) if existing else 1
    flags = {}
    if parsed.published_at and parsed.published_at > captured_at + timedelta(minutes=5):
        flags["published_after_capture"] = True
    if parsed.accessible_for_free is not None:
        flags["accessible_for_free"] = parsed.accessible_for_free
    row = db.execute_one(
        """INSERT INTO nfl_expert_articles (collector_version, source, url, revision, content_sha256, title, author,
               published_at, published_at_raw, published_at_method, published_precision, modified_at, captured_at,
               run_uuid, pass_label, http_status, raw_text, text_length, discovery, flags)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (url, content_sha256) DO NOTHING RETURNING id""",
        (COLLECTOR_VERSION, source.key, cand.url, revision, digest, parsed.title or cand.title, parsed.author,
         parsed.published_at, parsed.published_at_raw, parsed.published_at_method, parsed.published_precision,
         parsed.modified_at, captured_at, run_uuid, pass_label, status, parsed.text, len(parsed.text),
         json.dumps({"via": cand.via, "listed_title": cand.title,
                     "listed_at": cand.listed_at.isoformat() if cand.listed_at else None}),
         json.dumps(flags)))
    if row is None:
        again = db.execute_one("SELECT id FROM nfl_expert_articles WHERE url=%s AND content_sha256=%s",
                               (cand.url, digest))
        return "unchanged", again["id"]
    return ("new_revision" if existing else "new_article"), row["id"]


# --------------------------------------------------------------------------- run

def check_robots(sess: PoliteSession, source: Source) -> RobotsDecision:
    robots_url = source.base.rstrip("/") + "/robots.txt"
    fetched_at = datetime.now(timezone.utc)
    try:
        resp = sess.get(robots_url)
        status, body = resp.status_code, resp.text
    except requests.exceptions.RequestException as exc:
        return RobotsDecision(source.key, robots_url, fetched_at, None, None, "unreachable", str(exc)[:300])
    decision, evaluated = classify_robots_response(status, body, "/")
    return RobotsDecision(source.key, robots_url, fetched_at, status,
                          body if 200 <= status < 300 else None, decision,
                          "" if decision != "no_robots_file" else f"HTTP {status}: no restrictions (RFC 9309 2.3.1.3)")


def collect(db, *, source_keys: list[str] | None = None, lookback_days: float = 4.0, pass_label: str | None = None,
            max_articles: int | None = None, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    started = now
    run_uuid = str(uuid.uuid4())
    lookback = timedelta(days=lookback_days)
    label = pass_label or pass_label_for(now, _upcoming_kickoffs(db, now))
    weeks = target_weeks(db, now, lookback)
    sess = PoliteSession()
    summary: dict[str, dict] = {}
    totals = {"articles_new": 0, "revisions_new": 0, "unchanged": 0, "skipped": 0, "errors": 0}
    fetched = 0
    for key in source_keys or list(SOURCES):
        source = SOURCES[key]
        stats = {"robots": None, "candidates": 0, "nfl_dfs": 0, "new_article": 0, "new_revision": 0,
                 "unchanged": 0, "skipped": {}, "errors": 0}
        summary[key] = stats
        robots = check_robots(sess, source)
        _record_robots(db, run_uuid, robots)
        stats["robots"] = robots.decision
        if robots.decision in ("unreachable", "disallowed"):
            totals["errors"] += robots.decision == "unreachable"
            continue
        ctx = DiscoveryContext(now=now, lookback=lookback, target_weeks=weeks, robots=robots)
        try:
            cands = source.discover(sess, ctx)
        except Exception as exc:  # a broken source must not stop the others
            logger.exception("%s: discovery failed", key)
            stats["errors"] += 1
            totals["errors"] += 1
            stats["discovery_error"] = str(exc)[:300]
            continue
        seen: set[str] = set()
        picked = []
        for c in cands:
            if c.url in seen or not ctx.recent(c):
                continue
            seen.add(c.url)
            stats["candidates"] += 1
            if is_nfl_dfs(c.url, c.title, source.extra_include, source.implied_nfl):
                picked.append(c)
        stats["nfl_dfs"] = len(picked)
        for c in picked:
            if max_articles is not None and fetched >= max_articles:
                break
            outcome, status, article_id, detail = None, None, None, None
            if source.skip_path and re.search(source.skip_path, c.url):
                outcome, detail = "paywalled", "premium path; not fetched"
            elif not robots.allows(c.url):
                outcome, detail = "robots_disallowed", url_path(c.url)
            else:
                try:
                    resp = sess.get(c.url)
                    fetched += 1
                    status = resp.status_code
                    captured_at = datetime.now(timezone.utc)
                    if status != 200:
                        outcome, detail = "http_error", resp.text[:200]
                    else:
                        parsed = parse_article(resp.text, source)
                        if parsed.accessible_for_free is False:
                            outcome, detail = "paywalled", "JSON-LD isAccessibleForFree=false"
                        elif len(parsed.text) < MIN_TEXT_CHARS:
                            outcome, detail = "too_short", f"{len(parsed.text)} chars"
                        else:
                            outcome, article_id = store_article(db, run_uuid, label, source, c, status, parsed,
                                                                captured_at)
                except requests.exceptions.RequestException as exc:
                    outcome, detail = "fetch_error", str(exc)[:300]
            _log_fetch(db, run_uuid, key, c.url, outcome, status, article_id, detail)
            if outcome in ("new_article", "new_revision", "unchanged"):
                stats[outcome] += 1
            elif outcome in ("http_error", "fetch_error"):
                stats["errors"] += 1
                totals["errors"] += 1
            else:
                stats["skipped"][outcome] = stats["skipped"].get(outcome, 0) + 1
                totals["skipped"] += 1
        totals["articles_new"] += stats["new_article"]
        totals["revisions_new"] += stats["new_revision"]
        totals["unchanged"] += stats["unchanged"]

    reachable = [k for k, s in summary.items() if s["robots"] in ("allowed", "no_robots_file") and not s.get("discovery_error")]
    ok = bool(reachable)
    db.execute(
        """INSERT INTO nfl_expert_capture_runs (run_uuid, collector_version, pass_label, started_at, ok, sources,
               articles_new, revisions_new, unchanged, skipped, errors, detail)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (run_uuid, COLLECTOR_VERSION, label, started, ok, json.dumps(summary), totals["articles_new"],
         totals["revisions_new"], totals["unchanged"], totals["skipped"], totals["errors"],
         json.dumps({"lookback_days": lookback_days, "target_weeks": sorted(weeks), "requests": sess.requests_made,
                     "dropped_sources": DROPPED_SOURCES})))
    return {"run_uuid": run_uuid, "pass_label": label, "ok": ok, "sources": summary, **totals}


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--sources", help="comma-separated keys: " + ",".join(SOURCES))
    parser.add_argument("--lookback-days", type=float, default=4.0)
    parser.add_argument("--pass-label", help="override the computed pass label (e.g. manual)")
    parser.add_argument("--max-articles", type=int)
    args = parser.parse_args(argv)
    from config import load_config
    from db.database import DatabaseManager
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    ensure_tables(db)
    keys = [k.strip() for k in args.sources.split(",")] if args.sources else None
    result = collect(db, source_keys=keys, lookback_days=args.lookback_days, pass_label=args.pass_label,
                     max_articles=args.max_articles)
    print(json.dumps(result, indent=2, default=str))
    if not result["ok"]:
        print("::error::no source could be read this run", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
