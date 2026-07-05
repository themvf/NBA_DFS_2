"""Scrape MASN beat-writer articles for the information-latency pilot.

Phase 0 of the "MLB Beat-Writer Information-Latency Pilot" (CLAUDE.md,
2026-07-05). Orioles-only for now -- MASN was verified live to have no
active Nationals section (no "nationals" blog category, no active
Nationals-beat author in its sitemap); Nationals coverage is deferred until
a viable free source is found.

Source: Roch Kubatko's MASN blog (masnsports.com/blog/blogger/rochkubatko/).
robots.txt was checked and is fully permissive for this domain.

published_at is parsed from the article page's own displayed timestamp
(e.g. "July 05, 2026 4:00 am"), not scrape time -- this is the whole
point-in-time signal the pilot depends on.

Usage:
    python -m ingest.mlb_beat_articles                  # scrape + store new articles
    python -m ingest.mlb_beat_articles --limit 10
    python -m ingest.mlb_beat_articles --backfill-sitemap --max-fetch 60
        # one-time historical backfill via the site's own post-sitemap43.xml
        # (the listing page's pagination is broken -- verified 404 on
        # /page/2/ -- so this is the only way to reach a meaningful Phase 0
        # sample size without waiting weeks for organic accumulation).
        # Fetches candidate URLs newest-first, keeps only ones actually
        # authored by Roch Kubatko (the sitemap is site-wide, not
        # author-scoped), stops once max-fetch pages have been downloaded.
"""

from __future__ import annotations

import argparse
import logging
import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from config import load_config
from db.database import DatabaseManager
from db.queries import build_mlb_team_abbrev_cache, upsert_mlb_beat_article

logger = logging.getLogger(__name__)

_SOURCE = "masn_orioles"
_TEAM_ABBREV = "BAL"
_AUTHOR_NAME = "Roch Kubatko"
_LISTING_URL = "https://www.masnsports.com/blog/blogger/rochkubatko/"
_SITEMAP_URL = "https://www.masnsports.com/post-sitemap43.xml"
_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
_TIMEOUT = 20
_MAX_RETRIES = 4
_RETRY_BASE_SECONDS = 2.0
_DEFAULT_LIMIT = 20


def _call_with_retry(fn, label: str):
    """Fetch with exponential backoff, mirroring ingest/nba_stats.py's pattern."""
    delay = _RETRY_BASE_SECONDS
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return fn()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if attempt == _MAX_RETRIES:
                raise
            logger.warning("%s: network error on attempt %d/%d (%s). Retrying in %.0fs...",
                            label, attempt, _MAX_RETRIES, exc, delay)
            time.sleep(delay)
            delay *= 2
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            if attempt == _MAX_RETRIES or status not in (429, 500, 502, 503, 504):
                raise
            logger.warning("%s: HTTP %s on attempt %d/%d. Retrying in %.0fs...",
                            label, status, attempt, _MAX_RETRIES, delay)
            time.sleep(delay)
            delay *= 2


def _get(url: str) -> str:
    def _fetch():
        resp = requests.get(url, headers={"User-Agent": _USER_AGENT}, timeout=_TIMEOUT)
        resp.raise_for_status()
        return resp.text
    return _call_with_retry(_fetch, f"GET {url}")


def _parse_listing(html: str) -> list[str]:
    """Return article URLs found on the blogger listing page, most recent first."""
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    for card in soup.select("div.article-card"):
        link = card.find("a", href=True)
        if link and link["href"] not in urls:
            urls.append(link["href"])
    return urls


def _parse_published_at(date_str: str) -> str | None:
    """'July 05, 2026 4:00 am' -> ISO 8601 string, or None if unparseable."""
    try:
        dt = datetime.strptime(date_str.strip(), "%B %d, %Y %I:%M %p")
        return dt.isoformat()
    except ValueError:
        logger.warning("Could not parse date string: %r", date_str)
        return None


def _parse_article(html: str) -> dict | None:
    """Extract title, author, published_at, and body text from an article page."""
    soup = BeautifulSoup(html, "html.parser")

    title_el = soup.select_one("h1.entry-title")
    if title_el is None:
        return None
    title = title_el.get_text(strip=True)

    author = None
    published_at = None
    for li in soup.select("section.main li"):
        img = li.find("img", alt="user")
        a = li.find("a", href=True)
        if img is not None and a is not None:
            author = a.get_text(strip=True)
            continue
        img = li.find("img", alt="date")
        p = li.find("p")
        if img is not None and p is not None:
            published_at = _parse_published_at(p.get_text())

    paragraphs = [p.get_text(strip=True) for p in soup.select("p.wp-block-paragraph")]
    raw_text = "\n\n".join(p for p in paragraphs if p)

    return {"title": title, "author": author, "published_at": published_at, "raw_text": raw_text}


def _get_sitemap_urls(sitemap_url: str) -> list[tuple[str, str]]:
    """Return [(url, lastmod), ...] from a post-sitemap XML, newest first."""
    xml = _get(sitemap_url)
    entries = re.findall(r"<url>\s*<loc>(.*?)</loc>\s*<lastmod>(.*?)</lastmod>", xml)
    return sorted(entries, key=lambda e: e[1], reverse=True)


def _fetch_and_store_article(db: DatabaseManager, url: str, team_id: int | None,
                              require_author: str | None = None) -> bool:
    """Fetch one article page, verify authorship if requested, store it.

    Returns True if stored, False if skipped (fetch failure, unparseable,
    or wrong author -- e.g. a sitemap-sourced candidate not written by the
    beat writer this pilot is scoped to).
    """
    try:
        article_html = _get(url)
    except requests.exceptions.RequestException as exc:
        logger.warning("Failed to fetch %s: %s", url, exc)
        return False

    parsed = _parse_article(article_html)
    if parsed is None or not parsed["raw_text"]:
        logger.warning("Could not parse article body for %s", url)
        return False
    if require_author is not None and parsed["author"] != require_author:
        logger.debug("Skipping %s (author %r != %r)", url, parsed["author"], require_author)
        return False

    upsert_mlb_beat_article(
        db,
        source=_SOURCE,
        team_id=team_id,
        url=url,
        title=parsed["title"],
        published_at=parsed["published_at"],
        raw_text=parsed["raw_text"],
    )
    return True


def fetch_masn_orioles_articles(db: DatabaseManager, limit: int = _DEFAULT_LIMIT) -> int:
    """Scrape the Orioles MASN blog listing, store any articles not already saved.

    Returns the number of new articles stored.
    """
    abbrev_cache = build_mlb_team_abbrev_cache(db)
    team_id = abbrev_cache.get(_TEAM_ABBREV)

    existing = db.execute("SELECT url FROM mlb_beat_articles WHERE source = %s", (_SOURCE,))
    known_urls = {r["url"] for r in existing}

    listing_html = _get(_LISTING_URL)
    urls = _parse_listing(listing_html)[:limit]
    new_urls = [u for u in urls if u not in known_urls]

    if not new_urls:
        print(f"MLB beat articles ({_SOURCE}): no new articles (checked {len(urls)}, all already stored)")
        return 0

    stored = 0
    for url in new_urls:
        time.sleep(1)  # be polite to a small news site, not just robots.txt-compliant
        if _fetch_and_store_article(db, url, team_id):
            stored += 1

    print(f"MLB beat articles ({_SOURCE}): stored {stored} new article(s) of {len(new_urls)} candidates")
    return stored


def backfill_from_sitemap(db: DatabaseManager, max_fetch: int = 60) -> int:
    """One-time historical backfill via the site's own post-sitemap43.xml.

    The listing page's pagination is broken (verified 404 on /page/2/), so
    this is the only practical way to reach Phase 0's ~30-50 article target
    without waiting weeks for organic day-by-day accumulation. The sitemap
    is site-wide (all MASN authors, not just Kubatko), so every candidate is
    fetched and its actual byline checked before storing -- see
    _fetch_and_store_article(require_author=...).
    """
    abbrev_cache = build_mlb_team_abbrev_cache(db)
    team_id = abbrev_cache.get(_TEAM_ABBREV)

    existing = db.execute("SELECT url FROM mlb_beat_articles WHERE source = %s", (_SOURCE,))
    known_urls = {r["url"] for r in existing}

    candidates = [u for u, _ in _get_sitemap_urls(_SITEMAP_URL) if u not in known_urls]
    candidates = candidates[:max_fetch]

    stored = 0
    checked = 0
    for url in candidates:
        time.sleep(1)
        checked += 1
        if _fetch_and_store_article(db, url, team_id, require_author=_AUTHOR_NAME):
            stored += 1

    print(f"MLB beat articles ({_SOURCE}) sitemap backfill: stored {stored} of {checked} "
          f"candidates checked ({len(candidates)} available, capped at max_fetch={max_fetch})")
    return stored


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Scrape MASN Orioles beat-writer articles")
    parser.add_argument("--limit", type=int, default=_DEFAULT_LIMIT,
                         help="Max articles to check from the listing page (default 20)")
    parser.add_argument("--backfill-sitemap", action="store_true",
                         help="One-time historical backfill via post-sitemap43.xml")
    parser.add_argument("--max-fetch", type=int, default=60,
                         help="Max candidate pages to download during sitemap backfill (default 60)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)

    if args.backfill_sitemap:
        backfill_from_sitemap(db, max_fetch=args.max_fetch)
    else:
        fetch_masn_orioles_articles(db, limit=args.limit)
