"""Scrape new videos from a betting-picks YouTube channel for the picks
tracking pipeline (CLAUDE.md, "YouTube Picks Channel Tracking").

New-video detection uses the channel's free, no-API-key RSS feed
(youtube.com/feeds/videos.xml?channel_id=...) -- a stable, documented,
intentionally-public mechanism, unlike the transcript fetch below. Verified
live before building: returns exact video IDs, titles, and publish
timestamps with no auth.

Transcript fetch reuses youtube_transcript_api (same technique documented
in web/src/lib/youtube-transcript.ts -- innertube ANDROID client, avoids
the web client's PO-token requirement). YouTube blocks transcript requests
from datacenter AND most single proxy IPs (RequestBlocked/IpBlocked, the
"cloud IP" blocking class also hit with FanGraphs/stats.nba.com). The fix
is a ROTATING residential proxy -- a single fixed exit IP gets flagged
(verified 0/8 on a fixed Webshare IP vs 8/8 on the rotating backbone).
Preferred config: Webshare rotating-residential via WEBSHARE_PROXY_USERNAME
/WEBSHARE_PROXY_PASSWORD (WebshareProxyConfig rotates + auto-retries on
block); falls back to a plain YOUTUBE_PROXY_URL. See _get_proxy_config.

Usage:
    python -m ingest.youtube_picks_videos                  # scrape + store new videos
    python -m ingest.youtube_picks_videos --limit 5
"""

from __future__ import annotations

import argparse
import logging
import os
import time
import xml.etree.ElementTree as ET

import requests
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import CouldNotRetrieveTranscript, IpBlocked, RequestBlocked
from youtube_transcript_api.proxies import GenericProxyConfig, WebshareProxyConfig

from config import load_config
from db.database import DatabaseManager
from db.queries import get_active_youtube_pick_channels, upsert_youtube_pick_channel, upsert_youtube_pick_video

logger = logging.getLogger(__name__)

_CHANNEL_ID = "UC8hVLL1dC1NjEtL1208U--g"
_CHANNEL_NAME = "BettingPros"
_RSS_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "yt": "http://www.youtube.com/xml/schemas/2015",
}
_DEFAULT_LIMIT = 15
# Don't ingest videos published before this date. The RSS feed only returns
# the ~15 most recent uploads per channel, so we can't go back far anyway;
# this floor keeps a newly-added channel from pulling in stale old videos
# (games long finished) that happen to be in its recent feed.
_EARLIEST_PUBLISH = "2026-06-01"


def _get_proxy_config():
    """Prefer Webshare's rotating-residential config when creds are set --
    it uses the rotating backbone AND auto-retries through fresh residential
    IPs on an IpBlocked, which is what YouTube's blocking actually requires
    (a single fixed proxy IP gets flagged; verified 0/8 vs 8/8 for the
    rotating backbone). Falls back to a plain YOUTUBE_PROXY_URL, then none.

    Webshare setup: pass the ACCOUNT username (e.g. 'eewatahk'), NOT a
    per-endpoint 'username-us-1' -- WebshareProxyConfig appends '-rotate' and
    targets p.webshare.io itself."""
    ws_user = os.environ.get("WEBSHARE_PROXY_USERNAME")
    ws_pass = os.environ.get("WEBSHARE_PROXY_PASSWORD")
    if ws_user and ws_pass:
        return WebshareProxyConfig(proxy_username=ws_user, proxy_password=ws_pass)
    proxy_url = os.environ.get("YOUTUBE_PROXY_URL", "")
    if proxy_url:
        return GenericProxyConfig(http_url=proxy_url, https_url=proxy_url)
    return None


def _fetch_rss_entries(channel_id: str) -> list[dict]:
    """Return [{"video_id", "title", "published_at"}, ...], most recent first."""
    resp = requests.get(
        f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=20,
    )
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    entries = []
    for entry in root.findall("atom:entry", _RSS_NS):
        video_id_el = entry.find("yt:videoId", _RSS_NS)
        title_el = entry.find("atom:title", _RSS_NS)
        published_el = entry.find("atom:published", _RSS_NS)
        if video_id_el is None or title_el is None:
            continue
        entries.append({
            "video_id": video_id_el.text,
            "title": title_el.text,
            "published_at": published_el.text if published_el is not None else None,
        })
    return entries


def _fetch_transcript_text(video_id: str, attempts: int = 3) -> str:
    """Fetch a transcript, retrying transient network failures. YouTube
    (even through the residential proxy) intermittently resets the
    connection mid-fetch (SSLError / ConnectionError) -- a short retry with
    backoff clears most of those without failing the whole run."""
    api = YouTubeTranscriptApi(proxy_config=_get_proxy_config())
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            result = api.fetch(video_id)
            return " ".join(s.text for s in result)
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt < attempts - 1:
                time.sleep(2 * (attempt + 1))
    raise last_exc  # type: ignore[misc]


def _published_before_cutoff(published_at: str | None) -> bool:
    """True if the video predates _EARLIEST_PUBLISH. published_at is an ISO
    string (e.g. '2026-07-05T11:47:15+00:00'); a lexical compare of the
    date prefix is correct for ISO. Unknown dates are kept (not skipped)."""
    return bool(published_at) and published_at[:10] < _EARLIEST_PUBLISH


def fetch_new_pick_videos(
    db: DatabaseManager,
    channel_id: str = _CHANNEL_ID,
    channel_name: str = _CHANNEL_NAME,
    limit: int = _DEFAULT_LIMIT,
) -> dict:
    """Check the channel's RSS feed, fetch transcripts for any video not
    already stored and published on/after _EARLIEST_PUBLISH. Returns a stats
    dict {stored, ip_blocked, other_failed, candidates}."""
    existing = db.execute(
        "SELECT video_id FROM youtube_pick_videos WHERE channel_id = %s", (channel_id,)
    )
    known_ids = {r["video_id"] for r in existing}

    entries = _fetch_rss_entries(channel_id)[:limit]
    new_entries = [
        e for e in entries
        if e["video_id"] not in known_ids and not _published_before_cutoff(e["published_at"])
    ]

    stats = {"stored": 0, "ip_blocked": 0, "other_failed": 0, "candidates": len(new_entries)}
    if not new_entries:
        print(f"YouTube picks ({channel_name}): no new videos since {_EARLIEST_PUBLISH} (checked {len(entries)})")
        return stats

    for e in new_entries:
        time.sleep(1)
        try:
            transcript_text = _fetch_transcript_text(e["video_id"])
        except (IpBlocked, RequestBlocked) as exc:
            # YouTube blocking the request IP (the proxy is flagged/rotated,
            # or missing) -- track separately so the run can surface it.
            stats["ip_blocked"] += 1
            logger.warning("IP-blocked on %s (%r): %s", e["video_id"], e["title"], exc)
            continue
        except (CouldNotRetrieveTranscript, requests.exceptions.RequestException) as exc:
            # A single video's transcript failing (no captions, or a
            # transient proxy/network reset) must not crash the whole run --
            # the extraction + settlement steps still need to execute, and a
            # skipped video reappears as "new" on the next scheduled run.
            stats["other_failed"] += 1
            logger.warning("Skipping %s (%r): %s", e["video_id"], e["title"], exc)
            continue

        upsert_youtube_pick_video(
            db,
            channel_id=channel_id,
            channel_name=channel_name,
            video_id=e["video_id"],
            title=e["title"],
            published_at=e["published_at"],
            transcript_text=transcript_text,
        )
        stats["stored"] += 1

    print(f"YouTube picks ({channel_name}): stored {stats['stored']} of {len(new_entries)} "
          f"({stats['ip_blocked']} IP-blocked, {stats['other_failed']} other failure(s))")
    return stats


def _seed_default_channel_if_empty(db: DatabaseManager) -> None:
    """One-time backfill: register BettingPros in youtube_pick_channels if
    no channel has been added yet, so the pipeline keeps working exactly as
    before for anyone who already had it running against the hardcoded
    channel. New channels going forward are added via the web UI."""
    existing = db.execute("SELECT id FROM youtube_pick_channels LIMIT 1")
    if not existing:
        upsert_youtube_pick_channel(db, channel_id=_CHANNEL_ID, channel_name=_CHANNEL_NAME, handle="@bettingpros")


def _notify(text: str) -> None:
    """Best-effort push to Telegram/Discord if configured (same env vars as
    model/line_alerts.py). No-op if neither is set -- the log/GHA annotation
    is the always-on signal."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    discord = os.environ.get("DISCORD_WEBHOOK_URL")
    if token and chat_id:
        try:
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": chat_id, "text": text}, timeout=10)
        except requests.RequestException as exc:
            logger.warning("Telegram notify failed: %s", exc)
    if discord:
        try:
            requests.post(discord, json={"content": text}, timeout=10)
        except requests.RequestException as exc:
            logger.warning("Discord notify failed: %s", exc)


def _report_scrape_health(totals: dict, n_channels: int) -> None:
    """Print a summary and, if YouTube blocked any transcript fetch, surface
    it: a GitHub Actions annotation (visible on the run page without opening
    logs) plus an optional Telegram/Discord push. `error` level (vs
    `warning`) when nothing got through despite having candidates -- the
    proxy is likely fully down/flagged."""
    print(f"YouTube picks scrape: {totals['stored']} stored, {totals['ip_blocked']} IP-blocked, "
          f"{totals['other_failed']} other failure(s) across {n_channels} channel(s)")
    if totals["ip_blocked"] == 0:
        return
    hard_down = totals["stored"] == 0 and totals["candidates"] > 0
    msg = (f"YouTube proxy/IP blocking: {totals['ip_blocked']} transcript fetch(es) blocked by "
           f"YouTube (stored {totals['stored']} of {totals['candidates']} candidates). "
           f"Check YOUTUBE_PROXY_URL -- the proxy IP may be flagged, rotated, or missing.")
    # GitHub Actions annotation on the run summary page (::error:: / ::warning::)
    print(f"::{'error' if hard_down else 'warning'}::{msg}")
    _notify(("🔴 " if hard_down else "🟠 ") + msg)


def fetch_new_videos_for_all_channels(db: DatabaseManager, limit: int = _DEFAULT_LIMIT) -> int:
    """Run fetch_new_pick_videos() for every active channel in
    youtube_pick_channels. Channels are added via the web UI's "Add
    Channel" action; this just scrapes whatever's currently active."""
    _seed_default_channel_if_empty(db)
    channels = get_active_youtube_pick_channels(db)
    if not channels:
        print("YouTube picks: no active channels registered")
        return 0

    totals = {"stored": 0, "ip_blocked": 0, "other_failed": 0, "candidates": 0}
    for c in channels:
        s = fetch_new_pick_videos(db, channel_id=c["channel_id"], channel_name=c["channel_name"], limit=limit)
        for k in totals:
            totals[k] += s[k]
    _report_scrape_health(totals, len(channels))
    return totals["stored"]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Scrape new videos from tracked betting-picks YouTube channels")
    parser.add_argument("--limit", type=int, default=_DEFAULT_LIMIT,
                         help="Max recent videos to check per channel from the RSS feed (default 15)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    fetch_new_videos_for_all_channels(db, limit=args.limit)
