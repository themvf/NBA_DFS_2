"""Scrape new videos from a betting-picks YouTube channel for the picks
tracking pipeline (CLAUDE.md, "YouTube Picks Channel Tracking").

New-video detection uses the channel's free, no-API-key RSS feed
(youtube.com/feeds/videos.xml?channel_id=...) -- a stable, documented,
intentionally-public mechanism, unlike the transcript fetch below. Verified
live before building: returns exact video IDs, titles, and publish
timestamps with no auth.

Transcript fetch reuses youtube_transcript_api (same technique documented
in web/src/lib/youtube-transcript.ts -- innertube ANDROID client, avoids
the web client's PO-token requirement). Routed through YOUTUBE_PROXY_URL
if set: a live test against this exact channel showed YouTube blocks
transcript requests from this dev sandbox's IP after a handful of calls
(RequestBlocked, the same "cloud/datacenter IP" blocking class already hit
with FanGraphs/stats.nba.com elsewhere in this project) -- a residential
proxy fixed it, verified against the same video that had just failed.

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
from youtube_transcript_api._errors import CouldNotRetrieveTranscript
from youtube_transcript_api.proxies import GenericProxyConfig

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


def _get_proxy_config() -> GenericProxyConfig | None:
    proxy_url = os.environ.get("YOUTUBE_PROXY_URL", "")
    if not proxy_url:
        return None
    return GenericProxyConfig(http_url=proxy_url, https_url=proxy_url)


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


def fetch_new_pick_videos(
    db: DatabaseManager,
    channel_id: str = _CHANNEL_ID,
    channel_name: str = _CHANNEL_NAME,
    limit: int = _DEFAULT_LIMIT,
) -> int:
    """Check the channel's RSS feed, fetch transcripts for any video not
    already stored. Returns the number of new videos stored."""
    existing = db.execute(
        "SELECT video_id FROM youtube_pick_videos WHERE channel_id = %s", (channel_id,)
    )
    known_ids = {r["video_id"] for r in existing}

    entries = _fetch_rss_entries(channel_id)[:limit]
    new_entries = [e for e in entries if e["video_id"] not in known_ids]

    if not new_entries:
        print(f"YouTube picks ({channel_name}): no new videos (checked {len(entries)}, all already stored)")
        return 0

    stored = 0
    for e in new_entries:
        time.sleep(1)
        try:
            transcript_text = _fetch_transcript_text(e["video_id"])
        except (CouldNotRetrieveTranscript, requests.exceptions.RequestException) as exc:
            # A single video's transcript failing (no captions, or a
            # transient proxy/network reset) must not crash the whole run --
            # the extraction + settlement steps still need to execute, and a
            # skipped video reappears as "new" on the next scheduled run.
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
        stored += 1

    print(f"YouTube picks ({channel_name}): stored {stored} new video(s) of {len(new_entries)} candidates")
    return stored


def _seed_default_channel_if_empty(db: DatabaseManager) -> None:
    """One-time backfill: register BettingPros in youtube_pick_channels if
    no channel has been added yet, so the pipeline keeps working exactly as
    before for anyone who already had it running against the hardcoded
    channel. New channels going forward are added via the web UI."""
    existing = db.execute("SELECT id FROM youtube_pick_channels LIMIT 1")
    if not existing:
        upsert_youtube_pick_channel(db, channel_id=_CHANNEL_ID, channel_name=_CHANNEL_NAME, handle="@bettingpros")


def fetch_new_videos_for_all_channels(db: DatabaseManager, limit: int = _DEFAULT_LIMIT) -> int:
    """Run fetch_new_pick_videos() for every active channel in
    youtube_pick_channels. Channels are added via the web UI's "Add
    Channel" action; this just scrapes whatever's currently active."""
    _seed_default_channel_if_empty(db)
    channels = get_active_youtube_pick_channels(db)
    if not channels:
        print("YouTube picks: no active channels registered")
        return 0

    total = 0
    for c in channels:
        total += fetch_new_pick_videos(db, channel_id=c["channel_id"], channel_name=c["channel_name"], limit=limit)
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Scrape new videos from tracked betting-picks YouTube channels")
    parser.add_argument("--limit", type=int, default=_DEFAULT_LIMIT,
                         help="Max recent videos to check per channel from the RSS feed (default 15)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    fetch_new_videos_for_all_channels(db, limit=args.limit)
