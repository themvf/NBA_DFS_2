"""DeepSeek structured pick extraction for the YouTube Picks Channel
Tracking pipeline (CLAUDE.md, "YouTube Picks Channel Tracking").

Same discipline as model/mlb_beat_extraction.py: the LLM extracts
structured picks only, never judges whether a pick is good -- settlement
against real outcomes (not yet built) is a separate, later, numeric phase.
Every extracted pick must carry a "quote" that is a verbatim substring of
the transcript; anything that fails that check is discarded before it
reaches the database.

Transcripts here are auto-generated ASR captions (no punctuation,
run-on), messier than the beat-writer pilot's clean written prose --
the prompt tells the model to copy quotes exactly as they appear, not to
"clean up" the transcript text, so the grounding check still works.

Usage:
    python -m model.youtube_picks_extraction                 # process all pending videos
    python -m model.youtube_picks_extraction --limit 5
    python -m model.youtube_picks_extraction --report         # print videos + extracted picks
"""

from __future__ import annotations

import argparse
import json
import logging
import time

import httpx

from config import load_config
from db.database import DatabaseManager
from db.queries import (
    get_youtube_pick_videos_without_picks,
    insert_youtube_pick,
    mark_youtube_pick_video_extracted_empty,
)

logger = logging.getLogger(__name__)

MODEL_VERSION = "youtube-picks-deepseek-v1"

_SPORTS = ("nba", "mlb", "nfl", "nhl", "wnba", "soccer", "tennis", "f1", "other")
_BET_TYPES = ("moneyline", "spread", "total", "prop", "futures", "other")

_SYSTEM_PROMPT = """You are a betting-pick extraction tool for a sports-betting YouTube video \
transcript. You do NOT judge whether a pick is good or bad -- your only job is to identify \
every specific, gradable betting pick the speaker actually makes.

The transcript is auto-generated speech-to-text: no punctuation, run-on sentences, and \
occasional misheard words. Read it carefully anyway -- do not skip picks because the text \
around them is messy.

For each specific pick made (a concrete side the speaker recommends betting on):
- sport: one of nba, mlb, nfl, nhl, wnba, soccer, tennis, f1, other
- bet_type: one of moneyline, spread, total, prop, futures, other
- subject: the team or player the pick is about
- opponent: the opposing team, if a specific game is mentioned, else null
- selection: the actual side picked, in your own words (e.g. "Yankees moneyline", \
  "Over 8.5 runs", "Messi to score anytime", "Chiefs to win Super Bowl")
- odds_american: the American odds stated for this pick as an integer (e.g. -149, +217), or \
  null if no specific odds/price is mentioned for it
- game_context: any date/matchup detail mentioned (e.g. "tonight", "Sunday's game vs Boston"), \
  in your own words, or null if not stated
- confidence_label: any stated confidence descriptor exactly as implied (e.g. "best bet", \
  "lock", "lean", "longshot play"), or null if none stated
- quote: an exact, verbatim substring copied directly from the transcript that supports this \
  pick -- copy the messy transcript text exactly as it appears, do not fix grammar or \
  punctuation. This is checked programmatically; a quote that doesn't literally appear in the \
  transcript is discarded.

Only extract SPECIFIC, gradable picks -- a side, team, or player the speaker is recommending a \
bet on. Do not extract general discussion, injury news, or commentary that isn't itself a bet \
recommendation. If the video makes no specific picks, return an empty list.

Respond with ONLY a JSON object in this exact shape, no other text:
{"picks": [{"sport": "...", "bet_type": "...", "subject": "...", "opponent": "..."|null, \
"selection": "...", "odds_american": -149|null, "game_context": "..."|null, \
"confidence_label": "..."|null, "quote": "..."}]}
"""


def _call_deepseek(cfg, transcript_text: str) -> dict:
    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": cfg.model,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": f"Transcript:\n\n{transcript_text}"},
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
    }

    delay = cfg.retry_backoff_seconds
    last_exc: Exception | None = None
    for attempt in range(1, cfg.max_retries + 1):
        try:
            with httpx.Client(timeout=cfg.timeout_seconds) as client:
                resp = client.post(cfg.base_url, headers=headers, json=payload)
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"]
                return json.loads(content)
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            last_exc = exc
            logger.warning("DeepSeek call: network error on attempt %d/%d (%s). Retrying in %.0fs...",
                            attempt, cfg.max_retries, exc, delay)
        except httpx.HTTPStatusError as exc:
            last_exc = exc
            status = exc.response.status_code
            if status not in (429, 500, 502, 503, 504):
                raise
            logger.warning("DeepSeek call: HTTP %s on attempt %d/%d. Retrying in %.0fs...",
                            status, attempt, cfg.max_retries, delay)
        except (KeyError, json.JSONDecodeError) as exc:
            logger.warning("DeepSeek call: malformed response, treating as zero picks (%s)", exc)
            return {"picks": []}
        time.sleep(delay)
        delay *= 2
    raise RuntimeError(f"DeepSeek call failed after {cfg.max_retries} attempts") from last_exc


def _validate_picks(raw_picks: list, transcript_text: str) -> list[dict]:
    """Keep only picks with a valid sport/bet_type and a quote that is a
    real, verbatim substring of the transcript -- the grounding check
    against hallucination. See module docstring."""
    valid = []
    for p in raw_picks:
        if not isinstance(p, dict):
            continue
        sport = p.get("sport")
        bet_type = p.get("bet_type")
        quote = p.get("quote", "")
        subject = p.get("subject")
        selection = p.get("selection")
        if sport not in _SPORTS or bet_type not in _BET_TYPES:
            logger.debug("Discarding pick with unknown sport/bet_type: %r/%r", sport, bet_type)
            continue
        if not quote or quote not in transcript_text:
            logger.debug("Discarding pick with ungrounded quote: %r", quote)
            continue
        if not subject or not selection:
            logger.debug("Discarding pick missing subject/selection")
            continue
        odds_american = p.get("odds_american")
        try:
            odds_american = int(odds_american) if odds_american is not None else None
        except (TypeError, ValueError):
            odds_american = None
        valid.append({
            "sport": sport,
            "bet_type": bet_type,
            "subject": subject,
            "opponent": p.get("opponent") or None,
            "selection": selection,
            "odds_american": odds_american,
            "game_context": p.get("game_context") or None,
            "confidence_label": p.get("confidence_label") or None,
            "quote": quote,
        })
    return valid


def extract_picks_for_pending_videos(db: DatabaseManager, limit: int | None = None) -> dict:
    cfg = load_config().deepseek_api
    if not cfg.api_key:
        raise RuntimeError("DEEPSEEK_API_KEY not set -- see .env")

    videos = get_youtube_pick_videos_without_picks(db, MODEL_VERSION)
    if limit is not None:
        videos = videos[:limit]

    n_videos = 0
    n_picks = 0
    n_discarded = 0
    for v in videos:
        if not v["transcript_text"]:
            continue
        result = _call_deepseek(cfg, v["transcript_text"])
        raw_picks = result.get("picks", []) if isinstance(result, dict) else []
        valid_picks = _validate_picks(raw_picks, v["transcript_text"])
        n_discarded += len(raw_picks) - len(valid_picks)

        if valid_picks:
            for p in valid_picks:
                insert_youtube_pick(
                    db,
                    video_id=v["id"],
                    sport=p["sport"],
                    bet_type=p["bet_type"],
                    subject=p["subject"],
                    opponent=p["opponent"],
                    selection=p["selection"],
                    odds_american=p["odds_american"],
                    game_context=p["game_context"],
                    confidence_label=p["confidence_label"],
                    quote=p["quote"],
                    model_version=MODEL_VERSION,
                )
            n_picks += len(valid_picks)
        else:
            mark_youtube_pick_video_extracted_empty(db, v["id"], MODEL_VERSION)
        n_videos += 1

    print(f"YouTube picks extraction ({MODEL_VERSION}): {n_videos} videos processed, "
          f"{n_picks} picks stored, {n_discarded} ungrounded picks discarded")
    return {"videos": n_videos, "picks": n_picks, "discarded": n_discarded}


def print_report(db: DatabaseManager) -> None:
    rows = db.execute(
        """
        SELECT v.id, v.title, v.published_at,
               p.sport, p.bet_type, p.subject, p.opponent, p.selection,
               p.odds_american, p.game_context, p.confidence_label, p.quote
        FROM youtube_pick_videos v
        LEFT JOIN youtube_picks p
               ON p.video_id = v.id AND p.model_version = %s AND p.sport != '_none'
        ORDER BY v.published_at DESC, p.id ASC
        """,
        (MODEL_VERSION,),
    )
    current_id = None
    for r in rows:
        if r["id"] != current_id:
            current_id = r["id"]
            print(f"\n[{r['published_at']}] video {r['id']}")
        if r["sport"]:
            conf = f" ({r['confidence_label']})" if r["confidence_label"] else ""
            odds = f" @ {r['odds_american']:+d}" if r["odds_american"] is not None else ""
            print(f"  -> [{r['sport']}/{r['bet_type']}] {r['subject']} vs {r['opponent']}: "
                  f"{r['selection']}{odds}{conf}")
            print(f"     context: {r['game_context']}")
            print(f"     quote: \"{r['quote']}\"")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="DeepSeek YouTube picks extraction")
    parser.add_argument("--limit", type=int, default=None, help="Max videos to process")
    parser.add_argument("--report", action="store_true", help="Print videos + extracted picks")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)

    if args.report:
        print_report(db)
    else:
        extract_picks_for_pending_videos(db, limit=args.limit)
