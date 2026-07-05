"""DeepSeek structured-fact extraction for the MLB Beat-Writer
Information-Latency Pilot (CLAUDE.md, 2026-07-05) -- Phase 0.

The model's ONLY job is extraction, never edge-judgment: does this article
report one of a fixed, pre-registered set of facts, and what exact text
supports it. Whether any of this predicts market movement is a separate,
later, numeric phase (model/mlb_beat_timing_study.py, not yet built) -- this
file makes no claim about edge.

Hallucination control: every extracted fact must carry a "quote" that is a
verbatim substring of the source article. Any fact whose quote doesn't
literally appear in the article text is discarded before it reaches
mlb_beat_facts -- no human review needed for that specific failure mode.
Facts that survive the quote check may still be wrong in other ways (that's
what Phase 0's hand-labeled precision check is for); this is a floor, not a
full guarantee.

Usage:
    python -m model.mlb_beat_extraction                 # process all pending articles
    python -m model.mlb_beat_extraction --limit 5
    python -m model.mlb_beat_extraction --report         # print articles + extracted facts
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
    get_mlb_beat_articles_without_facts,
    insert_mlb_beat_fact,
    mark_mlb_beat_article_extracted_empty,
)

logger = logging.getLogger(__name__)

MODEL_VERSION = "beat-extract-deepseek-v1"

# Fixed candidate fact types -- pre-registered in CLAUDE.md. Do not add a
# fourth category here without a separately pre-registered spec update.
_FACT_TYPES = ("starter_change", "injury_status", "bullpen_availability")

_SYSTEM_PROMPT = """You are a fact-extraction tool for a baseball beat-writer article. \
You do NOT give opinions, predictions, or betting analysis. Your only job is to find \
whether the article reports any of exactly these three fact types:

1. starter_change -- a starting pitcher is confirmed, announced, or changed for an \
   upcoming game.
2. injury_status -- a named player's injury or IL status changes (placed on/activated \
   from IL, diagnosis, timeline, "day-to-day", ruled out, etc.).
3. bullpen_availability -- a note about which relievers are or are not available for \
   an upcoming game (e.g. unavailable after recent use, workload/fatigue note).

Rules:
- Only extract facts that are EXPLICITLY stated in the article text. Never infer, \
  guess, or use outside knowledge.
- For each fact found, "quote" MUST be an exact, verbatim substring copied directly \
  from the article -- do not paraphrase or alter it in any way. This is checked \
  programmatically; a quote that doesn't literally appear in the article is discarded.
- If the article contains none of these three fact types, return an empty facts list. \
  Most articles will have zero qualifying facts -- do not force a match.
- Respond with ONLY a JSON object in this exact shape, no other text:

{"facts": [{"fact_type": "starter_change|injury_status|bullpen_availability", \
"player_name": "string or null", "description": "one sentence, in your own words", \
"quote": "verbatim substring from the article"}]}
"""


def _call_deepseek(cfg, article_text: str) -> dict:
    """Call DeepSeek for one article, return the parsed JSON response."""
    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": cfg.model,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": f"Article text:\n\n{article_text}"},
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
            logger.warning("DeepSeek call: malformed response, treating as zero facts (%s)", exc)
            return {"facts": []}
        time.sleep(delay)
        delay *= 2
    raise RuntimeError(f"DeepSeek call failed after {cfg.max_retries} attempts") from last_exc


def _validate_facts(raw_facts: list, raw_text: str) -> list[dict]:
    """Keep only facts with a valid fact_type and a quote that is a real,
    verbatim substring of the source article. This is the grounding check
    against hallucination -- see module docstring."""
    valid = []
    for f in raw_facts:
        if not isinstance(f, dict):
            continue
        fact_type = f.get("fact_type")
        quote = f.get("quote", "")
        if fact_type not in _FACT_TYPES:
            logger.debug("Discarding fact with unknown fact_type: %r", fact_type)
            continue
        if not quote or quote not in raw_text:
            logger.debug("Discarding fact with ungrounded quote: %r", quote)
            continue
        valid.append({
            "fact_type": fact_type,
            "player_name": f.get("player_name") or None,
            "description": f.get("description") or "",
            "quote": quote,
        })
    return valid


def extract_facts_for_pending_articles(db: DatabaseManager, limit: int | None = None) -> dict:
    """Run DeepSeek extraction over every article not yet processed under
    MODEL_VERSION. Returns summary counts."""
    cfg = load_config().deepseek_api
    if not cfg.api_key:
        raise RuntimeError("DEEPSEEK_API_KEY not set -- see .env")

    articles = get_mlb_beat_articles_without_facts(db, MODEL_VERSION)
    if limit is not None:
        articles = articles[:limit]

    n_articles = 0
    n_facts = 0
    n_discarded = 0
    for a in articles:
        if not a["raw_text"]:
            continue
        result = _call_deepseek(cfg, a["raw_text"])
        raw_facts = result.get("facts", []) if isinstance(result, dict) else []
        valid_facts = _validate_facts(raw_facts, a["raw_text"])
        n_discarded += len(raw_facts) - len(valid_facts)

        if valid_facts:
            for f in valid_facts:
                insert_mlb_beat_fact(
                    db,
                    article_id=a["id"],
                    fact_type=f["fact_type"],
                    team_id=a["team_id"],
                    player_name=f["player_name"],
                    description=f["description"],
                    quote=f["quote"],
                    model_version=MODEL_VERSION,
                )
            n_facts += len(valid_facts)
        else:
            mark_mlb_beat_article_extracted_empty(db, a["id"], MODEL_VERSION)
        n_articles += 1

    print(f"MLB beat extraction ({MODEL_VERSION}): {n_articles} articles processed, "
          f"{n_facts} facts stored, {n_discarded} ungrounded facts discarded")
    return {"articles": n_articles, "facts": n_facts, "discarded": n_discarded}


def print_report(db: DatabaseManager) -> None:
    """Print every article and its extracted facts -- for the Phase 0
    hand-labeling precision/recall check."""
    rows = db.execute(
        """
        SELECT a.id, a.title, a.published_at, a.url,
               f.fact_type, f.player_name, f.description, f.quote
        FROM mlb_beat_articles a
        LEFT JOIN mlb_beat_facts f
               ON f.article_id = a.id AND f.model_version = %s AND f.fact_type != '_none'
        ORDER BY a.published_at DESC, f.id ASC
        """,
        (MODEL_VERSION,),
    )
    current_id = None
    for r in rows:
        if r["id"] != current_id:
            current_id = r["id"]
            print(f"\n[{r['published_at']}] {r['title']}")
            print(f"  {r['url']}")
        if r["fact_type"]:
            print(f"  -> {r['fact_type']} | {r['player_name']}: {r['description']}")
            print(f"     quote: \"{r['quote']}\"")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="DeepSeek beat-writer fact extraction")
    parser.add_argument("--limit", type=int, default=None, help="Max articles to process")
    parser.add_argument("--report", action="store_true", help="Print articles + extracted facts")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)

    if args.report:
        print_report(db)
    else:
        extract_facts_for_pending_articles(db, limit=args.limit)
