"""Typed official-news availability claims for NBA review (`nba-injury-news-v1`).

Section B of docs/nba-points-opportunity-reliability-spec.md. Review evidence
only: a claim never changes eligibility, an availability field, a forecast or a
probability, and no teammate usage adjustment follows from one.

A claim is recorded by hand from an official NBA or team article (nba.com and
its team pages). The original article is retrieved and its raw bytes kept; the
supporting passage must appear in the article text. Unknown timing stays
unknown: no invented timezone, publication time, return date or absence end.

Claim types:
  reported_status               a designation (out / doubtful / questionable / probable / available)
                                for one explicit game date; questionable is a status, not a discount
  reported_unavailable_interval an absence with a start date and a MINIMUM duration
                                ("at least a week"); never an end or return date
  reported_expected_return      a dated expectation of return; NOT confirmed availability

    python -m research.nba_points_injury_news add --url URL --player "Name" --team TOR \\
        --type reported_unavailable_interval --start 2026-10-28 --min-days 7 --passage "exact sentence"
    python -m research.nba_points_injury_news review --date 2026-10-31
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
import hashlib
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from model.nba_points_ladder import player_key, team_key

VERSION = "nba-injury-news-v1"
CLAIM_TYPES = ("reported_status", "reported_unavailable_interval", "reported_expected_return")
STATUSES = ("out", "doubtful", "questionable", "probable", "available")
MAX_ARTICLES_PER_RUN = 8
ARCHIVE = Path("artifacts/nba-injury-news")


def official_url(url: str) -> bool:
    p = urlparse(url)
    host = (p.hostname or "").lower()
    return p.scheme == "https" and (host == "nba.com" or host.endswith(".nba.com"))


class _Text(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "noscript"):
            self._skip += 1

    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript") and self._skip:
            self._skip -= 1

    def handle_data(self, data):
        if not self._skip:
            self.parts.append(data)


def article_text(raw: bytes) -> str:
    p = _Text()
    p.feed(raw.decode("utf-8", errors="replace"))
    return " ".join(" ".join(p.parts).split())


@dataclass(frozen=True)
class Claim:
    claim_type: str
    player: str
    team: str
    source_url: str
    content_sha256: str
    passage: str
    observed_at: str
    title: str | None = None
    published_at: str | None = None  # only when the article states it, with its own offset
    game_date: str | None = None  # reported_status
    start_date: str | None = None  # reported_unavailable_interval / reported_expected_return
    min_days: int | None = None  # reported_unavailable_interval
    expected_date: str | None = None  # reported_expected_return
    status: str | None = None  # reported_status
    verification: str = "passage_verified"
    version: str = VERSION
    player_key: str = field(init=False)
    team_key: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "player_key", player_key(self.player))
        object.__setattr__(self, "team_key", team_key(self.team))
        if self.status:
            object.__setattr__(self, "status", self.status.lower())


def validate(c: Claim) -> list[str]:
    errs = []
    if c.claim_type not in CLAIM_TYPES:
        errs.append(f"unknown claim type {c.claim_type}")
    if not official_url(c.source_url):
        errs.append("source must be an https nba.com article")
    if not c.passage.strip():
        errs.append("supporting passage required")
    obs = datetime.fromisoformat(c.observed_at)
    if obs.tzinfo is None:
        errs.append("observed_at needs a timezone")
    if c.published_at is not None:
        pub = datetime.fromisoformat(c.published_at)
        if pub.tzinfo is None:
            errs.append("published_at must carry the article's own offset, or be left unknown")
        elif obs.tzinfo is not None and pub > obs:
            errs.append("published after it was observed")
    if c.claim_type == "reported_status" and (not c.game_date or c.status not in STATUSES):
        errs.append("reported_status needs --game-date and one status word in the passage via --status")
    if c.claim_type == "reported_unavailable_interval":
        if not c.start_date or c.min_days is None or c.min_days < 1:
            errs.append("an interval needs --start and a positive --min-days (a minimum, never an end)")
        if c.expected_date:
            errs.append("an interval cannot carry an end or return date")
    if c.claim_type == "reported_expected_return" and not c.expected_date:
        errs.append("reported_expected_return needs --expected-date")
    return errs


def make_claim(**kw: Any) -> Claim:
    c = Claim(**kw)
    errs = validate(c)
    if errs:
        raise ValueError("; ".join(errs))
    return c


def claim_record(c: Claim) -> dict[str, Any]:
    d = asdict(c)
    d["sha256"] = hashlib.sha256(json.dumps(d, sort_keys=True).encode()).hexdigest()
    return d


def applies(rec: dict[str, Any], day: date) -> tuple[bool, str]:
    """Whether a stored claim may appear in the review for `day`, and in what role."""
    obs = datetime.fromisoformat(rec["observed_at"]).astimezone(timezone.utc).date()
    if obs > day:
        return False, "observed_after_review_date"
    t = rec["claim_type"]
    if t == "reported_status":
        return (rec["game_date"] == day.isoformat(), "status_for_this_date" if rec["game_date"] == day.isoformat() else "other_date")
    if t == "reported_unavailable_interval":
        start = date.fromisoformat(rec["start_date"])
        if day < start:
            return False, "before_interval"
        if day < start + timedelta(days=int(rec["min_days"])):
            return True, "inside_minimum_interval"
        return False, "past_minimum_interval_historical_only"
    if t == "reported_expected_return":
        return True, "expected_return_not_confirmed"
    return False, "unknown_type"


def review(records: list[dict[str, Any]], day: date) -> dict[str, Any]:
    """Applicable claims per player for one date; disagreements stay visible and are never resolved by omission."""
    by_player: dict[str, list[dict[str, Any]]] = {}
    for r in records:
        ok, role = applies(r, day)
        if ok:
            by_player.setdefault(r["player_key"], []).append({**r, "role": role})
    out = {}
    for key, claims in sorted(by_player.items()):
        teams = {c["team_key"] for c in claims}
        unavailable = any(c["role"] == "inside_minimum_interval" or c.get("status") in ("out", "doubtful") for c in claims)
        available = any(c.get("status") in ("available", "probable") for c in claims)
        out[key] = {"claims": claims,
                    "review": "conflicting_claims" if (unavailable and available) or len(teams) > 1 else "claims_present",
                    "teams": sorted(teams)}
    return {"version": VERSION, "date": day.isoformat(), "players": out,
            "note": "Review evidence only. No entry means unknown; nothing here changes eligibility or a forecast."}


def load(root: Path = ARCHIVE) -> list[dict[str, Any]]:
    recs = []
    for p in sorted(root.rglob("claim.json")) if root.exists() else []:
        r = json.loads(p.read_text(encoding="utf-8"))
        raw = p.with_name("article.html").read_bytes()
        if hashlib.sha256(raw).hexdigest() != r["content_sha256"]:
            raise ValueError(f"archived article changed: {p.parent}")
        recs.append(r)
    return recs


def store(c: Claim, raw: bytes, root: Path = ARCHIVE) -> Path:
    rec = claim_record(c)
    folder = root / f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S%fZ}-{rec['sha256'][:12]}"
    folder.mkdir(parents=True, exist_ok=False)
    (folder / "article.html").write_bytes(raw)
    (folder / "claim.json").write_text(json.dumps(rec, indent=2, sort_keys=True), encoding="utf-8")
    return folder


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("add")
    for flag in ("--url", "--player", "--team", "--type", "--passage"):
        a.add_argument(flag, required=True)
    for flag in ("--title", "--published-at", "--game-date", "--start", "--expected-date", "--status"):
        a.add_argument(flag)
    a.add_argument("--min-days", type=int)
    a.add_argument("--root", type=Path, default=ARCHIVE)
    r = sub.add_parser("review")
    r.add_argument("--date", type=date.fromisoformat, required=True)
    r.add_argument("--root", type=Path, default=ARCHIVE)
    args = ap.parse_args()
    if args.cmd == "review":
        print(json.dumps(review(load(args.root), args.date), indent=2))
        return 0
    if not official_url(args.url):
        ap.error("only https nba.com articles are accepted")
    import requests

    resp = requests.get(args.url, timeout=30, headers={"User-Agent": "nbadfs-research/1.0"})
    resp.raise_for_status()
    raw = resp.content
    observed = datetime.now(timezone.utc).isoformat()
    if " ".join(args.passage.split()) not in article_text(raw):
        raise SystemExit("passage not found in the retrieved article; nothing stored")
    c = make_claim(claim_type=args.type, player=args.player, team=args.team, source_url=args.url,
                   content_sha256=hashlib.sha256(raw).hexdigest(), passage=args.passage, observed_at=observed,
                   title=args.title, published_at=args.published_at, game_date=args.game_date, start_date=args.start,
                   min_days=args.min_days, expected_date=args.expected_date, status=args.status)
    print(store(c, raw, args.root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
