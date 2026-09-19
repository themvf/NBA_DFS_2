"""Capture a manually pasted DraftKings NFL specials board.

These markets ("first touchdown scorer", "highest scoring game", "most
receiving yards" and friends) are **not on The Odds API**, and DK's own board
endpoint sits behind an authenticated session plus bot detection -- the same
wall documented for ``ingest/ff_dk_bestball_adp.py``. So the board arrives the
same way Yahoo's pre-draft list does: a human copies it and pastes it into a
file. ``captured_at`` is therefore a real wall clock, not a floored cadence.

P0 of the programme (``docs/nfl-slate-specials-handoff.md`` §7) is nothing but
the **overround** these captures reveal, collected for two Sundays before a
single line of simulator is written. A family whose board sums far above 100%
cannot have our number meaningfully compared against a fair price, and finding
that out costs two pastes rather than three weeks of modelling.

Usage:
    python -m ingest.nfl_specials_market --season 2026 --week 3 \
        --family first_td_scorer --scope sunday_1pm --file paste.txt

    # see the parse and the overround without writing anything
    python -m ingest.nfl_specials_market ... --dry-run

Two paste layouts are accepted, because DK renders differently depending on
where you copy from:

    Christian McCaffrey        |   Christian McCaffrey  +750
    +750                       |   Saquon Barkley       +800
    Saquon Barkley             |
    +800                       |

Design notes worth knowing before changing this file:

* **A capture always succeeds if the prices parse.** Market prices are
  perishable -- DK's board is gone in an hour and cannot be re-fetched -- while
  player identities can be resolved at any later date. So a label we cannot
  map to a player is stored with ``selection_key = 'UNRESOLVED:<label>'`` and
  counted loudly, rather than failing the capture and losing the price. §3.4.
* **A price inside (-100, 100) fails the whole capture**, not just its row.
  Such a number is not a moneyline, so the paste is malformed and the rest of
  it cannot be trusted either.
* **Idempotent on identical text**: ``capture_key`` is derived from the paste's
  content plus its (season, week, family, scope), so re-running is a no-op.
  The cost of that choice is real and accepted: pasting a genuinely unchanged
  board later records no second observation, so this tool measures overround,
  not quote persistence. Persistence needs a time-keyed capture and is not
  part of P0.
* Deliberately no re-resolution pass here. Back-filling ``selection_key`` on
  old captures once a projection run exists is a separate job; this module only
  ever writes new captures.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_season_schedule import TEAM_ABBREV_OVERRIDES
# Imported, not re-implemented: the odds conversions are shared with every
# other ledger in this repo, and a second copy is a second thing to get wrong.
from model.soccer_bet_rating import american_to_prob
from model.nfl_slate_specials import (
    FAMILIES,
    FAMILY_POSITIONS,
    SLATE_SCOPES,
    selection_kind,
)

CAPTURE_FORMAT = "dk-specials-paste-v1"

# A moneyline is never strictly between -100 and +100. We still *parse* such a
# token so the paste can be rejected with a specific complaint instead of the
# number being mistaken for part of a player's name.
MIN_ABS_AMERICAN = 100

# Below this rapidfuzz score a name is left UNRESOLVED rather than guessed.
# Deliberately high: a wrong player silently mis-settles a bet, whereas an
# unresolved one is visible in the report and costs nothing but a follow-up.
NAME_MATCH_FLOOR = 88

_ODDS_IN_LINE = re.compile(r"(?<![\w.])([+-]\s?\d{1,6})(?![\d.])")
_BARE_INT = re.compile(r"^\d{1,3}$")
_INITIAL_SURNAME = re.compile(r"^([A-Za-z])\.?\s+(.+)$")
_SPLIT_GAME = re.compile(r"\s*(?:@|\bat\b|\bvs\.?\b|\bv\.?\b)\s*", re.I)


# ── pure parsing ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ParsedRow:
    """One selection as DK printed it, before any identity resolution."""
    source_order: int
    label: str
    context: tuple[str, ...]
    american: int


def normalize_paste(text: str) -> str:
    """Fold the dash and space variants a browser copy introduces."""
    text = text.replace("−", "-").replace("–", "-").replace("—", "-")
    text = text.replace(" ", " ").replace(" ", " ").replace(" ", " ")
    return text


def _as_american(token: str) -> int | None:
    token = token.replace(",", "").replace(" ", "").strip()
    if token.upper() in {"EVEN", "EV"}:
        return 100
    if re.fullmatch(r"[+-]\d{1,6}", token):
        return int(token)
    return None


def parse_board_text(text: str) -> tuple[list[ParsedRow], list[str]]:
    """Parse either DK layout into rows, plus a list of complaints.

    Returns ``(rows, problems)``. A non-empty ``problems`` means the caller must
    not write: we would rather lose a paste than record a board we misread.
    """
    problems: list[str] = []
    rows: list[ParsedRow] = []
    pending: list[str] = []

    for raw_line in normalize_paste(text).splitlines():
        line = raw_line.strip().strip("·").strip()
        if not line:
            continue
        # DK numbers some boards. A bare unsigned integer is decorative; a
        # price always carries an explicit sign.
        if _BARE_INT.fullmatch(line):
            continue

        whole_line = _as_american(line)
        if whole_line is not None:
            if not pending:
                problems.append(f"price {line!r} with no preceding selection label")
                continue
            rows.append(ParsedRow(len(rows) + 1, pending[0], tuple(pending[1:]), whole_line))
            pending = []
            continue

        found = _ODDS_IN_LINE.findall(line)
        prices = [value for value in (_as_american(token) for token in found) if value is not None]
        if len(prices) > 1:
            problems.append(f"line {line!r} carries {len(prices)} prices; cannot tell which is the selection's")
            continue
        if prices:
            inline_label = _ODDS_IN_LINE.sub("", line).strip(" .\t|-").strip()
            if inline_label:
                # Row layout. Anything still pending is extra context for this
                # selection (a team line DK printed above the price).
                label, context = inline_label, tuple(pending)
            elif pending:
                # Two-column layout: first line of the block names the
                # selection, the rest is context.
                label, context = pending[0], tuple(pending[1:])
            else:
                label, context = "", ()
            if not label:
                problems.append(f"price {line!r} with no selection label")
                pending = []
                continue
            rows.append(ParsedRow(len(rows) + 1, label, context, prices[0]))
            pending = []
            continue

        pending.append(line)

    if pending:
        problems.append(f"selection {pending[0]!r} has no price (trailing lines: {pending!r})")

    for row in rows:
        if abs(row.american) < MIN_ABS_AMERICAN:
            problems.append(
                f"{row.label!r} priced {row.american:+d}: no moneyline lies strictly "
                f"between -100 and +100, so this paste is malformed"
            )
    if not rows and not problems:
        problems.append("no selections found in the paste")
    truncated = incomplete_board_problem(rows)
    if truncated and not problems:
        problems.append(truncated)
    return rows, problems


# ── overround ────────────────────────────────────────────────────────────────

def incomplete_board_problem(rows: Sequence[ParsedRow]) -> str | None:
    """Complain when the implied probabilities sum below 100%.

    No book prices a mutually-exclusive market under 100% -- that is a free
    arbitrage -- so a sub-100% sum means the paste is truncated, which is the
    one failure that would quietly corrupt the single number P0 exists to
    measure. Pasting the top 20 of a 250-selection board would otherwise
    record a generous-looking negative overround.

    Deliberately no --allow-partial escape hatch: a flag that suppresses this
    would be set once and then always. If DK ever genuinely posts a sub-100%
    exclusive board, that is a finding worth a code change and a note here.
    """
    implied = sum(american_to_prob(row.american) for row in rows)
    if rows and implied < 1.0:
        return (
            f"{len(rows)} selections imply only {implied * 100:.1f}% -- a complete DK "
            f"board cannot sum below 100%, so this paste looks truncated"
        )
    return None


def compute_overround(rows: Sequence[ParsedRow]) -> float:
    """Σ implied probability − 1 over every selection DK printed.

    Over the whole board, not the resolved subset: the overround is a property
    of DK's pricing, and dropping the selections we happened to fail to name
    would understate it.
    """
    return sum(american_to_prob(row.american) for row in rows) - 1.0


# ── identity resolution (§3.4) ───────────────────────────────────────────────

def normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode()
    text = re.sub(r"\b(jr|sr|ii|iii|iv|v)\.?\b", "", text, flags=re.I)
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def canonical_team(raw: str | None) -> str | None:
    if not raw:
        return None
    token = raw.strip().upper()
    return TEAM_ABBREV_OVERRIDES.get(token, token)


@dataclass(frozen=True)
class Lookups:
    """Everything resolution needs, so resolution itself stays pure."""
    team_by_alias: dict[str, str]
    games: tuple[tuple[str, str], ...]          # (away_abbrev, home_abbrev)
    players: tuple[dict[str, Any], ...]


def build_team_aliases(teams: Iterable[dict[str, Any]]) -> dict[str, str]:
    """Map every unambiguous spelling of a team to its canonical abbreviation.

    A city shared by two teams (New York, Los Angeles) resolves to neither --
    an ambiguous alias is dropped, not arbitrated.
    """
    aliases: dict[str, list[str]] = {}
    for team in teams:
        abbrev = canonical_team(str(team["abbreviation"]))
        if not abbrev:
            continue
        name = str(team.get("name") or "")
        spellings = {name, str(team.get("odds_api_name") or ""), abbrev, str(team.get("city") or "")}
        if name:
            spellings.add(name.split()[-1])              # nickname: "Chiefs"
            spellings.add(" ".join(name.split()[-2:]))    # "Bay Packers" guard
        for spelling in spellings:
            key = normalize_name(spelling)
            if key:
                aliases.setdefault(key, []).append(abbrev)
    return {key: found[0] for key, found in aliases.items() if len(set(found)) == 1}


def resolve_team(label: str, lookups: Lookups) -> str | None:
    return lookups.team_by_alias.get(normalize_name(label))


def resolve_game(label: str, lookups: Lookups) -> str | None:
    """Return the canonical ``away@home`` key for a pasted game label.

    The order comes from **our schedule**, never from DK's print order, so a
    board that renders "Home vs Away" still stores the canonical key.
    """
    parts = [part for part in _SPLIT_GAME.split(label) if part.strip()]
    if len(parts) != 2:
        return None
    left, right = (resolve_team(part, lookups) for part in parts)
    if not left or not right or left == right:
        return None
    pair = {left, right}
    for away, home in lookups.games:
        if {away, home} == pair:
            return f"{away}@{home}"
    return None


def _player_candidates(family: str, lookups: Lookups) -> list[dict[str, Any]]:
    allowed = FAMILY_POSITIONS.get(family)
    if not allowed:
        return list(lookups.players)
    return [row for row in lookups.players if str(row.get("position") or "") in allowed]


def resolve_player(label: str, context: Sequence[str], family: str, lookups: Lookups) -> tuple[str | None, str]:
    """Resolve a printed player name to a selection key.

    Returns ``(selection_key, method)``. ``selection_key`` is the projection
    row's ``player_gsis_id`` when it has one, else ``normalized_name|team``.
    An ambiguous match resolves to None -- guessing mis-settles a bet.
    """
    candidates = _player_candidates(family, lookups)
    if not candidates:
        return None, "no_projection_rows"

    hint = None
    for piece in (*context, label):
        found = resolve_team(piece, lookups)
        if found:
            hint = found
            break
    if hint:
        narrowed = [row for row in candidates if canonical_team(row.get("team")) == hint]
        if narrowed:
            candidates = narrowed

    def key_for(row: dict[str, Any]) -> str:
        gsis = str(row.get("player_gsis_id") or "").strip()
        if gsis:
            return gsis
        return f"{row.get('normalized_name')}|{canonical_team(row.get('team')) or ''}"

    def unique(matches: list[dict[str, Any]], method: str) -> tuple[str | None, str]:
        keys = {key_for(row) for row in matches}
        if len(keys) == 1:
            return keys.pop(), method
        return None, "ambiguous" if matches else "unmatched"

    wanted = normalize_name(label)
    exact = [row for row in candidates if str(row.get("normalized_name") or "") == wanted]
    if exact:
        return unique(exact, "exact_normalized_name")

    # DK abbreviates on some boards: "C. McCaffrey".
    initial_match = _INITIAL_SURNAME.fullmatch(label.strip())
    if initial_match:
        initial = initial_match.group(1).lower()
        surname = normalize_name(initial_match.group(2))
        by_surname = [
            row for row in candidates
            if normalize_name(str(row.get("player_name") or "").split()[-1:][0] if row.get("player_name") else "") == surname
            and str(row.get("player_name") or "").lower().startswith(initial)
        ]
        if by_surname:
            return unique(by_surname, "initial_surname")

    try:
        from rapidfuzz import fuzz, process
    except ImportError:  # pragma: no cover - rapidfuzz is in requirements.txt
        return None, "unmatched_no_fuzzy"
    pool = {index: str(row.get("normalized_name") or "") for index, row in enumerate(candidates)}
    best = process.extractOne(wanted, pool, scorer=fuzz.ratio, score_cutoff=NAME_MATCH_FLOOR)
    if best is None:
        return None, "unmatched"
    _, score, index = best
    tied = [
        candidates[other]
        for other, name in pool.items()
        if fuzz.ratio(wanted, name) >= score - 1
    ]
    return unique(tied, f"fuzzy_{int(score)}")


@dataclass(frozen=True)
class ResolvedRow:
    row: ParsedRow
    selection_key: str
    method: str

    @property
    def resolved(self) -> bool:
        return not self.selection_key.startswith("UNRESOLVED:")


def resolve_selections(rows: Sequence[ParsedRow], family: str, lookups: Lookups) -> list[ResolvedRow]:
    kind = selection_kind(family)
    resolved: list[ResolvedRow] = []
    for row in rows:
        if kind == "team":
            key, method = resolve_team(row.label, lookups), "team_alias"
        elif kind == "game":
            key, method = resolve_game(row.label, lookups), "schedule_pair"
        else:
            key, method = resolve_player(row.label, row.context, family, lookups)
        if not key:
            resolved.append(ResolvedRow(row, f"UNRESOLVED:{row.label}", method if kind == "player" else "unmatched"))
        else:
            resolved.append(ResolvedRow(row, key, method))
    return resolved


def find_duplicate_keys(resolved: Sequence[ResolvedRow]) -> list[str]:
    """Selection keys appearing twice in one capture.

    A duplicated row double-counts its implied probability and silently
    inflates the overround -- the one number P0 exists to measure -- so this is
    a refusal, not a warning.
    """
    seen: dict[str, int] = {}
    for item in resolved:
        seen[item.selection_key] = seen.get(item.selection_key, 0) + 1
    return sorted(key for key, count in seen.items() if count > 1)


def capture_key_for(*, season: int, week: int, family: str, scope: str, raw_text: str) -> str:
    """Content-derived, so re-running the same paste is a no-op."""
    digest = hashlib.sha256(
        "\x1f".join([CAPTURE_FORMAT, str(season), str(week), family, scope, raw_text]).encode("utf-8")
    ).hexdigest()
    return f"{family}:{season}w{week}:{scope}:{digest[:16]}"


# ── database ─────────────────────────────────────────────────────────────────

def load_lookups(db: DatabaseManager, season: int, week: int) -> Lookups:
    teams = db.execute("SELECT abbreviation, name, odds_api_name, city FROM nfl_teams WHERE active")
    games = db.execute(
        """SELECT away.abbreviation AS away, home.abbreviation AS home
             FROM nfl_season_games g
             JOIN nfl_teams home ON home.team_id = g.home_team_id
             JOIN nfl_teams away ON away.team_id = g.away_team_id
            WHERE g.season = %s AND g.week = %s""",
        (season, week),
    )
    # Latest populated projection run for the week. nfl_dfs_projection_runs
    # has no status column -- "complete" is expressed as player_count > 0, and
    # the ordering matches the convention every other consumer uses
    # (as_of_at DESC, created_at DESC).
    players = db.execute(
        """WITH latest AS (
             SELECT run_id FROM nfl_dfs_projection_runs
              WHERE season = %s AND week = %s AND player_count > 0
              ORDER BY as_of_at DESC, created_at DESC LIMIT 1
           )
           SELECT p.player_gsis_id, p.player_name, p.normalized_name, p.team, p.position
             FROM nfl_dfs_player_projections p JOIN latest ON latest.run_id = p.run_id""",
        (season, week),
    )
    return Lookups(
        team_by_alias=build_team_aliases(teams),
        games=tuple((canonical_team(r["away"]) or "", canonical_team(r["home"]) or "") for r in games),
        players=tuple(dict(row) for row in players),
    )


def existing_capture(db: DatabaseManager, capture_key: str) -> int:
    row = db.execute_one(
        "SELECT COUNT(*) AS n FROM nfl_specials_market_captures WHERE capture_key = %s",
        (capture_key,),
    )
    return int(row["n"]) if row else 0


def write_capture(
    db: DatabaseManager,
    *,
    season: int,
    week: int,
    family: str,
    scope: str,
    book: str,
    captured_at: datetime,
    capture_key: str,
    raw_text: str,
    resolved: Sequence[ResolvedRow],
) -> int:
    db.execute_many(
        """INSERT INTO nfl_specials_market_captures
             (season, week, family, slate_scope, selection_key, selection_label,
              american, book, captured_at, capture_key, raw_text)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        [
            (
                season, week, family, scope, item.selection_key, item.row.label,
                item.row.american, book, captured_at, capture_key,
                raw_text if index == 0 else None,
            )
            for index, item in enumerate(resolved)
        ],
    )
    return len(resolved)


# ── orchestration ────────────────────────────────────────────────────────────

def run(
    *,
    season: int,
    week: int,
    family: str,
    scope: str,
    file_path: Path,
    db: DatabaseManager | None,
    book: str = "draftkings",
    captured_at: datetime | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    if family not in FAMILIES:
        raise ValueError(f"unknown family {family!r}; expected one of {list(FAMILIES)}")
    if scope not in SLATE_SCOPES:
        raise ValueError(f"unknown scope {scope!r}; expected one of {sorted(SLATE_SCOPES)}")

    raw_text = file_path.read_bytes().decode("utf-8-sig")
    rows, problems = parse_board_text(raw_text)
    if problems:
        raise ValueError(
            f"refusing to record {family} {season}w{week} {scope}: "
            + "; ".join(problems[:6])
            + (f" (+{len(problems) - 6} more)" if len(problems) > 6 else "")
        )

    lookups = load_lookups(db, season, week) if db is not None else Lookups({}, (), ())
    resolved = resolve_selections(rows, family, lookups)
    duplicates = find_duplicate_keys(resolved)
    if duplicates:
        raise ValueError(
            f"refusing to record {family} {season}w{week} {scope}: duplicated selections "
            f"would inflate the overround: {duplicates[:5]}"
        )

    key = capture_key_for(season=season, week=week, family=family, scope=scope, raw_text=raw_text)
    overround = compute_overround(rows)
    methods: dict[str, int] = {}
    for item in resolved:
        methods[item.method] = methods.get(item.method, 0) + 1

    report: dict[str, Any] = {
        "season": season,
        "week": week,
        "family": family,
        "slate_scope": scope,
        "book": book,
        "capture_key": key,
        "selections": len(resolved),
        "resolved": sum(item.resolved for item in resolved),
        "unresolved": [item.row.label for item in resolved if not item.resolved],
        "match_methods": methods,
        "overround": overround,
        "implied_sum": overround + 1.0,
        "written": 0,
        "already_captured": False,
    }

    if dry_run or db is None:
        return report
    if existing_capture(db, key):
        report["already_captured"] = True
        return report
    report["written"] = write_capture(
        db, season=season, week=week, family=family, scope=scope, book=book,
        captured_at=captured_at or datetime.now(timezone.utc), capture_key=key,
        raw_text=raw_text, resolved=resolved,
    )
    return report


def _print(report: dict[str, Any]) -> None:
    print(f"{report['family']}  {report['season']} week {report['week']}  {report['slate_scope']}")
    print(f"  selections     {report['selections']}")
    print(f"  resolved       {report['resolved']}/{report['selections']}")
    if report["unresolved"]:
        shown = ", ".join(report["unresolved"][:8])
        more = f" (+{len(report['unresolved']) - 8} more)" if len(report["unresolved"]) > 8 else ""
        print(f"  UNRESOLVED     {shown}{more}")
    print(f"  match methods  {report['match_methods']}")
    print(f"  implied sum    {report['implied_sum'] * 100:.1f}%")
    print(f"  OVERROUND      {report['overround'] * 100:+.1f}%   <- P0 measures exactly this")
    if report["already_captured"]:
        print("  already captured (identical paste) - nothing written")
    else:
        print(f"  rows written   {report['written']}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--week", type=int, required=True)
    parser.add_argument("--family", required=True, choices=list(FAMILIES))
    parser.add_argument("--scope", required=True, choices=sorted(SLATE_SCOPES))
    parser.add_argument("--file", required=True, type=Path)
    parser.add_argument("--book", default="draftkings")
    parser.add_argument("--dry-run", action="store_true", help="parse and report; write nothing")
    args = parser.parse_args(argv)

    # A dry run still connects when it can: resolving names is the half of this
    # tool most likely to be wrong on a given board, so a rehearsal that skips
    # it is not a rehearsal. Unreachable database, dry run only -> report the
    # overround anyway and say the keys were not checked.
    db = None
    try:
        db = DatabaseManager(load_config().database_url)
    except Exception as exc:
        if not args.dry_run:
            print(f"ERROR: database unavailable: {exc}", file=sys.stderr)
            return 1
        print(f"NOTE: no database ({exc}); selection keys were not resolved", file=sys.stderr)
    try:
        report = run(
            season=args.season, week=args.week, family=args.family, scope=args.scope,
            file_path=args.file, db=db, book=args.book, dry_run=args.dry_run,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    _print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
