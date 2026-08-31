"""Recover immutable, pre-cutoff Team Opportunity training facts.

The ordinary nflverse release assets are mutable and their current exact bytes
postdate the V2 simulated preseason cutoffs. Several historical GitHub forks
retain the repository blobs that existed before the 2021 and 2022 seasons. This
module pins those commits, verifies both SHA-256 and Git blob identity, and
builds an explicitly Tier-C training context from play-by-play plus annual
roster position data.

No unavailable source is replaced with zero. Weekly stats, participation,
schedule, transactions, quarterback, and play-caller context remain declared
missing. Later folds may reuse only these eligible 2020-2021 facts until a
newer immutable archive is located.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd
import pyreadr
import requests

from config import load_config
from ingest.ff_fantasypros import RefreshDatabase
from ingest.ff_independent import normalize_team
from ingest.ff_source_contracts import SnapshotProvenance, persist_source_snapshot
from ingest.ff_v2_historical_context import (
    _clean,
    build_team_week_facts,
    canonical_digest,
    persist_context_run,
)


TRANSFORM_VERSION = "ff-v2-archived-team-context-v1"
RUN_NAMESPACE = uuid.UUID("d3069d5a-b8a3-46e7-8200-6534da05f40e")
DEFAULT_CACHE_ROOT = Path("data/ff_v2_archives")
DEFAULT_ARTIFACT = Path("artifacts/ff_v2_archived_team_context.json")
USER_AGENT = "NBADFS-v2-archive-recovery/1.0"
DECLARED_MISSING_SOURCES = (
    "weekly-stats",
    "participation",
    "schedule",
    "transactions",
    "quarterback",
    "play-caller",
)


@dataclass(frozen=True)
class ArchivedFile:
    key: str
    contract_key: str
    season: int
    relative_path: str
    url: str
    repository: str
    commit_sha: str | None
    release_tag: str | None
    published_at: datetime
    sha256: str
    git_blob_sha: str | None
    row_count: int


@dataclass(frozen=True)
class ArchiveBundle:
    key: str
    cutoff: datetime
    seasons: tuple[int, ...]
    files: tuple[ArchivedFile, ...]


ROSTER_REPOSITORY = "jedwards757/nflfastR-roster"
ROSTER_COMMIT = "4a818c1f464839a70c6625952dcf08c7701d244c"
ROSTER_PUBLISHED = datetime(2021, 7, 31, 20, 20, 7, tzinfo=timezone.utc)


def _raw_url(repository: str, commit: str, path: str) -> str:
    return f"https://raw.githubusercontent.com/{repository}/{commit}/{path}"


ROSTER_FILES = {
    2020: ArchivedFile(
        key="roster-2020-jedwards-4a818c1f",
        contract_key="weekly-rosters",
        season=2020,
        relative_path="roster-2021-cutoff/roster_2020.csv",
        url=_raw_url(ROSTER_REPOSITORY, ROSTER_COMMIT, "data/seasons/roster_2020.csv"),
        repository=ROSTER_REPOSITORY,
        commit_sha=ROSTER_COMMIT,
        release_tag=None,
        published_at=ROSTER_PUBLISHED,
        sha256="bc516ebdd498cc42803f66826a31e4e1ba3233cef36bd5c2b6c1177e83b59191",
        git_blob_sha="299291f925fe9a19fda5f7e808829653ddb6df76",
        row_count=4624,
    ),
    2021: ArchivedFile(
        key="roster-2021-jedwards-4a818c1f",
        contract_key="weekly-rosters",
        season=2021,
        relative_path="roster-2021-cutoff/roster_2021.csv",
        url=_raw_url(ROSTER_REPOSITORY, ROSTER_COMMIT, "data/seasons/roster_2021.csv"),
        repository=ROSTER_REPOSITORY,
        commit_sha=ROSTER_COMMIT,
        release_tag=None,
        published_at=ROSTER_PUBLISHED,
        sha256="1ce7dc0f6cb3ae6355327efdc81e155513b7078a9aef8466368bacc8d7ef3715",
        git_blob_sha="7b36624ea87e7a4236d59a0e3456bf6c08489d0b",
        row_count=5196,
    ),
}


ARCHIVE_BUNDLES = (
    ArchiveBundle(
        key="2021-cutoff",
        cutoff=datetime(2021, 9, 8, 23, 59, 59, tzinfo=timezone.utc),
        seasons=(2020,),
        files=(
            ArchivedFile(
                key="pbp-2020-kevinmhinson-d1863ac6",
                contract_key="play-by-play",
                season=2020,
                relative_path="2021-cutoff/play_by_play_2020.parquet",
                url=_raw_url(
                    "kevinmhinson/nflfastR-data",
                    "d1863ac63c81f11565e944fa6a30efac7638d47b",
                    "data/play_by_play_2020.parquet",
                ),
                repository="kevinmhinson/nflfastR-data",
                commit_sha="d1863ac63c81f11565e944fa6a30efac7638d47b",
                release_tag=None,
                published_at=datetime(2021, 8, 24, 18, 37, 44, tzinfo=timezone.utc),
                sha256="d63f70caf123cf330b88d3fdd43dd02c8d472eba9df0fc41b3fb234b9a8a35fb",
                git_blob_sha="313ea8be52d28f60a2c739f37c3be2630112f0eb",
                row_count=48514,
            ),
            ROSTER_FILES[2020],
        ),
    ),
    ArchiveBundle(
        key="2022-cutoff",
        cutoff=datetime(2022, 9, 7, 23, 59, 59, tzinfo=timezone.utc),
        seasons=(2020, 2021),
        files=(
            ArchivedFile(
                key="pbp-2020-seekpinky-285363e2",
                contract_key="play-by-play",
                season=2020,
                relative_path="2022-cutoff/play_by_play_2020.parquet",
                url=_raw_url(
                    "seekpinky/nflfastR-data",
                    "285363e27120ddd4954b0c83fa76567d20d8b3c5",
                    "data/play_by_play_2020.parquet",
                ),
                repository="seekpinky/nflfastR-data",
                commit_sha="285363e27120ddd4954b0c83fa76567d20d8b3c5",
                release_tag=None,
                published_at=datetime(2022, 8, 2, 22, 28, 31, tzinfo=timezone.utc),
                sha256="4bb6d732e40514d88cdccee0c8d32fe65d6567e64bb71cc93a4257d19d61af80",
                git_blob_sha="c96062b1f3b97c38e065e3c1f0452c1646f8e5d6",
                row_count=48514,
            ),
            ArchivedFile(
                key="pbp-2021-seekpinky-285363e2",
                contract_key="play-by-play",
                season=2021,
                relative_path="2022-cutoff/play_by_play_2021.parquet",
                url=_raw_url(
                    "seekpinky/nflfastR-data",
                    "285363e27120ddd4954b0c83fa76567d20d8b3c5",
                    "data/play_by_play_2021.parquet",
                ),
                repository="seekpinky/nflfastR-data",
                commit_sha="285363e27120ddd4954b0c83fa76567d20d8b3c5",
                release_tag=None,
                published_at=datetime(2022, 8, 2, 22, 28, 31, tzinfo=timezone.utc),
                sha256="558c90cd06d51c8e8d0b3fa104c77f63828b3bd389dd650b028163760f26c213",
                git_blob_sha="adde2538b35cb7573cf443999e3558c58c125f42",
                row_count=50712,
            ),
            ROSTER_FILES[2020],
            ArchivedFile(
                key="roster-2021-official-20220809",
                contract_key="weekly-rosters",
                season=2021,
                relative_path="roster-official-2022-cutoff/roster_2021.rds",
                url=(
                    "https://github.com/nflverse/nflverse-data-archives/releases/download/"
                    "repo-nflfastr-roster/roster_2021.rds"
                ),
                repository="nflverse/nflverse-data-archives",
                commit_sha=None,
                release_tag="repo-nflfastr-roster",
                published_at=datetime(2022, 8, 9, 6, 9, 23, tzinfo=timezone.utc),
                sha256="29836bbd6b12359a87e17d661c36f02bd64aed294849e0a67a4f1026278568f3",
                git_blob_sha=None,
                row_count=4839,
            ),
            ROSTER_FILES[2021],
        ),
    ),
)


def _git_blob_sha(payload: bytes) -> str:
    header = f"blob {len(payload)}\0".encode("ascii")
    return hashlib.sha1(header + payload).hexdigest()  # noqa: S324 - Git object identity


def validate_payload(spec: ArchivedFile, payload: bytes) -> None:
    observed_sha256 = hashlib.sha256(payload).hexdigest()
    if observed_sha256 != spec.sha256:
        raise ValueError(f"SHA-256 mismatch for {spec.key}: {observed_sha256}")
    if spec.git_blob_sha is not None:
        observed_blob = _git_blob_sha(payload)
        if observed_blob != spec.git_blob_sha:
            raise ValueError(f"Git blob mismatch for {spec.key}: {observed_blob}")


def load_file(spec: ArchivedFile, cache_root: Path, *, refresh: bool = False) -> tuple[bytes, Path]:
    path = cache_root / spec.relative_path
    payload: bytes | None = None
    if path.exists() and not refresh:
        payload = path.read_bytes()
    if payload is None:
        response = requests.get(spec.url, timeout=120, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        payload = response.content
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    validate_payload(spec, payload)
    return payload, path


def _expand_roster(roster: pd.DataFrame, pbp: pd.DataFrame, season: int) -> pd.DataFrame:
    regular = pbp[(pbp["season"] == season) & (pbp["season_type"] == "REG")]
    weeks = sorted(pd.to_numeric(regular["week"], errors="coerce").dropna().astype(int).unique())
    season_roster = roster[roster["season"] == season].copy()
    if season_roster.empty or not weeks:
        raise ValueError(f"Archive lacks roster or regular-season weeks for {season}")
    return pd.concat([season_roster.assign(week=week) for week in weeks], ignore_index=True)


def _game_team_context(pbp: pd.DataFrame, season: int) -> dict[tuple[str, str], dict[str, Any]]:
    regular = pbp[(pbp["season"] == season) & (pbp["season_type"] == "REG")]
    context: dict[tuple[str, str], dict[str, Any]] = {}
    for game_id, game in regular.groupby("game_id", sort=True):
        first = game.iloc[0]
        game_date = str(first["game_date"])
        observed_day = pd.to_datetime(game_date, utc=True).to_pydatetime()
        home = normalize_team(first["home_team"])
        away = normalize_team(first["away_team"])
        for team, opponent in ((home, away), (away, home)):
            context[(str(game_id), team)] = {
                "game_date": game_date,
                "opponent": opponent,
                "kickoff_at": observed_day,
                "quarterback_gsis_id": None,
                "quarterback_name": None,
            }
    return context


def build_bundle_facts(
    bundle: ArchiveBundle,
    frames: Mapping[str, pd.DataFrame],
    snapshot_ids: Mapping[str, int],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    missing_positions = {"unknown_rusher_positions": 0, "unknown_receiver_positions": 0}
    empty_stats = pd.DataFrame(columns=["season", "week", "player_id", "position"])
    for season in bundle.seasons:
        pbp_spec = next(
            item for item in bundle.files if item.contract_key == "play-by-play" and item.season == season
        )
        roster_specs = [
            item for item in bundle.files if item.contract_key == "weekly-rosters" and item.season == season
        ]
        pbp = frames[pbp_spec.key]
        roster = _expand_roster(pd.concat([frames[item.key] for item in roster_specs], ignore_index=True), pbp, season)
        roster_snapshot_ids = [int(snapshot_ids[item.key]) for item in roster_specs]
        season_facts, season_missing = build_team_week_facts(
            pbp,
            empty_stats,
            roster,
            season=season,
            source_snapshot_ids={
                "play_by_play": int(snapshot_ids[pbp_spec.key]),
                "weekly_rosters": roster_snapshot_ids[0],
                **(
                    {"weekly_rosters_fallback": roster_snapshot_ids[1]}
                    if len(roster_snapshot_ids) > 1
                    else {}
                ),
            },
            game_team_context=_game_team_context(pbp, season),
        )
        for fact in season_facts:
            fact["derivation"] = {
                **fact["derivation"],
                "archive_transform_version": TRANSFORM_VERSION,
                "fallback_tier": "C",
                "declared_missing_sources": list(DECLARED_MISSING_SOURCES),
            }
            fact["fact_digest"] = canonical_digest(fact)
        facts.extend(season_facts)
        for key in missing_positions:
            missing_positions[key] += int(season_missing[key])
        if any(int(value) for value in season_missing.values()):
            raise ValueError(
                f"Archive position coverage is incomplete for {season}: {season_missing}. "
                "Refusing to classify unknown opportunity players as non-RBs."
            )
    coverage = {
        "fact_count": len(facts),
        "seasons": list(bundle.seasons),
        "teams": sorted({str(row["team"]) for row in facts}),
        "fallback_tier": "C",
        "declared_missing_sources": list(DECLARED_MISSING_SOURCES),
        **missing_positions,
    }
    return facts, coverage


def _persist_archive_file(
    db: RefreshDatabase,
    spec: ArchivedFile,
    path: Path,
    frame: pd.DataFrame,
    cutoff: datetime,
) -> int:
    return persist_source_snapshot(
        db,
        SnapshotProvenance(
            source="nflverse-git-archive" if spec.git_blob_sha else "nflverse-release-archive",
            dataset=spec.key,
            contract_key=spec.contract_key,
            season=spec.season,
            response_hash=spec.sha256,
            row_count=len(frame),
            request_params={
                "url": spec.url,
                "cache_path": str(path),
                "repository": spec.repository,
                "commit_sha": spec.commit_sha,
                "release_tag": spec.release_tag,
                "git_blob_sha": spec.git_blob_sha,
                "archive_transform_version": TRANSFORM_VERSION,
            },
            source_published_at=spec.published_at,
            fetched_at=datetime.now(timezone.utc),
            as_of_at=cutoff,
            model_eligible=True,
            eligibility_reason=(
                "exact Git blob committed before simulated preseason cutoff"
                if spec.git_blob_sha
                else "exact archived release asset published before simulated preseason cutoff"
            ),
        ),
    )


def build_artifact(
    db: RefreshDatabase,
    *,
    cache_root: Path = DEFAULT_CACHE_ROOT,
    refresh: bool = False,
) -> dict[str, Any]:
    bundle_artifacts: list[dict[str, Any]] = []
    for bundle in ARCHIVE_BUNDLES:
        frames: dict[str, pd.DataFrame] = {}
        paths: dict[str, Path] = {}
        snapshot_ids: dict[str, int] = {}
        for spec in bundle.files:
            payload, path = load_file(spec, cache_root, refresh=refresh)
            if path.suffix == ".csv":
                frame = pd.read_csv(path, low_memory=False)
            elif path.suffix == ".rds":
                frame = next(iter(pyreadr.read_r(path).values()))
            else:
                frame = pd.read_parquet(path)
            if len(frame) != spec.row_count:
                raise ValueError(f"Row-count mismatch for {spec.key}: {len(frame)}")
            validate_payload(spec, payload)
            frames[spec.key] = frame
            paths[spec.key] = path
            snapshot_ids[spec.key] = _persist_archive_file(db, spec, path, frame, bundle.cutoff)
        facts, coverage = build_bundle_facts(bundle, frames, snapshot_ids)
        stable = {
            "bundleKey": bundle.key,
            "cutoff": bundle.cutoff.isoformat(),
            "seasons": list(bundle.seasons),
            "transformVersion": TRANSFORM_VERSION,
            "sourceSnapshotIds": dict(sorted(snapshot_ids.items())),
            "sources": [
                {
                    "key": spec.key,
                    "contractKey": spec.contract_key,
                    "season": spec.season,
                    "url": spec.url,
                    "repository": spec.repository,
                    "commitSha": spec.commit_sha,
                    "releaseTag": spec.release_tag,
                    "publishedAt": spec.published_at.isoformat(),
                    "sha256": spec.sha256,
                    "gitBlobSha": spec.git_blob_sha,
                    "rowCount": spec.row_count,
                    "cachePath": str(paths[spec.key]),
                    "sourceSnapshotId": snapshot_ids[spec.key],
                }
                for spec in bundle.files
            ],
            "coverage": coverage,
            "factDigest": canonical_digest(sorted(row["fact_digest"] for row in facts)),
            "representativeFacts": _clean(facts[:4]),
        }
        run_id = str(uuid.uuid5(RUN_NAMESPACE, canonical_digest(stable)))
        artifact_digest = canonical_digest({**stable, "runId": run_id})
        persist_context_run(
            db,
            run_id=run_id,
            seasons=list(bundle.seasons),
            source_snapshot_ids=snapshot_ids,
            coverage=coverage,
            digest=artifact_digest,
            contexts=[],
            roster_rows=[],
            transactions=[],
            facts=facts,
            transform_version=TRANSFORM_VERSION,
        )
        bundle_artifacts.append(
            {**stable, "runId": run_id, "artifactDigest": artifact_digest, "factCount": len(facts)}
        )
    stable_artifact = {
        "schemaVersion": 1,
        "artifactType": "fantasy-football-v2-archived-team-context",
        "transformVersion": TRANSFORM_VERSION,
        "bundles": bundle_artifacts,
    }
    return {**stable_artifact, "artifactDigest": canonical_digest(stable_artifact)}


def run(args: argparse.Namespace) -> dict[str, Any]:
    config = load_config()
    if not config.database_url:
        raise RuntimeError("DATABASE_URL is required")
    db = RefreshDatabase(config.database_url)
    try:
        artifact = build_artifact(db, cache_root=Path(args.cache_root), refresh=args.refresh)
        path = Path(args.artifact)
        if args.verify:
            stored = json.loads(path.read_text(encoding="utf-8"))
            if stored != artifact:
                raise RuntimeError("Archived Team Opportunity context replay mismatch")
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        db.close()
    except Exception:
        db.close(error=True)
        raise
    return {
        "status": "verified" if args.verify else "persisted",
        "artifactDigest": artifact["artifactDigest"],
        "bundles": [
            {"key": item["bundleKey"], "runId": item["runId"], "factCount": item["factCount"]}
            for item in artifact["bundles"]
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-root", default=str(DEFAULT_CACHE_ROOT))
    parser.add_argument("--artifact", default=str(DEFAULT_ARTIFACT))
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--verify", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    print(json.dumps(run(parse_args()), indent=2, sort_keys=True))
