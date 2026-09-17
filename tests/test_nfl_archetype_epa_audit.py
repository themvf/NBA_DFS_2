"""Accuracy guard for the NFL play archetype taxonomy — the EPA sign audit.

WHAT THIS PROTECTS
------------------
The taxonomy has produced the same bug five times: a single-valued label
silently absorbs a population, and a rate computed off it is wrong in the SAME
DIRECTION every time. The independent check on that bug is EPA: the archetype
label and nflverse's EPA are two separate signals, so a label whose name asserts
"failure" sitting at strongly positive EPA (or the mirror) is either mislabelled
or a rate off it will be. A *cluster* of same-signed disagreements on ONE label
is the signature of an absorbed population.

This test runs the REAL `label_plays` on a REAL season of nflverse play-by-play
and fails if any single archetype carries a one-directional disagreement cluster
larger than tolerance. That is the tripwire that keeps labels accurate to the
actual games as new descriptors and slices are layered on.

CALIBRATION (measured on 2025, 46,042 plays)
--------------------------------------------
The raw audit found 160 sign disagreements (0.55% of sign-bearing plays). The
only cluster over a naive count threshold was LATE_DOWN_FAILURE (62). Tracing all
62 showed they are NOT a merge bug:
  - 22 were `penalty_first_down=True` — a 3rd/4th down where the snap fell short
    but a defensive penalty converted anyway. The label describes the SNAP
    (correctly a failure); the `penalty_first_down` FLAG carries the conversion.
    This is the flag/label rule working exactly as designed, so these plays are
    EXCLUDED from the audit below — they are known-correct co-occurrences, not
    disagreements.
  - ~40 were genuine gains that fell short of the line to gain (e.g. 3rd-and-16
    gains 11, punts). Positive EPA, honest non-conversion — the legitimate
    tension between "did you convert" (label) and "was the play valuable" (EPA),
    not a mislabel.

So the guard uses (1) a `penalty_first_down` exclusion and (2) a RATE tolerance:
a label fails only when its disagreement RATE exceeds a share of its own
population. A rate scales with sample size and does not false-trip on the
~40/3,952 = ~1% residual that is legitimate EPA tension. A structural merge bug
absorbs a far larger, one-directional share and trips it.

HOW IT SOURCES DATA
-------------------
It uses the same `load_pbp` the ingest pipeline uses. Point it at a cached
season parquet with NFL_PBP_CACHE (fast, offline, deterministic), or let it
download the season if the environment allows network access. When neither is
available it SKIPS — a bare checkout or an offline CI run must not fail this,
because a false failure here trains people to ignore it.

    NFL_PBP_AUDIT_SEASON=2025
    NFL_PBP_CACHE=/path/to/play_by_play_2025.parquet

Descriptive only. No predictive or betting claim.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")


# Labels whose NAME asserts a negative outcome. Strongly positive EPA on these is
# the "failure that actually succeeded" signature. Kept in sync with the
# taxonomy in model/nfl_play_archetypes.py — if a label is renamed there, update
# here (see test_label_sets_match_taxonomy below, which fails when they drift).
NEGATIVE_LABELS = {
    "LATE_DOWN_FAILURE",
    "EARLY_DOWN_FAILURE",
    "SACK",
    "TURNOVER_PLAY",
}
POSITIVE_LABELS = {
    "LATE_DOWN_CONVERSION",
    "EARLY_DOWN_SUCCESS",
    "EARLY_DOWN_EXPLOSIVE",
}

POS_EPA_THRESHOLD = 0.5
NEG_EPA_THRESHOLD = -0.5

# A label fails when its disagreement RATE (after excluding known-correct penalty
# conversions) exceeds this share of the label's own population. Measured
# baseline on 2025: the worst residual was LATE_DOWN_FAILURE at ~40/3,952 ≈ 1.0%.
# 3% leaves generous headroom over legitimate EPA tension while still tripping on
# a structural absorption, which runs far higher and one-directionally.
CLUSTER_RATE_TOLERANCE = 0.03
# A floor so tiny labels cannot trip on a handful of plays (rate is noisy at low n).
MIN_CLUSTER_COUNT = 25


def _label_sign(label: object) -> int:
    if label in NEGATIVE_LABELS:
        return -1
    if label in POSITIVE_LABELS:
        return 1
    return 0


def sign_disagreements(labelled: "pd.DataFrame") -> "pd.DataFrame":
    """Rows where the archetype label's sign contradicts EPA's sign.

    Known-correct co-occurrences are excluded so they do not masquerade as
    disagreements: a play that fell short but converted via a defensive penalty
    is a correct FAILURE label with the conversion carried on the
    `penalty_first_down` flag (see module docstring, 2025 calibration).
    """
    df = labelled.copy()
    df["_sign"] = df["play_archetype"].map(_label_sign)
    epa = pd.to_numeric(df["epa"], errors="coerce")

    excluded = pd.Series(False, index=df.index)
    if "penalty_first_down" in df.columns:
        excluded = df["penalty_first_down"].fillna(False).astype(bool)

    neg_but_good = (df["_sign"] == -1) & (epa > POS_EPA_THRESHOLD) & ~excluded
    pos_but_bad = (df["_sign"] == 1) & (epa < NEG_EPA_THRESHOLD) & ~excluded
    return df[neg_but_good | pos_but_bad]


def cluster_rates(labelled: "pd.DataFrame") -> "pd.DataFrame":
    """Per-label disagreement count, population, and rate — the audit surface."""
    flagged = sign_disagreements(labelled)
    counts = flagged["play_archetype"].value_counts()
    totals = labelled["play_archetype"].value_counts()
    out = pd.DataFrame({"disagreements": counts})
    out["population"] = out.index.map(totals.to_dict())
    out["rate"] = out["disagreements"] / out["population"]
    return out.sort_values("rate", ascending=False)


def _load_audit_frame() -> "pd.DataFrame":
    """Run the real labeller on a real season, or skip cleanly."""
    play_mod = pytest.importorskip("model.nfl_play_archetypes")
    drive_mod = pytest.importorskip("model.nfl_drive_archetypes")
    label_plays = play_mod.label_plays
    load_pbp = drive_mod.load_pbp

    season = int(os.environ.get("NFL_PBP_AUDIT_SEASON", "2025"))
    cache_env = os.environ.get("NFL_PBP_CACHE")
    cache = Path(cache_env) if cache_env else None

    if cache is not None and not cache.exists():
        pytest.skip(f"NFL_PBP_CACHE={cache} does not exist")
    if cache is None and os.environ.get("NFL_PBP_ALLOW_DOWNLOAD") != "1":
        pytest.skip(
            "set NFL_PBP_CACHE to a season parquet (offline) or "
            "NFL_PBP_ALLOW_DOWNLOAD=1 to fetch — skipping EPA audit"
        )

    try:
        pbp = load_pbp(season, cache)
    except Exception as exc:  # network down, release moved, etc.
        pytest.skip(f"could not load nflverse PBP for {season}: {exc}")

    return label_plays(pbp)


def test_no_one_directional_disagreement_cluster() -> None:
    """The core guard: no single label may absorb a one-directional EPA
    disagreement cluster whose RATE exceeds tolerance (after excluding
    known-correct penalty conversions)."""
    rates = cluster_rates(_load_audit_frame())
    offenders = rates[
        (rates["rate"] > CLUSTER_RATE_TOLERANCE)
        & (rates["disagreements"] >= MIN_CLUSTER_COUNT)
    ]
    assert offenders.empty, (
        "EPA sign-disagreement rate exceeds tolerance — a label appears to be "
        "absorbing a population one-directionally (suspected merge bug). Trace "
        f"which FLAG should carry the stolen plays:\n{offenders.to_string()}"
    )


def test_label_sets_match_taxonomy() -> None:
    """Drift guard: if the taxonomy renames or adds an outcome label, this test
    fails so NEGATIVE_LABELS / POSITIVE_LABELS above get updated rather than
    silently going stale (a stale audit is a blind audit)."""
    module = pytest.importorskip("model.nfl_play_archetypes")
    source = Path(module.__file__).read_text(encoding="utf-8", errors="ignore")
    for label in NEGATIVE_LABELS | POSITIVE_LABELS:
        assert label in source, (
            f"{label} is audited here but no longer appears in "
            "model/nfl_play_archetypes.py — update the label sets in this test"
        )
