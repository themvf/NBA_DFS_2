"""EPA-vs-label sign audit for the NFL play archetype taxonomy.

This is the standing early-warning surface Higgs recommends: the archetype label
and nflverse EPA are two INDEPENDENT signals, so when their signs disagree the
label is wrong or a rate computed off it will be. A *cluster* of same-signed
disagreements on one label is the signature of a population being silently
absorbed by that label -- the exact bug the taxonomy has produced five times.

Usage as a report:
    from epa_sign_audit import sign_audit
    audit = sign_audit(labelled_plays)      # DataFrame, worst disagreement first
    print(audit.head(50))

Usage as a guard test (see test_higgs_epa_sign_audit below): fail when any single
archetype has a same-signed disagreement cluster larger than a tolerance, because
that is what "a rate off this label is wrong in the same direction every time"
looks like in the data.

Descriptive only. No predictive or betting claim.
"""

from __future__ import annotations

import pandas as pd

# A label whose name asserts a bad/negative outcome. EPA well above zero on these
# is the "failure that actually succeeded" signature (e.g. a stuffed run that drew
# a 15-yard penalty, logged LATE_DOWN_FAILURE at +2.15 EPA).
NEGATIVE_LABELS = {
    "LATE_DOWN_FAILURE",
    "EARLY_DOWN_FAILURE",
    "SACK",
    "TURNOVER_PLAY",
}
# A label whose name asserts a good/positive outcome. EPA well below zero here is
# the mirror signature.
POSITIVE_LABELS = {
    "LATE_DOWN_CONVERSION",
    "EARLY_DOWN_SUCCESS",
    "EARLY_DOWN_EXPLOSIVE",
}

POS_EPA_THRESHOLD = 0.5
NEG_EPA_THRESHOLD = -0.5


def _label_sign(label: str) -> int:
    if label in NEGATIVE_LABELS:
        return -1
    if label in POSITIVE_LABELS:
        return 1
    return 0  # neutral / off-axis labels are not audited for sign


def sign_audit(plays: pd.DataFrame) -> pd.DataFrame:
    """Return rows where the archetype label's sign disagrees with EPA's sign,
    sorted by magnitude of disagreement (worst first).

    Expects columns: play_archetype, epa. Optional passthrough columns
    (season, game_id, desc, outcome) are carried when present.
    """
    df = plays.copy()
    df["_label_sign"] = df["play_archetype"].map(_label_sign)
    epa = pd.to_numeric(df["epa"], errors="coerce")

    negative_but_good = (df["_label_sign"] == -1) & (epa > POS_EPA_THRESHOLD)
    positive_but_bad = (df["_label_sign"] == 1) & (epa < NEG_EPA_THRESHOLD)
    flagged = df[negative_but_good | positive_but_bad].copy()

    flagged["disagreement"] = epa[negative_but_good | positive_but_bad].abs()
    keep = [
        c
        for c in ["season", "game_id", "desc", "play_archetype", "outcome"]
        if c in flagged.columns
    ]
    flagged = flagged[keep + ["epa", "disagreement"]]
    return flagged.sort_values("disagreement", ascending=False).reset_index(drop=True)


def cluster_counts(audit: pd.DataFrame) -> pd.Series:
    """How many disagreements land on each archetype. A large single-label count
    is the 'absorbed population' signature -- not scattered noise but a
    one-directional lump."""
    return audit["play_archetype"].value_counts()


# --------------------------------------------------------------------------- #
# Guard test. Turns critique item (1) -- "the enforcement rule is prose, not a
# test" -- into an executable check. Run with pytest. Tune the tolerance to your
# data; the point is that a NEW merge bug shows up here as a cluster before
# anyone publishes a rate off the offending label.
# --------------------------------------------------------------------------- #
def test_higgs_epa_sign_audit() -> None:
    import os

    # Wire this to your real labelled-plays source (Neon read, parquet, or a
    # fixture). Skip cleanly when the data is not available so the suite still
    # runs in a bare checkout.
    fixture = os.environ.get("NFL_LABELLED_PLAYS_PARQUET")
    if not fixture or not os.path.exists(fixture):
        import pytest

        pytest.skip("set NFL_LABELLED_PLAYS_PARQUET to run the EPA sign audit")

    plays = pd.read_parquet(fixture)
    audit = sign_audit(plays)
    counts = cluster_counts(audit)

    # A per-label cluster this large is not noise -- it is a population the label
    # is absorbing one-directionally. Tune to team-season scale in your data.
    tolerance = 40
    offenders = counts[counts > tolerance]
    assert offenders.empty, (
        "EPA sign-disagreement cluster(s) suggest a label is absorbing a "
        f"population one-directionally (suspected merge bug):\n{offenders}"
    )
