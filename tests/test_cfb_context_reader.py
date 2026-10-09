from datetime import datetime, timedelta, timezone

from model.cfb_context_reader import Candidate, Slot, Step, resolve_current, resolve_pinned


NOW = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)


def candidate(version=1, *, snapshot="s1", as_of=None, source_age=20, invalidated=False):
    return Candidate(snapshot, "m-" + snapshot, "pace", version, "cfb:team:1", "cfb:event:9", "base",
                     as_of or NOW - timedelta(seconds=10), NOW - timedelta(seconds=5), source_age,
                     "prospective", "complete", invalidated)


def slot(versions=(("pace", 1),), context_age=60, source_age=60):
    return Slot("pace", True, versions, context_age, source_age)


def resolve(steps, candidates):
    return resolve_current(steps=steps, candidates=candidates, requested_as_of=NOW,
                           evaluation_at=NOW, live_request=True, allowed_origins=("prospective",),
                           subject_key="cfb:team:1", target_event_key="cfb:event:9", scenario_key="base")


def test_exact_version_preference_then_recency_then_uuid():
    rows = [candidate(2, snapshot="new-version"), candidate(1, snapshot="older", as_of=NOW-timedelta(seconds=20)),
            candidate(1, snapshot="preferred", as_of=NOW-timedelta(seconds=5))]
    result = resolve([Step(0, "resolve", (slot((("pace", 1), ("pace", 2))),))], rows)
    assert result.result == "allow"
    assert result.snapshot_ids == ("preferred",)


def test_stale_missing_age_and_invalidated_candidates_fall_back_without_implicit_carry():
    rows = [candidate(snapshot="stale", as_of=NOW-timedelta(seconds=61)),
            candidate(snapshot="missing-age", source_age=None), candidate(snapshot="revoked", invalidated=True)]
    result = resolve([Step(0, "resolve", (slot(),)), Step(1, "pinned_baseline", baseline_manifest_id="baseline")], rows)
    assert result.result == "fallback"
    assert result.manifest_ids == ("baseline",)


def test_zero_freshness_is_exact_and_deny_has_no_data():
    exact = candidate(as_of=NOW, source_age=0)
    assert resolve([Step(0, "resolve", (slot(context_age=0, source_age=0),))], [exact]).result == "allow"
    denied = resolve([Step(0, "resolve", (slot(context_age=0, source_age=0),)), Step(1, "deny")], [candidate()])
    assert denied.result == "deny" and not denied.manifest_ids


def test_pinned_read_never_substitutes():
    assert resolve_pinned("original", replay_available=False, invalidated=False).result == "replay_unavailable"
    pinned = resolve_pinned("original", replay_available=True, invalidated=True)
    assert pinned.manifest_ids == ("original",) and pinned.reason_codes == ("manifest_invalidated",)
