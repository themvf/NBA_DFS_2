import pytest

from ingest.nfl_pfr_identity import roster_claims


HEADER = "season,gsis_id,pfr_id,full_name,position,team,week\n"
SOURCE = {"sha256": "source", "captured_at": "2026-09-27T14:00:00Z", "url": "https://example.test/provider.csv"}


def test_only_same_row_exact_provider_identifiers_are_claimed():
    content = (HEADER + "2026,00-0041013,JohnEm01,Emmett Johnson,RB,KC,1\n"
               + "2026,,Other00,Emmett Johnson,RB,KC,2\n"
               + "2025,00-0041013,OldEm00,Emmett Johnson,RB,KC,1\n").encode()
    claims = roster_claims(content, 2026, SOURCE)
    assert len(claims) == 1
    assert claims[0]["external_id"] == "JohnEm01" and claims[0]["gsis_id"] == "00-0041013"
    assert claims[0]["evidence"]["name_match_used"] is False


def test_conflicting_provider_claims_are_preserved_for_quarantine():
    content = (HEADER + "2026,00-0041013,JohnEm01,Emmett Johnson,RB,KC,1\n"
               + "2026,00-0041013,JohnEm01,Emmett Johnson,RB,KC,2\n"
               + "2026,00-0031111,JohnEm01,Different Person,RB,KC,2\n").encode()
    claims = roster_claims(content, 2026, SOURCE)
    assert len(claims) == 2 and len({r["gsis_id"] for r in claims}) == 2


def test_empty_or_schema_changed_provider_data_fails_closed():
    with pytest.raises(ValueError):
        roster_claims(b"name,id\nPerson,1\n", 2026, SOURCE)
    with pytest.raises(ValueError):
        roster_claims(HEADER.encode(), 2026, SOURCE)


def test_refresh_does_not_reinsert_claims_already_stored():
    # claim_digest covers the source capture time, so a digest-only conflict
    # re-inserted every claim on every run (240k rows for 2.2k claims).
    import inspect
    from ingest import nfl_pfr_identity
    source = inspect.getsource(nfl_pfr_identity.refresh_identity)
    assert "WHERE NOT EXISTS" in source
    for column in ("namespace", "external_id", "gsis_id", "season", "method", "team", "position"):
        assert f"c.{column}" in source
