from model.mlb_pitcher_lineup_report import (
    build_bucket_lookup,
    enrich_pitcher_row,
    score_pitcher_candidate,
    summarize_bucket_rows,
)


def test_summarize_bucket_rows_computes_hit_rates() -> None:
    rows = [
        enrich_pitcher_row(
            {
                "slate_id": 1,
                "name": "A",
                "team_abbrev": "AAA",
                "salary": 8000,
                "projection": 18.5,
                "projected_own_pct": 6.0,
                "actual_fpts": 24.0,
                "actual_own_pct": 3.2,
                "opp_implied": 3.1,
                "team_ml": -150,
            }
        ),
        enrich_pitcher_row(
            {
                "slate_id": 2,
                "name": "B",
                "team_abbrev": "BBB",
                "salary": 8200,
                "projection": 19.0,
                "projected_own_pct": 7.5,
                "actual_fpts": 18.0,
                "actual_own_pct": 6.5,
                "opp_implied": 3.0,
                "team_ml": -140,
            }
        ),
    ]

    summary = summarize_bucket_rows(
        rows,
        bucket_key="projection_bucket",
        smash_threshold=20.0,
        elite_threshold=25.0,
        underowned_threshold=5.0,
    )
    row = next(item for item in summary if item["bucket"] == "18-21.9")

    assert row["rows"] == 2
    assert row["hit20_rate"] == 50.0
    assert row["hit25_rate"] == 0.0
    assert row["underowned_hit20_rate"] == 50.0


def test_score_pitcher_candidate_uses_bucket_rates_and_notes() -> None:
    bucket_summaries = {
        "projection_bucket": [
            {"bucket": "18-21.9", "rows": 20, "hit20_rate": 50.0, "hit25_rate": 30.0, "underowned_hit20_rate": 12.0},
        ],
        "value_bucket": [
            {"bucket": "1.8-2.19x", "rows": 18, "hit20_rate": 42.0, "hit25_rate": 22.0, "underowned_hit20_rate": 11.0},
        ],
        "projected_own_bucket": [
            {"bucket": "5-9.9", "rows": 16, "hit20_rate": 33.0, "hit25_rate": 14.0, "underowned_hit20_rate": 20.0},
        ],
        "opp_implied_bucket": [
            {"bucket": "<3.2", "rows": 15, "hit20_rate": 44.0, "hit25_rate": 16.0, "underowned_hit20_rate": 10.0},
        ],
        "moneyline_bucket": [
            {"bucket": "fav160+", "rows": 12, "hit20_rate": 58.0, "hit25_rate": 21.0, "underowned_hit20_rate": 15.0},
        ],
        "salary_bucket": [],
    }
    lookup = build_bucket_lookup(bucket_summaries)

    candidate = enrich_pitcher_row(
        {
            "name": "Pitcher X",
            "team_abbrev": "ABC",
            "salary": 8500,
            "projection": 18.7,
            "linestar_proj": 17.9,
            "our_proj": 18.7,
            "projected_own_pct": 8.1,
            "our_own_pct": 7.2,
            "opp_implied": 3.0,
            "team_ml": -165,
            "is_home": True,
        }
    )
    scored = score_pitcher_candidate(candidate, lookup, min_sample=8)

    assert scored["projection_bucket"] == "18-21.9"
    assert scored["value_bucket"] == "1.8-2.19x"
    assert scored["projected_own_bucket"] == "5-9.9"
    assert scored["lineup_score"] == 33.21
    assert scored["contrarian_score"] == 15.45
    assert any("Projection 18-21.9" in note for note in scored["notes"])
