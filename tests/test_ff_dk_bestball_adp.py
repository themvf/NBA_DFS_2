from ingest.ff_dk_bestball_adp import parse_dk_predraft_csv


def test_parse_dk_predraft_csv_uses_row_order_and_preserves_missing_adp():
    rows = parse_dk_predraft_csv("""ID,Name,Position,ADP,Team,,Instructions
1214154,Jahmyr Gibbs,RB,1.083701,DET,,Rank players
1228244,Bijan Robinson,RB,2.0662584,ATL,,
1648361,Ben Patterson,WR,,CLE,,
""")

    assert len(rows) == 3
    assert rows[0]["playerId"] == 1214154
    assert rows[0]["rank"] == 1
    assert rows[0]["averageDraftPosition"] == 1.083701
    assert rows[2]["rank"] == 3
    assert rows[2]["averageDraftPosition"] is None
