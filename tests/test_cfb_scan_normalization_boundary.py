from model.line_alerts import scan


class QueryRecorder:
    def __init__(self):
        self.query = ""
        self.params = ()

    def execute(self, query, params):
        self.query, self.params = query, params
        return []


def test_cfb_scan_defers_newer_unnormalized_capture_instead_of_using_older_quote():
    db = QueryRecorder()
    assert scan(db, "cfb") == 0
    assert "WITH latest AS" in db.query
    assert db.query.index("SELECT * FROM latest") < db.query.index("FROM cfb_engine_captures")
    assert "c.history_id=latest.history_id" in db.query
    assert db.params == ("cfb",)


def test_other_sports_do_not_require_cfb_normalization_tables():
    db = QueryRecorder()
    assert scan(db, "nfl") == 0
    assert "cfb_engine_captures" not in db.query
