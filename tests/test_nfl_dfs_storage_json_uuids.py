"""json_uuids scans every NFL jsonb column; its alias must not collide with a real column."""
import re

from ingest import nfl_dfs_storage


class FakeDb:
    def __init__(self):
        self.queries = []

    def execute(self, sql, params=None):
        self.queries.append(sql)
        if "information_schema.columns" in sql:
            # nfl_ownership_news_freezes has a column literally named "m".
            return [{"table_name": "nfl_ownership_news_freezes", "column_name": "design"}]
        if "nfl_dfs_projection_runs" in sql:
            return [{"run_id": "11111111-1111-1111-1111-111111111111"}]
        return [{"id": "11111111-1111-1111-1111-111111111111"}, {"id": "not-a-run"}]


def test_scan_query_uses_qualified_aliases():
    db = FakeDb()
    found = nfl_dfs_storage.json_uuids(db)
    assert found == {"11111111-1111-1111-1111-111111111111"}
    scan = db.queries[-1]
    assert "uuid_match.arr[1]" in scan
    assert 'src."design"' in scan
    # A bare one-letter alias collides with any table that has such a column.
    assert not re.search(r"\bm\[1\]", scan)
