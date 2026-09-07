from pathlib import Path
import re
import pytest
from ingest.sportsbook_policy import BOOKMAKERS, BOOKMAKER_KEYS, requested_bookmakers, selected_books, selected_event

def test_six_books_shared_across_all_capture_paths():
    from ingest.event_closing_lines import BOOKMAKERS as closes
    from ingest.mlb_terminal_capture import BOOKMAKERS as mlb
    from ingest.mlb_prop_odds import BOOKMAKERS as props
    from ingest.cfb_schedule import CFB_BOOKMAKERS
    from ingest.nfl_prop_probe import BOOKMAKERS as nfl_props
    assert closes == mlb == props == BOOKMAKERS
    assert tuple(CFB_BOOKMAKERS) == tuple(nfl_props) == BOOKMAKER_KEYS
    source=Path('web/src/lib/sportsbook-policy.ts').read_text(encoding='utf-8')
    keys=re.search(r'SPORTSBOOK_KEYS = \[(.*?)\]', source).group(1)
    assert tuple(re.findall(r'"([^" ]+)"', keys)) == BOOKMAKER_KEYS
    assert len(BOOKMAKER_KEYS) == 6

def test_overrides_cannot_reenable_excluded_books():
    assert requested_bookmakers() == BOOKMAKERS
    assert set(requested_bookmakers('draftkings,caesars,bovada').split(',')) == {'draftkings','williamhill_us'}
    with pytest.raises(ValueError): requested_bookmakers('bovada,coral')

def test_legacy_caesars_deduplicated_and_payload_filtered():
    assert selected_books({'caesars':1,'williamhill_us':2,'coral':9}) == {'williamhill_us':2}
    assert selected_books({'caesars':1,'betway':9}) == {'williamhill_us':1}
    assert selected_event({'id':'x','bookmakers':[{'key':'draftkings'},{'key':'coral'},{'key':'polymarket'}]}) == {'id':'x','bookmakers':[{'key':'draftkings'}]}
