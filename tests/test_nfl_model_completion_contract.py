"""New shadow candidates cannot drift under the same frozen implementation pin."""
from hashlib import sha256
import json
from pathlib import Path


def test_shadow_registration_matches_frozen_implementation():
    root=Path(__file__).resolve().parents[1]
    registration=json.loads((root/'docs/nfl-model-completion-candidates.json').read_text(encoding='utf-8-sig'))
    assert registration['authority']=='shadow_only'
    assert registration['forecast_promotion_allowed'] is False
    assert len(registration['implementation_hashes'])>=8
    for path,expected in registration['implementation_hashes'].items():
        actual=sha256((root/path).read_bytes().replace(b'\r\n',b'\n')).hexdigest()
        assert actual==expected,f'{path} changed: re-register before real-outcome evaluation'
