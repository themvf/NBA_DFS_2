"""nfl-ownership-fit-showdown-v1: the committed model reproduces the shared fixture."""
import json
from pathlib import Path

import pandas as pd

from model.nfl_showdown_ownership_fit import FEATURES, VERSION, predict

ROOT = Path(__file__).resolve().parents[1]


def test_committed_model_reproduces_fixture():
    model = json.loads((ROOT / "web/src/data/nfl-showdown-ownership-fit-v1.json").read_text(encoding="utf-8"))
    fixture = json.loads((ROOT / "tests/fixtures/nfl_showdown_ownership_fit_fixture.json").read_text(encoding="utf-8"))
    assert model["version"] == VERSION and model["features"] == list(FEATURES)
    slate = pd.DataFrame(fixture["players"])
    flex, cpt = predict(model, slate)
    assert abs(flex.sum() - 500) < 0.01 and abs(cpt.sum() - 100) < 0.01
    for f, c, row in zip(flex, cpt, fixture["players"]):
        assert abs(f - row["flex"]) < 0.01 and abs(c - row["captain"]) < 0.01


def test_out_and_pointless_players_get_nothing():
    model = json.loads((ROOT / "web/src/data/nfl-showdown-ownership-fit-v1.json").read_text(encoding="utf-8"))
    slate = pd.DataFrame(json.loads((ROOT / "tests/fixtures/nfl_showdown_ownership_fit_fixture.json").read_text(encoding="utf-8"))["players"])
    slate["is_out"] = False
    slate.loc[0, "is_out"] = True
    slate.loc[1, "pts"] = 0.0
    flex, cpt = predict(model, slate)
    assert flex[0] == cpt[0] == flex[1] == cpt[1] == 0
    assert (flex + cpt <= 95 + 1e-9).all() and (cpt <= 50 + 1e-9).all()
