"""nfl-ownership-fit-v1 (Classic shadow): the frozen model reproduces the shared fixture."""
import json
from pathlib import Path

import pandas as pd

from model.nfl_ownership_fit import FEATURES, VERSION, feature_frame, predict

ROOT = Path(__file__).resolve().parents[1]


def _load():
    model = json.loads((ROOT / "web/src/data/nfl-ownership-fit-v1.json").read_text(encoding="utf-8"))
    fixture = json.loads((ROOT / "tests/fixtures/nfl_ownership_fit_fixture.json").read_text(encoding="utf-8"))
    return model, fixture


def test_frozen_model_reproduces_fixture():
    model, fixture = _load()
    assert model["version"] == VERSION and model["features"] == list(FEATURES)
    assert model["fitted_at"] and len(model["training_contests"]) >= 4
    pred = predict(model, feature_frame(pd.DataFrame(fixture["players"])))
    for v, row in zip(pred, fixture["players"]):
        assert abs(v - row["own"]) < 0.01
    assert (pred <= model["cap"] + 1e-9).all()
