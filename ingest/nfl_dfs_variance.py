"""Replay the frozen baseline study into separate auditable variance artifacts."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
from model.nfl_dfs_historical import artifact_digest
from model.nfl_dfs_variance import study


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--predictions", type=Path, default=Path("artifacts/nfl_dfs_research_36cbc63d06d706a9/8bab909112d93a5d/predictions.json.gz"))
    args = parser.parse_args()
    with gzip.open(args.predictions, "rt") as f:
        samples = json.load(f)
    report, predictions = study(samples)
    report["input_sha256"] = hashlib.sha256(args.predictions.read_bytes()).hexdigest()
    report["code_sha256"] = hashlib.sha256(Path("model/nfl_dfs_variance.py").read_bytes()).hexdigest()
    report["artifact_digest"] = artifact_digest({"report": report, "predictions": predictions})
    directory = Path("artifacts") / f"nfl_dfs_variance_{report['artifact_digest'][:16]}"
    directory.mkdir(exist_ok=True)
    for name, payload in (("report.json", report), ("predictions.json", predictions)):
        path = directory / name
        content = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        if path.exists():
            if path.read_text() != content:
                raise ValueError("Content-addressed artifact collision")
        else:
            with path.open("x") as f:
                f.write(content)
    print(json.dumps({"report": str(directory / "report.json"), "predictions": len(predictions), "positions": report["positions"]}, indent=2))


if __name__ == "__main__":
    main()
