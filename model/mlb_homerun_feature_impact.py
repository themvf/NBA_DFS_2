"""Generate a readable MLB HR feature-impact table from the v2 report.

Usage:
    python -m model.mlb_homerun_feature_impact
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from model.mlb_homerun_train import DEFAULT_OUTPUT, FEATURE_GROUPS

DEFAULT_MARKDOWN = Path(__file__).resolve().with_name("mlb_homerun_v2_feature_impact.md")
DEFAULT_JSON = Path(__file__).resolve().with_name("mlb_homerun_v2_feature_impact.json")


def _pct(value: Any, digits: int = 3) -> str:
    if value is None:
        return "-"
    return f"{float(value) * 100:.{digits}f}%"


def _num(value: Any, digits: int = 6) -> str:
    if value is None:
        return "-"
    return f"{float(value):.{digits}f}"


def _feature_group(feature: str) -> str:
    matches = [group for group, features in FEATURE_GROUPS.items() if feature in features]
    return ", ".join(matches) if matches else "base_context"


def _direction(coefficient: float | None, tree_importance: float | None) -> str:
    if coefficient is None:
        return "neutral"
    if abs(coefficient) < 0.01 and (tree_importance or 0) < 0.0005:
        return "neutral"
    return "higher HR probability" if coefficient > 0 else "lower HR probability"


def build_feature_impact(report_path: Path) -> dict[str, Any]:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    features: list[str] = report["features"]
    logistic_rows = {
        row["feature"]: row
        for row in report["featureAnalysis"]["logisticStandardizedCoefficients"]
    }
    tree_rows = {
        row["feature"]: row
        for row in report["featureAnalysis"]["histGradientBoostingPermutationImportance"]
    }
    deployed = report["deployableModel"]
    medians = dict(zip(deployed["features"], deployed["imputerMedians"], strict=True))

    rows = []
    for feature in features:
        logistic = logistic_rows.get(feature, {})
        tree = tree_rows.get(feature, {})
        coefficient = logistic.get("coefficient")
        tree_importance = tree.get("importanceMean")
        abs_coefficient = logistic.get("absCoefficient")
        rows.append({
            "feature": feature,
            "group": _feature_group(feature),
            "direction": _direction(coefficient, tree_importance),
            "logisticCoefficient": coefficient,
            "logisticAbsCoefficient": abs_coefficient,
            "treePermutationAveragePrecisionDrop": tree_importance,
            "treePermutationStd": tree.get("importanceStd"),
            "imputerMedian": medians.get(feature),
        })

    rows.sort(
        key=lambda row: (
            row["treePermutationAveragePrecisionDrop"] or 0.0,
            row["logisticAbsCoefficient"] or 0.0,
        ),
        reverse=True,
    )
    return {
        "modelVersion": report["modelVersion"],
        "featureSource": report["data"]["featureSource"],
        "testStart": report["data"]["testStart"],
        "testRows": report["data"]["testRows"],
        "testPositiveRows": report["data"]["testPositiveRows"],
        "groupedPermutationImportance": report["featureAnalysis"]["groupedPermutationImportance"],
        "features": rows,
    }


def write_markdown(summary: dict[str, Any], output: Path) -> None:
    lines = [
        "# MLB Homerun v2 Feature Impact",
        "",
        f"Model version: `{summary['modelVersion']}`",
        f"Feature source: `{summary['featureSource']}`",
        f"Test split: `{summary['testStart']}` onward, {summary['testRows']:,} rows, {summary['testPositiveRows']:,} HR-positive rows",
        "",
        "## Group Impact",
        "",
        "| Rank | Group | Avg Precision Drop |",
        "|---:|---|---:|",
    ]
    for idx, row in enumerate(summary["groupedPermutationImportance"], start=1):
        lines.append(f"| {idx} | {row['group']} | {_num(row['averagePrecisionDrop'])} |")

    lines.extend([
        "",
        "## Feature Impact",
        "",
        "Tree impact is permutation average-precision drop on the holdout set. Logistic coefficient is standardized; positive means the model associates a higher feature value with higher HR probability.",
        "",
        "| Rank | Feature | Group | Direction | Tree AP Drop | Logistic Coef | Median Fill |",
        "|---:|---|---|---|---:|---:|---:|",
    ])
    for idx, row in enumerate(summary["features"], start=1):
        lines.append(
            "| "
            f"{idx} | `{row['feature']}` | {row['group']} | {row['direction']} | "
            f"{_num(row['treePermutationAveragePrecisionDrop'])} | "
            f"{_num(row['logisticCoefficient'])} | "
            f"{_num(row['imputerMedian'], 4)} |"
        )

    lines.append("")
    output.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate MLB HR v2 feature impact artifacts.")
    parser.add_argument("--report", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--json", type=Path, default=DEFAULT_JSON)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = build_feature_impact(args.report)
    args.json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    write_markdown(summary, args.markdown)
    print(f"Wrote {args.markdown}")
    print(f"Wrote {args.json}")


if __name__ == "__main__":
    main()
