from __future__ import annotations

import argparse
import csv
import io
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def analyze_activity_log(text: str) -> dict[str, Any]:
    payload = json.loads(text)
    if "summary" in payload and "assets" in payload:
        return {"kind": "simulation_json", **payload}

    activities = payload.get("activitiesLog", "")
    if not activities:
        raise ValueError("JSON log does not contain activitiesLog")

    rows = list(csv.DictReader(io.StringIO(activities), delimiter=";"))
    by_asset = defaultdict(list)
    for row in rows:
        by_asset[row["product"]].append(row)

    assets = []
    total = 0.0
    for asset, asset_rows in by_asset.items():
        last = max(asset_rows, key=lambda row: int(float(row["timestamp"])))
        pnl = float(last["profit_and_loss"])
        total += pnl
        first_mid = float(asset_rows[0]["mid_price"])
        last_mid = float(last["mid_price"])
        assets.append(
            {
                "asset": asset,
                "pnl": pnl,
                "first_mid": first_mid,
                "last_mid": last_mid,
                "drift": last_mid - first_mid,
            }
        )

    assets.sort(key=lambda row: row["pnl"], reverse=True)
    return {
        "kind": "prosperity_log",
        "summary": {
            "assets": len(assets),
            "total_pnl": total,
            "positive_assets": sum(1 for row in assets if row["pnl"] > 0),
        },
        "assets": assets,
    }


def analyze_csv(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    if {"asset", "pnl", "edge", "carry"}.issubset(reader.fieldnames or []):
        grouped = defaultdict(lambda: {"pnl": 0.0, "edge": 0.0, "carry": 0.0, "trades": 0.0})
        for row in rows:
            asset = row["asset"]
            grouped[asset]["pnl"] += float(row.get("pnl", 0) or 0)
            grouped[asset]["edge"] += float(row.get("edge", 0) or 0)
            grouped[asset]["carry"] += float(row.get("carry", 0) or 0)
            grouped[asset]["trades"] += float(row.get("trades", 0) or 0)
        assets = [{"asset": asset, **values} for asset, values in grouped.items()]
        assets.sort(key=lambda row: row["pnl"], reverse=True)
        return {
            "kind": "decomposition_csv",
            "summary": {
                "assets": len(assets),
                "total_pnl": sum(row["pnl"] for row in assets),
                "total_edge": sum(row["edge"] for row in assets),
                "total_carry": sum(row["carry"] for row in assets),
            },
            "assets": assets,
        }

    raise ValueError("Unsupported CSV log format")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-file", required=True, type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.log_file.suffix.lower() == ".csv":
        result = analyze_csv(args.log_file)
    else:
        result = analyze_activity_log(args.log_file.read_text(encoding="utf-8"))
    print(json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()
