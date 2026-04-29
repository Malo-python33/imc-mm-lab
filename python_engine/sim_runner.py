from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import math
import pickle
import random
import shutil
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


RANDOM_DRIFT_VOL_FRACTION = 0.05
RANDOM_VOL_MULT_RANGE = (0.5, 1.75)
RANDOM_HURST_RANGE = (0.35, 0.75)
MIN_VOL = 0.01
MAX_VOL = 20.0
MIN_HURST = 0.05
MAX_HURST = 0.95
MARKET_CACHE_VERSION = 1


@dataclass
class BookRow:
    day: int
    timestamp: int
    product: str
    mid: float
    bid_offsets: list[tuple[float, int]]
    ask_offsets: list[tuple[float, int]]


@dataclass
class TakerEvent:
    day: int
    timestamp: int
    product: str
    side: str
    reserve_offset: float
    quantity: int


@dataclass
class Fill:
    path: int
    day: int
    timestamp: int
    product: str
    side: str
    price: int
    quantity: int
    mid: float
    edge: float
    position_after: int


@dataclass
class AssetState:
    position: int = 0
    cash: float = 0.0
    edge: float = 0.0
    carry: float = 0.0
    trades: int = 0
    quantity: int = 0
    min_position: int = 0
    max_position: int = 0
    abs_position_sum: float = 0.0
    position_sum: float = 0.0
    ticks: int = 0
    final_mid: float = 0.0
    first_mid: float | None = None
    last_mid: float | None = None
    fills: list[Fill] = field(default_factory=list)

    def mark_tick(self, mid: float) -> None:
        if self.first_mid is None:
            self.first_mid = mid
        self.last_mid = mid
        self.final_mid = mid
        self.position_sum += self.position
        self.abs_position_sum += abs(self.position)
        self.ticks += 1

    def pnl(self) -> float:
        return self.cash + self.position * self.final_mid


def parse_float(value: str | None, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    return float(value)


def parse_int(value: str | None, default: int = 0) -> int:
    if value is None or value == "":
        return default
    return int(float(value))


def infer_day_from_name(path: Path) -> int:
    stem = path.stem
    marker = "day_"
    if marker in stem:
        tail = stem.split(marker, 1)[1]
        digits = []
        for char in tail:
            if char == "-" or char.isdigit():
                digits.append(char)
            else:
                break
        if digits:
            return int("".join(digits))
    return 0


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def read_trade_ticks(paths: list[Path]) -> set[tuple[int, int]]:
    ticks: set[tuple[int, int]] = set()
    for path in paths:
        file_day = infer_day_from_name(path)
        with path.open("r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file, delimiter=";")
            for row in reader:
                product = str(row.get("symbol") or row.get("product") or "").strip()
                if not product:
                    continue
                quantity = abs(parse_int(row.get("quantity")))
                if quantity <= 0:
                    continue
                day = parse_int(row.get("day"), file_day)
                timestamp = parse_int(row.get("timestamp"))
                ticks.add((day, timestamp))
    return ticks


def read_prices(
    paths: list[Path],
    tick_filter: set[tuple[int, int]] | None = None,
) -> tuple[list[tuple[int, int]], dict[tuple[int, int, str], BookRow]]:
    rows_by_key: dict[tuple[int, int, str], BookRow] = {}
    ticks: set[tuple[int, int]] = set()

    for path in paths:
        with path.open("r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file, delimiter=";")
            for row in reader:
                day = parse_int(row.get("day"), infer_day_from_name(path))
                timestamp = parse_int(row.get("timestamp"))
                if tick_filter is not None and (day, timestamp) not in tick_filter:
                    continue
                product = str(row.get("product", "")).strip()
                if not product:
                    continue
                mid = parse_float(row.get("mid_price"))
                bid_offsets = []
                ask_offsets = []
                for level in (1, 2, 3):
                    bid_price = row.get(f"bid_price_{level}", "")
                    bid_volume = row.get(f"bid_volume_{level}", "")
                    ask_price = row.get(f"ask_price_{level}", "")
                    ask_volume = row.get(f"ask_volume_{level}", "")
                    if bid_price != "" and bid_volume != "":
                        bid_offsets.append((parse_float(bid_price) - mid, abs(parse_int(bid_volume))))
                    if ask_price != "" and ask_volume != "":
                        ask_offsets.append((parse_float(ask_price) - mid, abs(parse_int(ask_volume))))
                key = (day, timestamp, product)
                rows_by_key[key] = BookRow(day, timestamp, product, mid, bid_offsets, ask_offsets)
                ticks.add((day, timestamp))

    return sorted(ticks), rows_by_key


def read_taker_events(paths: list[Path], prices: dict[tuple[int, int, str], BookRow]) -> dict[tuple[int, int, str], list[TakerEvent]]:
    out: dict[tuple[int, int, str], list[TakerEvent]] = defaultdict(list)
    for path in paths:
        file_day = infer_day_from_name(path)
        with path.open("r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file, delimiter=";")
            for row in reader:
                day = parse_int(row.get("day"), file_day)
                timestamp = parse_int(row.get("timestamp"))
                product = str(row.get("symbol") or row.get("product") or "").strip()
                if not product:
                    continue
                book = prices.get((day, timestamp, product))
                if book is None:
                    continue
                trade_price = parse_float(row.get("price"))
                quantity = abs(parse_int(row.get("quantity")))
                if quantity <= 0:
                    continue
                side = "buy" if trade_price >= book.mid else "sell"
                out[(day, timestamp, product)].append(
                    TakerEvent(
                        day=day,
                        timestamp=timestamp,
                        product=product,
                        side=side,
                        reserve_offset=trade_price - book.mid,
                        quantity=quantity,
                    )
                )
    return out


def file_fingerprint(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def market_cache_path(args: argparse.Namespace) -> Path:
    payload = {
        "version": MARKET_CACHE_VERSION,
        "tick_mode": args.tick_mode,
        "prices": [file_fingerprint(path) for path in args.price_file],
        "trades": [file_fingerprint(path) for path in args.trade_file],
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:20]
    return args.output_dir.parent / ".cache" / f"market_{digest}.pkl"


def load_market_data(args: argparse.Namespace) -> tuple[
    list[tuple[int, int]],
    dict[tuple[int, int, str], BookRow],
    dict[tuple[int, int, str], list[TakerEvent]],
    dict[str, Any],
    list[str],
    set[tuple[int, int]],
    bool,
]:
    cache_path = market_cache_path(args) if args.tick_mode == "trade" else None
    if cache_path is not None and cache_path.is_file():
        try:
            with cache_path.open("rb") as file:
                cached = pickle.load(file)
            if cached.get("version") == MARKET_CACHE_VERSION:
                return (
                    cached["ticks"],
                    cached["books"],
                    cached["takers"],
                    cached["bot_summary"],
                    cached["products"],
                    cached["trade_ticks"],
                    True,
                )
        except Exception:
            pass

    trade_ticks = read_trade_ticks(args.trade_file)
    tick_filter = None if args.tick_mode == "all" else trade_ticks
    ticks, books = read_prices(args.price_file, tick_filter)
    takers = read_taker_events(args.trade_file, books)
    bot_summary = summarize_taker_events(takers)
    products = sorted({product for _day, _timestamp, product in books})

    if cache_path is not None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = cache_path.with_suffix(".tmp")
        with temp_path.open("wb") as file:
            pickle.dump(
                {
                    "version": MARKET_CACHE_VERSION,
                    "ticks": ticks,
                    "books": books,
                    "takers": takers,
                    "bot_summary": bot_summary,
                    "products": products,
                    "trade_ticks": trade_ticks,
                },
                file,
                protocol=pickle.HIGHEST_PROTOCOL,
            )
        temp_path.replace(cache_path)

    return ticks, books, takers, bot_summary, products, trade_ticks, False


def summarize_taker_events(takers: dict[tuple[int, int, str], list[TakerEvent]]) -> dict[str, Any]:
    total_events = 0
    total_quantity = 0
    unique_timestamps: set[tuple[int, int]] = set()
    per_asset: dict[str, dict[str, Any]] = {}
    per_timestamp: dict[tuple[int, int], dict[str, Any]] = {}

    for (day, timestamp, product), events in takers.items():
        unique_timestamps.add((day, timestamp))
        time_row = per_timestamp.setdefault(
            (day, timestamp),
            {"day": day, "timestamp": timestamp, "events": 0, "quantity": 0, "assets": set()},
        )
        asset_row = per_asset.setdefault(
            product,
            {
                "asset": product,
                "events": 0,
                "buy_events": 0,
                "sell_events": 0,
                "quantity": 0,
                "offset_sum": 0.0,
                "abs_offset_sum": 0.0,
                "min_offset": None,
                "max_offset": None,
                "timestamps": set(),
            },
        )
        for event in events:
            total_events += 1
            total_quantity += event.quantity
            time_row["events"] += 1
            time_row["quantity"] += event.quantity
            time_row["assets"].add(product)

            asset_row["events"] += 1
            asset_row["quantity"] += event.quantity
            asset_row["buy_events" if event.side == "buy" else "sell_events"] += 1
            asset_row["offset_sum"] += event.reserve_offset
            asset_row["abs_offset_sum"] += abs(event.reserve_offset)
            asset_row["min_offset"] = event.reserve_offset if asset_row["min_offset"] is None else min(asset_row["min_offset"], event.reserve_offset)
            asset_row["max_offset"] = event.reserve_offset if asset_row["max_offset"] is None else max(asset_row["max_offset"], event.reserve_offset)
            asset_row["timestamps"].add((day, timestamp))

    asset_rows = []
    for row in per_asset.values():
        events = max(1, int(row["events"]))
        asset_rows.append(
            {
                "asset": row["asset"],
                "events": row["events"],
                "buy_events": row["buy_events"],
                "sell_events": row["sell_events"],
                "quantity": row["quantity"],
                "avg_quantity": row["quantity"] / events,
                "avg_offset": row["offset_sum"] / events,
                "avg_abs_offset": row["abs_offset_sum"] / events,
                "min_offset": row["min_offset"] or 0.0,
                "max_offset": row["max_offset"] or 0.0,
                "unique_timestamps": len(row["timestamps"]),
            }
        )

    timestamp_rows = []
    for row in per_timestamp.values():
        timestamp_rows.append(
            {
                "day": row["day"],
                "timestamp": row["timestamp"],
                "events": row["events"],
                "quantity": row["quantity"],
                "assets": len(row["assets"]),
            }
        )

    asset_rows.sort(key=lambda row: row["events"], reverse=True)
    timestamp_rows.sort(key=lambda row: (row["day"], row["timestamp"]))
    top_timestamp_rows = sorted(timestamp_rows, key=lambda row: row["events"], reverse=True)[:30]

    return {
        "total_events": total_events,
        "total_quantity": total_quantity,
        "unique_timestamps": len(unique_timestamps),
        "asset_count": len(asset_rows),
        "assets": asset_rows,
        "top_timestamps": top_timestamp_rows,
    }


def install_datamodel(datamodel_path: Path, work_dir: Path) -> None:
    target = work_dir / "datamodel.py"
    if datamodel_path.resolve() != target.resolve():
        shutil.copyfile(datamodel_path, target)
    sys.path.insert(0, str(work_dir))


def load_strategy(strategy_path: Path, work_dir: Path) -> Any:
    target = work_dir / strategy_path.name
    if strategy_path.resolve() != target.resolve():
        shutil.copyfile(strategy_path, target)
    spec = importlib.util.spec_from_file_location("_uploaded_strategy", target)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot import strategy: {strategy_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["_uploaded_strategy"] = module
    spec.loader.exec_module(module)
    if not hasattr(module, "Trader"):
        raise RuntimeError("Uploaded strategy does not expose class Trader")
    return module.Trader


def sample_params(args: argparse.Namespace, rng: random.Random) -> tuple[float, float, float]:
    randomize_drift = args.randomize_params or args.randomize_drift
    randomize_vol = args.randomize_params or args.randomize_vol
    randomize_hurst = args.randomize_params or args.randomize_hurst

    base_vol = clamp(abs(args.vol), MIN_VOL, MAX_VOL)
    drift = (
        rng.uniform(-base_vol * RANDOM_DRIFT_VOL_FRACTION, base_vol * RANDOM_DRIFT_VOL_FRACTION)
        if randomize_drift
        else args.drift
    )
    vol = rng.uniform(*RANDOM_VOL_MULT_RANGE) * base_vol if randomize_vol else base_vol
    hurst = rng.uniform(*RANDOM_HURST_RANGE) if randomize_hurst else clamp(args.hurst, MIN_HURST, MAX_HURST)
    return drift, vol, hurst


def generate_mid_paths(
    products: list[str],
    ticks: list[tuple[int, int]],
    books: dict[tuple[int, int, str], BookRow],
    args: argparse.Namespace,
    path_index: int,
) -> tuple[dict[tuple[int, int, str], float], dict[str, dict[str, float]]]:
    rng = random.Random(args.seed + path_index * 1009)
    mids: dict[tuple[int, int, str], float] = {}
    params_by_product: dict[str, dict[str, float]] = {}

    for product in products:
        product_ticks = [tick for tick in ticks if (tick[0], tick[1], product) in books]
        if not product_ticks:
            continue
        drift, vol, hurst = sample_params(args, rng)
        rho = max(-0.95, min(0.95, 2.0 * hurst - 1.0))
        shock_scale = math.sqrt(max(1e-9, 1.0 - rho * rho))
        first_book = books[(product_ticks[0][0], product_ticks[0][1], product)]
        mid = first_book.mid
        prev_inc = 0.0
        params_by_product[product] = {"drift": drift, "vol": vol, "hurst": hurst}

        for idx, (day, timestamp) in enumerate(product_ticks):
            if idx > 0:
                z = rng.gauss(0.0, 1.0)
                inc = drift + rho * prev_inc + shock_scale * vol * z
                mid = max(1.0, mid + inc)
                prev_inc = inc
            mids[(day, timestamp, product)] = mid

    return mids, params_by_product


def build_order_depth(book: BookRow, sim_mid: float, datamodel: Any) -> Any:
    order_depth = datamodel.OrderDepth()
    for offset, volume in book.bid_offsets:
        price = int(round(sim_mid + offset))
        order_depth.buy_orders[price] = order_depth.buy_orders.get(price, 0) + volume
    for offset, volume in book.ask_offsets:
        price = int(round(sim_mid + offset))
        order_depth.sell_orders[price] = order_depth.sell_orders.get(price, 0) - volume
    return order_depth


def build_state(
    datamodel: Any,
    trader_data: str,
    day: int,
    timestamp: int,
    products: list[str],
    books: dict[tuple[int, int, str], BookRow],
    mids: dict[tuple[int, int, str], float],
    asset_states: dict[str, AssetState],
) -> Any:
    order_depths = {}
    listings = {}
    for product in products:
        book = books.get((day, timestamp, product))
        if book is None:
            continue
        sim_mid = mids[(day, timestamp, product)]
        order_depths[product] = build_order_depth(book, sim_mid, datamodel)
        listings[product] = datamodel.Listing(product, product, "XIRECS")

    position = {product: state.position for product, state in asset_states.items() if state.position != 0}
    observations = datamodel.Observation({}, {})
    return datamodel.TradingState(
        trader_data,
        timestamp,
        listings,
        order_depths,
        {},
        {},
        position,
        observations,
    )


def match_orders(
    path_index: int,
    day: int,
    timestamp: int,
    product: str,
    orders: list[Any],
    takers: list[TakerEvent],
    sim_mid: float,
    state: AssetState,
) -> None:
    if not orders or not takers:
        return

    remaining = [event.quantity for event in takers]
    for order in orders:
        order_qty = int(order.quantity)
        if order_qty == 0:
            continue
        order_price = int(order.price)

        if order_qty > 0:
            open_qty = order_qty
            for idx, event in enumerate(takers):
                if open_qty <= 0:
                    break
                if remaining[idx] <= 0 or event.side != "sell":
                    continue
                reserve_price = sim_mid + event.reserve_offset
                if order_price < reserve_price:
                    continue
                fill_qty = min(open_qty, remaining[idx])
                apply_fill(path_index, day, timestamp, product, "buy", order_price, fill_qty, sim_mid, state)
                open_qty -= fill_qty
                remaining[idx] -= fill_qty

        if order_qty < 0:
            open_qty = -order_qty
            for idx, event in enumerate(takers):
                if open_qty <= 0:
                    break
                if remaining[idx] <= 0 or event.side != "buy":
                    continue
                reserve_price = sim_mid + event.reserve_offset
                if order_price > reserve_price:
                    continue
                fill_qty = min(open_qty, remaining[idx])
                apply_fill(path_index, day, timestamp, product, "sell", order_price, fill_qty, sim_mid, state)
                open_qty -= fill_qty
                remaining[idx] -= fill_qty


def apply_fill(
    path_index: int,
    day: int,
    timestamp: int,
    product: str,
    side: str,
    price: int,
    quantity: int,
    mid: float,
    state: AssetState,
) -> None:
    if side == "buy":
        signed_qty = quantity
        state.cash -= price * quantity
    else:
        signed_qty = -quantity
        state.cash += price * quantity

    edge = signed_qty * (mid - price)
    state.edge += edge
    state.position += signed_qty
    state.trades += 1
    state.quantity += quantity
    state.min_position = min(state.min_position, state.position)
    state.max_position = max(state.max_position, state.position)
    state.fills.append(
        Fill(
            path=path_index,
            day=day,
            timestamp=timestamp,
            product=product,
            side=side,
            price=price,
            quantity=quantity,
            mid=mid,
            edge=edge,
            position_after=state.position,
        )
    )


def run_one_path(
    path_index: int,
    trader_cls: Any,
    datamodel: Any,
    products: list[str],
    ticks: list[tuple[int, int]],
    books: dict[tuple[int, int, str], BookRow],
    takers: dict[tuple[int, int, str], list[TakerEvent]],
    args: argparse.Namespace,
) -> dict[str, Any]:
    trader = trader_cls()
    trader_data = ""
    mids, params_by_product = generate_mid_paths(products, ticks, books, args, path_index)
    asset_states = {product: AssetState() for product in products}

    for day, timestamp in ticks:
        for product in products:
            key = (day, timestamp, product)
            if key not in mids:
                continue
            state = asset_states[product]
            mid = mids[key]
            if state.last_mid is not None:
                state.carry += state.position * (mid - state.last_mid)
            state.mark_tick(mid)

        state_obj = build_state(datamodel, trader_data, day, timestamp, products, books, mids, asset_states)
        result, _conversions, trader_data = trader.run(state_obj)

        for product, orders in result.items():
            key = (day, timestamp, product)
            if key not in mids:
                continue
            match_orders(
                path_index,
                day,
                timestamp,
                product,
                list(orders),
                takers.get(key, []),
                mids[key],
                asset_states[product],
            )

    asset_rows = []
    fills_preview = []
    total_pnl = total_edge = total_carry = 0.0
    total_trades = 0
    for product, state in asset_states.items():
        pnl = state.pnl()
        total_pnl += pnl
        total_edge += state.edge
        total_carry += state.carry
        total_trades += state.trades
        asset_rows.append(
            {
                "path": path_index,
                "asset": product,
                "pnl": pnl,
                "edge": state.edge,
                "carry": state.carry,
                "trades": state.trades,
                "quantity": state.quantity,
                "end_position": state.position,
                "min_position": state.min_position,
                "max_position": state.max_position,
                "avg_position": state.position_sum / state.ticks if state.ticks else 0.0,
                "avg_abs_position": state.abs_position_sum / state.ticks if state.ticks else 0.0,
                "first_mid": state.first_mid or 0.0,
                "last_mid": state.last_mid or 0.0,
                "drift": params_by_product.get(product, {}).get("drift", 0.0),
                "vol": params_by_product.get(product, {}).get("vol", 0.0),
                "hurst": params_by_product.get(product, {}).get("hurst", 0.5),
            }
        )
        if path_index == 0:
            fills_preview.extend(fill.__dict__ for fill in state.fills[:200])

    return {
        "path": path_index,
        "pnl": total_pnl,
        "edge": total_edge,
        "carry": total_carry,
        "trades": total_trades,
        "assets": asset_rows,
        "fills_preview": fills_preview[:500],
    }


def summarize_paths(path_results: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(row["pnl"]) for row in path_results]
    edges = [float(row["edge"]) for row in path_results]
    carries = [float(row["carry"]) for row in path_results]
    pnls_sorted = sorted(pnls)

    def pct(q: float) -> float:
        if not pnls_sorted:
            return 0.0
        idx = min(len(pnls_sorted) - 1, max(0, round((len(pnls_sorted) - 1) * q)))
        return pnls_sorted[idx]

    return {
        "paths": len(path_results),
        "mean_pnl": sum(pnls) / len(pnls) if pnls else 0.0,
        "median_pnl": pct(0.5),
        "p05_pnl": pct(0.05),
        "p95_pnl": pct(0.95),
        "worst_pnl": min(pnls) if pnls else 0.0,
        "best_pnl": max(pnls) if pnls else 0.0,
        "positive_rate": sum(1 for pnl in pnls if pnl > 0) / len(pnls) if pnls else 0.0,
        "mean_edge": sum(edges) / len(edges) if edges else 0.0,
        "mean_carry": sum(carries) / len(carries) if carries else 0.0,
        "mean_robust_pnl": sum(edge + min(carry, 0.0) for edge, carry in zip(edges, carries)) / len(edges) if edges else 0.0,
    }


def summarize_assets(path_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for result in path_results:
        for row in result["assets"]:
            grouped[row["asset"]].append(row)

    out = []
    for asset, rows in grouped.items():
        pnls = [float(row["pnl"]) for row in rows]
        edges = [float(row["edge"]) for row in rows]
        carries = [float(row["carry"]) for row in rows]
        out.append(
            {
                "asset": asset,
                "mean_pnl": sum(pnls) / len(pnls),
                "mean_edge": sum(edges) / len(edges),
                "mean_carry": sum(carries) / len(carries),
                "positive_rate": sum(1 for pnl in pnls if pnl > 0) / len(pnls),
                "mean_robust_pnl": sum(edge + min(carry, 0.0) for edge, carry in zip(edges, carries)) / len(rows),
                "mean_trades": sum(float(row["trades"]) for row in rows) / len(rows),
                "mean_abs_position": sum(float(row["avg_abs_position"]) for row in rows) / len(rows),
            }
        )
    return sorted(out, key=lambda row: row["mean_pnl"], reverse=True)


def write_csv_outputs(output_dir: Path, path_results: list[dict[str, Any]]) -> dict[str, str]:
    path_csv = output_dir / "path_summary.csv"
    asset_csv = output_dir / "asset_path_summary.csv"
    fills_csv = output_dir / "fills_preview_path0.csv"

    with path_csv.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["path", "pnl", "edge", "carry", "trades"])
        writer.writeheader()
        for result in path_results:
            writer.writerow({key: result[key] for key in ["path", "pnl", "edge", "carry", "trades"]})

    asset_fields = [
        "path",
        "asset",
        "pnl",
        "edge",
        "carry",
        "trades",
        "quantity",
        "end_position",
        "min_position",
        "max_position",
        "avg_position",
        "avg_abs_position",
        "first_mid",
        "last_mid",
        "drift",
        "vol",
        "hurst",
    ]
    with asset_csv.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=asset_fields)
        writer.writeheader()
        for result in path_results:
            writer.writerows(result["assets"])

    fill_fields = ["path", "day", "timestamp", "product", "side", "price", "quantity", "mid", "edge", "position_after"]
    with fills_csv.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fill_fields)
        writer.writeheader()
        if path_results:
            writer.writerows(path_results[0].get("fills_preview", []))

    return {
        "path_summary": str(path_csv),
        "asset_path_summary": str(asset_csv),
        "fills_preview": str(fills_csv),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", required=True, type=Path)
    parser.add_argument("--datamodel", required=True, type=Path)
    parser.add_argument("--price-file", action="append", required=True, type=Path)
    parser.add_argument("--trade-file", action="append", required=True, type=Path)
    parser.add_argument("--simulations", type=int, default=1)
    parser.add_argument("--drift", type=float, default=0.0)
    parser.add_argument("--vol", type=float, default=1.0)
    parser.add_argument("--hurst", type=float, default=0.5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--randomize-params", action="store_true")
    parser.add_argument("--randomize-drift", action="store_true")
    parser.add_argument("--randomize-vol", action="store_true")
    parser.add_argument("--randomize-hurst", action="store_true")
    parser.add_argument("--tick-mode", choices=["trade", "all"], default="trade")
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    install_datamodel(args.datamodel, args.output_dir)
    import datamodel  # type: ignore

    trader_cls = load_strategy(args.strategy, args.output_dir)
    ticks, books, takers, bot_summary, products, trade_ticks, cache_hit = load_market_data(args)

    path_results = [
        run_one_path(index, trader_cls, datamodel, products, ticks, books, takers, args)
        for index in range(max(1, args.simulations))
    ]
    files = write_csv_outputs(args.output_dir, path_results)
    path_asset_limit = 200
    path_assets = [
        row
        for result in path_results[:path_asset_limit]
        for row in result["assets"]
    ]

    response = {
        "summary": summarize_paths(path_results),
        "assets": summarize_assets(path_results),
        "paths": [{key: result[key] for key in ["path", "pnl", "edge", "carry", "trades"]} for result in path_results],
        "path_assets": path_assets,
        "path_asset_limit": path_asset_limit,
        "fills_preview": path_results[0].get("fills_preview", []) if path_results else [],
        "bots": bot_summary,
        "engine": {
            "tick_mode": args.tick_mode,
            "ticks": len(ticks),
            "trade_ticks": len(trade_ticks),
            "book_rows": len(books),
            "products": len(products),
            "cache_hit": cache_hit,
            "vol_range": [MIN_VOL, MAX_VOL],
            "random_vol_multiplier_range": list(RANDOM_VOL_MULT_RANGE),
            "random_drift_abs_max": RANDOM_DRIFT_VOL_FRACTION,
            "random_hurst_range": list(RANDOM_HURST_RANGE),
        },
        "files": files,
    }
    print(json.dumps(response, separators=(",", ":")))


if __name__ == "__main__":
    main()
