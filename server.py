from __future__ import annotations

import cgi
import json
import shutil
import subprocess
import sys
import time
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
WEB_DIR = ROOT / "web"
RUNS_DIR = ROOT / "runs"
DEFAULT_DATA_DIR = ROOT / "data"
MAX_UPLOAD_BYTES = 256 * 1024 * 1024


def sanitize_file_name(raw: str | None) -> str:
    if not raw:
        return "upload.bin"
    name = Path(raw).name
    return "".join(char if char.isascii() and (char.isalnum() or char in "._-") else "_" for char in name)


def field_items(form: cgi.FieldStorage, name: str) -> list[cgi.FieldStorage]:
    item = form[name] if name in form else []
    if isinstance(item, list):
        return item
    return [item]


def field_text(form: cgi.FieldStorage, name: str, default: str) -> str:
    if name not in form:
        return default
    item = form[name]
    if isinstance(item, list):
        item = item[0]
    value = item.value
    return str(value) if value not in (None, "") else default


def save_file_field(item: cgi.FieldStorage, run_dir: Path) -> Path:
    file_name = sanitize_file_name(item.filename)
    target = run_dir / file_name
    with target.open("wb") as output:
        if item.file is not None:
            shutil.copyfileobj(item.file, output)
        else:
            value = item.value
            if isinstance(value, str):
                output.write(value.encode("utf-8"))
            else:
                output.write(value)
    return target


def run_python_engine(args: list[str]) -> Any:
    process = subprocess.run(
        [sys.executable, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if process.returncode != 0:
        raise RuntimeError(process.stderr.strip() or process.stdout.strip() or "python engine failed")
    return json.loads(process.stdout)


def default_datamodel() -> Path:
    path = DEFAULT_DATA_DIR / "datamodel.py"
    if not path.is_file():
        raise FileNotFoundError(f"default datamodel not found: {path}")
    return path


def default_price_files() -> list[Path]:
    paths = sorted(DEFAULT_DATA_DIR.glob("prices_round_5_day_*.csv"))
    if not paths:
        raise FileNotFoundError("default Round 5 prices CSV files not found")
    return paths


def default_trade_files() -> list[Path]:
    paths = sorted(DEFAULT_DATA_DIR.glob("trades_round_5_day_*.csv"))
    if not paths:
        raise FileNotFoundError("default Round 5 trades CSV files not found")
    return paths


def default_data_summary() -> dict[str, Any]:
    prices = default_price_files()
    trades = default_trade_files()
    return {
        "datamodel": str(default_datamodel()),
        "prices": [str(path) for path in prices],
        "trades": [str(path) for path in trades],
        "price_count": len(prices),
        "trade_count": len(trades),
    }


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, directory=str(WEB_DIR), **kwargs)

    def do_GET(self) -> None:
        if self.path == "/api/default-data":
            try:
                self.send_json(default_data_summary())
            except Exception as exc:
                self.send_json({"error": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        if self.path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def do_POST(self) -> None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length > MAX_UPLOAD_BYTES:
                self.send_json({"error": "upload too large"}, HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
                return

            if self.path == "/api/run":
                self.send_json(self.handle_run())
                return
            if self.path == "/api/analyze-log":
                self.send_json(self.handle_log_analysis())
                return
            self.send_json({"error": "unknown endpoint"}, HTTPStatus.NOT_FOUND)
        except Exception as exc:
            self.send_json({"error": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def parse_form(self) -> cgi.FieldStorage:
        return cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={
                "REQUEST_METHOD": "POST",
                "CONTENT_TYPE": self.headers.get("Content-Type", ""),
                "CONTENT_LENGTH": self.headers.get("Content-Length", "0"),
            },
        )

    def handle_run(self) -> Any:
        form = self.parse_form()
        run_dir = RUNS_DIR / f"sim_{int(time.time() * 1000)}"
        run_dir.mkdir(parents=True, exist_ok=True)

        strategies = [item for item in field_items(form, "strategy") if item.filename]
        if not strategies:
            raise ValueError("missing strategy .py file")
        strategy_file = save_file_field(strategies[0], run_dir)

        datamodels = [item for item in field_items(form, "datamodel") if item.filename]
        if datamodels:
            datamodel_file = save_file_field(datamodels[0], run_dir)
        else:
            fallback = default_datamodel()
            datamodel_file = run_dir / "datamodel.py"
            shutil.copyfile(fallback, datamodel_file)

        price_files = [save_file_field(item, run_dir) for item in field_items(form, "prices") if item.filename]
        trade_files = [save_file_field(item, run_dir) for item in field_items(form, "trades") if item.filename]
        if not price_files:
            price_files = default_price_files()
        if not trade_files:
            trade_files = default_trade_files()

        args = [
            "python_engine/sim_runner.py",
            "--strategy",
            str(strategy_file),
            "--datamodel",
            str(datamodel_file),
            "--simulations",
            field_text(form, "simulations", "1"),
            "--drift",
            field_text(form, "drift", "0"),
            "--vol",
            field_text(form, "vol", "1"),
            "--hurst",
            field_text(form, "hurst", "0.5"),
            "--seed",
            field_text(form, "seed", "42"),
            "--output-dir",
            str(run_dir),
        ]
        if field_text(form, "randomize_params", "false").lower() in {"1", "true", "yes", "on"}:
            args.append("--randomize-params")
        if field_text(form, "randomize_drift", "false").lower() in {"1", "true", "yes", "on"}:
            args.append("--randomize-drift")
        if field_text(form, "randomize_vol", "false").lower() in {"1", "true", "yes", "on"}:
            args.append("--randomize-vol")
        if field_text(form, "randomize_hurst", "false").lower() in {"1", "true", "yes", "on"}:
            args.append("--randomize-hurst")

        for path in price_files:
            args.extend(["--price-file", str(path)])
        for path in trade_files:
            args.extend(["--trade-file", str(path)])

        return run_python_engine(args)

    def handle_log_analysis(self) -> Any:
        form = self.parse_form()
        run_dir = RUNS_DIR / f"log_{int(time.time() * 1000)}"
        run_dir.mkdir(parents=True, exist_ok=True)
        logs = [item for item in field_items(form, "log") if item.filename]
        if not logs:
            raise ValueError("missing log file")
        log_file = save_file_field(logs[0], run_dir)
        return run_python_engine(["python_engine/log_analyzer.py", "--log-file", str(log_file)])

    def send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    RUNS_DIR.mkdir(exist_ok=True)
    server = ThreadingHTTPServer(("127.0.0.1", 8080), Handler)
    print("Brownian MM Lab ready: http://127.0.0.1:8080")
    print("Drop any IMC-style Trader .py file. Local data/ datamodel/prices/trades are loaded automatically.")
    server.serve_forever()


if __name__ == "__main__":
    main()
