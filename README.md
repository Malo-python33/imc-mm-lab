# Brownian MM Lab

Local Python app for stress-testing IMC market-making strategies against simulated mid-price paths.

## Run

```powershell
python server.py
```

Then open:

```text
http://127.0.0.1:8080
```

## Inputs

- Any IMC-style strategy `.py` file exposing `class Trader`.
- `datamodel.py` is loaded automatically from `data/datamodel.py`.
- IMC prices are loaded automatically from `data/prices_round_5_day_X.csv`.
- IMC trades are loaded automatically from `data/trades_round_5_day_X.csv`.

The UI only asks you to drop the strategy file.

## Bundled Data

This project is self-contained for GitHub. The `data/` folder includes:

- `datamodel.py`
- `prices_round_5_day_2.csv`
- `prices_round_5_day_3.csv`
- `prices_round_5_day_4.csv`
- `trades_round_5_day_2.csv`
- `trades_round_5_day_3.csv`
- `trades_round_5_day_4.csv`

## Simulation Model

For every uploaded price row, the engine keeps the historical book shape:

- bid/ask offsets versus historical mid
- visible volumes
- timestamps
- products

For every uploaded trade row, the engine keeps the market-taker footprint:

- timestamp
- product
- quantity
- inferred side
- reserve price offset versus historical mid

The UI shows the extracted taker/bot map directly:

- events by asset
- buy/sell split
- total quantity
- average reserve offset versus mid
- number of hardcoded timestamps detected

Then it replaces the mid-price path with a simulated Brownian-style path.

The Hurst parameter is implemented as an AR-style persistence approximation:

```text
rho = 2 * hurst - 1
increment = drift + rho * previous_increment + sqrt(1 - rho^2) * vol * gaussian_noise
```

This is not a mathematically exact fractional Brownian motion, but it gives a useful stress-control:

- `hurst < 0.5`: more mean-reverting increments
- `hurst = 0.5`: ordinary random walk increments
- `hurst > 0.5`: more persistent/trending increments

## Outputs

Each run writes CSV files in `runs/<run_id>/`:

- `path_summary.csv`
- `asset_path_summary.csv`
- `fills_preview_path0.csv`

The UI also shows:

- PnL distribution
- edge vs carry
- robust PnL: `edge + min(carry, 0)`
- asset ranking
- path ranking
- per-path asset detail for the first 200 paths

## Parameter Randomization

Drift, volatility, and Hurst can be fixed or randomized independently:

- fixed: uses the value in the input
- random drift: uniform around zero, scaled by the volatility input
- random vol: random multiplier around the volatility input
- random Hurst: random value between `0.35` and `0.75`

## Architecture

- `server.py`: local Python server and upload handler.
- `web/`: drag-and-drop UI.
- `python_engine/sim_runner.py`: imports the uploaded `Trader` and runs the Brownian replay.
- `python_engine/log_analyzer.py`: analyzes existing logs or decomposition CSV files.
