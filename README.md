# BTC London Session Strategy

Multi-timeframe backtesting engine for a BTC/USDT intraday strategy based on Tokyo range dynamics and London session liquidity behavior.

---

## Overview

This project implements a quantitative trading strategy using:

* **H1 data** → defines the Tokyo session range and daily validation
* **M15 data** → executes trades during the London session
* **1-minute data (on demand)** → resolves ambiguous TP/SL events intrabar
* **Dynamic position sizing** → adapts risk based on volatility conditions

The objective is to model a structured intraday setup and evaluate its performance across multiple years of historical market data.

---

## Strategy Logic

1. Define the **Tokyo session range**
2. Validate the trading day using a **specific H1 candle**
3. Monitor price during the London session for entry conditions
4. Execute trades based on breakout behavior
5. Apply:

   * Stop Loss
   * Take Profit
6. If both TP and SL are touched within the same candle:

   * Resolve execution using **1-minute Binance data**
7. Track performance and export results

---

## Project Structure

```
.
├── backtest.py          # Main backtesting engine
├── download_data.py     # Optional script to download historical data
├── requirements.txt
├── README.md
└── csv/
    ├── BTC_USDT_1h.csv
    └── BTC_USDT_15m.csv
```

---

## Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## How to Run

The project is designed to run directly using the included CSV files:

```bash
python backtest.py
```

This will:

* run the full backtest
* print a performance summary in the terminal
* generate an Excel report (`backtest_report.xlsx`)

---

## Data Usage

### Default (Recommended)

The repository already includes the required datasets:

* `BTC_USDT_1h.csv`
* `BTC_USDT_15m.csv`

👉 This is the intended way to run the project.

---

### Optional: Rebuild the Dataset

If you prefer not to use the included CSV files, you can regenerate them:

```bash
python download_data.py
```

This script downloads historical Binance data and rebuilds the dataset locally.

---

### Intrabar Data (1-minute)

For certain edge cases (when both TP and SL are touched within the same candle), the backtest:

* automatically downloads **1-minute Binance data**
* stores it locally in `data_1m/`
* reuses it in future runs

No manual setup is required for this step.

---

## Results

Backtest period: August 2017 to August 2025

### Performance Summary

* Initial capital: **$2,000**
* Final capital: **$3,413.93**
* Net profit: **$1,413.93**
* Total trades: **878**
* Win rate: **19.1%**
* Profit factor: **1.28**
* Max drawdown: **11.5%**

### Trade Statistics

* Winning trades: **168**
* Losing trades: **710**
* Long trades: **419**
* Short trades: **459**
* Average trade: **$1.61**
* Best trade: **$216.17**
* Worst trade: **-$50.28**

### Equity Curve

![Equity Curve](equity_curve.png)

### Interpretation

The strategy operates with a relatively low win rate, but remains profitable due to favorable trade asymmetry and controlled drawdowns.

This suggests that the edge is not derived from frequent accuracy, but from capturing larger directional moves when session structure and volatility conditions align.

### Key Insights

- The strategy relies on **low win rate but positive expectancy**, indicating asymmetric payoff distribution.
- Performance is driven by capturing **high-volatility moves during London session**, rather than frequent small wins.
- The Tokyo range acts as a **structural reference**, filtering out non-tradable days.
- Intrabar resolution using 1-minute data improves realism by reducing execution ambiguity.

---

## Features

* Multi-timeframe backtesting (H1 + M15 + 1m)
* Dynamic position sizing
* Intrabar execution logic
* Realistic TP/SL resolution
* Full performance analytics:

  * PnL
  * Win rate
  * Drawdown
  * Expectancy
* Excel export with detailed breakdown

---

## Notes

* This project is intended for **research and educational purposes only**
* It does **not** constitute financial advice
* Results depend on data quality and execution assumptions

---

## Author

Developed as a personal quantitative research project focused on intraday BTC behavior and session-based market dynamics.
