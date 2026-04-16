# BTC Tokyo Session Backtest

Backtest of a BTC/USDT strategy based on the Tokyo session range, H1 validation candle, M15 entries, and 1m intrabar resolution for ambiguous candles.

## Files

- `backtest.py`: main backtest script
- `download_data.py`: downloads 1h and 15m data
- `csv/`: local CSV files used by the backtest

## Run

```bash
python backtest.py