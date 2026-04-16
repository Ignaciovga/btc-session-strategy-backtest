import os
import requests
import zipfile
from io import BytesIO
from pathlib import Path

import pandas as pd


SYMBOL = "BTCUSDT"
START_YEAR = 2017
END_YEAR = 2025

OUTPUT_DIR = Path("csv")
OUTPUT_DIR.mkdir(exist_ok=True)

INTERVALS = {
    "1h": OUTPUT_DIR / "BTC_USDT_1h.csv",
    "15m": OUTPUT_DIR / "BTC_USDT_15m.csv",
}


def download_month_zip(symbol: str, interval: str, year: int, month: int) -> bytes | None:
    filename = f"{symbol}-{interval}-{year}-{month:02d}.zip"
    url = (
        f"https://data.binance.vision/data/spot/monthly/klines/"
        f"{symbol}/{interval}/{filename}"
    )

    response = requests.get(url, timeout=60)
    if response.status_code != 200:
        return None
    return response.content


def zip_bytes_to_df(zip_content: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(BytesIO(zip_content)) as zf:
        csv_names = [name for name in zf.namelist() if name.endswith(".csv")]
        if not csv_names:
            raise ValueError("Zip file does not contain a CSV.")
        with zf.open(csv_names[0]) as f:
            df = pd.read_csv(f, header=None)

    df.columns = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
        "ignore",
    ]

    df = df[["timestamp", "open", "high", "low", "close", "volume"]].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.dropna().sort_values("timestamp").reset_index(drop=True)


def build_interval_csv(symbol: str, interval: str, start_year: int, end_year: int, output_path: Path) -> None:
    all_parts: list[pd.DataFrame] = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            print(f"Downloading {symbol} {interval} {year}-{month:02d} ...")
            zip_content = download_month_zip(symbol, interval, year, month)
            if zip_content is None:
                continue

            try:
                month_df = zip_bytes_to_df(zip_content)
                all_parts.append(month_df)
            except Exception as exc:
                print(f"Skipping {year}-{month:02d}: {exc}")

    if not all_parts:
        raise RuntimeError(f"No data downloaded for {symbol} {interval}")

    final_df = pd.concat(all_parts, ignore_index=True)
    final_df = final_df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(output_path, index=False)
    print(f"Saved {len(final_df):,} rows to {output_path}")


def main() -> None:
    for interval, output_path in INTERVALS.items():
        build_interval_csv(
            symbol=SYMBOL,
            interval=interval,
            start_year=START_YEAR,
            end_year=END_YEAR,
            output_path=output_path,
        )


if __name__ == "__main__":
    main()