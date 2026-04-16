import math
import os
import requests
import zipfile
from io import BytesIO
from typing import Optional

import pandas as pd
from pandas.api.types import DatetimeTZDtype
from openpyxl.utils import get_column_letter

from pathlib import Path

# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).resolve().parent
CSV_DIR = BASE_DIR / "csv"
DATA_PATH = BASE_DIR / "data_1m"

START_DATE = "2017-08-01"
END_DATE = "2026-01-01"

H1_PATH = CSV_DIR / "BTC_USDT_1h.csv"
M15_PATH = CSV_DIR / "BTC_USDT_15m.csv"
OUTPUT_EXCEL = BASE_DIR / "backtest_report.xlsx"

INITIAL_CAPITAL = 2000
RISK_CONSTANT = 50
CHANNEL_PROXIMITY = 0.12

# Commission per side:
# 0.00005 = 0.005% on entry + 0.005% on exit
COMMISSION_RATE = 0.00005

VALIDATION_MIN_CLOSE_POSITION = 0.10
VALIDATION_MAX_CLOSE_POSITION = 0.90

TOKYO_START_HOUR = 23
TOKYO_END_HOUR = 6
VALIDATION_CANDLE_HOUR = 7

SESSION_START_HOUR = 8
SESSION_START_MINUTE = 15
SESSION_END_HOUR = 20

PRE_ALLOWED_START_HOUR = 8
PRE_ALLOWED_START_MINUTE = 15
ALLOWED_ENTRY_HOURS = {10, 11, 12, 13, 14}

STOP_LOSS_EXTENSION = 0.236
TAKE_PROFIT_LEVEL = 0.382

CHANNEL_NORMALIZER_MIN = 1.0
CHANNEL_NORMALIZER_MAX = 3.0
CHANNEL_BASE_CAPITAL = 1000


# =========================
# DATA LOADING
# =========================
def load_market_data(
    h1_path: str,
    m15_path: str,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_h1 = pd.read_csv(h1_path)
    df_m15 = pd.read_csv(m15_path)

    df_h1["timestamp"] = pd.to_datetime(df_h1["timestamp"], utc=True)
    df_m15["timestamp"] = pd.to_datetime(df_m15["timestamp"], utc=True)

    df_h1 = df_h1.set_index("timestamp").sort_index()
    df_m15 = df_m15.set_index("timestamp").sort_index()

    df_h1 = df_h1.loc[start_date:end_date]
    df_m15 = df_m15.loc[start_date:end_date]

    if df_h1.empty:
        raise ValueError("H1 dataframe is empty after filtering date range.")

    if df_m15.empty:
        raise ValueError("M15 dataframe is empty after filtering date range.")

    required_columns = ["open", "high", "low", "close"]
    for column in required_columns:
        if column not in df_h1.columns:
            raise ValueError(f"Missing column '{column}' in H1 data.")
        if column not in df_m15.columns:
            raise ValueError(f"Missing column '{column}' in M15 data.")

    return df_h1, df_m15


def remove_timezone_from_datetimes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for column in df.columns:
        dtype = df[column].dtype
        if isinstance(dtype, DatetimeTZDtype):
            df[column] = df[column].dt.tz_localize(None)

    return df


# =========================
# HELPERS
# =========================
def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(value, max_value))


def calculate_position_size_details(
    trade_side: str,
    entry_price: float,
    stop_loss: float,
    tokyo_range: float,
    tokyo_low: float,
    risk_constant: float,
) -> dict:
    if entry_price <= 0 or stop_loss <= 0 or tokyo_low <= 0:
        return {
            "position_size": 0.0,
            "risk_based_size": 0.0,
            "channel_based_size": 0.0,
            "range_pct": 0.0,
            "range_normalizer": 0.0,
        }

    if trade_side == "short":
        risk_fraction = (stop_loss / entry_price) - 1
    elif trade_side == "long":
        risk_fraction = 1 - (stop_loss / entry_price)
    else:
        raise ValueError(f"Invalid trade_side: {trade_side}")

    if risk_fraction <= 0:
        risk_based_size = 0.0
    else:
        risk_based_size = risk_constant / risk_fraction

    range_pct = (tokyo_range / tokyo_low) * 100 if tokyo_low != 0 else 0.0
    range_normalizer = clamp(
        range_pct,
        CHANNEL_NORMALIZER_MIN,
        CHANNEL_NORMALIZER_MAX,
    )
    channel_based_size = CHANNEL_BASE_CAPITAL * range_normalizer

    position_size = min(risk_based_size, channel_based_size)

    return {
        "position_size": max(position_size, 0.0),
        "risk_based_size": max(risk_based_size, 0.0),
        "channel_based_size": max(channel_based_size, 0.0),
        "range_pct": range_pct,
        "range_normalizer": range_normalizer,
    }


def calculate_trade_pnl(
    trade_side: str,
    position_size: float,
    entry_price: float,
    exit_price: float,
    commission_rate: float,
) -> tuple[float, float, float]:
    if position_size <= 0 or entry_price <= 0 or exit_price <= 0:
        return 0.0, 0.0, 0.0

    if trade_side == "short":
        gross_pnl = position_size * (1 - (exit_price / entry_price))
    elif trade_side == "long":
        gross_pnl = position_size * ((exit_price / entry_price) - 1)
    else:
        raise ValueError(f"Invalid trade_side: {trade_side}")

    entry_commission = position_size * commission_rate
    exit_commission = position_size * commission_rate
    total_commission = entry_commission + exit_commission

    net_pnl = gross_pnl - total_commission
    return gross_pnl, total_commission, net_pnl


def finalize_trade_record(
    trade_record: dict,
    exit_time: pd.Timestamp,
    exit_price: float,
    exit_reason: str,
    trade_side: str,
    position_size: float,
    entry_price: float,
    commission_rate: float,
) -> dict:
    gross_pnl, commission, net_pnl = calculate_trade_pnl(
        trade_side=trade_side,
        position_size=position_size,
        entry_price=entry_price,
        exit_price=exit_price,
        commission_rate=commission_rate,
    )

    trade_record["exit_time"] = exit_time
    trade_record["exit_price"] = exit_price
    trade_record["exit_reason"] = exit_reason
    trade_record["gross_pnl"] = gross_pnl
    trade_record["commission"] = commission
    trade_record["net_pnl"] = net_pnl
    trade_record["pnl"] = net_pnl

    if position_size != 0:
        trade_record["return_pct_on_position"] = (net_pnl / position_size) * 100

    if trade_record["entry_time"] is not None and exit_time is not None:
        holding_delta = exit_time - trade_record["entry_time"]
        trade_record["holding_minutes"] = holding_delta.total_seconds() / 60

    return trade_record


# =========================
# DAILY CONTEXT
# =========================
def create_day_record(day: pd.Timestamp) -> dict:
    return {
        "date": day.date(),
        "year": day.year,
        "month": day.month,
        "month_name": day.strftime("%B"),
        "weekday": day.weekday(),
        "weekday_name": day.strftime("%A"),
        "is_tradable": False,
        "skip_reason": None,
        "tokyo_start": None,
        "tokyo_end": None,
        "tokyo_high": None,
        "tokyo_low": None,
        "tokyo_range": None,
        "validation_high": None,
        "validation_low": None,
        "validation_close": None,
        "close_position_in_range": None,
        "trade_taken": False,
        "trade_side": None,
        "entry_time": None,
        "entry_price": None,
        "stop_loss": None,
        "take_profit": None,
        "exit_time": None,
        "exit_price": None,
        "exit_reason": None,
        "position_size": 0.0,
        "risk_based_size": 0.0,
        "channel_based_size": 0.0,
        "range_pct": 0.0,
        "range_normalizer": 0.0,
        "gross_pnl": 0.0,
        "commission": 0.0,
        "day_pnl": 0.0,
        "capital_after_day": None,
        "invalidated_day": False,
    }


def evaluate_tradable_day(day: pd.Timestamp, df_h1: pd.DataFrame) -> dict:
    day_record = create_day_record(day)

    tokyo_start = day - pd.Timedelta(days=1) + pd.Timedelta(hours=TOKYO_START_HOUR)
    tokyo_end = day + pd.Timedelta(hours=TOKYO_END_HOUR)
    validation_candle_time = day + pd.Timedelta(hours=VALIDATION_CANDLE_HOUR)

    day_record["tokyo_start"] = tokyo_start
    day_record["tokyo_end"] = tokyo_end

    if tokyo_end not in df_h1.index:
        day_record["skip_reason"] = "incomplete_h1_data"
        return day_record

    tokyo_range_df = df_h1.loc[tokyo_start:tokyo_end]

    if tokyo_range_df.empty:
        day_record["skip_reason"] = "missing_tokyo_session"
        return day_record

    tokyo_high = tokyo_range_df["high"].max()
    tokyo_low = tokyo_range_df["low"].min()
    tokyo_range_size = tokyo_high - tokyo_low

    day_record["tokyo_high"] = tokyo_high
    day_record["tokyo_low"] = tokyo_low
    day_record["tokyo_range"] = tokyo_range_size

    if tokyo_range_size == 0:
        day_record["skip_reason"] = "zero_tokyo_range"
        return day_record

    validation_candle = df_h1.loc[validation_candle_time:validation_candle_time]

    if validation_candle.empty:
        day_record["skip_reason"] = "missing_validation_candle"
        return day_record

    validation_high = validation_candle["high"].iloc[0]
    validation_low = validation_candle["low"].iloc[0]
    validation_close = validation_candle["close"].iloc[0]
    close_position_in_range = (validation_close - tokyo_low) / tokyo_range_size

    day_record["validation_high"] = validation_high
    day_record["validation_low"] = validation_low
    day_record["validation_close"] = validation_close
    day_record["close_position_in_range"] = close_position_in_range

    if (
        validation_high > tokyo_high
        or validation_low < tokyo_low
        or close_position_in_range < VALIDATION_MIN_CLOSE_POSITION
        or close_position_in_range > VALIDATION_MAX_CLOSE_POSITION
    ):
        day_record["skip_reason"] = "validation_candle_failed"
        return day_record

    day_record["is_tradable"] = True
    return day_record


# =========================
# TRADE PROCESSING
# =========================
def process_intraday_session(
    day: pd.Timestamp,
    df_m15: pd.DataFrame,
    tokyo_high: float,
    tokyo_low: float,
    tokyo_range: float,
    channel_proximity: float,
    risk_constant: float,
    commission_rate: float,
) -> dict:
    session_start = day + pd.Timedelta(hours=SESSION_START_HOUR, minutes=SESSION_START_MINUTE)
    session_end = day + pd.Timedelta(hours=SESSION_END_HOUR)

    intraday_window = df_m15.loc[session_start:session_end]

    trade_record = {
        "date": day.date(),
        "year": day.year,
        "month": day.month,
        "month_name": day.strftime("%B"),
        "weekday": day.weekday(),
        "weekday_name": day.strftime("%A"),
        "trade_taken": False,
        "trade_side": None,
        "tokyo_high": tokyo_high,
        "tokyo_low": tokyo_low,
        "tokyo_range": tokyo_range,
        "upper_entry": None,
        "lower_entry": None,
        "entry_time": None,
        "entry_price": None,
        "stop_loss": None,
        "take_profit": None,
        "exit_time": None,
        "exit_price": None,
        "exit_reason": None,
        "position_size": 0.0,
        "risk_based_size": 0.0,
        "channel_based_size": 0.0,
        "range_pct": 0.0,
        "range_normalizer": 0.0,
        "gross_pnl": 0.0,
        "commission": 0.0,
        "net_pnl": 0.0,
        "pnl": 0.0,
        "return_pct_on_position": 0.0,
        "holding_minutes": 0.0,
        "invalidated_day": False,
    }

    if intraday_window.empty:
        trade_record["exit_reason"] = "missing_m15_data"
        return trade_record

    upper_entry = tokyo_high + (tokyo_range * channel_proximity)
    lower_entry = tokyo_low - (tokyo_range * channel_proximity)
    take_profit = tokyo_low + (TAKE_PROFIT_LEVEL * tokyo_range)

    trade_record["upper_entry"] = upper_entry
    trade_record["lower_entry"] = lower_entry

    position_open = False
    trade_side = None
    entry_price = None
    stop_loss = None
    position_size = 0.0

    pre_allowed_start = day + pd.Timedelta(
    hours=PRE_ALLOWED_START_HOUR,
    minutes=PRE_ALLOWED_START_MINUTE,
    )
    pre_allowed_end = day + pd.Timedelta(hours=min(ALLOWED_ENTRY_HOURS)) - pd.Timedelta(minutes=1)

    pre_allowed_window = df_m15.loc[pre_allowed_start:pre_allowed_end]

    if not pre_allowed_window.empty:
        touched_before_allowed_hours = (
            (pre_allowed_window["high"] >= upper_entry) |
            (pre_allowed_window["low"] <= lower_entry)
        ).any()

        if touched_before_allowed_hours:
            trade_record["exit_reason"] = "setup_touched_before_allowed_hours"
            return trade_record

    for timestamp, row in intraday_window.iterrows():
        candle_high = row["high"]
        candle_low = row["low"]
        candle_close = row["close"]

        if not position_open:
            if timestamp.hour not in ALLOWED_ENTRY_HOURS:
                continue

            if candle_high >= upper_entry and candle_low <= lower_entry:
                trade_record["invalidated_day"] = True
                trade_record["exit_reason"] = "both_sides_touched_same_candle"
                return trade_record

            if candle_high >= upper_entry:
                trade_side = "short"
                entry_price = upper_entry
                stop_loss = tokyo_high + (tokyo_range * STOP_LOSS_EXTENSION)

                sizing = calculate_position_size_details(
                    trade_side=trade_side,
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    tokyo_range=tokyo_range,
                    tokyo_low=tokyo_low,
                    risk_constant=risk_constant,
                )

                position_size = sizing["position_size"]
                if position_size <= 0:
                    trade_record["exit_reason"] = "invalid_position_size"
                    return trade_record

                position_open = True

                trade_record["trade_taken"] = True
                trade_record["trade_side"] = trade_side
                trade_record["entry_time"] = timestamp
                trade_record["entry_price"] = entry_price
                trade_record["stop_loss"] = stop_loss
                trade_record["take_profit"] = take_profit
                trade_record["position_size"] = position_size
                trade_record["risk_based_size"] = sizing["risk_based_size"]
                trade_record["channel_based_size"] = sizing["channel_based_size"]
                trade_record["range_pct"] = sizing["range_pct"]
                trade_record["range_normalizer"] = sizing["range_normalizer"]

                touched_tp_same_candle = candle_low <= take_profit
                touched_sl_same_candle = candle_high >= stop_loss

                if touched_tp_same_candle and touched_sl_same_candle:
                    real = resolve_tp_sl_intrabar(
                        timestamp,
                        entry_price,
                        stop_loss,
                        take_profit,
                        trade_side,
                    )

                    if real == "SL":
                        return finalize_trade_record(
                            trade_record=trade_record,
                            exit_time=timestamp,
                            exit_price=stop_loss,
                            exit_reason="stop_loss_intrabar",
                            trade_side=trade_side,
                            position_size=position_size,
                            entry_price=entry_price,
                            commission_rate=commission_rate,
                        )

                    elif real == "TP":
                        return finalize_trade_record(
                            trade_record=trade_record,
                            exit_time=timestamp,
                            exit_price=take_profit,
                            exit_reason="take_profit_intrabar",
                            trade_side=trade_side,
                            position_size=position_size,
                            entry_price=entry_price,
                            commission_rate=commission_rate,
                        )

                    else:
                        trade_record["invalidated_day"] = True
                        trade_record["exit_reason"] = "intrabar_unknown"
                        return trade_record

                if touched_sl_same_candle:
                    return finalize_trade_record(
                        trade_record=trade_record,
                        exit_time=timestamp,
                        exit_price=stop_loss,
                        exit_reason="stop_loss_same_candle",
                        trade_side=trade_side,
                        position_size=position_size,
                        entry_price=entry_price,
                        commission_rate=commission_rate,
                    )

                continue

            if candle_low <= lower_entry:
                trade_side = "long"
                entry_price = lower_entry
                stop_loss = tokyo_low - (tokyo_range * STOP_LOSS_EXTENSION)

                sizing = calculate_position_size_details(
                    trade_side=trade_side,
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    tokyo_range=tokyo_range,
                    tokyo_low=tokyo_low,
                    risk_constant=risk_constant,
                )

                position_size = sizing["position_size"]
                if position_size <= 0:
                    trade_record["exit_reason"] = "invalid_position_size"
                    return trade_record

                position_open = True

                trade_record["trade_taken"] = True
                trade_record["trade_side"] = trade_side
                trade_record["entry_time"] = timestamp
                trade_record["entry_price"] = entry_price
                trade_record["stop_loss"] = stop_loss
                trade_record["take_profit"] = take_profit
                trade_record["position_size"] = position_size
                trade_record["risk_based_size"] = sizing["risk_based_size"]
                trade_record["channel_based_size"] = sizing["channel_based_size"]
                trade_record["range_pct"] = sizing["range_pct"]
                trade_record["range_normalizer"] = sizing["range_normalizer"]

                touched_tp_same_candle = candle_high >= take_profit
                touched_sl_same_candle = candle_low <= stop_loss

                if touched_tp_same_candle and touched_sl_same_candle:
                    real = resolve_tp_sl_intrabar(
                        timestamp,
                        entry_price,
                        stop_loss,
                        take_profit,
                        trade_side,
                    )

                    if real == "SL":
                        return finalize_trade_record(
                            trade_record=trade_record,
                            exit_time=timestamp,
                            exit_price=stop_loss,
                            exit_reason="stop_loss_intrabar",
                            trade_side=trade_side,
                            position_size=position_size,
                            entry_price=entry_price,
                            commission_rate=commission_rate,
                        )

                    elif real == "TP":
                        return finalize_trade_record(
                            trade_record=trade_record,
                            exit_time=timestamp,
                            exit_price=take_profit,
                            exit_reason="take_profit_intrabar",
                            trade_side=trade_side,
                            position_size=position_size,
                            entry_price=entry_price,
                            commission_rate=commission_rate,
                        )

                    else:
                        trade_record["invalidated_day"] = True
                        trade_record["exit_reason"] = "intrabar_unknown"
                        return trade_record

                if touched_sl_same_candle:
                    return finalize_trade_record(
                        trade_record=trade_record,
                        exit_time=timestamp,
                        exit_price=stop_loss,
                        exit_reason="stop_loss_same_candle",
                        trade_side=trade_side,
                        position_size=position_size,
                        entry_price=entry_price,
                        commission_rate=commission_rate,
                    )

                continue

            if timestamp.hour > max(ALLOWED_ENTRY_HOURS):
                trade_record["exit_reason"] = "no_entry_in_allowed_hours"
                return trade_record

            continue

        if trade_side == "short":
            if candle_low <= take_profit:
                return finalize_trade_record(
                    trade_record=trade_record,
                    exit_time=timestamp,
                    exit_price=take_profit,
                    exit_reason="take_profit",
                    trade_side=trade_side,
                    position_size=position_size,
                    entry_price=entry_price,
                    commission_rate=commission_rate,
                )

            if candle_high >= stop_loss:
                return finalize_trade_record(
                    trade_record=trade_record,
                    exit_time=timestamp,
                    exit_price=stop_loss,
                    exit_reason="stop_loss",
                    trade_side=trade_side,
                    position_size=position_size,
                    entry_price=entry_price,
                    commission_rate=commission_rate,
                )

        elif trade_side == "long":
            if candle_high >= take_profit:
                return finalize_trade_record(
                    trade_record=trade_record,
                    exit_time=timestamp,
                    exit_price=take_profit,
                    exit_reason="take_profit",
                    trade_side=trade_side,
                    position_size=position_size,
                    entry_price=entry_price,
                    commission_rate=commission_rate,
                )

            if candle_low <= stop_loss:
                return finalize_trade_record(
                    trade_record=trade_record,
                    exit_time=timestamp,
                    exit_price=stop_loss,
                    exit_reason="stop_loss",
                    trade_side=trade_side,
                    position_size=position_size,
                    entry_price=entry_price,
                    commission_rate=commission_rate,
                )

        if timestamp.hour == SESSION_END_HOUR and timestamp.minute == 0:
            return finalize_trade_record(
                trade_record=trade_record,
                exit_time=timestamp,
                exit_price=candle_close,
                exit_reason="forced_close",
                trade_side=trade_side,
                position_size=position_size,
                entry_price=entry_price,
                commission_rate=commission_rate,
            )

    if position_open:
        last_timestamp = intraday_window.index[-1]
        last_close = intraday_window.iloc[-1]["close"]

        return finalize_trade_record(
            trade_record=trade_record,
            exit_time=last_timestamp,
            exit_price=last_close,
            exit_reason="session_end_fallback",
            trade_side=trade_side,
            position_size=position_size,
            entry_price=entry_price,
            commission_rate=commission_rate,
        )

    trade_record["exit_reason"] = "no_entry_in_allowed_hours"
    return trade_record


# =========================
# SUMMARY / METRICS
# =========================
def build_empty_summary(daily_df: pd.DataFrame, initial_capital: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary_df = pd.DataFrame([{
        "initial_capital": initial_capital,
        "final_capital": initial_capital,
        "net_profit": 0.0,
        "gross_profit": 0.0,
        "gross_loss": 0.0,
        "gross_pnl_total": 0.0,
        "total_commission": 0.0,
        "total_trades": 0,
        "winning_trades": 0,
        "losing_trades": 0,
        "breakeven_trades": 0,
        "win_rate": 0.0,
        "profit_factor": 0.0,
        "expectancy": 0.0,
        "average_trade": 0.0,
        "average_win": 0.0,
        "average_loss": 0.0,
        "best_trade": 0.0,
        "worst_trade": 0.0,
        "max_drawdown": 0.0,
        "max_drawdown_pct": 0.0,
        "tradable_days": int(daily_df["is_tradable"].sum()) if "is_tradable" in daily_df.columns else 0,
        "non_tradable_days": int((~daily_df["is_tradable"]).sum()) if "is_tradable" in daily_df.columns else 0,
        "days_with_trade": int(daily_df["trade_taken"].sum()) if "trade_taken" in daily_df.columns else 0,
        "long_trades": 0,
        "short_trades": 0,
        "avg_holding_minutes": 0.0,
        "commission_rate_per_side": COMMISSION_RATE,
    }])

    equity_df = pd.DataFrame(columns=[
        "trade_number", "date", "net_pnl", "gross_pnl", "commission",
        "equity", "running_max", "drawdown", "drawdown_pct"
    ])
    return summary_df, equity_df




def build_summary(
    trades_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    initial_capital: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if trades_df.empty:
        return build_empty_summary(daily_df, initial_capital)

    trades_df = trades_df.copy()
    trades_df = trades_df[
        (trades_df["trade_taken"] == True) &
        (trades_df["invalidated_day"] == False)
    ].copy()

    if trades_df.empty:
        return build_empty_summary(daily_df, initial_capital)

    trades_df = trades_df.sort_values(["entry_time", "exit_time"]).reset_index(drop=True)
    trades_df["trade_number"] = range(1, len(trades_df) + 1)

    trades_df["equity"] = initial_capital + trades_df["net_pnl"].cumsum()
    trades_df["running_max"] = trades_df["equity"].cummax()
    trades_df["drawdown"] = trades_df["equity"] - trades_df["running_max"]
    trades_df["drawdown_pct"] = (trades_df["drawdown"] / trades_df["running_max"]) * 100

    winning_trades_df = trades_df[trades_df["net_pnl"] > 0]
    losing_trades_df = trades_df[trades_df["net_pnl"] < 0]
    breakeven_trades_df = trades_df[trades_df["net_pnl"] == 0]

    gross_profit = winning_trades_df["net_pnl"].sum()
    gross_loss = abs(losing_trades_df["net_pnl"].sum())
    gross_pnl_total = trades_df["gross_pnl"].sum()
    total_commission = trades_df["commission"].sum()

    total_trades = len(trades_df)
    winning_trades = len(winning_trades_df)
    losing_trades = len(losing_trades_df)
    breakeven_trades = len(breakeven_trades_df)

    win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")
    expectancy = trades_df["net_pnl"].mean() if total_trades > 0 else 0.0
    average_trade = expectancy
    average_win = winning_trades_df["net_pnl"].mean() if winning_trades > 0 else 0.0
    average_loss = losing_trades_df["net_pnl"].mean() if losing_trades > 0 else 0.0
    best_trade = trades_df["net_pnl"].max()
    worst_trade = trades_df["net_pnl"].min()
    max_drawdown = abs(trades_df["drawdown"].min())
    max_drawdown_pct = abs(trades_df["drawdown_pct"].min())
    final_capital = trades_df["equity"].iloc[-1]
    net_profit = final_capital - initial_capital
    avg_holding_minutes = trades_df["holding_minutes"].mean() if "holding_minutes" in trades_df.columns else 0.0

    summary_df = pd.DataFrame([{
        "initial_capital": initial_capital,
        "final_capital": final_capital,
        "net_profit": net_profit,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "gross_pnl_total": gross_pnl_total,
        "total_commission": total_commission,
        "total_trades": total_trades,
        "winning_trades": winning_trades,
        "losing_trades": losing_trades,
        "breakeven_trades": breakeven_trades,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "expectancy": expectancy,
        "average_trade": average_trade,
        "average_win": average_win,
        "average_loss": average_loss,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "max_drawdown": max_drawdown,
        "max_drawdown_pct": max_drawdown_pct,
        "tradable_days": int(daily_df["is_tradable"].sum()),
        "non_tradable_days": int((~daily_df["is_tradable"]).sum()),
        "days_with_trade": int(len(trades_df)),
        "long_trades": int((trades_df["trade_side"] == "long").sum()),
        "short_trades": int((trades_df["trade_side"] == "short").sum()),
        "avg_holding_minutes": avg_holding_minutes,
        "commission_rate_per_side": COMMISSION_RATE,
    }])

    equity_df = trades_df[[
        "trade_number",
        "date",
        "net_pnl",
        "gross_pnl",
        "commission",
        "equity",
        "running_max",
        "drawdown",
        "drawdown_pct",
    ]].copy()

    return summary_df, equity_df

# =========================
# INTRABAR MODULE
# =========================

SYMBOL = "BTCUSDT"

data_cache = {}

def download_binance_1m(symbol, year, month):
    os.makedirs(DATA_PATH, exist_ok=True)

    filename = f"{symbol}-1m-{year}-{month:02d}.zip"
    csv_name = filename.replace(".zip", ".csv")
    csv_path = os.path.join(DATA_PATH, csv_name)

    if os.path.exists(csv_path):
        return csv_path

    url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/1m/{filename}"

    r = requests.get(url)
    if r.status_code != 200:
        return None

    with zipfile.ZipFile(BytesIO(r.content)) as z:
        z.extractall(DATA_PATH)

    return csv_path


def load_1m(csv_path):

    df = pd.read_csv(csv_path, header=None)

    df.columns = [
        "open_time","open","high","low","close","volume",
        "close_time","qav","num_trades","taker_base","taker_quote","ignore"
    ]

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)

    df = df.astype({
        "open": float,
        "high": float,
        "low": float,
        "close": float
    })

    return df[["open_time","open","high","low","close"]]

def get_1m_data(timestamp):
    key = f"{timestamp.year}-{timestamp.month}"

    if key not in data_cache:
        csv_path = download_binance_1m(SYMBOL, timestamp.year, timestamp.month)
        if csv_path is None:
            return None
        data_cache[key] = load_1m(csv_path)

    return data_cache[key]


def get_window(df, timestamp):
    start = timestamp - pd.Timedelta(minutes=15)
    end   = timestamp + pd.Timedelta(minutes=15)

    return df[
        (df["open_time"] >= start) &
        (df["open_time"] <= end)
    ]


def resolve_intrabar(df, entry, sl, tp, side):
    position_open = False

    for _, row in df.iterrows():
        high = row["high"]
        low = row["low"]

        if not position_open:
            if side == "long" and low <= entry:
                position_open = True
            elif side == "short" and high >= entry:
                position_open = True

        if position_open:
            if side == "long":
                if low <= sl:
                    return "SL"
                if high >= tp:
                    return "TP"
            else:
                if high >= sl:
                    return "SL"
                if low <= tp:
                    return "TP"

    return "UNKNOWN"


def resolve_tp_sl_intrabar(timestamp, entry_price, stop_loss, take_profit, trade_side):
    timestamp = pd.to_datetime(timestamp, utc=True)
    df_1m = get_1m_data(timestamp)

    if df_1m is None:
        return "UNKNOWN"

    df_window = get_window(df_1m, timestamp)

    return resolve_intrabar(
        df_window,
        entry_price,
        stop_loss,
        take_profit,
        trade_side
    )

def print_backtest_summary(summary_df: pd.DataFrame) -> None:
    row = summary_df.iloc[0]

    print("\nBacktest Summary")
    print("-" * 45)
    print(f"Initial capital:     {row['initial_capital']:.2f}")
    print(f"Final capital:       {row['final_capital']:.2f}")
    print(f"Net profit:          {row['net_profit']:.2f}")
    print(f"Gross PnL total:     {row['gross_pnl_total']:.2f}")
    print(f"Total commission:    {row['total_commission']:.2f}")
    print(f"Total trades:        {int(row['total_trades'])}")
    print(f"Winning trades:      {int(row['winning_trades'])}")
    print(f"Losing trades:       {int(row['losing_trades'])}")
    print(f"Breakeven trades:    {int(row['breakeven_trades'])}")
    print(f"Win rate:            {row['win_rate']:.2f}%")
    print(f"Gross profit:        {row['gross_profit']:.2f}")
    print(f"Gross loss:          {row['gross_loss']:.2f}")
    print(f"Profit factor:       {row['profit_factor']:.2f}")
    print(f"Expectancy:          {row['expectancy']:.2f}")
    print(f"Average trade:       {row['average_trade']:.2f}")
    print(f"Average win:         {row['average_win']:.2f}")
    print(f"Average loss:        {row['average_loss']:.2f}")
    print(f"Best trade:          {row['best_trade']:.2f}")
    print(f"Worst trade:         {row['worst_trade']:.2f}")
    print(f"Max drawdown:        {row['max_drawdown']:.2f}")
    print(f"Max drawdown %:      {row['max_drawdown_pct']:.2f}%")
    print(f"Tradable days:       {int(row['tradable_days'])}")
    print(f"Non-tradable days:   {int(row['non_tradable_days'])}")
    print(f"Days with trade:     {int(row['days_with_trade'])}")
    print(f"Long trades:         {int(row['long_trades'])}")
    print(f"Short trades:        {int(row['short_trades'])}")
    print(f"Avg holding minutes: {row['avg_holding_minutes']:.2f}")
    print(f"Commission / side:   {row['commission_rate_per_side'] * 100:.4f}%")


# =========================
# EXCEL EXPORT
# =========================
def autofit_worksheet_columns(worksheet) -> None:
    for column_cells in worksheet.columns:
        max_length = 0
        column_letter = get_column_letter(column_cells[0].column)

        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            max_length = max(max_length, len(value))

        worksheet.column_dimensions[column_letter].width = min(max(max_length + 2, 12), 40)


def export_backtest_to_excel(
    output_path: str,
    summary_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    equity_df: pd.DataFrame,
) -> None:
    summary_export = remove_timezone_from_datetimes(summary_df)
    daily_export = remove_timezone_from_datetimes(daily_df)
    trades_export = remove_timezone_from_datetimes(trades_df)
    equity_export = remove_timezone_from_datetimes(equity_df)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary_export.to_excel(writer, sheet_name="summary", index=False)
        daily_export.to_excel(writer, sheet_name="daily_summary", index=False)
        trades_export.to_excel(writer, sheet_name="trades", index=False)
        equity_export.to_excel(writer, sheet_name="equity_curve", index=False)

        for sheet_name, df in {
            "summary": summary_export,
            "daily_summary": daily_export,
            "trades": trades_export,
            "equity_curve": equity_export,
        }.items():
            ws = writer.sheets[sheet_name]
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions
            autofit_worksheet_columns(ws)

    print(f"\nBacktest report exported to: {output_path}")


# =========================
# MAIN BACKTEST
# =========================
def run_backtest() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_h1, df_m15 = load_market_data(H1_PATH, M15_PATH, START_DATE, END_DATE)

    daily_results = []
    trade_results = []
    capital = INITIAL_CAPITAL

    for day in pd.date_range(start=START_DATE, end=END_DATE, freq="D", tz="UTC"):
        day_record = evaluate_tradable_day(day, df_h1)

        if day_record["is_tradable"]:
            trade_record = process_intraday_session(
                day=day,
                df_m15=df_m15,
                tokyo_high=day_record["tokyo_high"],
                tokyo_low=day_record["tokyo_low"],
                tokyo_range=day_record["tokyo_range"],
                channel_proximity=CHANNEL_PROXIMITY,
                risk_constant=RISK_CONSTANT,
                commission_rate=COMMISSION_RATE,
            )

            trade_results.append(trade_record)

            day_record["trade_taken"] = trade_record["trade_taken"]
            day_record["trade_side"] = trade_record["trade_side"]
            day_record["entry_time"] = trade_record["entry_time"]
            day_record["entry_price"] = trade_record["entry_price"]
            day_record["stop_loss"] = trade_record["stop_loss"]
            day_record["take_profit"] = trade_record["take_profit"]
            day_record["exit_time"] = trade_record["exit_time"]
            day_record["exit_price"] = trade_record["exit_price"]
            day_record["exit_reason"] = trade_record["exit_reason"]
            day_record["position_size"] = trade_record["position_size"]
            day_record["risk_based_size"] = trade_record["risk_based_size"]
            day_record["channel_based_size"] = trade_record["channel_based_size"]
            day_record["range_pct"] = trade_record["range_pct"]
            day_record["range_normalizer"] = trade_record["range_normalizer"]
            day_record["gross_pnl"] = trade_record["gross_pnl"]
            day_record["commission"] = trade_record["commission"]
            day_record["day_pnl"] = trade_record["net_pnl"]
            day_record["invalidated_day"] = trade_record["invalidated_day"]

            capital += trade_record["net_pnl"]

            if trade_record["invalidated_day"]:
                day_record["skip_reason"] = "invalidated_in_m15"

        day_record["capital_after_day"] = capital
        daily_results.append(day_record)

    daily_df = pd.DataFrame(daily_results)
    trades_df = pd.DataFrame(trade_results)

    summary_df, equity_df = build_summary(trades_df, daily_df, INITIAL_CAPITAL)

    return summary_df, daily_df, trades_df, equity_df


if __name__ == "__main__":
    summary_df, daily_df, trades_df, equity_df = run_backtest()

    print_backtest_summary(summary_df)

    export_backtest_to_excel(
        output_path=OUTPUT_EXCEL,
        summary_df=summary_df,
        daily_df=daily_df,
        trades_df=trades_df,
        equity_df=equity_df,
    )

