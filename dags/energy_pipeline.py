"""
Energy market bid pipeline

Two-stage Airflow pipeline for the intraday energy bid feed:

    ingest_and_clean  ->  aggregate_hourly

Stage 1 reads the raw bid export (CSV), normalises it, drops unusable rows
and writes a clean Parquet dataset. Stage 2 reads that dataset and produces
a summary per trading hour (counts, average price, VWAP, spread).

"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# --- Paths ----------------------------------------------------------------
RAW_DATA_PATH = "/opt/airflow/dags/energy_data.csv"
CLEANED_PATH = "/opt/airflow/processed_data/cleaned_bids.parquet"
SUMMARY_PATH = "/opt/airflow/output/hourly_summary_{ds}.csv"

# Rows missing Price or Volume are dropped. Above this fraction the input is
# treated as too degraded to summarise and the task fails instead.
MAX_MISSING_FRACTION = 0.1

# Share of rows allowed to carry a Sell_Buy value outside the known vocabulary.
MAX_UNRECOGNISED_FRACTION = 0.01

# --- DAG config -----------------------------------------------------------
default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def ingest_and_clean(**context) -> None:
    """Stage 1 — load the raw bid export, clean it, and persist Parquet.

    Cleaning steps:
      * parse timestamps to UTC
      * normalise the buy/sell side to lower case
      * drop rows missing a price or a volume
      * derive the trading date and hour from the timestamp
    """
    logger.info("Reading raw bids from %s", RAW_DATA_PATH)
    df = pd.read_csv(RAW_DATA_PATH)
    logger.info("Loaded %d raw rows", len(df))

    df["Timestamp"] = pd.to_datetime(df["Timestamp"], utc=True)

    # Adding a map to catch any unexpected values in the Sell_Buy column and convert them to NaN
    df["side"] = df["Sell_Buy"].str.strip().str.lower().map({"buy": "buy", "sell": "sell"})

    missing = df[["Price", "Volume"]].isna().any(axis=1)
    if missing.mean() > MAX_MISSING_FRACTION:
        raise ValueError(
            f"{missing.mean():.1%} of rows are missing Price or Volume, "
            f"above the {MAX_MISSING_FRACTION:.0%} threshold"
        )
    logger.info("Dropping %d rows missing Price or Volume", missing.sum())
    df = df[~missing]

    unrecognised = df.loc[df["side"].isna(), "Sell_Buy"]
    if len(unrecognised) / len(df) > MAX_UNRECOGNISED_FRACTION:
        raise ValueError(
            f"{len(unrecognised) / len(df):.1%} of rows have an unrecognised "
            f"Sell_Buy value, above the {MAX_UNRECOGNISED_FRACTION:.0%} threshold"
        )
    if not unrecognised.empty:
        logger.warning(
            "Dropping %d rows with unrecognised Sell_Buy values: %s",
            len(unrecognised),
            unrecognised.value_counts().to_dict(),
        )
    df = df.dropna(subset=["side"])
    df["date"] = df["Timestamp"].dt.date
    df["hour"] = df["Timestamp"].dt.hour

    os.makedirs(os.path.dirname(CLEANED_PATH), exist_ok=True)
    df.to_parquet(CLEANED_PATH, index=False)
    logger.info("Wrote %d cleaned rows to %s", len(df), CLEANED_PATH)


def _vwap(group: pd.DataFrame) -> float:
    """Volume-weighted average price for a group of bids."""
    total_volume = group["Volume"].sum()
    if total_volume == 0:
        return float("nan")
    return (group["Price"] * group["Volume"]).sum() / total_volume


def aggregate_hourly(**context) -> None:
    """Stage 2 — build the hourly market summary from the cleaned dataset."""
    logger.info("Reading cleaned bids from %s", CLEANED_PATH)
    df = pd.read_parquet(CLEANED_PATH)

    records = []
    for (date, hour), hourly in df.groupby(["date", "hour"]):
        buys = hourly[hourly["side"] == "buy"]
        sells = hourly[hourly["side"] == "sell"]
        records.append(
            {
                "date": date,
                "hour": hour,
                "buy_count": len(buys),
                "sell_count": len(sells),
                "buy_avg_price": round(buys["Price"].mean(), 2),
                "sell_avg_price": round(sells["Price"].mean(), 2),
                "buy_total_volume": buys["Volume"].sum(),
                "sell_total_volume": sells["Volume"].sum(),
                "buy_vwap": round(_vwap(buys), 2),
                "sell_vwap": round(_vwap(sells), 2),
                "market_spread": round(
                    buys["Price"].mean() - sells["Price"].mean(), 2
                ),
            }
        )

    summary = pd.DataFrame(records).sort_values(["date", "hour"])

    summary_path = SUMMARY_PATH.format(ds=context["ds"])
    os.makedirs(os.path.dirname(summary_path), exist_ok=True)
    summary.to_csv(summary_path, index=False)
    logger.info("Wrote hourly summary (%d hours) to %s", len(summary), summary_path)


with DAG(
    dag_id="energy_pipeline",
    default_args=default_args,
    description="Ingest, clean and aggregate intraday energy bids",
    schedule_interval=None,
    catchup=False,
    tags=["energy", "terra-one"],
) as dag:

    clean = PythonOperator(
        task_id="ingest_and_clean",
        python_callable=ingest_and_clean,
    )

    aggregate = PythonOperator(
        task_id="aggregate_hourly",
        python_callable=aggregate_hourly,
    )

    clean >> aggregate
