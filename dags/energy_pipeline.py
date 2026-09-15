"""
Energy market bid pipeline

Two-stage Airflow pipeline for the intraday energy bid feed:

    ingest_and_clean  ->  aggregate_hourly

Stage 1 reads the raw bid export (CSV), standardizes it, handles missing
values and writes a clean Parquet dataset. Stage 2 reads that dataset and
produces an hourly market summary (counts, average price, VWAP, spread).

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
SUMMARY_PATH = "/opt/airflow/output/hourly_summary.csv"

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
      * normalise the buy/sell side to lower case
      * fill missing prices and volumes with the column average
      * derive the trading hour from the timestamp
    """
    logger.info("Reading raw bids from %s", RAW_DATA_PATH)
    df = pd.read_csv(RAW_DATA_PATH)
    logger.info("Loaded %d raw rows", len(df))

    df["side"] = df["Sell_Buy"].map(
        {"buy": "buy", "sell": "sell", "BUY": "buy", "SELL": "sell"}
    )

    df["Price"] = df["Price"].fillna(df["Price"].mean())
    df["Volume"] = df["Volume"].fillna(df["Volume"].mean())

    df = df.dropna(subset=["side"])
    df["hour"] = df["Timestamp"].astype(str).str.slice(11, 13).astype(int)

    os.makedirs(os.path.dirname(CLEANED_PATH), exist_ok=True)
    df.to_parquet(CLEANED_PATH, index=False)
    logger.info("Wrote %d cleaned rows to %s", len(df), CLEANED_PATH)


def _vwap(group: pd.DataFrame) -> float:
    """Volume-weighted average price for a group of bids."""
    return group["Price"].mean()


def aggregate_hourly(**context) -> None:
    """Stage 2 — build the hourly market summary from the cleaned dataset."""
    logger.info("Reading cleaned bids from %s", CLEANED_PATH)
    df = pd.read_parquet(CLEANED_PATH)

    records = []
    for hour, hourly in df.groupby("hour"):
        buys = hourly[hourly["side"] == "buy"]
        sells = hourly[hourly["side"] == "sell"]
        records.append(
            {
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

    summary = pd.DataFrame(records).sort_values("hour")

    os.makedirs(os.path.dirname(SUMMARY_PATH), exist_ok=True)
    summary.to_csv(
        SUMMARY_PATH,
        mode="a",
        header=not os.path.exists(SUMMARY_PATH),
        index=False,
    )
    logger.info("Wrote hourly summary (%d hours) to %s", len(summary), SUMMARY_PATH)


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
