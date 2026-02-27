#!/usr/bin/env python
import os
import argparse
import polars as pl


def build_1m_features(ticks: pl.LazyFrame) -> pl.LazyFrame:
    # Normalize
    ticks = ticks.with_columns(
        pl.col("utc_now").cast(pl.Datetime("us", "UTC")).alias("ts"),
        pl.col("symbol").alias("symbol"),
        pl.col("price").cast(pl.Float64).alias("price"),
        pl.col("size").cast(pl.Float64).alias("size"),
        pl.col("side").alias("side"),
    )

    ticks = ticks.with_columns(
        pl.when(pl.col("side") == "BUY").then(pl.col("size")).otherwise(0.0).alias("buy_vol"),
        pl.when(pl.col("side") == "SELL").then(pl.col("size")).otherwise(0.0).alias("sell_vol"),
    )

    # Minute bucket
    ticks = ticks.with_columns(pl.col("ts").dt.truncate("1m").alias("minute"))

    # POC and TPO from volume by price
    vol_by_price = (
        ticks.group_by(["symbol", "minute", "price"])
        .agg(
            pl.sum("size").alias("vol"),
            pl.sum("buy_vol").alias("vol_buy"),
            pl.sum("sell_vol").alias("vol_sell"),
        )
        .with_columns((pl.col("vol_buy") - pl.col("vol_sell")).alias("delta"))
    )

    poc = (
        vol_by_price.sort("vol", descending=True)
        .group_by(["symbol", "minute"])
        .agg(
            pl.first("price").alias("poc"),
            pl.first("vol").alias("poc_vol"),
            pl.len().alias("tpo"),
        )
    )

    # OHLC + volumes
    base = (
        ticks.group_by(["symbol", "minute"])
        .agg(
            pl.first("price").alias("open"),
            pl.max("price").alias("high"),
            pl.min("price").alias("low"),
            pl.last("price").alias("close"),
            pl.sum("size").alias("total_vol"),
            pl.sum("buy_vol").alias("vol_buy"),
            pl.sum("sell_vol").alias("vol_sell"),
            pl.len().alias("ticks"),
        )
        .with_columns((pl.col("vol_buy") - pl.col("vol_sell")).alias("delta"))
    )

    # Buy/Sell at High/Low
    hl = base.select(["symbol", "minute", "high", "low"])
    ticks_hl = ticks.join(hl, on=["symbol", "minute"], how="inner")
    at_high = (
        ticks_hl.filter(pl.col("price") == pl.col("high"))
        .group_by(["symbol", "minute"])
        .agg(
            pl.sum("buy_vol").alias("buy_at_high"),
            pl.sum("sell_vol").alias("sell_at_high"),
        )
    )
    at_low = (
        ticks_hl.filter(pl.col("price") == pl.col("low"))
        .group_by(["symbol", "minute"])
        .agg(
            pl.sum("buy_vol").alias("buy_at_low"),
            pl.sum("sell_vol").alias("sell_at_low"),
        )
    )

    base = base.join(at_high, on=["symbol", "minute"], how="left").join(at_low, on=["symbol", "minute"], how="left")

    # CVD (cumulative delta)
    base = base.sort(["symbol", "minute"]).with_columns(
        pl.col("delta").cum_sum().over("symbol").alias("cvd")
    )

    out = base.join(poc, on=["symbol", "minute"], how="left")

    # Business-friendly aliases (no duplicates)
    out = out.rename(
        {
            "open": "open_price",
            "high": "high_price",
            "low": "low_price",
            "close": "close_price",
            "total_vol": "contracts_total",
            "vol_buy": "contracts_buy",
            "vol_sell": "contracts_sell",
            "ticks": "trades_count",
            "delta": "delta_contracts",
            "cvd": "cvd_contracts",
            "poc": "poc_price",
            "poc_vol": "poc_contracts",
            "tpo": "tpo_levels",
            "buy_at_high": "contracts_buy_at_high",
            "sell_at_high": "contracts_sell_at_high",
            "buy_at_low": "contracts_buy_at_low",
            "sell_at_low": "contracts_sell_at_low",
        }
    )

    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet-dir", required=True, help="Directory containing ticks_simple parquet files")
    ap.add_argument("--symbol", default=None, help="Filter by symbol")
    ap.add_argument("--out", required=True, help="Output parquet path")
    args = ap.parse_args()

    files = []
    for name in os.listdir(args.parquet_dir):
        if name.endswith("__ticks_simple.parquet"):
            files.append(os.path.join(args.parquet_dir, name))
    if not files:
        print("No ticks_simple.parquet files found.")
        return 1

    lf = pl.scan_parquet(files)
    if args.symbol:
        lf = lf.filter(pl.col("symbol") == args.symbol)

    features = build_1m_features(lf)
    features.collect(engine="streaming").write_parquet(args.out, compression="zstd")
    print(f"Wrote: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
