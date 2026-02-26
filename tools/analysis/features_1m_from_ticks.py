#!/usr/bin/env python
import os
import argparse
import polars as pl


def build_1m_features(ticks: pl.LazyFrame) -> pl.LazyFrame:
    # Normalize
    ticks = ticks.with_columns(
        pl.col("utc_now").str.to_datetime(strict=False, time_zone="UTC").alias("ts"),
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
            pl.count().alias("tpo"),
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
            pl.count().alias("ticks"),
        )
        .with_columns((pl.col("vol_buy") - pl.col("vol_sell")).alias("delta"))
    )

    # CVD (cumulative delta)
    base = base.sort(["symbol", "minute"]).with_columns(
        pl.col("delta").cumsum().over("symbol").alias("cvd")
    )

    return base.join(poc, on=["symbol", "minute"], how="left")


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
