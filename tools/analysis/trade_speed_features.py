#!/usr/bin/env python
import os
import argparse
import polars as pl


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet-dir", required=True, help="Directory containing ticks_simple parquet files")
    ap.add_argument("--symbol", default=None, help="Filter by symbol")
    ap.add_argument("--out", required=True, help="Output parquet path")
    ap.add_argument("--large-trade", type=float, default=5.0, help="Threshold for large trade size")
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

    lf = lf.with_columns(
        pl.col("utc_now").cast(pl.Datetime("us", "UTC")).alias("ts"),
        pl.col("size").cast(pl.Float64).alias("size"),
    )

    out = (
        lf.group_by([pl.col("symbol"), pl.col("ts").dt.truncate("1m").alias("minute")])
        .agg(
            pl.len().alias("trades_per_min"),
            pl.mean("size").alias("avg_contract_size"),
            pl.sum("size").alias("contracts_total"),
            (pl.col("size") >= args.large_trade).cast(pl.Int64).sum().alias("large_trades_count"),
        )
    )

    out.collect(engine="streaming").write_parquet(args.out, compression="zstd")
    print(f"Wrote: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
