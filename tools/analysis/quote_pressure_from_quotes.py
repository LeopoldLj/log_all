#!/usr/bin/env python
import os
import re
import argparse
import polars as pl


QUOTE_RE = re.compile(
    r"Bid=(?P<bid>[^,]+),\s*BidSize=(?P<bid_size>[^,]+),\s*Ask=(?P<ask>[^,]+),\s*AskSize=(?P<ask_size>[^,]+)"
)


def extract_quote_fields(s: str):
    if s is None:
        return None
    m = QUOTE_RE.search(s)
    if not m:
        return None
    return m.groupdict()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet-dir", required=True, help="Directory containing quotes parquet files")
    ap.add_argument("--symbol", default=None, help="Filter by symbol")
    ap.add_argument("--out", required=True, help="Output parquet path")
    args = ap.parse_args()

    files = []
    for name in os.listdir(args.parquet_dir):
        if name.endswith("__quotes.parquet"):
            files.append(os.path.join(args.parquet_dir, name))
    if not files:
        print("No quotes.parquet files found.")
        return 1

    lf = pl.scan_parquet(files)
    if args.symbol:
        lf = lf.filter(pl.col("symbol") == args.symbol)

    # Parse quote_dump text into columns
    lf = lf.with_columns(
        pl.col("quote_dump").map_elements(extract_quote_fields, return_dtype=pl.Object).alias("q")
    ).with_columns(
        pl.col("q").struct.field("bid").cast(pl.Float64).alias("bid"),
        pl.col("q").struct.field("bid_size").cast(pl.Float64).alias("bid_size"),
        pl.col("q").struct.field("ask").cast(pl.Float64).alias("ask"),
        pl.col("q").struct.field("ask_size").cast(pl.Float64).alias("ask_size"),
    ).drop("q")

    lf = lf.with_columns(
        pl.col("utc_now").str.to_datetime(strict=False, time_zone="UTC").alias("ts"),
        ((pl.col("bid") + pl.col("ask")) / 2.0).alias("mid"),
        (pl.col("ask") - pl.col("bid")).alias("spread"),
        ((pl.col("bid_size") - pl.col("ask_size")) / (pl.col("bid_size") + pl.col("ask_size"))).alias("book_imbalance"),
    )

    # Aggregate to 1 minute for simple pressure features
    out = (
        lf.group_by([pl.col("symbol"), pl.col("ts").dt.truncate("1m").alias("minute")])
        .agg(
            pl.mean("spread").alias("spread_mean"),
            pl.mean("book_imbalance").alias("imbalance_mean"),
            pl.max("book_imbalance").alias("imbalance_max"),
            pl.min("book_imbalance").alias("imbalance_min"),
            pl.mean("bid_size").alias("bid_size_mean"),
            pl.mean("ask_size").alias("ask_size_mean"),
            pl.count().alias("quotes"),
        )
    )

    out.collect(engine="streaming").write_parquet(args.out, compression="zstd")
    print(f"Wrote: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
