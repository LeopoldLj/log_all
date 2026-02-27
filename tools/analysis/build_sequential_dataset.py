#!/usr/bin/env python
import os
import argparse
import polars as pl


def df_to_html_table(df: pl.DataFrame, title: str) -> str:
    cols = df.columns
    rows = df.rows()
    html = [f"<h2>{title}</h2>", "<table border='1' cellspacing='0' cellpadding='4'>"]
    html.append("<tr>" + "".join(f"<th>{c}</th>" for c in cols) + "</tr>")
    for r in rows:
        html.append("<tr>" + "".join(f"<td>{v}</td>" for v in r) + "</tr>")
    html.append("</table>")
    return "\n".join(html)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True, help="Directory containing *_alias.parquet files")
    ap.add_argument("--out-base", required=True, help="Base path without extension")
    ap.add_argument("--symbol", default=None, help="Filter by symbol")
    args = ap.parse_args()

    base = args.dir
    f1 = os.path.join(base, "features_1m_alias.parquet")
    f2 = os.path.join(base, "quote_pressure_1m_alias.parquet")
    f3 = os.path.join(base, "trade_speed_1m_alias.parquet")
    for f in (f1, f2, f3):
        if not os.path.exists(f):
            print(f"Missing file: {f}")
            return 1

    feat = pl.read_parquet(f1)
    quote = pl.read_parquet(f2)
    speed = pl.read_parquet(f3)

    df = feat.join(quote, on=["symbol", "minute"], how="left").join(speed, on=["symbol", "minute"], how="left")
    if args.symbol:
        df = df.filter(pl.col("symbol") == args.symbol)
    df = df.sort(["symbol", "minute"])

    out_parquet = args.out_base + ".parquet"
    out_csv = args.out_base + ".csv"
    out_json = args.out_base + ".json"
    out_html = args.out_base + ".html"

    os.makedirs(os.path.dirname(out_parquet), exist_ok=True)
    df.write_parquet(out_parquet, compression="zstd")
    df.write_csv(out_csv)
    df.write_ndjson(out_json)

    html = [
        "<html><head><meta charset='utf-8'><title>Sequential Dataset</title></head><body>",
        "<h1>Sequential Dataset (1m)</h1>",
        df_to_html_table(df.head(500), "First 500 rows (chronological)"),
        "</body></html>",
    ]
    with open(out_html, "w", encoding="utf-8") as f:
        f.write("\n".join(html))

    print(f"Wrote: {out_parquet}")
    print(f"Wrote: {out_csv}")
    print(f"Wrote: {out_json}")
    print(f"Wrote: {out_html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
