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
    ap.add_argument("--parquet", required=True, help="Path to features_1m_alias.parquet")
    ap.add_argument("--out", required=True, help="Output HTML report path")
    args = ap.parse_args()

    if not os.path.exists(args.parquet):
        print(f"Missing file: {args.parquet}")
        return 1

    df = pl.read_parquet(args.parquet)
    needed = [
        "symbol",
        "minute",
        "high_price",
        "low_price",
        "contracts_buy_at_high",
        "contracts_sell_at_high",
        "contracts_buy_at_low",
        "contracts_sell_at_low",
    ]
    cols = [c for c in needed if c in df.columns]
    view = df.select(cols)

    top_buy_high = view.sort("contracts_buy_at_high", descending=True).head(10)
    top_sell_high = view.sort("contracts_sell_at_high", descending=True).head(10)
    top_buy_low = view.sort("contracts_buy_at_low", descending=True).head(10)
    top_sell_low = view.sort("contracts_sell_at_low", descending=True).head(10)

    html = [
        "<html><head><meta charset='utf-8'><title>High/Low Aggressive Volume</title></head><body>",
        "<h1>High/Low Aggressive Volume (1m)</h1>",
        df_to_html_table(top_buy_high, "Top 10 Buy at High"),
        df_to_html_table(top_sell_high, "Top 10 Sell at High"),
        df_to_html_table(top_buy_low, "Top 10 Buy at Low"),
        df_to_html_table(top_sell_low, "Top 10 Sell at Low"),
        "</body></html>",
    ]

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(html))

    print(f"Wrote HTML report: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
