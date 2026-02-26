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


def svg_line_chart(points, width=900, height=220, title=""):
    if not points:
        return f"<h3>{title}</h3><p>No data</p>"
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    xmin, xmax = min(xs), max(xs)
    ymin, ymax = min(ys), max(ys)
    if ymax == ymin:
        ymax = ymin + 1
    pad = 20
    w = width - 2 * pad
    h = height - 2 * pad
    def sx(x):
        return pad + (x - xmin) * w / (xmax - xmin) if xmax != xmin else pad
    def sy(y):
        return pad + (ymax - y) * h / (ymax - ymin)
    path = " ".join(
        ("M" if i == 0 else "L") + f"{sx(x):.1f},{sy(y):.1f}"
        for i, (x, y) in enumerate(points)
    )
    return f"""
<h3>{title}</h3>
<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">
  <rect x="0" y="0" width="{width}" height="{height}" fill="#fff" stroke="#ddd"/>
  <path d="{path}" fill="none" stroke="#1f77b4" stroke-width="1.5"/>
</svg>
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True, help="Directory containing *_alias.parquet files")
    ap.add_argument("--out", required=True, help="Output HTML report path")
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

    row_count = df.height
    min_ts = df.select(pl.col("minute").min()).item()
    max_ts = df.select(pl.col("minute").max()).item()

    cols = [
        "contracts_total",
        "contracts_buy",
        "contracts_sell",
        "delta_contracts",
        "cvd_contracts",
        "spread_avg",
        "imbalance_avg",
        "trades_per_min",
        "avg_contract_size",
        "large_trades_count",
    ]
    cols = [c for c in cols if c in df.columns]

    stats = df.select(
        [pl.col(c).mean().alias(f"{c}_mean") for c in cols]
        + [pl.col(c).std().alias(f"{c}_std") for c in cols]
        + [pl.col(c).min().alias(f"{c}_min") for c in cols]
        + [pl.col(c).max().alias(f"{c}_max") for c in cols]
        + [pl.col(c).quantile(0.1).alias(f"{c}_p10") for c in cols]
        + [pl.col(c).quantile(0.5).alias(f"{c}_p50") for c in cols]
        + [pl.col(c).quantile(0.9).alias(f"{c}_p90") for c in cols]
    )

    head = df.head(20)

    top_vol = df.sort("contracts_total", descending=True).head(5)
    top_pos_delta = df.sort("delta_contracts", descending=True).head(5)
    top_neg_delta = df.sort("delta_contracts").head(5)
    dominance = df.select(
        (pl.sum("contracts_buy") / pl.sum("contracts_total")).alias("buy_ratio"),
        (pl.sum("contracts_sell") / pl.sum("contracts_total")).alias("sell_ratio"),
    )
    dom_buy = dominance["buy_ratio"][0]
    dom_sell = dominance["sell_ratio"][0]
    trend_comment = "Équilibré"
    if dom_buy > 0.55:
        trend_comment = "Dominance acheteurs (pression d'achat globale)"
    elif dom_sell > 0.55:
        trend_comment = "Dominance vendeurs (pression de vente globale)"

    delta_pos_ratio = df.select(((pl.col("delta_contracts") > 0).cast(pl.Int32).mean()).alias("pos_ratio"))["pos_ratio"][0]
    delta_comment = "Delta mixte"
    if delta_pos_ratio > 0.6:
        delta_comment = "Delta positif majoritaire (acheteurs actifs)"
    elif delta_pos_ratio < 0.4:
        delta_comment = "Delta négatif majoritaire (vendeurs actifs)"

    spread_mean = df.select(pl.col("spread_avg").mean()).item()
    spread_comment = "Spread stable"
    if spread_mean is not None and spread_mean > 1.5:
        spread_comment = "Spread élevé (liquidité plus faible)"

    # Extra comments
    p10_vol = df.select(pl.col("contracts_total").quantile(0.10)).item()
    p90_vol = df.select(pl.col("contracts_total").quantile(0.90)).item()
    p90_imb = df.select(pl.col("imbalance_avg").quantile(0.90)).item()
    p10_imb = df.select(pl.col("imbalance_avg").quantile(0.10)).item()

    compression_comment = "Compression: non détectée"
    if p10_vol is not None and spread_mean is not None:
        # Compression if many minutes have very low volume and spread stable
        low_vol_ratio = df.select((pl.col("contracts_total") <= p10_vol).cast(pl.Int32).mean()).item()
        if low_vol_ratio is not None and low_vol_ratio > 0.25 and spread_mean <= 1.5:
            compression_comment = "Compression: volumes faibles récurrents et spread stable"

    volume_spike_comment = "Explosion de volume: non détectée"
    if p90_vol is not None:
        high_vol_ratio = df.select((pl.col("contracts_total") >= p90_vol).cast(pl.Int32).mean()).item()
        if high_vol_ratio is not None and high_vol_ratio > 0.10:
            volume_spike_comment = "Explosion de volume: fréquence élevée des minutes > p90"

    imbalance_comment = "Imbalance extrême: non détectée"
    if p90_imb is not None and p10_imb is not None:
        if p90_imb > 0.4 or p10_imb < -0.4:
            imbalance_comment = "Imbalance extrême détectée (>|0.4|)"

    # Regime change: sign changes in delta within last 10 minutes
    regime_comment = "Changement de régime: non détecté"
    recent = df.sort("minute").tail(10)
    if recent.height >= 3:
        signs = recent.select((pl.col("delta_contracts") > 0).cast(pl.Int32)).to_series().to_list()
        changes = 0
        for i in range(1, len(signs)):
            if signs[i] != signs[i - 1]:
                changes += 1
        if changes >= 3:
            regime_comment = "Changement de régime: alternances rapides du delta (>=3 en 10 minutes)"

    sample = df.sort("minute").select(
        pl.col("minute"),
        pl.col("cvd_contracts"),
        pl.col("delta_contracts"),
        pl.col("contracts_total"),
        pl.col("imbalance_avg"),
        pl.col("spread_avg"),
        pl.col("trades_per_min"),
        pl.col("contracts_buy"),
        pl.col("contracts_sell"),
    ).tail(500)

    idx = list(range(sample.height))
    cvd_points = list(zip(idx, sample["cvd_contracts"].to_list()))
    delta_points = list(zip(idx, sample["delta_contracts"].to_list()))
    vol_points = list(zip(idx, sample["contracts_total"].to_list()))
    imb_points = list(zip(idx, sample["imbalance_avg"].fill_null(0).to_list()))
    spread_points = list(zip(idx, sample["spread_avg"].fill_null(0).to_list()))
    tpm_points = list(zip(idx, sample["trades_per_min"].fill_null(0).to_list()))
    buy_points = list(zip(idx, sample["contracts_buy"].fill_null(0).to_list()))
    sell_points = list(zip(idx, sample["contracts_sell"].fill_null(0).to_list()))

    html = [
        "<html><head><meta charset='utf-8'><title>Quantower Features Report</title></head><body>",
        "<h1>Quantower Features Report (1m)</h1>",
        f"<p><b>Rows:</b> {row_count} | <b>Range:</b> {min_ts} → {max_ts}</p>",
        "<h2>Résumé métier</h2>",
        f"<p><b>Dominance globale</b> — Acheteurs: {dom_buy:.2%}, Vendeurs: {dom_sell:.2%}</p>",
        f"<p><b>Lecture</b> — {trend_comment}. {delta_comment}. {spread_comment}.</p>",
        f"<p><b>Compression</b> — {compression_comment}.</p>",
        f"<p><b>Explosion de volume</b> — {volume_spike_comment}.</p>",
        f"<p><b>Imbalance extrême</b> — {imbalance_comment}.</p>",
        f"<p><b>Changement de régime</b> — {regime_comment}.</p>",
        df_to_html_table(top_vol, "Top 5 minutes par volume (contracts_total)"),
        df_to_html_table(top_pos_delta, "Top 5 deltas positifs"),
        df_to_html_table(top_neg_delta, "Top 5 deltas négatifs"),
        "<h2>Graphiques (échantillon des 500 dernières minutes)</h2>",
        svg_line_chart(cvd_points, title="CVD (cumul delta)"),
        svg_line_chart(delta_points, title="Delta par minute"),
        svg_line_chart(vol_points, title="Volume total (contracts_total)"),
        svg_line_chart(imb_points, title="Imbalance moyenne (bid/ask)"),
        svg_line_chart(spread_points, title="Spread moyen"),
        svg_line_chart(tpm_points, title="Trades par minute"),
        svg_line_chart(buy_points, title="Contracts Buy (agressif)"),
        svg_line_chart(sell_points, title="Contracts Sell (agressif)"),
        df_to_html_table(head, "Preview (first 20 rows)"),
        df_to_html_table(stats, "Summary statistics"),
        "</body></html>",
    ]

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(html))

    print(f"Wrote HTML report: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
