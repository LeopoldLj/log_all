# Prompt Réutilisable (Template)

## Contexte
Projet Quantower C# + pipeline Python Polars pour logging temps réel, conversion Parquet, et analyse.

## Objectifs
1. Mettre à jour l’indicateur Quantower (`log_all`) si nécessaire.
2. Assurer la conversion CSV → Parquet (zstd) dans le datalake.
3. Générer des features 1m et un rapport HTML lisible métier.

## Contraintes
- Pas de L2, pas de reconstruction.
- Données brutes uniquement.
- Conversion après inactivité (90 min).

## Répertoires
- Datalake : `C:\data`
- Parquet : `C:\data\parquet`
- CSV archivés : `C:\data\archive_csv`

## Actions attendues
- Vérifier que les logs sont bien écrits (ticks + quotes).
- Convertir en Parquet sans perte, puis archiver en ZIP.
- Générer `features_1m_alias`, `quote_pressure_1m_alias`, `trade_speed_1m_alias`.
- Générer le rapport HTML (`report_1m.html`).
- Générer le rapport High/Low (`report_highlow_1m.html`).
- Générer un dataset séquentiel (`sequential_1m.*`).

## Questions à poser
1. Sur quel instrument/symbole ?
2. Période d’analyse ?
3. Seuils métier (imbalance, delta, volume) à ajuster ?

## Commandes utiles
```bash
tools\run_convert_logs.bat
python tools/analysis/features_1m_from_ticks.py --parquet-dir "C:\data\parquet" --symbol "/NQ:XCME" --out "C:\data\parquet\features_1m_alias.parquet"
python tools/analysis/quote_pressure_from_quotes.py --parquet-dir "C:\data\parquet" --symbol "/NQ:XCME" --out "C:\data\parquet\quote_pressure_1m_alias.parquet"
python tools/analysis/trade_speed_features.py --parquet-dir "C:\data\parquet" --symbol "/NQ:XCME" --out "C:\data\parquet\trade_speed_1m_alias.parquet" --large-trade 5
python tools/analysis/build_html_report.py --dir "C:\data\parquet" --out "C:\data\parquet\report_1m.html"
python tools/analysis/build_html_highlow_report.py --parquet "C:\data\parquet\features_1m_alias.parquet" --out "C:\data\parquet\report_highlow_1m.html"
python tools/analysis/build_sequential_dataset.py --dir "C:\data\parquet" --symbol "/NQ:XCME" --out-base "C:\data\parquet\sequential_1m"
```

