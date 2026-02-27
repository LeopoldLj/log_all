# Specs

## Objectif
Projet C# Quantower + pipeline Python pour logger des données brutes en temps réel, convertir en Parquet compressé et générer des features/rapports analytiques.

## Indicateur Quantower
Nom : `log_all`  
Version : `v1.0.3`

### Loggings (données brutes)
- **Ticks simples** : `ticks_simple.csv`  
  Colonnes : `utc_now,symbol,price,size,side`
- **Quotes L1** : `quotes.csv`  
  Colonnes : `utc_now,symbol,quote_dump`
- **Events** : `events.csv`  
  Colonnes : `utc_now,event,details`

### Contraintes
- Aucune reconstruction (pas d’heuristique).
- Aucune donnée L2.
- Données brutes uniquement.

## Conversion Parquet
Script : `tools/convert_quantower_logs.py`  
Runner : `tools/run_convert_logs.bat`

### Comportement
- Inactivité minimale : **90 minutes**
- Parquet compressé `zstd`
- Parquets écrits dans : `C:\data\parquet` (via `QT_PARQUET_DIR`)
- Archivage ZIP des CSV dans : `C:\data\archive_csv`
- Suppression CSV : **après conversion OK**, sauf si fichier verrouillé

Variables d’environnement clés :
- `QT_LOG_DIR` (datalake)
- `QT_PARQUET_DIR`
- `QT_ARCHIVE_DIR`
- `QT_ZIP_ARCHIVE`
- `QT_INACTIVITY_MIN`
 - `QT_KEEP_CSV` (mode test)

## Analyse (branche `analysis`)
Scripts Polars :
- `tools/analysis/features_1m_from_ticks.py`  
  OHLC 1m, volumes, delta, CVD, POC, TPO (alias métier)  
  + Buy/Sell au High/Low (agressifs)
- `tools/analysis/quote_pressure_from_quotes.py`  
  Spread/imbalance L1 (alias métier)
- `tools/analysis/trade_speed_features.py`  
  Vitesse & taille trades (alias métier)

Rapport HTML :
- `tools/analysis/build_html_report.py`  
  Résumé métier + graphiques + stats + commentaires automatiques
- `tools/analysis/build_html_highlow_report.py`  
  Top 10 Buy/Sell au High/Low
- `tools/analysis/build_sequential_dataset.py`  
  Dataset séquentiel (parquet/csv/json/html)

## Git / Branches
- `main` : logger Quantower + pipeline Parquet (sans scripts d’analyse)
- `analysis` : scripts d’analyse + rapport HTML

