# Specs

## Objectif
Projet C# Quantower + pipeline d’analyse Python pour logger des données de marché en temps réel, convertir en Parquet compressé et générer des features/rapports.

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

## Analyse (branche `analysis`)
Scripts Polars :
- `tools/analysis/features_1m_from_ticks.py`  
  OHLC 1m, volumes, delta, CVD, POC, TPO (alias métier)
- `tools/analysis/quote_pressure_from_quotes.py`  
  Spread/imbalance L1 (alias métier)
- `tools/analysis/trade_speed_features.py`  
  Vitesse & taille trades (alias métier)

Rapport HTML :
- `tools/analysis/build_html_report.py`  
  Résumé métier + graphiques + stats

## Git / Branches
- `main` : logger Quantower + pipeline Parquet (sans scripts d’analyse)
- `analysis` : scripts d’analyse + rapport HTML

