# Worklog

## 2026-02-27
- Indicateur Quantower `log_all` en v1.0.3.
- Logging brut uniquement : `ticks_simple.csv`, `quotes.csv`, `events.csv`.
- Pipeline CSV → Parquet (zstd) avec archivage ZIP, inactivité 90 min.
- Tâche planifiée Windows : exécution horaire du convertisseur.
- Scripts d’analyse (branche `analysis`) :
  - Features 1m depuis ticks (alias métier, POC, CVD, delta).
  - Quote pressure 1m (spread, imbalance).
  - Trade speed 1m (trades/min, tailles, gros trades).
  - Rapport HTML enrichi (graphiques + commentaires métier).
  - Rapport High/Low agressif.
  - Dataset séquentiel (parquet/csv/json/html).
- Datalake test : `C:\Users\lolo_\Documents\QuantowerLogs\parquet_test`.
- GitHub : `main` (logger + pipeline) / `analysis` (analyse + rapports).

