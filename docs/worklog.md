# Worklog

## 2026-02-25 → 2026-02-27 (récap complet)
- **Indicateur Quantower `log_all`**
  - Migration du template vers un logger brut.
  - Version actuelle : **v1.0.3** (nom affiché `log_all (v1.0.3)`).
  - Loggings conservés : `ticks_simple.csv`, `quotes.csv`, `events.csv`.
  - Logs supprimés : `trades.csv` (dump complet), `per_minute.csv`, `footprint_*`.
  - Ajout filtre taille min (`MinTradeSize`) et parsing agresseur (`BUY/SELL/UNKNOWN`).
  - Nettoyage auto des anciennes DLL et génération d’une DLL versionnée (`log_all_vX.Y.Z.dll`).

- **Pipeline CSV → Parquet**
  - Conversion automatique après **90 minutes d’inactivité**.
  - Compression **zstd**.
  - Parquets dans `C:\data\parquet` (configurable via `QT_PARQUET_DIR`).
  - Archivage ZIP des CSV dans `C:\data\archive_csv`.
  - Suppression remplacée par archivage (fichier verrouillé ignoré).
  - Task Scheduler : exécution **toutes les heures**.
  - Logs d’exécution quotidiens : `tools/convert_logs_YYYY-MM-DD.log` (purge 30 jours).

- **Analyse (branche `analysis`)**
  - Scripts Polars :
    - `features_1m_from_ticks.py` (OHLC, volumes, delta, CVD, POC, TPO, Buy/Sell High/Low).
    - `quote_pressure_from_quotes.py` (spread + imbalance L1).
    - `trade_speed_features.py` (trades/min, taille moyenne, gros trades).
  - Alias métier ajoutés (contracts_buy/sell, delta_contracts, etc.).
  - Rapport HTML enrichi : graphes + commentaires métier.
  - Rapport High/Low agressif dédié.
  - Dataset séquentiel (parquet/csv/json/html).

- **Datalake**
  - Test : `C:\Users\lolo_\Documents\QuantowerLogs\parquet_test`
  - Datalake cible : `C:\data` (modifiable via `QT_LOG_DIR`)

- **Git / GitHub**
  - Repo : `https://github.com/LeopoldLj/log_all`
  - Branches :
    - `main` : logger Quantower + pipeline (sans analyse)
    - `analysis` : scripts d’analyse + rapports
  - Docs ajoutés : `docs/specs.md`, `docs/prompt.md`, `docs/worklog.md`
