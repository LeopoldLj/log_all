#!/usr/bin/env python
import os
import sys
import time
import traceback
from datetime import datetime, timezone

import polars as pl


LOG_DIR = os.environ.get(
    "QT_LOG_DIR",
    os.path.join(os.path.expanduser("~"), "Documents", "QuantowerLogs"),
)
INACTIVITY_MINUTES = int(os.environ.get("QT_INACTIVITY_MIN", "90"))
COMPRESS = os.environ.get("QT_PARQUET_COMPRESSION", "zstd")
DELETE_ON_SUNDAY_ONLY = os.environ.get("QT_DELETE_ON_SUNDAY_ONLY", "1") != "0"
ARCHIVE_DIR = os.environ.get("QT_ARCHIVE_DIR", os.path.join(LOG_DIR, "archive_csv"))
ZIP_ARCHIVE = os.environ.get("QT_ZIP_ARCHIVE", "1") != "0"
KEEP_CSV = os.environ.get("QT_KEEP_CSV", "0") != "0"
PARQUET_DIR = os.environ.get("QT_PARQUET_DIR", os.path.join(LOG_DIR, "parquet"))


def utc_now():
    return datetime.now(timezone.utc)


def file_idle_seconds(path: str) -> float:
    mtime = os.path.getmtime(path)
    return (utc_now().timestamp() - mtime)


def count_lines(path: str) -> int:
    count = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            count += chunk.count(b"\n")
    return count


def safe_row_count_csv(path: str) -> int:
    # line count - header
    lines = count_lines(path)
    return max(lines - 1, 0)


def safe_row_count_parquet(path: str) -> int:
    return pl.scan_parquet(path).select(pl.len()).collect(engine="streaming").item()


def convert_csv_to_parquet(csv_path: str, parquet_path: str) -> None:
    # Use streaming scan to handle large files
    lf = pl.scan_csv(csv_path, infer_schema_length=1000, try_parse_dates=True)
    df = lf.collect(engine="streaming")
    df.write_parquet(parquet_path, compression=COMPRESS)


def should_convert(path: str) -> bool:
    if not os.path.isfile(path):
        return False
    if file_idle_seconds(path) < INACTIVITY_MINUTES * 60:
        return False
    return True


def explain_skip(reason: str) -> None:
    print(f"  - Ignoré: {reason}")


def main() -> int:
    if not os.path.isdir(LOG_DIR):
        print(f"Dossier de logs introuvable: {LOG_DIR}")
        return 1

    targets = []
    for name in os.listdir(LOG_DIR):
        if not name.startswith("log_all__"):
            continue
        if not name.endswith(".csv"):
            continue
        if "__ticks_simple.csv" in name or "__quotes.csv" in name or "__events.csv" in name:
            targets.append(os.path.join(LOG_DIR, name))

    if not targets:
        print("Aucun fichier CSV trouvé.")
        return 0

    converted = 0
    skipped = 0
    errors = 0
    is_sunday = datetime.now().weekday() == 6

    for csv_path in sorted(targets):
        if not should_convert(csv_path):
            skipped += 1
            explain_skip(f"fichier encore actif (< {INACTIVITY_MINUTES} min d'inactivité): {os.path.basename(csv_path)}")
            continue

        os.makedirs(PARQUET_DIR, exist_ok=True)
        parquet_path = os.path.join(PARQUET_DIR, os.path.basename(csv_path[:-4] + ".parquet"))
        if os.path.exists(parquet_path):
            skipped += 1
            explain_skip(f"Parquet déjà présent: {os.path.basename(parquet_path)}")
            continue

        try:
            # Capture mtime to ensure file didn't change during conversion
            mtime_before = os.path.getmtime(csv_path)
            convert_csv_to_parquet(csv_path, parquet_path)
            mtime_after = os.path.getmtime(csv_path)

            if mtime_after != mtime_before:
                explain_skip(f"fichier modifié pendant la conversion, suppression annulée: {os.path.basename(csv_path)}")
                skipped += 1
                continue

            csv_rows = safe_row_count_csv(csv_path)
            pq_rows = safe_row_count_parquet(parquet_path)
            if csv_rows == pq_rows and csv_rows >= 0:
                try:
                    if KEEP_CSV:
                        skipped += 1
                        explain_skip(f"conservé (mode test, pas de suppression): {os.path.basename(csv_path)}")
                    elif DELETE_ON_SUNDAY_ONLY and not is_sunday:
                        skipped += 1
                        explain_skip(f"suppression désactivée hors dimanche: {os.path.basename(csv_path)}")
                    else:
                        os.makedirs(ARCHIVE_DIR, exist_ok=True)
                        dst = os.path.join(ARCHIVE_DIR, os.path.basename(csv_path))
                        if os.path.exists(dst):
                            os.remove(dst)
                        os.replace(csv_path, dst)
                        if ZIP_ARCHIVE:
                            zip_path = dst + ".zip"
                            if os.path.exists(zip_path):
                                os.remove(zip_path)
                            import zipfile
                            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                                zf.write(dst, arcname=os.path.basename(dst))
                            os.remove(dst)
                        converted += 1
                        if ZIP_ARCHIVE:
                            print(f"Archivé et compressé (zip) après conversion OK: {os.path.basename(csv_path)}")
                        else:
                            print(f"Archivé après conversion OK: {os.path.basename(csv_path)}")
                except PermissionError:
                    explain_skip(f"fichier verrouillé par un autre processus: {os.path.basename(csv_path)}")
                    skipped += 1
            else:
                print(f"  - Erreur d'intégrité: lignes CSV={csv_rows} Parquet={pq_rows} pour {os.path.basename(csv_path)}")
                skipped += 1
        except Exception:
            errors += 1
            print(f"Erreur lors de la conversion: {os.path.basename(csv_path)}")
            traceback.print_exc()

    print(f"Terminé. convertis={converted} ignores={skipped} erreurs={errors}")
    return 0 if errors == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
