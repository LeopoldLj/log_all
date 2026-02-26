@echo off
setlocal

set QT_LOG_DIR=%USERPROFILE%\Documents\QuantowerLogs
set QT_INACTIVITY_MIN=90
set QT_PARQUET_COMPRESSION=zstd
set QT_DELETE_ON_SUNDAY_ONLY=0
set QT_ARCHIVE_DIR=%QT_LOG_DIR%\archive_csv
set QT_ZIP_ARCHIVE=1
set QT_PARQUET_DIR=%QT_LOG_DIR%\parquet

python -m pip show polars >nul 2>&1
if errorlevel 1 (
  echo Installing polars...
  python -m pip install --user polars
)

set LOG_FILE=%~dp0convert_logs_%DATE:~6,4%-%DATE:~3,2%-%DATE:~0,2%.log
echo ===== %DATE% %TIME% ===== >> "%LOG_FILE%"
python "%~dp0convert_quantower_logs.py" >> "%LOG_FILE%" 2>&1

forfiles /p "%~dp0" /m "convert_logs_*.log" /d -30 /c "cmd /c del @path" >nul 2>&1
