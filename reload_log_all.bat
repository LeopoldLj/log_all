@echo off
setlocal

set DOTNET_CLI_HOME=%~dp0.dotnet
set DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1

dotnet build
if errorlevel 1 (
  echo Build failed. Fix errors and retry.
  exit /b 1
)

echo.
echo Build OK. Quantower must reload the indicator:
echo - Remove log_all from the chart
echo - Add it again
echo (or restart Quantower)
echo.
