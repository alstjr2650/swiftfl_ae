@echo off
setlocal
pushd "%~dp0.." || exit /b 1
if /i "%~1"=="plot" (
    python scripts\plot_traffic_results.py --stage %~2 --show
) else (
    python scripts\run_traffic_experiment.py %* --cpu
)
set "traffic_exit=%errorlevel%"
popd
exit /b %traffic_exit%
