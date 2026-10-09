@echo off
rem Weekly CFB early-season pattern watch (cfb-pattern-watch-v1). Local only; appends to artifacts/cfb_pattern_watch/ledger.jsonl.
set LOGFILE=%~dp0refresh_cfb_pattern_watch.log
cd /d "%~dp0"
echo %date% %time% - Starting CFB pattern watch >> "%LOGFILE%"
python -m model.cfb_pattern_watch >> "%LOGFILE%" 2>&1
echo %date% %time% - Done (exit code %ERRORLEVEL%) >> "%LOGFILE%"
