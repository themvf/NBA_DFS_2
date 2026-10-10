@echo off
rem CFB pattern board (cfb-pattern-board-v1): frozen selections, quote freshness, drift. Local only.
set LOGFILE=%~dp0refresh_cfb_pattern_board.log
cd /d "%~dp0"
echo %date% %time% - board >> "%LOGFILE%"
python -m model.cfb_pattern_board >> "%LOGFILE%" 2>&1
echo %date% %time% - done (exit code %ERRORLEVEL%) >> "%LOGFILE%"
