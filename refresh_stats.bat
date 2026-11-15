@echo off
set LOGFILE=%~dp0refresh_stats.log
rem DATABASE_URL comes from .env (config.py loads it); never put it here.
cd /d "%~dp0"
echo %date% %time% - Starting NBA stats refresh >> "%LOGFILE%"
C:\Python313\python.exe -m ingest.nba_stats >> "%LOGFILE%" 2>&1
set RC=%ERRORLEVEL%
echo %date% %time% - Done (exit code %RC%) >> "%LOGFILE%"
exit /b %RC%
