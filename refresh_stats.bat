@echo off
set LOGFILE=%~dp0refresh_stats.log
set DATABASE_URL=postgresql://neondb_owner:npg_GoHvZ45fkEYC@ep-holy-lab-ampee3vi-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require
cd /d "%~dp0"
echo %date% %time% - Starting NBA stats refresh >> "%LOGFILE%"
C:\Python313\python.exe -m ingest.nba_stats >> "%LOGFILE%" 2>&1
echo %date% %time% - Done (exit code %ERRORLEVEL%) >> "%LOGFILE%"
