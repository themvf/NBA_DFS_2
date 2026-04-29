@echo off
set LOGFILE=%~dp0refresh_mlb_vegas.log
set DATABASE_URL=postgresql://neondb_owner:npg_GoHvZ45fkEYC@ep-holy-lab-ampee3vi-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require
cd /d "%~dp0"
echo %date% %time% - Starting MLB Vegas refresh >> "%LOGFILE%"
C:\Python313\python.exe -m ingest.refresh_mlb_vegas >> "%LOGFILE%" 2>&1
echo %date% %time% - Done (exit code %ERRORLEVEL%) >> "%LOGFILE%"
