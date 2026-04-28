@echo off
echo Refreshing NBA stats...
set DATABASE_URL=postgresql://neondb_owner:npg_GoHvZ45fkEYC@ep-holy-lab-ampee3vi-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require
cd /d "%~dp0"
python -m ingest.nba_stats
echo.
echo Done!
