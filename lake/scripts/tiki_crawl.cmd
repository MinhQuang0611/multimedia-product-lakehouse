@echo off
cd /d "%~dp0.."
if not exist data\_logs mkdir data\_logs
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmm"') do set TS=%%i
call conda run -n btl --no-capture-output python run.py crawl >> "data\_logs\tiki_crawl_%TS%.log" 2>&1
