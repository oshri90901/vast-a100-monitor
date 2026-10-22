@echo off
cd /d "%~dp0"
python report.py --csv reports %*
pause
