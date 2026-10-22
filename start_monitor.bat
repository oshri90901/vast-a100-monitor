@echo off
REM Optional: set your Vast.ai API key (https://cloud.vast.ai/manage-keys/)
REM set VAST_API_KEY=your_key_here
cd /d "%~dp0"
python monitor.py --interval 60 >> monitor.log 2>&1
