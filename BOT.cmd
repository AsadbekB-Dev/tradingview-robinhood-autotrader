@echo off
cd /d "%~dp0"
set "BOTPY=%~dp0.venv\Scripts\python.exe"
if not exist "%BOTPY%" (
  echo [BOT] One-time setup has not been completed on this PC.
  echo Run SETUP_ONCE.cmd first.
  pause
  exit /b 1
)
if not exist "%~dp0tools\cloudflared.exe" (
  echo [BOT] cloudflared.exe is missing.
  echo Run REPAIR_SETUP.cmd.
  pause
  exit /b 1
)
"%BOTPY%" -c "from zoneinfo import ZoneInfo; ZoneInfo('America/New_York')" >nul 2>&1
if errorlevel 1 (
  echo [BOT] Python environment is incomplete.
  echo Run REPAIR_SETUP.cmd. Normal startup will not install packages automatically.
  pause
  exit /b 1
)
"%BOTPY%" "%~dp0tools\bot_app.py"
