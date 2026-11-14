@echo off
rem Push entry (Windows): run preflight, then push only if it passes
rem tier is picked from the pending commits (docs only -> fast, code -> scoped)
rem usage: 推送.bat [--full] [--remote origin] [--branch <branch>]
setlocal
set "SCRIPT_DIR=%~dp0"

where python >nul 2>nul
if errorlevel 1 (
  echo python not found in PATH. Install Python 3 first.
  exit /b 2
)

cd /d "%SCRIPT_DIR%..\..\.."
python "scripts\tools\preflight\推送.py" %*
exit /b %errorlevel%
