@echo off
setlocal
cd /d "%~dp0"
echo [saby] starting web console...
echo [saby] logs: "%~dp0console.log"
echo [saby] opening: http://127.0.0.1:8765
echo.

start "" http://127.0.0.1:8765
saby_export_console.exe >> console.log 2>&1
set EXIT_CODE=%ERRORLEVEL%

echo.
echo [saby] process exited with code %EXIT_CODE%
echo [saby] check console.log for details
pause
