@echo off
setlocal
cd /d "%~dp0"
echo [saby] starting web console...
echo [saby] logs: "%~dp0console.log"
echo.

saby_export_console.exe >> console.log 2>&1
set EXIT_CODE=%ERRORLEVEL%

echo.
echo [saby] process exited with code %EXIT_CODE%
echo [saby] check console.log for details
pause
