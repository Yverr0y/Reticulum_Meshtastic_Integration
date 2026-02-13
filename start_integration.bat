@echo off
setlocal

cd /d "%~dp0"

set "VENV_DIR=.venv"
set "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"

if not exist "%PYTHON_EXE%" (
    echo [INFO] Creating virtual environment at %VENV_DIR%...
    py -3 -m venv "%VENV_DIR%"
    if errorlevel 1 (
        echo [ERROR] Failed to create virtual environment.
        exit /b 1
    )
)

call "%VENV_DIR%\Scripts\activate.bat"
if errorlevel 1 (
    echo [ERROR] Failed to activate virtual environment.
    exit /b 1
)

echo [INFO] Installing project dependencies...
"%PYTHON_EXE%" -m pip install --upgrade pip >nul
"%PYTHON_EXE%" -m pip install -e .
if errorlevel 1 (
    echo [ERROR] Dependency installation failed.
    exit /b 1
)

if not exist "config.ini" (
    if exist "config..ini" (
        echo [ERROR] config.ini not found. Found config..ini instead.
        echo         Rename config..ini to config.ini and run again.
    ) else (
        echo [ERROR] config.ini not found in %cd%.
    )
    exit /b 1
)

echo [INFO] Starting integration with config.ini...
"%PYTHON_EXE%" -m rch_mesh_bridge.cli start --foreground --config "config.ini"
set "EXIT_CODE=%ERRORLEVEL%"

endlocal & exit /b %EXIT_CODE%

