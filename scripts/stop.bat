@echo off
REM =============================================================================
REM SCRIPT DE DETENCION - ECOSISTEMA BIG DATA
REM =============================================================================

echo.
echo ============================================================
echo    Deteniendo servicios del ecosistema Big Data...
echo ============================================================
echo.

cd /d "%~dp0.."

docker-compose down

echo.
echo [OK] Todos los servicios han sido detenidos.
echo.
pause
