@echo off
REM =============================================================================
REM EJECUTAR AGREGACIONES EN MONGODB
REM =============================================================================

echo.
echo ============================================================
echo    Ejecutando agregaciones en MongoDB...
echo ============================================================
echo.

cd /d "%~dp0.."

docker exec -it mongodb mongosh --username admin --password admin123 --authenticationDatabase admin /docker-entrypoint-initdb.d/03_aggregations.js

echo.
echo [OK] Agregaciones de MongoDB completadas.
echo.
pause
