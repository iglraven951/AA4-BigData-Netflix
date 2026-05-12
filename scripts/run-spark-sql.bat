@echo off
REM =============================================================================
REM EJECUTAR SPARK SQL - Script 03
REM =============================================================================

echo.
echo ============================================================
echo    Ejecutando procesamiento con Spark SQL...
echo ============================================================
echo.

cd /d "%~dp0.."

docker exec -it spark-master spark-submit ^
    --master local[*] ^
    /spark-apps/03_spark_sql.py

echo.
echo [OK] Procesamiento Spark SQL completado.
echo.
pause
