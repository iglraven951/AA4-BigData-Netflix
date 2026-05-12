@echo off
REM =============================================================================
REM EJECUTAR SPARK RDD - Script 01
REM =============================================================================

echo.
echo ============================================================
echo    Ejecutando procesamiento con Spark RDD...
echo ============================================================
echo.

cd /d "%~dp0.."

docker exec -it spark-master spark-submit ^
    --master local[*] ^
    /spark-apps/01_spark_rdd.py

echo.
echo [OK] Procesamiento RDD completado.
echo.
pause
