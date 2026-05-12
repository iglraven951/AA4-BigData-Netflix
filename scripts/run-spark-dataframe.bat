@echo off
REM =============================================================================
REM EJECUTAR SPARK DATAFRAME - Script 02
REM =============================================================================

echo.
echo ============================================================
echo    Ejecutando procesamiento con Spark DataFrame...
echo ============================================================
echo.

cd /d "%~dp0.."

docker exec -it spark-master spark-submit ^
    --master local[*] ^
    /spark-apps/02_spark_dataframe.py

echo.
echo [OK] Procesamiento DataFrame completado.
echo.
pause
