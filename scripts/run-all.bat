@echo off
REM =============================================================================
REM EJECUTAR TODOS LOS SCRIPTS - DEMO COMPLETA
REM =============================================================================

echo.
echo ============================================================
echo    DEMO COMPLETA DEL ECOSISTEMA BIG DATA
echo    Netflix Analytics Platform
echo ============================================================
echo.

cd /d "%~dp0.."

REM Verificar contenedores
docker-compose ps | findstr "Up" >nul
if errorlevel 1 (
    echo [!] Los contenedores no estan corriendo.
    echo     Ejecutando start.bat primero...
    call scripts\start.bat
    timeout /t 10 /nobreak >nul
)

echo.
echo ============================================================
echo    PASO 1: Procesamiento con Spark RDD
echo ============================================================
docker exec spark-master spark-submit --master local[*] /spark-apps/01_spark_rdd.py
echo.

echo ============================================================
echo    PASO 2: Procesamiento con Spark DataFrame
echo ============================================================
docker exec spark-master spark-submit --master local[*] /spark-apps/02_spark_dataframe.py
echo.

echo ============================================================
echo    PASO 3: Procesamiento con Spark SQL
echo ============================================================
docker exec spark-master spark-submit --master local[*] /spark-apps/03_spark_sql.py
echo.

echo ============================================================
echo    PASO 4: Agregaciones en MongoDB
echo ============================================================
docker exec mongodb mongosh --username admin --password admin123 --authenticationDatabase admin /docker-entrypoint-initdb.d/03_aggregations.js
echo.

echo ============================================================
echo    DEMO COMPLETADA EXITOSAMENTE
echo ============================================================
echo.
echo Resultados disponibles en:
echo   - Spark: /resultados/ (dentro del contenedor)
echo   - MongoDB: http://localhost:8082 (Mongo Express)
echo.
pause
