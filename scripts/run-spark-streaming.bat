@echo off
echo ============================================================
echo    INICIANDO SPARK STRUCTURED STREAMING (SECCION E - Paso 2)
echo ============================================================
echo.

cd /d "%~dp0.."

echo Verificando que Kafka y Spark esten corriendo...
docker ps | findstr kafka >nul 2>&1
if errorlevel 1 (
    echo [!] Kafka no esta corriendo. Iniciando...
    docker-compose up -d zookeeper kafka kafka-ui
    timeout /t 15 /nobreak >nul
)

docker ps | findstr spark-master >nul 2>&1
if errorlevel 1 (
    echo [!] Spark no esta corriendo. Iniciando...
    docker-compose up -d spark-master spark-worker
    timeout /t 10 /nobreak >nul
)

echo.
echo NOTA: Ejecuta este script DESPUES de iniciar el productor
echo       (run-kafka-producer.bat) en otra terminal.
echo.
echo Presiona cualquier tecla para iniciar el streaming...
pause >nul

echo.
echo Ejecutando Spark Structured Streaming...
echo (Presiona Ctrl+C para detener)
echo.

docker exec -it spark-master spark-submit ^
    --master local[*] ^
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 ^
    /spark-apps/07_spark_streaming.py

echo.
echo ============================================================
echo    STREAMING FINALIZADO
echo ============================================================
pause
