@echo off
echo ============================================================
echo    DEMO COMPLETA DE STREAMING (SECCION E)
echo ============================================================
echo.
echo    Este script ejecuta la demo completa de streaming:
echo    1. Inicia Kafka si no esta corriendo
echo    2. Abre una ventana con el PRODUCTOR de eventos
echo    3. Abre otra ventana con el CONSUMIDOR Spark Streaming
echo.
echo ============================================================
echo.

cd /d "%~dp0.."

echo [1/4] Verificando Docker...
docker info >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker no esta corriendo. Por favor inicia Docker Desktop.
    pause
    exit /b 1
)

echo [2/4] Iniciando servicios de Kafka...
docker-compose up -d zookeeper kafka kafka-ui spark-master spark-worker
echo Esperando 20 segundos para que los servicios inicien...
timeout /t 20 /nobreak >nul

echo [3/4] Instalando dependencias...
docker exec spark-master pip install kafka-python --quiet 2>nul

echo [4/4] Abriendo ventanas de demo...
echo.
echo INSTRUCCIONES:
echo    - Se abriran DOS ventanas de terminal
echo    - VENTANA 1: Productor de eventos (genera eventos)
echo    - VENTANA 2: Consumidor Spark Streaming (procesa eventos)
echo.
echo Presiona cualquier tecla para iniciar la demo...
pause >nul

REM Abrir productor en nueva ventana
start "KAFKA PRODUCER" cmd /c "cd /d "%~dp0.." && docker exec -it spark-master python /spark-apps/06_kafka_producer.py --rate 5 --duration 180 && pause"

REM Esperar un poco para que el productor inicie
timeout /t 5 /nobreak >nul

REM Abrir consumidor en nueva ventana
start "SPARK STREAMING" cmd /c "cd /d "%~dp0.." && docker exec -it spark-master spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 /spark-apps/07_spark_streaming.py && pause"

echo.
echo ============================================================
echo    DEMO INICIADA
echo ============================================================
echo.
echo    Observa las dos ventanas que se abrieron:
echo    - KAFKA PRODUCER: Genera eventos simulados
echo    - SPARK STREAMING: Procesa eventos en tiempo real
echo.
echo    Para detener: Cierra ambas ventanas o presiona Ctrl+C
echo.
echo    Accesos web:
echo    - Kafka UI: http://localhost:8083
echo    - Spark UI: http://localhost:8080
echo.
pause
