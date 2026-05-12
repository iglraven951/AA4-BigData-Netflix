@echo off
echo ============================================================
echo    INICIANDO PRODUCTOR KAFKA (SECCION E - Paso 1)
echo ============================================================
echo.

cd /d "%~dp0.."

echo Verificando que Kafka este corriendo...
docker ps | findstr kafka >nul 2>&1
if errorlevel 1 (
    echo [!] Kafka no esta corriendo. Iniciando...
    docker-compose up -d zookeeper kafka kafka-ui
    echo Esperando a que Kafka inicie...
    timeout /t 15 /nobreak >nul
)

echo.
echo Instalando dependencias de Python...
docker exec spark-master pip install kafka-python --quiet 2>nul

echo.
echo Ejecutando productor de eventos...
echo (Presiona Ctrl+C para detener)
echo.

docker exec -it spark-master python /spark-apps/06_kafka_producer.py --rate 5 --duration 120

echo.
echo ============================================================
echo    PRODUCTOR FINALIZADO
echo ============================================================
pause
