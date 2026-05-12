@echo off
echo ============================================================
echo    INICIANDO CLUSTER KAFKA + ZOOKEEPER
echo ============================================================
echo.

cd /d "%~dp0.."

echo Iniciando Zookeeper, Kafka y Kafka UI...
docker-compose up -d zookeeper kafka kafka-ui

echo.
echo Esperando a que los servicios inicien...
timeout /t 20 /nobreak

echo.
echo Verificando estado de los servicios...
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | findstr -i "zookeeper kafka"

echo.
echo ============================================================
echo    KAFKA INICIADO
echo ============================================================
echo.
echo    Accesos:
echo    - Kafka UI: http://localhost:8083
echo    - Kafka Broker: localhost:29092
echo    - Zookeeper: localhost:2181
echo.
pause
