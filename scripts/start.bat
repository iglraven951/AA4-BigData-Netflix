@echo off
REM =============================================================================
REM SCRIPT DE INICIO - ECOSISTEMA BIG DATA NETFLIX ANALYTICS
REM =============================================================================
REM Este script inicia todos los contenedores del ecosistema
REM =============================================================================

echo.
echo ============================================================
echo    ECOSISTEMA BIG DATA - NETFLIX ANALYTICS
echo    Iniciando todos los servicios...
echo ============================================================
echo.

cd /d "%~dp0.."

REM Verificar que Docker esta corriendo
docker info >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker no esta corriendo. Por favor inicie Docker Desktop.
    pause
    exit /b 1
)

echo [1/3] Deteniendo contenedores existentes...
docker-compose down 2>nul

echo.
echo [2/3] Iniciando contenedores...
docker-compose up -d

echo.
echo [3/3] Esperando a que los servicios esten listos...
timeout /t 30 /nobreak >nul

echo.
echo ============================================================
echo    SERVICIOS INICIADOS
echo ============================================================
echo.
echo    HADOOP:
echo      - NameNode UI:        http://localhost:9870
echo      - ResourceManager UI: http://localhost:8088
echo      - History Server:     http://localhost:8188
echo.
echo    SPARK:
echo      - Spark Master UI:    http://localhost:8080
echo      - Spark Worker UI:    http://localhost:8081
echo.
echo    MONGODB:
echo      - MongoDB:            localhost:27017
echo      - Mongo Express UI:   http://localhost:8082
echo.
echo ============================================================
echo.

docker-compose ps

echo.
echo Para ver los logs: docker-compose logs -f
echo Para detener: scripts\stop.bat
echo.
pause
