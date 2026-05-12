@echo off
echo ============================================================
echo    EJECUTANDO PROCESAMIENTO BATCH COMPLETO (SECCION D)
echo ============================================================
echo.

cd /d "%~dp0.."

echo Verificando que Spark este corriendo...
docker ps | findstr spark-master >nul 2>&1
if errorlevel 1 (
    echo [!] Spark no esta corriendo. Iniciando contenedores...
    docker-compose up -d spark-master spark-worker
    timeout /t 10 /nobreak >nul
)

echo.
echo Ejecutando 05_batch_completo.py...
echo.

docker exec spark-master spark-submit --master local[*] /spark-apps/05_batch_completo.py

echo.
echo ============================================================
echo    PROCESAMIENTO BATCH COMPLETADO
echo ============================================================
pause
