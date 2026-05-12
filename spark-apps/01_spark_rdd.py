"""
=============================================================================
SCRIPT 1: PROCESAMIENTO CON SPARK RDD
=============================================================================
Este script demuestra el uso de RDD (Resilient Distributed Dataset)
para procesar los logs de actividad de Netflix.

RDD es la estructura de datos fundamental de Spark, inmutable y distribuida.
=============================================================================
"""

from pyspark import SparkContext, SparkConf
import json
from datetime import datetime
import shutil
import os

# Configuracion de Spark
conf = SparkConf().setAppName("Netflix_RDD_Analysis").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

print("=" * 60)
print("ANALISIS DE LOGS CON SPARK RDD")
print("=" * 60)

# ==================== CARGAR DATOS ====================
# Leer archivo de logs como RDD
logs_rdd = sc.textFile("/datos/logs_actividad.txt")

print(f"\n[1] Total de lineas en logs: {logs_rdd.count()}")

# ==================== TRANSFORMACIONES CON RDD ====================

# Filtrar solo acciones de PLAY (reproducciones)
plays_rdd = logs_rdd.filter(lambda line: "action=PLAY" in line)
print(f"[2] Total de reproducciones: {plays_rdd.count()}")

# Extraer usuario y contenido de cada reproduccion
def extract_play_info(line):
    """Extrae informacion de la linea de log"""
    parts = line.split()
    user = ""
    content = ""
    for part in parts:
        if part.startswith("user="):
            user = part.split("=")[1]
        if part.startswith("content_id="):
            content = part.split("=")[1]
    return (user, content)

user_plays_rdd = plays_rdd.map(extract_play_info)

# Contar reproducciones por usuario
plays_per_user = user_plays_rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
print("\n[3] Reproducciones por usuario (Top 10):")
for user, count in plays_per_user.sortBy(lambda x: -x[1]).take(10):
    print(f"    {user}: {count} reproducciones")

# ==================== ANALISIS DE ACCIONES ====================

# Extraer tipo de accion de cada linea
def extract_action(line):
    """Extrae el tipo de accion del log"""
    for part in line.split():
        if part.startswith("action="):
            return part.split("=")[1]
    return "UNKNOWN"

actions_rdd = logs_rdd.map(extract_action)

# Contar acciones por tipo
action_counts = actions_rdd.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
print("\n[4] Conteo de acciones:")
for action, count in action_counts.sortBy(lambda x: -x[1]).collect():
    print(f"    {action}: {count}")

# ==================== ANALISIS DE ERRORES ====================

# Filtrar lineas de error
errors_rdd = logs_rdd.filter(lambda line: "ERROR" in line)
print(f"\n[5] Total de errores detectados: {errors_rdd.count()}")
print("    Errores encontrados:")
for error in errors_rdd.collect():
    print(f"    - {error[:80]}...")

# ==================== ANALISIS POR PAIS ====================

def extract_country(line):
    """Extrae el pais del log"""
    for part in line.split():
        if part.startswith("country="):
            return part.split("=")[1]
    return "Unknown"

login_rdd = logs_rdd.filter(lambda line: "action=LOGIN" in line)
country_logins = login_rdd.map(extract_country).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

print("\n[6] Logins por pais:")
for country, count in country_logins.sortBy(lambda x: -x[1]).collect():
    print(f"    {country}: {count} logins")

# ==================== GUARDAR RESULTADOS ====================

# Eliminar carpeta si existe para evitar error
output_path = "/resultados/rdd_action_counts"
if os.path.exists(output_path):
    shutil.rmtree(output_path)

# Guardar conteo de acciones como archivo de texto
action_counts.saveAsTextFile(output_path)
print("\n[7] Resultados guardados en /resultados/rdd_action_counts")

# Cerrar SparkContext
sc.stop()

print("\n" + "=" * 60)
print("PROCESAMIENTO RDD COMPLETADO")
print("=" * 60)
