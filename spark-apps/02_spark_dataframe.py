"""
=============================================================================
SCRIPT 2: PROCESAMIENTO CON SPARK DATAFRAME
=============================================================================
Este script demuestra el uso de DataFrames de Spark para procesar
los archivos CSV y JSON del ecosistema Netflix.

DataFrame es una API de alto nivel, mas expresiva y optimizada que RDD.
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, max, min, desc, asc
from pyspark.sql.functions import when, lit, concat, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType

# Crear SparkSession
spark = SparkSession.builder \
    .appName("Netflix_DataFrame_Analysis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("ANALISIS DE DATOS CON SPARK DATAFRAME")
print("=" * 70)

# ==================== CARGAR DATOS CSV ====================
print("\n[1] CARGANDO ARCHIVOS CSV...")

# Cargar catalogo de contenido
catalogo_df = spark.read.csv("/datos/catalogo_contenido.csv", header=True, inferSchema=True)
print(f"    - Catalogo: {catalogo_df.count()} registros")

# Cargar usuarios
usuarios_df = spark.read.csv("/datos/usuarios.csv", header=True, inferSchema=True)
print(f"    - Usuarios: {usuarios_df.count()} registros")

# ==================== CARGAR DATOS JSON ====================
print("\n[2] CARGANDO ARCHIVOS JSON...")

# Cargar visualizaciones
visualizaciones_df = spark.read.json("/datos/visualizaciones.json")
print(f"    - Visualizaciones: {visualizaciones_df.count()} registros")

# Cargar valoraciones
valoraciones_df = spark.read.json("/datos/valoraciones.json")
print(f"    - Valoraciones: {valoraciones_df.count()} registros")

# ==================== MOSTRAR SCHEMAS ====================
print("\n[3] ESTRUCTURA DE LOS DATAFRAMES:")
print("\n    Catalogo:")
catalogo_df.printSchema()

print("\n    Usuarios:")
usuarios_df.printSchema()

# ==================== ANALISIS DEL CATALOGO ====================
print("\n" + "=" * 70)
print("[4] ANALISIS DEL CATALOGO DE CONTENIDO")
print("=" * 70)

# Conteo por tipo (serie vs pelicula)
print("\n    Contenido por tipo:")
catalogo_df.groupBy("tipo").count().orderBy(desc("count")).show()

# Conteo por genero
print("\n    Top 10 generos mas populares:")
catalogo_df.groupBy("genero").count().orderBy(desc("count")).show(10)

# Calificacion promedio por genero
print("\n    Calificacion promedio por genero:")
catalogo_df.groupBy("genero") \
    .agg(
        spark_round(avg("calificacion"), 2).alias("calificacion_promedio"),
        count("*").alias("cantidad")
    ) \
    .orderBy(desc("calificacion_promedio")) \
    .show(10)

# Contenido por pais
print("\n    Contenido por pais de origen:")
catalogo_df.groupBy("pais").count().orderBy(desc("count")).show(10)

# ==================== ANALISIS DE USUARIOS ====================
print("\n" + "=" * 70)
print("[5] ANALISIS DE USUARIOS")
print("=" * 70)

# Usuarios por plan de suscripcion
print("\n    Distribucion por plan:")
usuarios_df.groupBy("plan").count().orderBy(desc("count")).show()

# Usuarios por pais
print("\n    Usuarios por pais:")
usuarios_df.groupBy("pais").count().orderBy(desc("count")).show()

# Usuarios por dispositivo principal
print("\n    Dispositivo preferido:")
usuarios_df.groupBy("dispositivo_principal").count().orderBy(desc("count")).show()

# Estadisticas de edad
print("\n    Estadisticas de edad de usuarios:")
usuarios_df.agg(
    spark_round(avg("edad"), 1).alias("edad_promedio"),
    min("edad").alias("edad_minima"),
    max("edad").alias("edad_maxima")
).show()

# ==================== ANALISIS DE VISUALIZACIONES ====================
print("\n" + "=" * 70)
print("[6] ANALISIS DE VISUALIZACIONES")
print("=" * 70)

# Tasa de completado
print("\n    Tasa de completado de contenido:")
visualizaciones_df.groupBy("completado") \
    .count() \
    .withColumn("porcentaje", spark_round(col("count") / visualizaciones_df.count() * 100, 2)) \
    .show()

# Visualizaciones por dispositivo
print("\n    Visualizaciones por dispositivo:")
visualizaciones_df.groupBy("dispositivo").count().orderBy(desc("count")).show()

# Duracion promedio de visualizacion
print("\n    Duracion promedio de visualizacion (minutos):")
visualizaciones_df.agg(
    spark_round(avg("duracion_vista_min"), 1).alias("duracion_promedio"),
    sum("duracion_vista_min").alias("duracion_total")
).show()

# ==================== ANALISIS DE VALORACIONES ====================
print("\n" + "=" * 70)
print("[7] ANALISIS DE VALORACIONES")
print("=" * 70)

# Distribucion de puntuaciones
print("\n    Distribucion de puntuaciones:")
valoraciones_df.groupBy("puntuacion").count().orderBy("puntuacion").show()

# Puntuacion promedio
print("\n    Estadisticas de puntuaciones:")
valoraciones_df.agg(
    spark_round(avg("puntuacion"), 2).alias("puntuacion_promedio"),
    min("puntuacion").alias("minima"),
    max("puntuacion").alias("maxima")
).show()

# ==================== JOINS ENTRE DATAFRAMES ====================
print("\n" + "=" * 70)
print("[8] ANALISIS CRUZADO (JOINS)")
print("=" * 70)

# Join: Valoraciones con Catalogo para ver que contenido tiene mejores ratings
print("\n    Contenido mejor valorado:")
valoraciones_con_catalogo = valoraciones_df.join(
    catalogo_df,
    valoraciones_df.content_id == catalogo_df.id,
    "inner"
)

valoraciones_con_catalogo.groupBy("titulo", "tipo", "genero") \
    .agg(
        spark_round(avg("puntuacion"), 2).alias("rating_promedio"),
        count("*").alias("num_valoraciones")
    ) \
    .filter(col("num_valoraciones") >= 1) \
    .orderBy(desc("rating_promedio")) \
    .show(15, truncate=False)

# ==================== GUARDAR RESULTADOS ====================
print("\n" + "=" * 70)
print("[9] GUARDANDO RESULTADOS")
print("=" * 70)

# Guardar analisis de catalogo como CSV
catalogo_stats = catalogo_df.groupBy("genero", "tipo") \
    .agg(
        count("*").alias("cantidad"),
        spark_round(avg("calificacion"), 2).alias("calificacion_promedio")
    )
catalogo_stats.write.mode("overwrite").csv("/resultados/df_catalogo_stats", header=True)
print("    - Estadisticas de catalogo guardadas en /resultados/df_catalogo_stats")

# Guardar analisis de usuarios como JSON
usuarios_stats = usuarios_df.groupBy("pais", "plan").count()
usuarios_stats.write.mode("overwrite").json("/resultados/df_usuarios_stats")
print("    - Estadisticas de usuarios guardadas en /resultados/df_usuarios_stats")

# Cerrar SparkSession
spark.stop()

print("\n" + "=" * 70)
print("PROCESAMIENTO DATAFRAME COMPLETADO")
print("=" * 70)
