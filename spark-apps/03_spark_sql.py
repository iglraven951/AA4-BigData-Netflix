"""
=============================================================================
SCRIPT 3: PROCESAMIENTO CON SPARK SQL
=============================================================================
Este script demuestra el uso de Spark SQL para realizar consultas
SQL sobre los datos de Netflix.

Spark SQL permite usar sintaxis SQL estandar sobre DataFrames.
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, max, min, desc
from pyspark.sql.functions import round as spark_round, current_date, datediff

# Crear SparkSession
spark = SparkSession.builder \
    .appName("Netflix_SparkSQL_Analysis") \
    .master("local[*]") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("ANALISIS DE DATOS CON SPARK SQL")
print("=" * 70)

# ==================== CARGAR DATOS ====================
print("\n[1] CARGANDO DATOS Y CREANDO VISTAS TEMPORALES...")

# Cargar todos los datasets
catalogo_df = spark.read.csv("/datos/catalogo_contenido.csv", header=True, inferSchema=True)
usuarios_df = spark.read.csv("/datos/usuarios.csv", header=True, inferSchema=True)
visualizaciones_df = spark.read.json("/datos/visualizaciones.json")
valoraciones_df = spark.read.json("/datos/valoraciones.json")

# Crear vistas temporales para usar SQL
catalogo_df.createOrReplaceTempView("catalogo")
usuarios_df.createOrReplaceTempView("usuarios")
visualizaciones_df.createOrReplaceTempView("visualizaciones")
valoraciones_df.createOrReplaceTempView("valoraciones")

print("    Vistas creadas: catalogo, usuarios, visualizaciones, valoraciones")

# ==================== CONSULTAS SQL ====================
print("\n" + "=" * 70)
print("[2] CONSULTAS SQL SOBRE EL CATALOGO")
print("=" * 70)

# Query 1: Total de contenido por tipo
print("\n    QUERY 1: Contenido por tipo")
query1 = """
    SELECT
        tipo,
        COUNT(*) as cantidad,
        ROUND(AVG(calificacion), 2) as calificacion_promedio,
        ROUND(AVG(duracion_min), 0) as duracion_promedio
    FROM catalogo
    GROUP BY tipo
    ORDER BY cantidad DESC
"""
spark.sql(query1).show()

# Query 2: Top 10 contenido mejor calificado
print("\n    QUERY 2: Top 10 contenido mejor calificado")
query2 = """
    SELECT
        titulo,
        tipo,
        genero,
        calificacion,
        anio
    FROM catalogo
    ORDER BY calificacion DESC
    LIMIT 10
"""
spark.sql(query2).show(truncate=False)

# Query 3: Contenido por decada
print("\n    QUERY 3: Contenido por decada")
query3 = """
    SELECT
        CASE
            WHEN anio BETWEEN 1980 AND 1989 THEN '1980s'
            WHEN anio BETWEEN 1990 AND 1999 THEN '1990s'
            WHEN anio BETWEEN 2000 AND 2009 THEN '2000s'
            WHEN anio BETWEEN 2010 AND 2019 THEN '2010s'
            WHEN anio >= 2020 THEN '2020s'
            ELSE 'Otro'
        END as decada,
        COUNT(*) as cantidad,
        ROUND(AVG(calificacion), 2) as calificacion_promedio
    FROM catalogo
    GROUP BY decada
    ORDER BY decada
"""
spark.sql(query3).show()

# ==================== CONSULTAS SQL SOBRE USUARIOS ====================
print("\n" + "=" * 70)
print("[3] CONSULTAS SQL SOBRE USUARIOS")
print("=" * 70)

# Query 4: Distribucion de usuarios por plan y pais
print("\n    QUERY 4: Usuarios por plan y pais")
query4 = """
    SELECT
        pais,
        plan,
        COUNT(*) as cantidad,
        ROUND(AVG(edad), 1) as edad_promedio
    FROM usuarios
    GROUP BY pais, plan
    ORDER BY pais, cantidad DESC
"""
spark.sql(query4).show(20)

# Query 5: Segmentacion de usuarios por edad
print("\n    QUERY 5: Segmentacion de usuarios por grupo de edad")
query5 = """
    SELECT
        CASE
            WHEN edad BETWEEN 18 AND 25 THEN 'Jovenes (18-25)'
            WHEN edad BETWEEN 26 AND 35 THEN 'Adultos jovenes (26-35)'
            WHEN edad BETWEEN 36 AND 45 THEN 'Adultos (36-45)'
            WHEN edad > 45 THEN 'Adultos mayores (45+)'
            ELSE 'Otro'
        END as grupo_edad,
        COUNT(*) as cantidad,
        plan
    FROM usuarios
    GROUP BY grupo_edad, plan
    ORDER BY grupo_edad, cantidad DESC
"""
spark.sql(query5).show()

# ==================== CONSULTAS SQL CON JOINS ====================
print("\n" + "=" * 70)
print("[4] CONSULTAS SQL CON JOINS")
print("=" * 70)

# Query 6: Visualizaciones con informacion de catalogo
print("\n    QUERY 6: Analisis de visualizaciones por genero")
query6 = """
    SELECT
        c.genero,
        c.tipo,
        COUNT(*) as total_visualizaciones,
        SUM(CASE WHEN v.completado = true THEN 1 ELSE 0 END) as completadas,
        ROUND(SUM(CASE WHEN v.completado = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as tasa_completado
    FROM visualizaciones v
    JOIN catalogo c ON v.content_id = c.id
    GROUP BY c.genero, c.tipo
    ORDER BY total_visualizaciones DESC
"""
spark.sql(query6).show(15)

# Query 7: Valoraciones promedio por genero
print("\n    QUERY 7: Rating promedio por genero")
query7 = """
    SELECT
        c.genero,
        COUNT(r.rating_id) as total_valoraciones,
        ROUND(AVG(r.puntuacion), 2) as rating_promedio,
        MIN(r.puntuacion) as rating_minimo,
        MAX(r.puntuacion) as rating_maximo
    FROM valoraciones r
    JOIN catalogo c ON r.content_id = c.id
    GROUP BY c.genero
    HAVING COUNT(r.rating_id) >= 2
    ORDER BY rating_promedio DESC
"""
spark.sql(query7).show()

# Query 8: Usuarios mas activos (mas visualizaciones)
print("\n    QUERY 8: Usuarios mas activos")
query8 = """
    SELECT
        u.nombre,
        u.pais,
        u.plan,
        COUNT(v.view_id) as total_visualizaciones,
        SUM(v.duracion_vista_min) as minutos_totales
    FROM usuarios u
    JOIN visualizaciones v ON u.user_id = v.user_id
    GROUP BY u.user_id, u.nombre, u.pais, u.plan
    ORDER BY total_visualizaciones DESC
    LIMIT 10
"""
spark.sql(query8).show(truncate=False)

# ==================== CONSULTAS SQL AVANZADAS ====================
print("\n" + "=" * 70)
print("[5] CONSULTAS SQL AVANZADAS")
print("=" * 70)

# Query 9: Contenido con mejor engagement (visualizado y bien valorado)
print("\n    QUERY 9: Contenido con mejor engagement")
query9 = """
    SELECT
        c.titulo,
        c.tipo,
        c.genero,
        COUNT(DISTINCT v.view_id) as visualizaciones,
        COUNT(DISTINCT r.rating_id) as valoraciones,
        ROUND(AVG(r.puntuacion), 2) as rating_promedio,
        ROUND(SUM(CASE WHEN v.completado = true THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT v.view_id), 2) as tasa_completado
    FROM catalogo c
    LEFT JOIN visualizaciones v ON c.id = v.content_id
    LEFT JOIN valoraciones r ON c.id = r.content_id
    GROUP BY c.id, c.titulo, c.tipo, c.genero
    HAVING COUNT(DISTINCT v.view_id) > 0
    ORDER BY visualizaciones DESC, rating_promedio DESC
    LIMIT 15
"""
spark.sql(query9).show(truncate=False)

# Query 10: Analisis de preferencias por pais
print("\n    QUERY 10: Generos preferidos por pais de usuario")
query10 = """
    SELECT
        u.pais,
        c.genero,
        COUNT(*) as visualizaciones
    FROM usuarios u
    JOIN visualizaciones v ON u.user_id = v.user_id
    JOIN catalogo c ON v.content_id = c.id
    GROUP BY u.pais, c.genero
    ORDER BY u.pais, visualizaciones DESC
"""
spark.sql(query10).show(30)

# ==================== GUARDAR RESULTADOS ====================
print("\n" + "=" * 70)
print("[6] GUARDANDO RESULTADOS DE CONSULTAS SQL")
print("=" * 70)

# Guardar Query 6 como Parquet (formato optimizado)
resultado_q6 = spark.sql(query6)
resultado_q6.write.mode("overwrite").parquet("/resultados/sql_visualizaciones_por_genero")
print("    - Query 6 guardada en /resultados/sql_visualizaciones_por_genero (Parquet)")

# Guardar Query 9 como CSV
resultado_q9 = spark.sql(query9)
resultado_q9.write.mode("overwrite").csv("/resultados/sql_engagement_contenido", header=True)
print("    - Query 9 guardada en /resultados/sql_engagement_contenido (CSV)")

# Guardar Query 10 como JSON
resultado_q10 = spark.sql(query10)
resultado_q10.write.mode("overwrite").json("/resultados/sql_preferencias_pais")
print("    - Query 10 guardada en /resultados/sql_preferencias_pais (JSON)")

# Cerrar SparkSession
spark.stop()

print("\n" + "=" * 70)
print("PROCESAMIENTO SPARK SQL COMPLETADO")
print("=" * 70)
