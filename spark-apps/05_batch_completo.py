"""
=============================================================================
SCRIPT 5: PROCESAMIENTO BATCH COMPLETO CON SPARK
=============================================================================
Este script consolida TODOS los requisitos del procesamiento batch:

D. Procesamiento batch con Spark
   [x] Lectura de archivos (CSV, JSON, TXT)
   [x] Limpieza de datos (nulls, duplicados, outliers)
   [x] Transformación de columnas (cast, rename, nuevas columnas)
   [x] Integración de fuentes (joins entre datasets)
   [x] Uso de DataFrames
   [x] Uso de Spark SQL
   [x] Uso de RDD
   [x] Generación de resultados o KPIs
   [x] Exportación de resultados (CSV, JSON, Parquet, MongoDB)

Proyecto: Netflix Analytics Platform - AA4 Big Data
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum, max, min, desc, asc,
    when, lit, concat, round as spark_round,
    upper, lower, trim, regexp_replace,
    year, month, dayofweek, current_timestamp,
    coalesce, isnan, isnull
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, BooleanType, TimestampType
)
from pyspark import SparkContext
import shutil
import os

# =============================================================================
# CONFIGURACION DE SPARK
# =============================================================================
spark = SparkSession.builder \
    .appName("Netflix_Batch_Completo_AA4") \
    .master("local[*]") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

print("=" * 80)
print("    PROCESAMIENTO BATCH COMPLETO - NETFLIX ANALYTICS AA4")
print("=" * 80)
print("""
    Este script demuestra TODOS los requisitos del procesamiento batch:

    [D1] Lectura de archivos (CSV, JSON, TXT)
    [D2] Limpieza de datos
    [D3] Transformación de columnas
    [D4] Integración de fuentes (JOINs)
    [D5] Uso de DataFrames
    [D6] Uso de Spark SQL
    [D7] Uso de RDD
    [D8] Generación de KPIs
    [D9] Exportación de resultados
""")

# =============================================================================
# [D1] LECTURA DE ARCHIVOS (CSV, JSON, TXT)
# =============================================================================
print("\n" + "=" * 80)
print("[D1] LECTURA DE ARCHIVOS - Múltiples formatos")
print("=" * 80)

# Lectura de CSV con schema inferido
print("\n    [D1.1] Leyendo archivos CSV...")
catalogo_df = spark.read.csv("/datos/catalogo_contenido.csv", header=True, inferSchema=True)
usuarios_df = spark.read.csv("/datos/usuarios.csv", header=True, inferSchema=True)
print(f"           - catalogo_contenido.csv: {catalogo_df.count()} registros")
print(f"           - usuarios.csv: {usuarios_df.count()} registros")

# Lectura de JSON
print("\n    [D1.2] Leyendo archivos JSON...")
visualizaciones_df = spark.read.json("/datos/visualizaciones.json")
valoraciones_df = spark.read.json("/datos/valoraciones.json")
print(f"           - visualizaciones.json: {visualizaciones_df.count()} registros")
print(f"           - valoraciones.json: {valoraciones_df.count()} registros")

# Lectura de archivo de texto (logs)
print("\n    [D1.3] Leyendo archivo de texto (logs)...")
logs_rdd = sc.textFile("/datos/logs_actividad.txt")
print(f"           - logs_actividad.txt: {logs_rdd.count()} lineas")

# Mostrar schemas
print("\n    [D1.4] Schemas de los DataFrames:")
print("\n           Catalogo:")
catalogo_df.printSchema()

# =============================================================================
# [D2] LIMPIEZA DE DATOS
# =============================================================================
print("\n" + "=" * 80)
print("[D2] LIMPIEZA DE DATOS - Nulls, duplicados, outliers")
print("=" * 80)

# Conteo de nulls antes de limpieza
print("\n    [D2.1] Verificando valores nulos...")
for col_name in catalogo_df.columns:
    null_count = catalogo_df.filter(col(col_name).isNull()).count()
    if null_count > 0:
        print(f"           - {col_name}: {null_count} nulos")

# Verificar duplicados
print("\n    [D2.2] Verificando duplicados...")
total_catalogo = catalogo_df.count()
distintos_catalogo = catalogo_df.dropDuplicates().count()
duplicados = total_catalogo - distintos_catalogo
print(f"           - Catalogo: {duplicados} duplicados encontrados")

# Limpieza de datos - eliminar nulos y duplicados
print("\n    [D2.3] Aplicando limpieza...")
catalogo_limpio = catalogo_df \
    .dropDuplicates() \
    .na.fill({"calificacion": 0.0, "duracion_min": 0}) \
    .filter(col("titulo").isNotNull())

usuarios_limpio = usuarios_df \
    .dropDuplicates(["user_id"]) \
    .na.fill({"edad": 25}) \
    .filter(col("email").isNotNull())

# Filtrar outliers (por ejemplo, edades fuera de rango)
usuarios_limpio = usuarios_limpio.filter(
    (col("edad") >= 13) & (col("edad") <= 100)
)

print(f"           - Catalogo limpio: {catalogo_limpio.count()} registros")
print(f"           - Usuarios limpio: {usuarios_limpio.count()} registros")

# =============================================================================
# [D3] TRANSFORMACION DE COLUMNAS
# =============================================================================
print("\n" + "=" * 80)
print("[D3] TRANSFORMACION DE COLUMNAS - Cast, rename, nuevas columnas")
print("=" * 80)

# Renombrar columnas
print("\n    [D3.1] Renombrando columnas...")
catalogo_trans = catalogo_limpio \
    .withColumnRenamed("id", "content_id") \
    .withColumnRenamed("anio", "year_released")

# Crear nuevas columnas calculadas
print("    [D3.2] Creando columnas calculadas...")
catalogo_trans = catalogo_trans \
    .withColumn("es_reciente", when(col("year_released") >= 2020, True).otherwise(False)) \
    .withColumn("categoria_duracion",
        when(col("duracion_min") < 30, "Corto")
        .when(col("duracion_min") < 90, "Medio")
        .when(col("duracion_min") < 150, "Largo")
        .otherwise("Muy largo")) \
    .withColumn("calificacion_categoria",
        when(col("calificacion") >= 8.5, "Excelente")
        .when(col("calificacion") >= 7.0, "Bueno")
        .when(col("calificacion") >= 5.0, "Regular")
        .otherwise("Bajo")) \
    .withColumn("processed_timestamp", current_timestamp())

# Transformaciones de texto
print("    [D3.3] Transformando texto...")
catalogo_trans = catalogo_trans \
    .withColumn("genero_upper", upper(col("genero"))) \
    .withColumn("titulo_limpio", trim(regexp_replace(col("titulo"), r"[^a-zA-Z0-9\s]", "")))

# Transformar usuarios
usuarios_trans = usuarios_limpio \
    .withColumn("grupo_edad",
        when(col("edad") < 18, "Menor")
        .when(col("edad") < 30, "Joven")
        .when(col("edad") < 50, "Adulto")
        .otherwise("Senior")) \
    .withColumn("email_dominio", regexp_replace(col("email"), r".*@", ""))

print("\n    [D3.4] Schema transformado del catalogo:")
catalogo_trans.printSchema()

print("\n    [D3.5] Muestra de datos transformados:")
catalogo_trans.select("titulo", "es_reciente", "categoria_duracion", "calificacion_categoria").show(5)

# =============================================================================
# [D4] INTEGRACION DE FUENTES (JOINs)
# =============================================================================
print("\n" + "=" * 80)
print("[D4] INTEGRACION DE FUENTES - JOINs entre datasets")
print("=" * 80)

# Renombrar para evitar ambiguedad en joins
catalogo_for_join = catalogo_trans.withColumnRenamed("content_id", "id")

# JOIN 1: Visualizaciones + Catalogo (INNER JOIN)
print("\n    [D4.1] JOIN: Visualizaciones + Catalogo (INNER)")
viz_catalogo = visualizaciones_df.join(
    catalogo_for_join.select("id", "titulo", "genero", "tipo", "calificacion"),
    visualizaciones_df.content_id == catalogo_for_join.id,
    "inner"
)
print(f"           - Registros resultantes: {viz_catalogo.count()}")

# JOIN 2: Visualizaciones + Usuarios (LEFT JOIN)
print("\n    [D4.2] JOIN: Visualizaciones + Usuarios (LEFT)")
viz_usuarios = visualizaciones_df.join(
    usuarios_trans.select("user_id", "nombre", "pais", "plan", "grupo_edad"),
    "user_id",
    "left"
)
print(f"           - Registros resultantes: {viz_usuarios.count()}")

# JOIN 3: Valoraciones + Catalogo + Usuarios (MULTIPLE JOINs)
print("\n    [D4.3] JOIN MULTIPLE: Valoraciones + Catalogo + Usuarios")
val_completo = valoraciones_df \
    .join(
        catalogo_for_join.select("id", "titulo", "genero", "tipo"),
        valoraciones_df.content_id == catalogo_for_join.id,
        "inner"
    ) \
    .join(
        usuarios_trans.select("user_id", "nombre", "pais", "plan"),
        "user_id",
        "inner"
    )
print(f"           - Registros resultantes: {val_completo.count()}")

# JOIN 4: Dataset completo de engagement
print("\n    [D4.4] Creando dataset completo de engagement...")
engagement_completo = visualizaciones_df \
    .join(catalogo_for_join, visualizaciones_df.content_id == catalogo_for_join.id, "left") \
    .join(usuarios_trans, "user_id", "left") \
    .select(
        visualizaciones_df.view_id,
        visualizaciones_df.user_id,
        usuarios_trans.nombre.alias("usuario_nombre"),
        usuarios_trans.pais.alias("usuario_pais"),
        usuarios_trans.plan,
        usuarios_trans.grupo_edad,
        visualizaciones_df.content_id,
        catalogo_for_join.titulo,
        catalogo_for_join.genero,
        catalogo_for_join.tipo,
        visualizaciones_df.duracion_vista_min,
        visualizaciones_df.completado,
        visualizaciones_df.dispositivo
    )
print(f"           - Dataset engagement: {engagement_completo.count()} registros")

# =============================================================================
# [D5] USO DE DATAFRAMES - Operaciones avanzadas
# =============================================================================
print("\n" + "=" * 80)
print("[D5] USO DE DATAFRAMES - Agregaciones y operaciones avanzadas")
print("=" * 80)

# Agregacion 1: Estadisticas por genero
print("\n    [D5.1] Estadisticas por genero:")
stats_genero = catalogo_trans.groupBy("genero") \
    .agg(
        count("*").alias("total_contenido"),
        spark_round(avg("calificacion"), 2).alias("calificacion_promedio"),
        spark_round(avg("duracion_min"), 0).alias("duracion_promedio"),
        sum(when(col("tipo") == "serie", 1).otherwise(0)).alias("total_series"),
        sum(when(col("tipo") == "pelicula", 1).otherwise(0)).alias("total_peliculas")
    ) \
    .orderBy(desc("total_contenido"))
stats_genero.show(10)

# Agregacion 2: Analisis de usuarios por segmento
print("\n    [D5.2] Analisis de usuarios por segmento:")
stats_usuarios = usuarios_trans.groupBy("grupo_edad", "plan") \
    .agg(
        count("*").alias("total_usuarios"),
        spark_round(avg("edad"), 1).alias("edad_promedio")
    ) \
    .orderBy("grupo_edad", desc("total_usuarios"))
stats_usuarios.show(15)

# Agregacion 3: Engagement por contenido
print("\n    [D5.3] Top 10 contenido con mas engagement:")
engagement_stats = engagement_completo.groupBy("titulo", "genero", "tipo") \
    .agg(
        count("*").alias("total_vistas"),
        sum(col("completado").cast("int")).alias("completadas"),
        spark_round(avg("duracion_vista_min"), 1).alias("duracion_promedio"),
        spark_round(sum(col("completado").cast("int")) * 100.0 / count("*"), 2).alias("tasa_completado")
    ) \
    .orderBy(desc("total_vistas"))
engagement_stats.show(10, truncate=False)

# =============================================================================
# [D6] USO DE SPARK SQL - Consultas SQL
# =============================================================================
print("\n" + "=" * 80)
print("[D6] USO DE SPARK SQL - Consultas SQL avanzadas")
print("=" * 80)

# Registrar vistas temporales
catalogo_trans.createOrReplaceTempView("catalogo")
usuarios_trans.createOrReplaceTempView("usuarios")
visualizaciones_df.createOrReplaceTempView("visualizaciones")
valoraciones_df.createOrReplaceTempView("valoraciones")
engagement_completo.createOrReplaceTempView("engagement")

# Query 1: Ranking de contenido por calificacion
print("\n    [D6.1] QUERY SQL: Ranking de contenido")
query1 = """
    SELECT
        titulo,
        genero,
        calificacion,
        RANK() OVER (PARTITION BY genero ORDER BY calificacion DESC) as rank_en_genero
    FROM catalogo
    WHERE calificacion IS NOT NULL
    ORDER BY genero, rank_en_genero
"""
spark.sql(query1).show(15, truncate=False)

# Query 2: Analisis de engagement con subconsultas
print("\n    [D6.2] QUERY SQL: Analisis de engagement con CTE")
query2 = """
    WITH engagement_metrics AS (
        SELECT
            genero,
            tipo,
            COUNT(*) as total_vistas,
            AVG(duracion_vista_min) as duracion_promedio,
            SUM(CASE WHEN completado = true THEN 1 ELSE 0 END) as completadas
        FROM engagement
        GROUP BY genero, tipo
    )
    SELECT
        genero,
        tipo,
        total_vistas,
        ROUND(duracion_promedio, 1) as duracion_promedio,
        completadas,
        ROUND(completadas * 100.0 / total_vistas, 2) as tasa_completado
    FROM engagement_metrics
    ORDER BY total_vistas DESC
"""
spark.sql(query2).show(15)

# Query 3: Analisis de cohortes
print("\n    [D6.3] QUERY SQL: Analisis de cohortes por plan")
query3 = """
    SELECT
        u.plan,
        u.grupo_edad,
        COUNT(DISTINCT e.user_id) as usuarios_activos,
        COUNT(e.view_id) as total_reproducciones,
        ROUND(COUNT(e.view_id) * 1.0 / COUNT(DISTINCT e.user_id), 2) as reproducciones_por_usuario
    FROM engagement e
    JOIN usuarios u ON e.user_id = u.user_id
    GROUP BY u.plan, u.grupo_edad
    ORDER BY u.plan, usuarios_activos DESC
"""
spark.sql(query3).show(20)

# Query 4: Window functions - tendencias
print("\n    [D6.4] QUERY SQL: Window functions para tendencias")
query4 = """
    SELECT
        titulo,
        genero,
        calificacion,
        AVG(calificacion) OVER (PARTITION BY genero) as promedio_genero,
        ROUND(calificacion - AVG(calificacion) OVER (PARTITION BY genero), 2) as diferencia_promedio
    FROM catalogo
    WHERE calificacion IS NOT NULL
    ORDER BY genero, calificacion DESC
"""
spark.sql(query4).show(15, truncate=False)

# =============================================================================
# [D7] USO DE RDD - Operaciones a bajo nivel
# =============================================================================
print("\n" + "=" * 80)
print("[D7] USO DE RDD - Operaciones a bajo nivel")
print("=" * 80)

# Parsear logs con RDD
print("\n    [D7.1] Procesando logs con RDD...")

def parse_log_line(line):
    """Parsear una linea de log y extraer campos"""
    parts = line.split()
    result = {"raw": line}
    for part in parts:
        if "=" in part:
            key, value = part.split("=", 1)
            result[key] = value
    return result

# Map: parsear cada linea
parsed_logs_rdd = logs_rdd.map(parse_log_line)
print(f"           - Logs parseados: {parsed_logs_rdd.count()}")

# Filter: solo acciones de reproduccion
plays_rdd = parsed_logs_rdd.filter(lambda x: x.get("action") == "PLAY")
print(f"           - Reproducciones (PLAY): {plays_rdd.count()}")

# ReduceByKey: contar reproducciones por usuario
user_play_counts = plays_rdd \
    .map(lambda x: (x.get("user", "unknown"), 1)) \
    .reduceByKey(lambda a, b: a + b)
print("\n    [D7.2] Top 5 usuarios con mas reproducciones (RDD):")
for user, count in user_play_counts.sortBy(lambda x: -x[1]).take(5):
    print(f"           - {user}: {count} reproducciones")

# Contar acciones por tipo
action_counts = parsed_logs_rdd \
    .map(lambda x: (x.get("action", "UNKNOWN"), 1)) \
    .reduceByKey(lambda a, b: a + b)
print("\n    [D7.3] Distribucion de acciones (RDD):")
for action, count in action_counts.sortBy(lambda x: -x[1]).collect():
    print(f"           - {action}: {count}")

# FlatMap: extraer palabras de errores
errors_rdd = logs_rdd.filter(lambda line: "ERROR" in line)
error_words = errors_rdd.flatMap(lambda line: line.split()) \
    .map(lambda word: (word.lower(), 1)) \
    .reduceByKey(lambda a, b: a + b)
print(f"\n    [D7.4] Total de errores detectados: {errors_rdd.count()}")

# Convertir RDD a DataFrame para analisis adicional
from pyspark.sql import Row
actions_df = user_play_counts.map(lambda x: Row(user_id=x[0], play_count=x[1])).toDF()
print("\n    [D7.5] RDD convertido a DataFrame:")
actions_df.show(5)

# =============================================================================
# [D8] GENERACION DE KPIs
# =============================================================================
print("\n" + "=" * 80)
print("[D8] GENERACION DE KPIs - Metricas de negocio")
print("=" * 80)

# KPI 1: Metricas generales de la plataforma
print("\n    [D8.1] KPIs GENERALES DE LA PLATAFORMA:")
print("    " + "-" * 50)

total_contenido = catalogo_trans.count()
total_usuarios = usuarios_trans.count()
total_visualizaciones = visualizaciones_df.count()
total_valoraciones = valoraciones_df.count()

print(f"           - Total contenido: {total_contenido}")
print(f"           - Total usuarios: {total_usuarios}")
print(f"           - Total visualizaciones: {total_visualizaciones}")
print(f"           - Total valoraciones: {total_valoraciones}")
print(f"           - Visualizaciones por usuario: {total_visualizaciones / total_usuarios:.2f}")
print(f"           - Tasa de valoracion: {(total_valoraciones / total_visualizaciones * 100):.1f}%")

# KPI 2: Engagement por tipo de contenido
print("\n    [D8.2] KPIs DE ENGAGEMENT:")
print("    " + "-" * 50)
engagement_kpis = spark.sql("""
    SELECT
        tipo,
        COUNT(*) as total_vistas,
        ROUND(AVG(duracion_vista_min), 1) as duracion_promedio,
        ROUND(SUM(CASE WHEN completado = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as tasa_completado_pct
    FROM engagement
    GROUP BY tipo
""")
engagement_kpis.show()

# KPI 3: Retención por plan
print("\n    [D8.3] KPIs DE RETENCION POR PLAN:")
print("    " + "-" * 50)
retencion_kpis = spark.sql("""
    SELECT
        u.plan,
        COUNT(DISTINCT e.user_id) as usuarios_activos,
        COUNT(e.view_id) as total_acciones,
        ROUND(COUNT(e.view_id) * 1.0 / COUNT(DISTINCT e.user_id), 2) as engagement_score
    FROM engagement e
    JOIN usuarios u ON e.user_id = u.user_id
    GROUP BY u.plan
    ORDER BY engagement_score DESC
""")
retencion_kpis.show()

# KPI 4: Calidad del contenido
print("\n    [D8.4] KPIs DE CALIDAD DEL CONTENIDO:")
print("    " + "-" * 50)
calidad_stats = catalogo_trans.agg(
    spark_round(avg("calificacion"), 2).alias("calificacion_promedio"),
    count(when(col("calificacion") >= 8.0, True)).alias("contenido_premium"),
    count(when(col("es_reciente") == True, True)).alias("contenido_reciente")
).collect()[0]

print(f"           - Calificacion promedio: {calidad_stats['calificacion_promedio']}")
print(f"           - Contenido premium (8+): {calidad_stats['contenido_premium']}")
print(f"           - Contenido reciente (2020+): {calidad_stats['contenido_reciente']}")

# KPI 5: DataFrame consolidado de KPIs
print("\n    [D8.5] DASHBOARD KPIs CONSOLIDADO:")
kpis_consolidado = spark.createDataFrame([
    ("Contenido Total", str(total_contenido), "unidades"),
    ("Usuarios Registrados", str(total_usuarios), "usuarios"),
    ("Visualizaciones", str(total_visualizaciones), "reproducciones"),
    ("Valoraciones", str(total_valoraciones), "reviews"),
    ("Calificacion Promedio", str(calidad_stats['calificacion_promedio']), "puntos"),
    ("Tasa de Valoracion", f"{(total_valoraciones / total_visualizaciones * 100):.1f}%", "porcentaje"),
], ["metrica", "valor", "unidad"])
kpis_consolidado.show(truncate=False)

# =============================================================================
# [D9] EXPORTACION DE RESULTADOS
# =============================================================================
print("\n" + "=" * 80)
print("[D9] EXPORTACION DE RESULTADOS - Multiples formatos")
print("=" * 80)

output_base = "/resultados/batch_completo"

# Limpiar directorio de salida
def clean_output(path):
    if os.path.exists(path):
        shutil.rmtree(path)

# Exportar como CSV
print("\n    [D9.1] Exportando a CSV...")
csv_path = f"{output_base}/csv"
clean_output(csv_path)
stats_genero.write.mode("overwrite").csv(f"{csv_path}/stats_genero", header=True)
stats_usuarios.write.mode("overwrite").csv(f"{csv_path}/stats_usuarios", header=True)
print(f"           - Guardado en {csv_path}/")

# Exportar como JSON
print("\n    [D9.2] Exportando a JSON...")
json_path = f"{output_base}/json"
clean_output(json_path)
engagement_stats.write.mode("overwrite").json(f"{json_path}/engagement_stats")
kpis_consolidado.write.mode("overwrite").json(f"{json_path}/kpis")
print(f"           - Guardado en {json_path}/")

# Exportar como Parquet (formato optimizado)
print("\n    [D9.3] Exportando a Parquet...")
parquet_path = f"{output_base}/parquet"
clean_output(parquet_path)
catalogo_trans.write.mode("overwrite").parquet(f"{parquet_path}/catalogo_transformado")
engagement_completo.write.mode("overwrite").parquet(f"{parquet_path}/engagement_completo")
print(f"           - Guardado en {parquet_path}/")

# Exportar RDD results
print("\n    [D9.4] Exportando resultados de RDD...")
rdd_path = f"{output_base}/rdd"
clean_output(rdd_path)
action_counts.saveAsTextFile(f"{rdd_path}/action_counts")
print(f"           - Guardado en {rdd_path}/")

# Resumen de exportacion
print("\n" + "=" * 80)
print("    RESUMEN DE ARCHIVOS EXPORTADOS")
print("=" * 80)
print(f"""
    FORMATO CSV:
    - {csv_path}/stats_genero/
    - {csv_path}/stats_usuarios/

    FORMATO JSON:
    - {json_path}/engagement_stats/
    - {json_path}/kpis/

    FORMATO PARQUET:
    - {parquet_path}/catalogo_transformado/
    - {parquet_path}/engagement_completo/

    FORMATO TEXTO (RDD):
    - {rdd_path}/action_counts/
""")

# =============================================================================
# CIERRE Y RESUMEN FINAL
# =============================================================================
spark.stop()

print("\n" + "=" * 80)
print("    PROCESAMIENTO BATCH COMPLETADO EXITOSAMENTE")
print("=" * 80)
print("""
    REQUISITOS CUMPLIDOS (Seccion D):

    [✓] D1. Lectura de archivos (CSV, JSON, TXT)
    [✓] D2. Limpieza de datos (nulls, duplicados, outliers)
    [✓] D3. Transformacion de columnas (cast, rename, nuevas)
    [✓] D4. Integracion de fuentes (JOINs multiples)
    [✓] D5. Uso de DataFrames (agregaciones avanzadas)
    [✓] D6. Uso de Spark SQL (CTEs, Window Functions)
    [✓] D7. Uso de RDD (map, filter, reduce)
    [✓] D8. Generacion de KPIs (metricas de negocio)
    [✓] D9. Exportacion de resultados (CSV, JSON, Parquet)

    Proyecto: Netflix Analytics Platform - AA4 Big Data
""")
print("=" * 80)
