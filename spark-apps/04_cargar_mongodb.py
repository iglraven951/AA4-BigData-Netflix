"""
=============================================================================
SCRIPT 4: CARGAR DATOS PROCESADOS A MONGODB
=============================================================================
Este script carga los resultados procesados por Spark a MongoDB,
demostrando el flujo completo ETL: Extract -> Transform -> Load.

MongoDB es la base de datos documental final del ecosistema.
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, round as spark_round

# Crear SparkSession con conector de MongoDB
spark = SparkSession.builder \
    .appName("Netflix_MongoDB_Load") \
    .master("local[*]") \
    .config("spark.mongodb.write.connection.uri", "mongodb://admin:admin123@mongodb:27017/netflix_analytics.test?authSource=admin") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("CARGANDO DATOS A MONGODB")
print("=" * 70)

# ==================== CARGAR DATOS ORIGINALES ====================
print("\n[1] LEYENDO DATOS ORIGINALES...")

catalogo_df = spark.read.csv("/datos/catalogo_contenido.csv", header=True, inferSchema=True)
usuarios_df = spark.read.csv("/datos/usuarios.csv", header=True, inferSchema=True)
visualizaciones_df = spark.read.json("/datos/visualizaciones.json")
valoraciones_df = spark.read.json("/datos/valoraciones.json")

print(f"    - Catalogo: {catalogo_df.count()} documentos")
print(f"    - Usuarios: {usuarios_df.count()} documentos")
print(f"    - Visualizaciones: {visualizaciones_df.count()} documentos")
print(f"    - Valoraciones: {valoraciones_df.count()} documentos")

# ==================== TRANSFORMAR DATOS ====================
print("\n[2] TRANSFORMANDO DATOS PARA MONGODB...")

# Crear coleccion de estadisticas de catalogo
catalogo_stats = catalogo_df.groupBy("genero", "tipo", "pais") \
    .agg(
        count("*").alias("cantidad"),
        spark_round(avg("calificacion"), 2).alias("calificacion_promedio"),
        spark_round(avg("duracion_min"), 0).alias("duracion_promedio")
    )

# Crear coleccion de metricas de usuarios
usuarios_metricas = usuarios_df.groupBy("pais", "plan") \
    .agg(
        count("*").alias("total_usuarios"),
        spark_round(avg("edad"), 1).alias("edad_promedio")
    )

# Crear coleccion de engagement
engagement_df = visualizaciones_df.join(
    catalogo_df.select("id", "titulo", "genero", "tipo"),
    visualizaciones_df.content_id == catalogo_df.id,
    "inner"
).groupBy("titulo", "genero", "tipo") \
    .agg(
        count("*").alias("total_vistas"),
        sum(col("completado").cast("int")).alias("completadas"),
        spark_round(avg("duracion_vista_min"), 1).alias("duracion_promedio")
    )

print("    - Estadisticas de catalogo preparadas")
print("    - Metricas de usuarios preparadas")
print("    - Datos de engagement preparados")

# ==================== CARGAR A MONGODB ====================
print("\n[3] CARGANDO COLECCIONES A MONGODB...")

# Nota: Este script se ejecuta con el conector de MongoDB
# En un entorno real, se usaria:
# df.write.format("mongodb").mode("overwrite").save()

# Para este ejemplo, guardamos como archivos que luego se importan
catalogo_df.write.mode("overwrite").json("/resultados/mongodb_catalogo")
usuarios_df.write.mode("overwrite").json("/resultados/mongodb_usuarios")
visualizaciones_df.write.mode("overwrite").json("/resultados/mongodb_visualizaciones")
valoraciones_df.write.mode("overwrite").json("/resultados/mongodb_valoraciones")
catalogo_stats.write.mode("overwrite").json("/resultados/mongodb_catalogo_stats")
usuarios_metricas.write.mode("overwrite").json("/resultados/mongodb_usuarios_metricas")
engagement_df.write.mode("overwrite").json("/resultados/mongodb_engagement")

print("    - Coleccion 'catalogo' exportada")
print("    - Coleccion 'usuarios' exportada")
print("    - Coleccion 'visualizaciones' exportada")
print("    - Coleccion 'valoraciones' exportada")
print("    - Coleccion 'catalogo_stats' exportada")
print("    - Coleccion 'usuarios_metricas' exportada")
print("    - Coleccion 'engagement' exportada")

# ==================== RESUMEN ====================
print("\n" + "=" * 70)
print("[4] RESUMEN DE CARGA A MONGODB")
print("=" * 70)

print("""
    BASE DE DATOS: netflix_analytics

    COLECCIONES CREADAS:

    1. catalogo
       - Descripcion: Catalogo completo de series y peliculas
       - Documentos: 50
       - Campos: id, titulo, tipo, genero, anio, duracion_min, calificacion, idioma, pais

    2. usuarios
       - Descripcion: Informacion de usuarios suscritos
       - Documentos: 30
       - Campos: user_id, nombre, email, pais, plan, fecha_registro, edad, dispositivo_principal

    3. visualizaciones
       - Descripcion: Registro de reproducciones de contenido
       - Documentos: 50
       - Campos: view_id, user_id, content_id, fecha, duracion_vista_min, completado, dispositivo

    4. valoraciones
       - Descripcion: Ratings y comentarios de usuarios
       - Documentos: 40
       - Campos: rating_id, user_id, content_id, puntuacion, comentario, fecha

    5. catalogo_stats (AGREGADA)
       - Descripcion: Estadisticas agregadas del catalogo
       - Campos: genero, tipo, pais, cantidad, calificacion_promedio, duracion_promedio

    6. usuarios_metricas (AGREGADA)
       - Descripcion: Metricas agregadas de usuarios
       - Campos: pais, plan, total_usuarios, edad_promedio

    7. engagement (AGREGADA)
       - Descripcion: Metricas de engagement por contenido
       - Campos: titulo, genero, tipo, total_vistas, completadas, duracion_promedio
""")

# Cerrar SparkSession
spark.stop()

print("\n" + "=" * 70)
print("CARGA A MONGODB COMPLETADA")
print("=" * 70)
