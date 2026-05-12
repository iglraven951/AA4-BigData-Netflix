"""
=============================================================================
SCRIPT 7: SPARK STRUCTURED STREAMING CON KAFKA
=============================================================================
Este script implementa el consumidor de streaming con Spark:

E. Procesamiento streaming con Kafka
   [x] Lectura con Spark Structured Streaming
   [x] Procesamiento por micro-batches
   [x] Generacion de alertas o resumenes
   [x] Salida en consola, archivo o MongoDB

El consumidor procesa eventos en tiempo real:
- Agregaciones por ventana de tiempo
- Deteccion de anomalias
- Alertas por errores
- KPIs en tiempo real

Proyecto: Netflix Analytics Platform - AA4 Big Data
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, avg, sum, max, min,
    current_timestamp, when, lit, concat, expr,
    to_json, struct, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, BooleanType, TimestampType, LongType
)
import time
import sys

# =============================================================================
# CONFIGURACION
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_EVENTS = "netflix-events"
TOPIC_ALERTS = "netflix-alerts"
CHECKPOINT_PATH = "/resultados/streaming/checkpoints"
OUTPUT_PATH = "/resultados/streaming/output"

# =============================================================================
# SCHEMAS DE EVENTOS
# =============================================================================
# Schema principal de eventos
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("content_id", StringType(), True),
    StructField("device", StringType(), True),
    StructField("country", StringType(), True),
    StructField("position_seconds", IntegerType(), True),
    StructField("quality", StringType(), True),
    StructField("buffering_time_ms", IntegerType(), True),
    StructField("pause_duration_seconds", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("feedback_type", StringType(), True),
    StructField("search_query", StringType(), True),
    StructField("results_count", IntegerType(), True),
    StructField("filter_genre", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("error_code", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("severity", StringType(), True)
])

# =============================================================================
# CREAR SPARK SESSION
# =============================================================================
def create_spark_session():
    """Crear SparkSession con configuracion de Kafka"""
    return SparkSession.builder \
        .appName("Netflix_Spark_Streaming_AA4") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

# =============================================================================
# FUNCIONES DE PROCESAMIENTO
# =============================================================================
def read_kafka_stream(spark, topic):
    """Leer stream de Kafka"""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

def parse_events(df):
    """Parsear eventos JSON de Kafka"""
    return df \
        .select(
            col("key").cast("string").alias("key"),
            from_json(col("value").cast("string"), event_schema).alias("event"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select(
            "key",
            "kafka_timestamp",
            "event.*"
        ) \
        .withColumn("event_timestamp",
            when(col("timestamp").isNotNull(),
                 col("timestamp").cast("timestamp"))
            .otherwise(current_timestamp()))

# =============================================================================
# PROCESAMIENTO 1: METRICAS POR VENTANA DE TIEMPO
# =============================================================================
def process_windowed_metrics(events_df):
    """
    Procesar metricas agregadas por ventana de tiempo (micro-batches)
    - Ventana de 30 segundos con slide de 10 segundos
    - Conteo de eventos por tipo
    - Usuarios activos
    - Dispositivos mas usados
    """
    print("\n[STREAM 1] Metricas por ventana de tiempo")

    windowed_metrics = events_df \
        .withWatermark("event_timestamp", "1 minute") \
        .groupBy(
            window(col("event_timestamp"), "30 seconds", "10 seconds"),
            col("event_type")
        ) \
        .agg(
            count("*").alias("event_count"),
            count(col("user_id")).alias("user_actions"),
            spark_round(avg(col("buffering_time_ms")), 2).alias("avg_buffering_ms")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "event_type",
            "event_count",
            "user_actions",
            "avg_buffering_ms"
        )

    return windowed_metrics

# =============================================================================
# PROCESAMIENTO 2: USUARIOS ACTIVOS EN TIEMPO REAL
# =============================================================================
def process_active_users(events_df):
    """
    Contar usuarios unicos activos por ventana de tiempo
    """
    print("\n[STREAM 2] Usuarios activos por ventana")

    active_users = events_df \
        .withWatermark("event_timestamp", "1 minute") \
        .groupBy(
            window(col("event_timestamp"), "1 minute", "30 seconds"),
            col("country")
        ) \
        .agg(
            expr("approx_count_distinct(user_id)").alias("unique_users"),
            count("*").alias("total_events")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "country",
            "unique_users",
            "total_events"
        )

    return active_users

# =============================================================================
# PROCESAMIENTO 3: DETECCION DE ANOMALIAS
# =============================================================================
def detect_anomalies(events_df):
    """
    Detectar anomalias en tiempo real:
    - Demasiados eventos de un usuario (posible bot)
    - Buffering alto (problema de red)
    - Muchos errores en poco tiempo
    """
    print("\n[STREAM 3] Deteccion de anomalias")

    anomalies = events_df \
        .withWatermark("event_timestamp", "2 minutes") \
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("user_id")
        ) \
        .agg(
            count("*").alias("event_count"),
            sum(when(col("event_type") == "ERROR", 1).otherwise(0)).alias("error_count"),
            spark_round(avg(col("buffering_time_ms")), 0).alias("avg_buffering")
        ) \
        .withColumn("is_anomaly",
            when(col("event_count") > 50, lit("HIGH_ACTIVITY"))
            .when(col("error_count") > 5, lit("MANY_ERRORS"))
            .when(col("avg_buffering") > 5000, lit("HIGH_BUFFERING"))
            .otherwise(lit(None))
        ) \
        .filter(col("is_anomaly").isNotNull()) \
        .select(
            col("window.start").alias("window_start"),
            "user_id",
            "event_count",
            "error_count",
            "avg_buffering",
            "is_anomaly",
            current_timestamp().alias("detected_at")
        )

    return anomalies

# =============================================================================
# PROCESAMIENTO 4: ALERTAS DE ERRORES
# =============================================================================
def process_error_alerts(spark):
    """
    Procesar alertas de errores en tiempo real desde topic separado
    """
    print("\n[STREAM 4] Alertas de errores")

    alerts_stream = read_kafka_stream(spark, TOPIC_ALERTS)

    alerts = alerts_stream \
        .select(
            from_json(col("value").cast("string"), event_schema).alias("alert")
        ) \
        .select("alert.*") \
        .withColumn("alert_timestamp", current_timestamp()) \
        .withColumn("alert_level",
            when(col("severity") == "CRITICAL", 4)
            .when(col("severity") == "HIGH", 3)
            .when(col("severity") == "MEDIUM", 2)
            .otherwise(1)
        ) \
        .select(
            "event_id",
            "error_code",
            "error_message",
            "severity",
            "alert_level",
            "user_id",
            "device",
            "alert_timestamp"
        )

    return alerts

# =============================================================================
# PROCESAMIENTO 5: KPIs EN TIEMPO REAL
# =============================================================================
def calculate_realtime_kpis(events_df):
    """
    Calcular KPIs agregados en tiempo real
    """
    print("\n[STREAM 5] KPIs en tiempo real")

    kpis = events_df \
        .withWatermark("event_timestamp", "2 minutes") \
        .groupBy(
            window(col("event_timestamp"), "1 minute")
        ) \
        .agg(
            count("*").alias("total_events"),
            expr("approx_count_distinct(user_id)").alias("active_users"),
            sum(when(col("event_type") == "PLAY", 1).otherwise(0)).alias("plays"),
            sum(when(col("event_type") == "RATE", 1).otherwise(0)).alias("ratings"),
            sum(when(col("event_type") == "SEARCH", 1).otherwise(0)).alias("searches"),
            sum(when(col("event_type") == "ERROR", 1).otherwise(0)).alias("errors"),
            spark_round(avg(when(col("rating").isNotNull(), col("rating"))), 2).alias("avg_rating")
        ) \
        .withColumn("error_rate",
            spark_round(col("errors") * 100.0 / col("total_events"), 2)
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "total_events",
            "active_users",
            "plays",
            "ratings",
            "searches",
            "errors",
            "error_rate",
            "avg_rating"
        )

    return kpis

# =============================================================================
# FUNCIONES DE SALIDA
# =============================================================================
def write_to_console(df, output_mode="update", trigger_interval="10 seconds"):
    """Escribir a consola (para debugging)"""
    return df.writeStream \
        .format("console") \
        .outputMode(output_mode) \
        .trigger(processingTime=trigger_interval) \
        .option("truncate", "false") \
        .start()

def write_to_files(df, path, output_mode="append", trigger_interval="30 seconds"):
    """Escribir a archivos JSON"""
    return df.writeStream \
        .format("json") \
        .outputMode(output_mode) \
        .trigger(processingTime=trigger_interval) \
        .option("path", path) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{path.split('/')[-1]}") \
        .start()

def write_to_memory(df, table_name, output_mode="complete"):
    """Escribir a tabla en memoria (para queries)"""
    return df.writeStream \
        .format("memory") \
        .queryName(table_name) \
        .outputMode(output_mode) \
        .start()

# =============================================================================
# MAIN - ORQUESTACION DE STREAMS
# =============================================================================
def main():
    print("""
    =============================================================================
                SPARK STRUCTURED STREAMING - NETFLIX ANALYTICS
    =============================================================================

    Este consumidor procesa eventos de Kafka en tiempo real:

    STREAMS ACTIVOS:
    [1] Metricas por ventana de tiempo (30s window, 10s slide)
    [2] Usuarios activos por pais (1min window, 30s slide)
    [3] Deteccion de anomalias (actividad inusual, errores)
    [4] Alertas de errores (severity levels)
    [5] KPIs en tiempo real (plays, ratings, errors)

    SALIDAS:
    - Consola: Metricas y alertas en tiempo real
    - Archivos: JSON en /resultados/streaming/
    - Memoria: Tablas consultables con SQL

    Presiona Ctrl+C para detener.
    =============================================================================
    """)

    # Crear SparkSession
    print("[INIT] Creando SparkSession...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Leer stream de eventos
        print("[INIT] Conectando a Kafka...")
        raw_events = read_kafka_stream(spark, TOPIC_EVENTS)
        events_df = parse_events(raw_events)

        # Iniciar streams de procesamiento
        print("\n" + "=" * 70)
        print("INICIANDO STREAMS DE PROCESAMIENTO")
        print("=" * 70)

        # Stream 1: Metricas por ventana -> Consola
        windowed_metrics = process_windowed_metrics(events_df)
        query_metrics = windowed_metrics.writeStream \
            .format("console") \
            .outputMode("update") \
            .trigger(processingTime="10 seconds") \
            .option("truncate", "false") \
            .queryName("windowed_metrics") \
            .start()

        # Stream 2: Usuarios activos -> Archivos JSON
        active_users = process_active_users(events_df)
        query_users = active_users.writeStream \
            .format("json") \
            .outputMode("append") \
            .trigger(processingTime="30 seconds") \
            .option("path", f"{OUTPUT_PATH}/active_users") \
            .option("checkpointLocation", f"{CHECKPOINT_PATH}/active_users") \
            .queryName("active_users") \
            .start()

        # Stream 3: Anomalias -> Consola y archivos
        anomalies = detect_anomalies(events_df)
        query_anomalies = anomalies.writeStream \
            .format("console") \
            .outputMode("update") \
            .trigger(processingTime="15 seconds") \
            .queryName("anomaly_detection") \
            .start()

        # Stream 4: Alertas de errores
        alerts = process_error_alerts(spark)
        query_alerts = alerts.writeStream \
            .format("json") \
            .outputMode("append") \
            .trigger(processingTime="5 seconds") \
            .option("path", f"{OUTPUT_PATH}/alerts") \
            .option("checkpointLocation", f"{CHECKPOINT_PATH}/alerts") \
            .queryName("error_alerts") \
            .start()

        # Stream 5: KPIs -> Consola
        kpis = calculate_realtime_kpis(events_df)
        query_kpis = kpis.writeStream \
            .format("console") \
            .outputMode("complete") \
            .trigger(processingTime="20 seconds") \
            .option("truncate", "false") \
            .queryName("realtime_kpis") \
            .start()

        print("\n" + "=" * 70)
        print("TODOS LOS STREAMS INICIADOS")
        print("=" * 70)
        print("""
    STREAMS ACTIVOS:
    - windowed_metrics: Metricas cada 10s (consola)
    - active_users: Usuarios por pais cada 30s (archivos)
    - anomaly_detection: Anomalias cada 15s (consola)
    - error_alerts: Alertas cada 5s (archivos)
    - realtime_kpis: KPIs cada 20s (consola)

    Esperando eventos de Kafka...
    (Ejecuta 06_kafka_producer.py en otra terminal para generar eventos)
        """)

        # Esperar a que terminen los streams
        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        print("\n\n[!] Deteniendo streams...")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Detener todos los streams
        for stream in spark.streams.active:
            stream.stop()
        spark.stop()
        print("\n[DONE] Streaming detenido.")

# =============================================================================
# EJECUCION
# =============================================================================
if __name__ == "__main__":
    main()
