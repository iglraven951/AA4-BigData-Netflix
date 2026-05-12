# AA4 Big Data - Secciones D y E

## Resumen de Implementación

Este documento describe la implementación de las secciones D y E de la actividad AA4.

---

## D. Procesamiento Batch con Spark

### Archivo Principal: `spark-apps/05_batch_completo.py`

Este script consolida TODOS los requisitos del procesamiento batch:

| Requisito | Estado | Ubicación en código |
|-----------|--------|---------------------|
| Lectura de archivos | ✅ | Líneas 60-80 (CSV, JSON, TXT) |
| Limpieza de datos | ✅ | Líneas 85-120 (nulls, duplicados, outliers) |
| Transformación de columnas | ✅ | Líneas 125-180 (rename, cast, nuevas columnas) |
| Integración de fuentes | ✅ | Líneas 185-240 (JOINs múltiples) |
| Uso de DataFrames | ✅ | Líneas 245-290 (agregaciones avanzadas) |
| Uso de Spark SQL | ✅ | Líneas 295-380 (CTEs, Window Functions) |
| Uso de RDD | ✅ | Líneas 385-450 (map, filter, reduce) |
| Generación de KPIs | ✅ | Líneas 455-530 (métricas de negocio) |
| Exportación de resultados | ✅ | Líneas 535-580 (CSV, JSON, Parquet) |

### Ejecutar Batch Processing

```bash
# Opción 1: Script automatizado
scripts/run-batch-completo.bat

# Opción 2: Manual con Docker
docker exec spark-master spark-submit --master local[*] /spark-apps/05_batch_completo.py
```

### Resultados Generados

```
resultados/batch_completo/
├── csv/
│   ├── stats_genero/
│   └── stats_usuarios/
├── json/
│   ├── engagement_stats/
│   └── kpis/
├── parquet/
│   ├── catalogo_transformado/
│   └── engagement_completo/
└── rdd/
    └── action_counts/
```

---

## E. Procesamiento Streaming con Kafka

### Arquitectura de Streaming

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PRODUCTOR     │───>│     KAFKA       │───>│    SPARK        │
│ 06_kafka_       │    │  (Topics)       │    │  STREAMING      │
│ producer.py     │    │                 │    │ 07_spark_       │
│                 │    │ - netflix-events│    │ streaming.py    │
│ Genera eventos: │    │ - netflix-alerts│    │                 │
│ - PLAY          │    │                 │    │ Procesa:        │
│ - PAUSE         │    │                 │    │ - Agregaciones  │
│ - RATE          │    │                 │    │ - Anomalías     │
│ - SEARCH        │    │                 │    │ - KPIs          │
│ - ERROR         │    │                 │    │ - Alertas       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Archivos de Streaming

| Archivo | Descripción |
|---------|-------------|
| `06_kafka_producer.py` | Productor de eventos simulados |
| `07_spark_streaming.py` | Consumidor con Spark Structured Streaming |

### Requisitos Implementados

| Requisito | Estado | Descripción |
|-----------|--------|-------------|
| Creación de productor | ✅ | `06_kafka_producer.py` genera eventos PLAY, PAUSE, RATE, SEARCH, ERROR |
| Creación de topic | ✅ | `netflix-events` y `netflix-alerts` (auto-creados) |
| Envío de eventos simulados | ✅ | 5 eventos/segundo con distribución realista |
| Lectura con Structured Streaming | ✅ | `07_spark_streaming.py` lee de Kafka |
| Procesamiento por micro-batches | ✅ | Ventanas de 10s, 30s, 1min según el stream |
| Generación de alertas | ✅ | Detección de anomalías y errores |
| Salida consola/archivo/MongoDB | ✅ | Múltiples outputs configurados |

### Ejecutar Streaming Demo

```bash
# Opción 1: Demo completa (abre 2 ventanas automáticamente)
scripts/run-streaming-demo.bat

# Opción 2: Manual (2 terminales separadas)
# Terminal 1 - Productor:
scripts/run-kafka-producer.bat

# Terminal 2 - Consumidor:
scripts/run-spark-streaming.bat
```

### Topics de Kafka

| Topic | Descripción | Eventos |
|-------|-------------|---------|
| `netflix-events` | Eventos normales de usuario | PLAY, PAUSE, RATE, SEARCH, LOGIN |
| `netflix-alerts` | Eventos de error para alertas | ERROR (con severidad) |

### Streams de Procesamiento

1. **Windowed Metrics** (10s trigger)
   - Conteo de eventos por tipo
   - Ventana de 30 segundos

2. **Active Users** (30s trigger)
   - Usuarios únicos por país
   - Ventana de 1 minuto

3. **Anomaly Detection** (15s trigger)
   - Actividad alta (>50 eventos/min)
   - Muchos errores (>5/min)
   - Buffering alto (>5000ms)

4. **Error Alerts** (5s trigger)
   - Alertas por severidad
   - Guardado en archivos JSON

5. **Real-time KPIs** (20s trigger)
   - Plays, ratings, searches
   - Tasa de error
   - Rating promedio

---

## Servicios Docker (11 Containers)

### Iniciar Todos los Servicios

```bash
cd AA3-BigData
docker-compose up -d
```

### Verificar Estado

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

### URLs de Acceso

| Servicio | URL | Descripción |
|----------|-----|-------------|
| Kafka UI | http://localhost:8083 | Visualización de topics y mensajes |
| Spark Master | http://localhost:8080 | Estado del cluster Spark |
| Spark Worker | http://localhost:8081 | Worker status |
| Mongo Express | http://localhost:8082 | Interfaz MongoDB |
| HDFS NameNode | http://localhost:9870 | HDFS Web UI |
| YARN ResourceManager | http://localhost:8088 | YARN jobs |

---

## Estructura de Archivos

```
AA3-BigData/
├── docker-compose.yml          # 11 servicios (incluye Kafka)
├── spark-apps/
│   ├── 01_spark_rdd.py         # RDD básico
│   ├── 02_spark_dataframe.py   # DataFrame ops
│   ├── 03_spark_sql.py         # SQL queries
│   ├── 04_cargar_mongodb.py    # MongoDB loader
│   ├── 05_batch_completo.py    # ★ BATCH COMPLETO (Sección D)
│   ├── 06_kafka_producer.py    # ★ PRODUCTOR KAFKA (Sección E)
│   └── 07_spark_streaming.py   # ★ SPARK STREAMING (Sección E)
├── scripts/
│   ├── run-batch-completo.bat  # Ejecutar batch
│   ├── run-kafka-producer.bat  # Iniciar productor
│   ├── run-spark-streaming.bat # Iniciar streaming
│   ├── start-kafka.bat         # Solo Kafka
│   └── run-streaming-demo.bat  # Demo completa
├── datos/                       # Archivos de datos
├── resultados/                  # Outputs generados
└── SECCION_D_E_README.md       # Este archivo
```

---

## Requisitos Técnicos

- Docker Desktop con al menos 8GB RAM asignados
- Puertos disponibles: 8080-8088, 9092, 27017, 2181

---

## Troubleshooting

### Kafka no conecta
```bash
# Reiniciar Kafka
docker-compose restart zookeeper kafka
# Esperar 30 segundos
```

### Spark Streaming no recibe eventos
```bash
# Verificar que el productor está corriendo
docker logs kafka-producer
# Verificar topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Errores de memoria
```bash
# Aumentar memoria en Docker Desktop Settings > Resources
# Mínimo recomendado: 8GB RAM, 4 CPUs
```
