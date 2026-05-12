# Guía de Presentación - AA4 Big Data
## Netflix Analytics Platform

**Duración Total**: 17 minutos  
**Equipo**: 5 integrantes

---

# INTEGRANTE 1: Caso, Problema, Objetivos y Datos
**Tiempo: 3 minutos**

---

## 1.1 Introducción al Caso (30 seg)

> *"Buenos días/tardes. Hoy presentaremos nuestra plataforma de analytics para Netflix, una solución Big Data que transforma datos masivos en insights accionables."*

### La Empresa
- **Netflix**: Plataforma de streaming líder mundial
- **230+ millones** de suscriptores globales
- **Petabytes de datos** generados diariamente

---

## 1.2 El Problema (1 min)

### Situación Inicial
```
┌─────────────────────────────────────────────────────────────────┐
│                    PROBLEMAS IDENTIFICADOS                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ❌ Datos dispersos en múltiples formatos (CSV, JSON, TXT)     │
│  ❌ Sin procesamiento en tiempo real                           │
│  ❌ Reportes manuales que toman semanas                        │
│  ❌ No hay detección de anomalías                              │
│  ❌ Decisiones basadas en datos obsoletos                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Impacto del Problema
| Métrica | Antes | Impacto |
|---------|-------|---------|
| Detección de errores | 2-3 días | Usuarios frustrados |
| Reportes de negocio | 2 semanas | Oportunidades perdidas |
| Análisis en tiempo real | No existe | Sin visibilidad |

---

## 1.3 Objetivos del Proyecto (45 seg)

### Objetivo General
> Implementar una plataforma Big Data que permita procesar datos en batch y streaming para generar insights en tiempo real.

### Objetivos Específicos

| # | Objetivo | Tecnología |
|---|----------|------------|
| 1 | Procesar archivos de múltiples formatos | Apache Spark |
| 2 | Limpiar y transformar datos masivos | Spark DataFrames |
| 3 | Generar KPIs de negocio | Spark SQL |
| 4 | Procesar eventos en tiempo real | Kafka + Spark Streaming |
| 5 | Detectar anomalías automáticamente | Streaming Analytics |
| 6 | Almacenar datos con propiedades ACID | MongoDB |

---

## 1.4 Datos Utilizados (45 seg)

### Fuentes de Datos

| Archivo | Formato | Registros | Contenido |
|---------|---------|-----------|-----------|
| `netflix_titles.csv` | CSV | ~8,800 | Catálogo de películas y series |
| `usuarios.json` | JSON | ~1,000 | Información de usuarios |
| `visualizaciones.txt` | TXT | ~50,000 | Historial de reproducciones |
| `valoraciones.csv` | CSV | ~10,000 | Ratings de usuarios |

### Eventos en Streaming
```
Tipos de eventos generados:
├── PLAY      (40%) - Reproducciones iniciadas
├── PAUSE     (25%) - Pausas de contenido
├── RATE      (15%) - Valoraciones
├── SEARCH    (15%) - Búsquedas
└── ERROR     (5%)  - Errores técnicos
```

### Volumen Simulado
- **5 eventos por segundo**
- **300 eventos por minuto**
- **~18,000 eventos por hora**

---

## Puntos Clave para Mencionar
- [ ] Nombrar la empresa y el contexto
- [ ] Explicar los 3 problemas principales
- [ ] Listar los 6 objetivos específicos
- [ ] Describir las 4 fuentes de datos
- [ ] Mencionar el volumen de eventos streaming

---

# INTEGRANTE 2: Arquitectura Big Data y Flujo del Dato
**Tiempo: 3 minutos**

---

## 2.1 Arquitectura General (1 min)

### Diagrama de Arquitectura

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        NETFLIX ANALYTICS PLATFORM                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                  │
│   │   FUENTES    │    │   INGESTA    │    │ PROCESAMIENTO│                  │
│   │              │    │              │    │              │                  │
│   │  CSV  JSON   │───▶│    HDFS      │───▶│    SPARK     │                  │
│   │  TXT  Stream │    │    KAFKA     │    │   BATCH +    │                  │
│   │              │    │              │    │  STREAMING   │                  │
│   └──────────────┘    └──────────────┘    └──────────────┘                  │
│                                                  │                           │
│                                                  ▼                           │
│   ┌──────────────────────────────────────────────────────────────────┐      │
│   │                        ALMACENAMIENTO                             │      │
│   │   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐            │      │
│   │   │   MongoDB   │   │   Parquet   │   │    JSON     │            │      │
│   │   │  (NoSQL)    │   │ (Columnar)  │   │  (Flexible) │            │      │
│   │   └─────────────┘   └─────────────┘   └─────────────┘            │      │
│   └──────────────────────────────────────────────────────────────────┘      │
│                                                  │                           │
│                                                  ▼                           │
│   ┌──────────────────────────────────────────────────────────────────┐      │
│   │                         ANÁLISIS                                  │      │
│   │   📊 KPIs    🔔 Alertas    📈 Tendencias    🎯 Segmentos         │      │
│   └──────────────────────────────────────────────────────────────────┘      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2.2 Infraestructura Docker (1 min)

### 11 Contenedores Desplegados

```
┌─────────────────────────────────────────────────────────────────┐
│                    DOCKER COMPOSE - 11 SERVICIOS                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  🐘 HADOOP CLUSTER (5 containers):                             │
│  ├── namenode          → HDFS Master, puerto 9870              │
│  ├── datanode          → Almacenamiento distribuido            │
│  ├── resourcemanager   → YARN Master, puerto 8088              │
│  ├── nodemanager       → Ejecución de tareas                   │
│  └── historyserver     → Historial de jobs                     │
│                                                                 │
│  ⚡ SPARK CLUSTER (2 containers):                               │
│  ├── spark-master      → Master UI, puerto 8080                │
│  └── spark-worker      → Worker de procesamiento               │
│                                                                 │
│  📡 KAFKA CLUSTER (3 containers):                               │
│  ├── zookeeper         → Coordinación, puerto 2181             │
│  ├── kafka             → Broker, puerto 9092                   │
│  └── kafka-ui          → Interfaz web, puerto 8083             │
│                                                                 │
│  🍃 MONGODB (2 containers):                                     │
│  ├── mongodb           → Base de datos, puerto 27017           │
│  └── mongo-express     → Interfaz web, puerto 8082             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Comando para Iniciar
```bash
docker-compose up -d
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

---

## 2.3 Flujo del Dato (1 min)

### Flujo Batch (Procesamiento por Lotes)

```
CSV/JSON/TXT  ───▶  HDFS  ───▶  SPARK  ───▶  TRANSFORMACIÓN  ───▶  PARQUET/MongoDB
     │                              │
     │     1. Lectura               │     2. Limpieza
     │        múltiples             │        Nulls, duplicados
     │        formatos              │        outliers
     │                              │
     └──────────────────────────────┴────────────────────────────▶  KPIs + Reportes
```

### Flujo Streaming (Tiempo Real)

```
EVENTOS  ───▶  KAFKA  ───▶  SPARK STREAMING  ───▶  ANÁLISIS  ───▶  ALERTAS
   │              │                │                    │
   │   Topics:    │   Micro-       │   Ventanas:        │   Outputs:
   │   - events   │   batches      │   - 30 seg         │   - Consola
   │   - alerts   │   cada 10s     │   - 1 min          │   - JSON
   │              │                │                    │   - MongoDB
```

---

## URLs de Monitoreo

| Servicio | URL | Uso |
|----------|-----|-----|
| Spark Master | http://localhost:8080 | Ver jobs y workers |
| Kafka UI | http://localhost:8083 | Ver topics y mensajes |
| Mongo Express | http://localhost:8082 | Ver colecciones |
| HDFS NameNode | http://localhost:9870 | Ver sistema de archivos |
| YARN | http://localhost:8088 | Ver recursos del cluster |

---

## Puntos Clave para Mencionar
- [ ] Explicar las 3 capas: Ingesta, Procesamiento, Almacenamiento
- [ ] Mencionar los 11 contenedores Docker
- [ ] Diferenciar flujo Batch vs Streaming
- [ ] Mostrar las URLs de monitoreo

---

# INTEGRANTE 3: Procesamiento Batch con Spark
**Tiempo: 4 minutos**

---

## 3.1 Introducción a Spark (30 seg)

### ¿Por qué Apache Spark?

| Característica | MapReduce | Spark |
|---------------|-----------|-------|
| Velocidad | 1x | **100x más rápido** |
| Procesamiento | Disco | **En memoria** |
| APIs | Solo Java | **Python, SQL, Scala** |
| Streaming | No nativo | **Integrado** |

### Componentes Utilizados
```
Apache Spark 3.1.1
├── RDD (Resilient Distributed Datasets)
├── DataFrames (Datos estructurados)
├── Spark SQL (Consultas SQL)
└── Structured Streaming (Tiempo real)
```

---

## 3.2 RDD - Resilient Distributed Datasets (1 min)

### Concepto
> RDD es la abstracción fundamental de Spark: una colección distribuida e inmutable de objetos.

### Código Implementado

```python
# Crear RDD desde DataFrame
eventos_rdd = eventos_df.rdd

# TRANSFORMACIONES (lazy)
# 1. Map: Extraer tipo de evento
tipos_rdd = eventos_rdd.map(lambda row: (row.event_type, 1))

# 2. Filter: Solo eventos de error
errores_rdd = eventos_rdd.filter(lambda row: row.event_type == "ERROR")

# 3. ReduceByKey: Contar por tipo
conteo_rdd = tipos_rdd.reduceByKey(lambda a, b: a + b)

# ACCIONES (ejecutan el plan)
resultados = conteo_rdd.collect()
# Output: [('PLAY', 4521), ('PAUSE', 2834), ('ERROR', 523), ...]
```

### Operaciones RDD Utilizadas

| Tipo | Operación | Descripción |
|------|-----------|-------------|
| Transformación | `map()` | Transformar cada elemento |
| Transformación | `filter()` | Filtrar elementos |
| Transformación | `reduceByKey()` | Agregar por clave |
| Acción | `collect()` | Traer resultados al driver |
| Acción | `count()` | Contar elementos |

---

## 3.3 DataFrames (1 min)

### Concepto
> DataFrames son RDDs con esquema, optimizados para operaciones estructuradas.

### Código Implementado

```python
# LECTURA de múltiples formatos
catalogo_df = spark.read.csv("datos/netflix_titles.csv", header=True)
usuarios_df = spark.read.json("datos/usuarios.json")
logs_df = spark.read.text("datos/visualizaciones.txt")

# LIMPIEZA de datos
catalogo_limpio = (catalogo_df
    .dropna(subset=['titulo', 'tipo'])      # Eliminar nulls
    .dropDuplicates(['titulo'])              # Eliminar duplicados
    .filter(F.col('release_year') > 1990)    # Filtrar outliers
)

# TRANSFORMACIÓN de columnas
catalogo_transformado = (catalogo_limpio
    .withColumnRenamed('title', 'titulo')           # Renombrar
    .withColumn('anio', F.col('release_year').cast('int'))  # Cast
    .withColumn('es_pelicula', F.col('tipo') == 'Movie')    # Nueva columna
    .withColumn('decada', (F.col('anio') / 10).cast('int') * 10)
)

# INTEGRACIÓN con JOINs
datos_completos = (visualizaciones_df
    .join(usuarios_df, "user_id", "inner")
    .join(catalogo_df, "show_id", "left")
)
```

### Operaciones DataFrame Utilizadas

| Categoría | Operaciones |
|-----------|-------------|
| Lectura | `read.csv()`, `read.json()`, `read.text()` |
| Limpieza | `dropna()`, `dropDuplicates()`, `filter()` |
| Transformación | `withColumn()`, `withColumnRenamed()`, `cast()` |
| Integración | `join()` (inner, left, right, outer) |
| Agregación | `groupBy()`, `agg()`, `count()`, `sum()`, `avg()` |

---

## 3.4 Spark SQL (1 min)

### Concepto
> Spark SQL permite ejecutar consultas SQL estándar sobre DataFrames.

### Código Implementado

```python
# Registrar DataFrame como tabla temporal
catalogo_df.createOrReplaceTempView("catalogo")
usuarios_df.createOrReplaceTempView("usuarios")
visualizaciones_df.createOrReplaceTempView("visualizaciones")

# CONSULTA con CTE (Common Table Expression)
query_cte = """
WITH stats_genero AS (
    SELECT 
        listed_in as genero,
        COUNT(*) as total_titulos,
        AVG(CASE WHEN type = 'Movie' THEN 1 ELSE 0 END) as pct_peliculas
    FROM catalogo
    GROUP BY listed_in
)
SELECT * FROM stats_genero
WHERE total_titulos > 10
ORDER BY total_titulos DESC
"""
resultado_cte = spark.sql(query_cte)

# CONSULTA con Window Functions
query_window = """
SELECT 
    user_id,
    show_id,
    watch_date,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY watch_date DESC) as ranking,
    SUM(duration_minutes) OVER (PARTITION BY user_id) as total_minutos_usuario
FROM visualizaciones
"""
resultado_window = spark.sql(query_window)
```

### Funcionalidades SQL Utilizadas

| Funcionalidad | Ejemplo |
|---------------|---------|
| CTEs | `WITH stats AS (...) SELECT * FROM stats` |
| Window Functions | `ROW_NUMBER() OVER (PARTITION BY...)` |
| Agregaciones | `COUNT()`, `SUM()`, `AVG()`, `MAX()` |
| Joins | `JOIN tabla ON condicion` |
| Subqueries | `WHERE x IN (SELECT...)` |

---

## 3.5 KPIs Generados (30 seg)

### Métricas de Negocio

```python
# KPIs calculados con Spark
kpis = {
    "total_titulos": catalogo_df.count(),                    # 8,807
    "total_usuarios": usuarios_df.count(),                   # 1,000
    "total_visualizaciones": visualizaciones_df.count(),     # 50,000
    "rating_promedio": valoraciones_df.agg(F.avg("rating")), # 3.8
    "genero_mas_popular": "Dramas",
    "pais_mas_contenido": "United States"
}
```

### Exportación de Resultados

```python
# CSV - Para reportes
stats_genero.write.csv("resultados/csv/stats_genero", header=True)

# JSON - Para APIs
kpis_df.write.json("resultados/json/kpis")

# Parquet - Para análisis posterior (columnar, comprimido)
catalogo_transformado.write.parquet("resultados/parquet/catalogo")
```

---

## Comando para Ejecutar

```bash
# Ejecutar procesamiento batch completo
./scripts/run-batch-completo.bat

# O manualmente
docker exec spark-master spark-submit \
    --master local[*] \
    /spark-apps/05_batch_completo.py
```

---

## Puntos Clave para Mencionar
- [ ] Explicar por qué Spark es 100x más rápido que MapReduce
- [ ] Mostrar ejemplo de RDD con map, filter, reduce
- [ ] Mostrar lectura de CSV, JSON, TXT con DataFrames
- [ ] Mostrar consulta SQL con CTE o Window Function
- [ ] Mencionar los 3 formatos de exportación (CSV, JSON, Parquet)

---

# INTEGRANTE 4: MongoDB - Modelo, Colecciones, Carga y Consultas
**Tiempo: 3 minutos**

---

## 4.1 ¿Por qué MongoDB? (30 seg)

### Comparación con Bases Relacionales

| Característica | SQL (MySQL) | NoSQL (MongoDB) |
|---------------|-------------|-----------------|
| Esquema | Fijo, rígido | **Flexible, dinámico** |
| Escalabilidad | Vertical | **Horizontal (sharding)** |
| Formato | Tablas | **Documentos JSON** |
| Joins | Nativos | Embebidos o $lookup |
| Mejor para | Transacciones | **Big Data, tiempo real** |

### MongoDB en Nuestro Proyecto
- **Versión**: MongoDB 7.0
- **Puerto**: 27017
- **UI**: Mongo Express en puerto 8082

---

## 4.2 Modelo de Datos (1 min)

### Propiedades ACID Implementadas

| Propiedad | Implementación | Ejemplo |
|-----------|---------------|---------|
| **Atomicity** | Transacciones multi-documento | Actualizar usuario + historial |
| **Consistency** | Validadores de esquema | Email único, rating 1-5 |
| **Isolation** | Read/Write Concern | `majority` para consistencia |
| **Durability** | Write Concern `w:1` | Confirmación de escritura |

### Colecciones Definidas

```
netflix_analytics (database)
├── catalogo           # Películas y series
├── usuarios           # Información de usuarios
├── visualizaciones    # Historial de reproducciones
├── valoraciones       # Ratings de usuarios
└── eventos_streaming  # Eventos en tiempo real
```

### Esquema de Documento (Catálogo)

```json
{
  "_id": ObjectId("..."),
  "show_id": "s1",
  "titulo": "Stranger Things",
  "tipo": "TV Show",
  "director": "The Duffer Brothers",
  "pais": "United States",
  "fecha_agregado": ISODate("2023-01-15"),
  "release_year": 2016,
  "rating": "TV-14",
  "duracion": "4 Seasons",
  "generos": ["Sci-Fi", "Horror", "Drama"],
  "descripcion": "...",
  "metadata": {
    "created_at": ISODate("..."),
    "updated_at": ISODate("..."),
    "source": "batch_processing"
  }
}
```

---

## 4.3 Carga de Datos desde Spark (1 min)

### Código de Carga

```python
from pyspark.sql import SparkSession

# Configuración de conexión
mongo_uri = "mongodb://mongodb:27017/netflix_analytics"

# Escribir DataFrame a MongoDB
catalogo_df.write \
    .format("mongodb") \
    .option("uri", mongo_uri) \
    .option("collection", "catalogo") \
    .mode("overwrite") \
    .save()

# Escribir eventos de streaming
eventos_df.write \
    .format("mongodb") \
    .option("uri", mongo_uri) \
    .option("collection", "eventos_streaming") \
    .mode("append") \
    .save()
```

### Script de Carga Completa

```bash
# Ejecutar carga a MongoDB
docker exec spark-master spark-submit \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
    /spark-apps/04_cargar_mongodb.py
```

---

## 4.4 Consultas MongoDB (30 seg)

### Consultas Implementadas

```javascript
// 1. Contar documentos por tipo
db.catalogo.aggregate([
  { $group: { _id: "$tipo", total: { $sum: 1 } } }
])
// Output: [{ _id: "Movie", total: 6131 }, { _id: "TV Show", total: 2676 }]

// 2. Top 5 países con más contenido
db.catalogo.aggregate([
  { $group: { _id: "$pais", total: { $sum: 1 } } },
  { $sort: { total: -1 } },
  { $limit: 5 }
])

// 3. Buscar por género (índice)
db.catalogo.find({ generos: "Drama" }).explain("executionStats")

// 4. Eventos de error en últimos 5 minutos
db.eventos_streaming.find({
  event_type: "ERROR",
  timestamp: { $gte: new Date(Date.now() - 5*60*1000) }
})
```

### Índices Creados

```javascript
// Índices para optimización
db.catalogo.createIndex({ "show_id": 1 }, { unique: true })
db.catalogo.createIndex({ "generos": 1 })
db.catalogo.createIndex({ "pais": 1, "release_year": -1 })
db.eventos_streaming.createIndex({ "timestamp": -1 })
db.eventos_streaming.createIndex({ "user_id": 1, "timestamp": -1 })
```

---

## Acceso a Mongo Express

```
URL: http://localhost:8082
Base de datos: netflix_analytics
```

---

## Puntos Clave para Mencionar
- [ ] Explicar por qué MongoDB para Big Data (flexible, escalable)
- [ ] Mencionar las 4 propiedades ACID
- [ ] Mostrar estructura de un documento JSON
- [ ] Mostrar código de carga desde Spark
- [ ] Mostrar 1-2 consultas de agregación

---

# INTEGRANTE 5: Kafka Streaming, Visualizaciones, GitHub y Conclusiones
**Tiempo: 4 minutos**

---

## 5.1 Apache Kafka - Streaming (1.5 min)

### ¿Qué es Kafka?

> Sistema de mensajería distribuido para procesamiento de eventos en tiempo real.

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PRODUCTOR     │───▶│     KAFKA       │───▶│   CONSUMIDOR    │
│                 │    │                 │    │                 │
│ Genera eventos: │    │ Topics:         │    │ Spark Streaming │
│ - PLAY          │    │ - netflix-events│    │                 │
│ - PAUSE         │    │ - netflix-alerts│    │ Procesa:        │
│ - RATE          │    │                 │    │ - Agregaciones  │
│ - SEARCH        │    │ 5 eventos/seg   │    │ - Anomalías     │
│ - ERROR         │    │                 │    │ - Alertas       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Productor de Eventos (06_kafka_producer.py)

```python
# Tipos de eventos generados
event_types = {
    "PLAY": 0.40,    # 40% reproducciones
    "PAUSE": 0.25,   # 25% pausas
    "RATE": 0.15,    # 15% valoraciones
    "SEARCH": 0.15,  # 15% búsquedas
    "ERROR": 0.05    # 5% errores
}

# Estructura del evento
evento = {
    "event_id": str(uuid4()),
    "user_id": f"user_{random.randint(1, 1000)}",
    "event_type": "PLAY",
    "show_id": f"s{random.randint(1, 8807)}",
    "timestamp": datetime.now().isoformat(),
    "country": random.choice(["US", "MX", "ES", "AR"]),
    "device": random.choice(["mobile", "tv", "web"]),
    "duration_ms": random.randint(1000, 180000)
}
```

### Consumidor Spark Streaming (07_spark_streaming.py)

```python
# Leer de Kafka
eventos_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "netflix-events") \
    .load()

# Procesar con ventanas de tiempo
metricas = eventos_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        F.window("timestamp", "30 seconds"),
        "event_type"
    ) \
    .count()

# Detección de anomalías
anomalias = eventos_df \
    .groupBy(F.window("timestamp", "1 minute"), "user_id") \
    .agg(F.count("*").alias("eventos")) \
    .filter(F.col("eventos") > 50)  # Más de 50 eventos/min = anomalía
```

### Ejecución del Demo

```bash
# Terminal 1: Iniciar productor
./scripts/run-kafka-producer.bat

# Terminal 2: Iniciar consumidor
./scripts/run-spark-streaming.bat

# O demo completa (abre ambas ventanas)
./scripts/run-streaming-demo.bat
```

---

## 5.2 Visualizaciones y Monitoreo (1 min)

### Interfaces Web Disponibles

| Herramienta | URL | Qué Ver |
|-------------|-----|---------|
| **Kafka UI** | http://localhost:8083 | Topics, mensajes, consumidores |
| **Spark UI** | http://localhost:8080 | Jobs, stages, ejecutores |
| **Mongo Express** | http://localhost:8082 | Colecciones, documentos |

### Kafka UI - Qué Mostrar

```
1. Topics creados:
   - netflix-events (mensajes de usuario)
   - netflix-alerts (alertas de error)

2. Mensajes en tiempo real:
   - Ver JSON de eventos
   - Ver particiones
   - Ver offset

3. Consumer Groups:
   - spark-streaming-consumer
   - Lag de mensajes
```

### Spark UI - Qué Mostrar

```
1. Streaming Queries:
   - Estado: ACTIVE
   - Input rate: ~5 rows/sec
   - Processing rate
   - Batch duration

2. Jobs completados:
   - Micro-batches procesados
   - Tiempo de procesamiento
```

---

## 5.3 GitHub y Versionamiento (1 min)

### Estructura del Repositorio

```
AA3-BigData/
├── .github/
│   └── workflows/
│       └── ci.yml          # Pipeline CI/CD
├── spark-apps/             # Código PySpark
├── datos/                  # Datasets
├── scripts/                # Scripts de ejecución
├── docs/                   # Documentación
├── docker-compose.yml      # Infraestructura
├── .gitignore              # Archivos ignorados
└── README.md               # Documentación principal
```

### Flujo de Trabajo Git

```
main (producción)
  │
  ├── develop (desarrollo)
  │     │
  │     ├── feature/batch-processing
  │     ├── feature/kafka-streaming
  │     └── feature/mongodb-integration
```

### Convención de Commits

```bash
feat(batch): implementar lectura de archivos CSV, JSON, TXT
fix(streaming): corregir timeout de conexión Kafka
docs: actualizar guía de presentación
```

### GitHub Actions (CI/CD)

```yaml
# Pipeline automático
on: [push, pull_request]

jobs:
  validate:
    - Lint código Python
    - Verificar sintaxis PySpark
    - Validar docker-compose
```

---

## 5.4 Conclusiones (30 seg)

### Resultados Obtenidos

```
┌─────────────────────────────────────────────────────────────────┐
│                    RESULTADOS DEL PROYECTO                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ✅ Procesamiento batch de 8,807 títulos en < 30 segundos      │
│  ✅ Streaming de 5 eventos/segundo en tiempo real              │
│  ✅ Detección de anomalías en ventanas de 1 minuto             │
│  ✅ Alertas automáticas por errores                            │
│  ✅ KPIs de negocio calculados y exportados                    │
│  ✅ Datos almacenados con propiedades ACID                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Mejoras en Tiempos

| Proceso | Antes | Después | Mejora |
|---------|-------|---------|--------|
| Detección errores | 2-3 días | 5 segundos | 99.99% |
| Reportes | 2 semanas | 1 hora | 99.7% |
| Análisis tiempo real | No existía | Inmediato | ∞ |

### Tecnologías Dominadas

```
Apache Spark 3.1.1    → Batch + Streaming
Apache Kafka 7.5.0    → Event Streaming
MongoDB 7.0           → NoSQL con ACID
Docker Compose        → Orquestación
GitHub Actions        → CI/CD
```

### Próximos Pasos (Futuro)

1. **Machine Learning**: Modelos predictivos de churn
2. **Real-time Recommendations**: Sistema de recomendaciones
3. **Delta Lake**: Evolución a lakehouse con ACID completo

---

## Cierre

> *"Hemos demostrado cómo una arquitectura Big Data moderna puede transformar datos masivos en insights accionables, combinando procesamiento batch para análisis histórico y streaming para decisiones en tiempo real."*

**¿Preguntas?**

---

## Puntos Clave para Mencionar
- [ ] Explicar flujo Productor → Kafka → Consumidor
- [ ] Mostrar Kafka UI con mensajes en tiempo real
- [ ] Mencionar la estructura del repositorio GitHub
- [ ] Mostrar convención de commits
- [ ] Resumir los 6 resultados principales
- [ ] Mencionar las 5 tecnologías dominadas

---

# RESUMEN DE TIEMPOS

| # | Integrante | Tema | Tiempo |
|---|------------|------|--------|
| 1 | Integrante 1 | Caso, Problema, Objetivos, Datos | 3 min |
| 2 | Integrante 2 | Arquitectura y Flujo del Dato | 3 min |
| 3 | Integrante 3 | Spark: RDD, DataFrames, SQL | 4 min |
| 4 | Integrante 4 | MongoDB: Modelo, Carga, Consultas | 3 min |
| 5 | Integrante 5 | Kafka, Visualizaciones, GitHub, Conclusiones | 4 min |
| | | **TOTAL** | **17 min** |

---

# CHECKLIST PRE-PRESENTACIÓN

```
ANTES DE PRESENTAR:
[ ] Docker Desktop iniciado
[ ] docker-compose up -d ejecutado
[ ] Verificar 11 contenedores corriendo
[ ] Abrir Kafka UI (localhost:8083)
[ ] Abrir Mongo Express (localhost:8082)
[ ] Abrir Spark UI (localhost:8080)
[ ] Tener terminales listas para demo
[ ] Tener slides/docs abiertos
```

---

*Documento preparado para AA4 - Big Data CERTUS*
*Mayo 2026*
