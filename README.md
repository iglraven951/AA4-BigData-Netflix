# AA4 Big Data - Netflix Analytics Platform

Plataforma de analisis de datos de streaming inspirada en Netflix, desarrollada como proyecto de evaluacion AA4 para el curso de Big Data en Instituto CERTUS.

## Descripcion del Proyecto

Este proyecto demuestra el procesamiento de grandes volumenes de datos utilizando tecnologias Big Data modernas:

- **Procesamiento Batch**: Apache Spark (RDD, DataFrames, SQL)
- **Procesamiento Streaming**: Apache Kafka + Spark Structured Streaming
- **Almacenamiento**: MongoDB con propiedades ACID
- **Infraestructura**: Docker Compose con 11 contenedores

## Tecnologias Utilizadas

| Tecnologia | Version | Proposito |
|------------|---------|-----------|
| Apache Spark | 3.1.1 | Motor de procesamiento |
| Apache Kafka | 7.5.0 | Streaming en tiempo real |
| MongoDB | 7.0 | Base de datos NoSQL |
| Hadoop HDFS | 3.2.1 | Sistema de archivos distribuido |
| Docker | - | Contenedores |
| Python | 3.8+ | Lenguaje de programacion |

## Estructura del Proyecto

```
AA4-TRABAJO-FINAL/
├── spark-apps/           # Scripts de procesamiento Spark
│   ├── 01_spark_rdd.py          # Operaciones RDD basicas
│   ├── 02_spark_dataframe.py    # Operaciones DataFrame
│   ├── 03_spark_sql.py          # Consultas SQL
│   ├── 04_cargar_mongodb.py     # Carga a MongoDB
│   ├── 05_batch_completo.py     # BATCH COMPLETO (Seccion D)
│   ├── 06_kafka_producer.py     # Productor Kafka (Seccion E)
│   └── 07_spark_streaming.py    # Spark Streaming (Seccion E)
├── datos/                # Archivos de datos de entrada
│   ├── catalogo.csv             # Catalogo Netflix (~8,800 titulos)
│   ├── usuarios.csv             # Informacion de usuarios
│   ├── visualizaciones.csv      # Historial de reproducciones
│   └── valoraciones.csv         # Ratings de usuarios
├── scripts/              # Scripts de ejecucion
│   ├── start.bat                # Iniciar cluster
│   ├── stop.bat                 # Detener cluster
│   ├── run-batch-completo.bat   # Ejecutar batch
│   └── run-streaming-demo.bat   # Ejecutar streaming
├── docs/                 # Documentacion
│   ├── INFORME_VIDEO_AA4.html   # Informe para presentacion
│   ├── 03_MODELO_DATOS_ACID.md  # Modelo de datos
│   ├── 04_DATA_STORYTELLING.md  # Narrativa del proyecto
│   └── 05_VERSIONAMIENTO_GITHUB.md
├── docker-compose.yml    # Configuracion de contenedores
└── README.md             # Este archivo
```

## Requisitos Previos

- Docker Desktop instalado y ejecutandose
- 8 GB de RAM minimo disponible para Docker
- Git (para clonar el repositorio)

## Instalacion y Ejecucion

### 1. Clonar el repositorio

```bash
git clone https://github.com/TU_USUARIO/AA4-TRABAJO-FINAL.git
cd AA4-TRABAJO-FINAL
```

### 2. Iniciar el cluster

```bash
docker-compose up -d
```

Esto levantara 11 contenedores:
- Hadoop: namenode, datanode, resourcemanager, nodemanager, historyserver
- Spark: spark-master, spark-worker
- Kafka: zookeeper, kafka, kafka-ui
- MongoDB: mongodb, mongo-express

### 3. Verificar que todo esta corriendo

```bash
docker ps
```

### 4. Ejecutar procesamiento Batch (Seccion D)

```bash
./scripts/run-batch-completo.bat
```

### 5. Ejecutar procesamiento Streaming (Seccion E)

Terminal 1 - Iniciar productor:
```bash
./scripts/run-kafka-producer.bat
```

Terminal 2 - Iniciar consumidor:
```bash
./scripts/run-spark-streaming.bat
```

## Interfaces Web

| Servicio | URL | Descripcion |
|----------|-----|-------------|
| Spark UI | http://localhost:8080 | Estado de trabajos Spark |
| Kafka UI | http://localhost:8083 | Monitoreo de topics Kafka |
| Mongo Express | http://localhost:8082 | Explorar datos MongoDB |
| HDFS UI | http://localhost:9870 | Sistema de archivos HDFS |
| YARN | http://localhost:8088 | Gestor de recursos |

## Contenido por Seccion (Rubrica AA4)

### Seccion D - Procesamiento Batch (4 puntos)
- Lectura de archivos CSV, JSON, TXT
- Limpieza de datos (nulos, duplicados, outliers)
- Transformacion de columnas
- Integracion con JOINs
- Operaciones RDD (map, filter, reduce)
- Consultas Spark SQL (CTEs, Window Functions)
- Generacion de KPIs
- Exportacion a CSV, JSON, Parquet

### Seccion E - Procesamiento Streaming (4 puntos)
- Productor Kafka con eventos simulados
- Topics de Kafka
- Spark Structured Streaming
- Micro-batches cada 10 segundos
- Ventanas temporales (30s, 1min)
- Deteccion de anomalias
- Alertas en tiempo real
- Salida a consola, archivos y MongoDB

### Modelo de Datos ACID (4 puntos)
- MongoDB con transacciones ACID
- 5 colecciones (catalogo, usuarios, visualizaciones, valoraciones, eventos)
- Validadores de esquema
- Indices optimizados

### Data Storytelling (4 puntos)
- Narrativa: Comienzo, Problema, Solucion, Final
- KPIs de negocio
- Visualizaciones

### Versionamiento GitHub (4 puntos)
- Repositorio organizado
- Commits semanticos
- Documentacion completa

## Detener el Cluster

```bash
docker-compose down
```

## Autores

- Integrante 1
- Integrante 2
- Integrante 3
- Integrante 4
- Integrante 5

## Institucion

Instituto CERTUS - Big Data y Analisis de Datos - Mayo 2026
