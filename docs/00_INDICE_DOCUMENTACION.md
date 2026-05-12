# Índice de Documentación - AA4 Big Data

## Documentos del Proyecto

| # | Documento | Descripción | Rúbrica |
|---|-----------|-------------|---------|
| 01 | [Arquitectura General](./01_ARQUITECTURA.md) | Arquitectura del sistema Big Data | Sección A |
| 02 | [Infraestructura Docker](./02_INFRAESTRUCTURA_DOCKER.md) | 11 contenedores y configuración | Sección B |
| 03 | [Modelo de Datos ACID](./03_MODELO_DATOS_ACID.md) | Propiedades ACID y esquemas | Modelo de Datos |
| 04 | [Data Storytelling](./04_DATA_STORYTELLING.md) | Narrativa: Comienzo, Problema, Solución, Final | Data Storytelling |
| 05 | [Versionamiento GitHub](./05_VERSIONAMIENTO_GITHUB.md) | Git, GitHub, CI/CD, DevOps | Versionamiento |

---

## Secciones de Evaluación Cubiertas

### Sección D - Procesamiento Batch (4 pts)

| Requisito | Archivo | Estado |
|-----------|---------|--------|
| Lectura de archivos (CSV, JSON, TXT) | `spark-apps/05_batch_completo.py` | ✅ |
| Limpieza de datos | `spark-apps/05_batch_completo.py` | ✅ |
| Transformación de columnas | `spark-apps/05_batch_completo.py` | ✅ |
| Integración de fuentes (JOINs) | `spark-apps/05_batch_completo.py` | ✅ |
| Uso de DataFrames | `spark-apps/05_batch_completo.py` | ✅ |
| Uso de Spark SQL | `spark-apps/05_batch_completo.py` | ✅ |
| Uso de RDD | `spark-apps/05_batch_completo.py` | ✅ |
| Generación de KPIs | `spark-apps/05_batch_completo.py` | ✅ |
| Exportación de resultados | `spark-apps/05_batch_completo.py` | ✅ |

### Sección E - Procesamiento Streaming (4 pts)

| Requisito | Archivo | Estado |
|-----------|---------|--------|
| Creación de productor | `spark-apps/06_kafka_producer.py` | ✅ |
| Creación de topic | `06_kafka_producer.py` (auto-create) | ✅ |
| Envío de eventos simulados | `spark-apps/06_kafka_producer.py` | ✅ |
| Lectura con Structured Streaming | `spark-apps/07_spark_streaming.py` | ✅ |
| Procesamiento por micro-batches | `spark-apps/07_spark_streaming.py` | ✅ |
| Generación de alertas | `spark-apps/07_spark_streaming.py` | ✅ |
| Salida en consola/archivo/MongoDB | `spark-apps/07_spark_streaming.py` | ✅ |

### Versionamiento de Código (4 pts)

| Requisito | Documento | Estado |
|-----------|-----------|--------|
| Configuración de Git | `docs/05_VERSIONAMIENTO_GITHUB.md` | ✅ |
| Estructura de repositorio | `docs/05_VERSIONAMIENTO_GITHUB.md` | ✅ |
| Flujo de trabajo Git | `docs/05_VERSIONAMIENTO_GITHUB.md` | ✅ |
| GitHub Actions CI/CD | `docs/05_VERSIONAMIENTO_GITHUB.md` | ✅ |
| Convención de commits | `docs/05_VERSIONAMIENTO_GITHUB.md` | ✅ |
| Mejores prácticas | `docs/05_VERSIONAMIENTO_GITHUB.md` | ✅ |

### Modelo de Datos ACID (4 pts)

| Requisito | Documento | Estado |
|-----------|-----------|--------|
| Propiedades ACID explicadas | `docs/03_MODELO_DATOS_ACID.md` | ✅ |
| Arquitectura de datos | `docs/03_MODELO_DATOS_ACID.md` | ✅ |
| Esquemas JSON/MongoDB | `docs/03_MODELO_DATOS_ACID.md` | ✅ |
| Diagrama ER | `docs/03_MODELO_DATOS_ACID.md` | ✅ |
| Estrategias de optimización | `docs/03_MODELO_DATOS_ACID.md` | ✅ |

### Data Storytelling (4 pts)

| Requisito | Documento | Estado |
|-----------|-----------|--------|
| Comienzo (Contexto) | `docs/04_DATA_STORYTELLING.md` | ✅ |
| Problema (Desafíos) | `docs/04_DATA_STORYTELLING.md` | ✅ |
| Solución (Arquitectura) | `docs/04_DATA_STORYTELLING.md` | ✅ |
| Final (Resultados) | `docs/04_DATA_STORYTELLING.md` | ✅ |
| ROI y métricas | `docs/04_DATA_STORYTELLING.md` | ✅ |

---

## Archivos de Código Principales

```
spark-apps/
├── 01_spark_rdd.py           # Procesamiento RDD básico
├── 02_spark_dataframe.py     # Operaciones DataFrame
├── 03_spark_sql.py           # Consultas SQL
├── 04_cargar_mongodb.py      # Carga a MongoDB
├── 05_batch_completo.py      # ★ SECCIÓN D COMPLETA
├── 06_kafka_producer.py      # ★ PRODUCTOR KAFKA
└── 07_spark_streaming.py     # ★ STREAMING COMPLETO
```

---

## Scripts de Ejecución

| Script | Descripción | Uso |
|--------|-------------|-----|
| `run-batch-completo.bat` | Ejecutar Sección D | `./scripts/run-batch-completo.bat` |
| `run-kafka-producer.bat` | Iniciar productor | `./scripts/run-kafka-producer.bat` |
| `run-spark-streaming.bat` | Iniciar consumidor | `./scripts/run-spark-streaming.bat` |
| `run-streaming-demo.bat` | Demo completa E | `./scripts/run-streaming-demo.bat` |

---

## Guía de Presentación (15-20 minutos)

### Estructura Sugerida

| Tiempo | Sección | Contenido |
|--------|---------|-----------|
| 0-3 min | Introducción | Contexto, problema, objetivos |
| 3-7 min | Arquitectura | Docker, Spark, Kafka, MongoDB |
| 7-10 min | Demo Batch | Ejecutar y mostrar `05_batch_completo.py` |
| 10-14 min | Demo Streaming | Productor + Consumidor en vivo |
| 14-17 min | Resultados | KPIs, métricas, ROI |
| 17-20 min | Conclusiones | Lecciones aprendidas, siguientes pasos |

### Demo en Vivo - Comandos

```bash
# 1. Mostrar contenedores
docker ps --format "table {{.Names}}\t{{.Status}}"

# 2. Ejecutar batch
./scripts/run-batch-completo.bat

# 3. Mostrar resultados
ls resultados/batch_completo/

# 4. Iniciar streaming demo
./scripts/run-streaming-demo.bat

# 5. Mostrar Kafka UI
# Abrir http://localhost:8083

# 6. Mostrar Mongo Express
# Abrir http://localhost:8082
```

---

## Checklist Final de Entrega

- [ ] Código fuente completo (`spark-apps/`)
- [ ] Docker Compose funcional (`docker-compose.yml`)
- [ ] Scripts de ejecución (`scripts/`)
- [ ] Documentación ACID (`docs/03_MODELO_DATOS_ACID.md`)
- [ ] Data Storytelling (`docs/04_DATA_STORYTELLING.md`)
- [ ] Guía de Versionamiento (`docs/05_VERSIONAMIENTO_GITHUB.md`)
- [ ] README actualizado (`SECCION_D_E_README.md`)
- [ ] Repositorio GitHub configurado
- [ ] Demo probada y funcional

---

*AA4 Big Data - CERTUS*
*Mayo 2026*
