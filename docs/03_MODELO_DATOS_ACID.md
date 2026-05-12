# Modelo de Datos - Netflix Analytics Platform

## Propiedades ACID en el Modelo de Datos

### ¿Qué es ACID?

ACID es un conjunto de propiedades que garantizan la integridad de las transacciones en bases de datos:

| Propiedad | Descripción | Implementación en Netflix Analytics |
|-----------|-------------|-------------------------------------|
| **A**tomicity | Todas las operaciones se completan o ninguna | Transacciones Spark con `checkpoint` |
| **C**onsistency | Los datos siempre están en estado válido | Validaciones y schemas estrictos |
| **I**solation | Transacciones concurrentes no interfieren | Particionamiento en Spark/Kafka |
| **D**urability | Los datos persisten tras commit | MongoDB con replica sets, HDFS |

---

## Arquitectura del Modelo de Datos

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CAPA DE INGESTA (Raw Data)                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│   │   CSV        │  │   JSON       │  │   TXT        │  │   KAFKA      │   │
│   │   Files      │  │   Files      │  │   Logs       │  │   Streaming  │   │
│   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │
│          │                 │                 │                 │           │
│          └────────────────┬┴─────────────────┴─────────────────┘           │
│                           ▼                                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                        CAPA DE PROCESAMIENTO (Spark)                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                         APACHE SPARK                                │   │
│   │  ┌─────────┐    ┌─────────────┐    ┌─────────────┐                 │   │
│   │  │   RDD   │───>│  DataFrame  │───>│  Spark SQL  │                 │   │
│   │  └─────────┘    └─────────────┘    └─────────────┘                 │   │
│   │       │                │                  │                         │   │
│   │       ▼                ▼                  ▼                         │   │
│   │  ┌─────────────────────────────────────────────────────────────┐   │   │
│   │  │              TRANSFORMACIONES ETL                           │   │   │
│   │  │  • Limpieza   • Joins   • Agregaciones   • Window Functions │   │   │
│   │  └─────────────────────────────────────────────────────────────┘   │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                           │                                                 │
│                           ▼                                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                        CAPA DE ALMACENAMIENTO (MongoDB)                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                         MONGODB                                     │   │
│   │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │   │
│   │  │  catalogo   │  │  usuarios   │  │visualizac.  │                 │   │
│   │  └─────────────┘  └─────────────┘  └─────────────┘                 │   │
│   │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │   │
│   │  │ valoraciones│  │   kpis      │  │  alertas    │                 │   │
│   │  └─────────────┘  └─────────────┘  └─────────────┘                 │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Entidades del Modelo de Datos

### 1. Catálogo de Contenido (`catalogo`)

| Campo | Tipo | Descripción | Restricciones |
|-------|------|-------------|---------------|
| `id` | String | Identificador único | PK, NOT NULL |
| `titulo` | String | Nombre del contenido | NOT NULL, max 255 |
| `tipo` | Enum | "serie" o "pelicula" | NOT NULL |
| `genero` | String | Género principal | NOT NULL |
| `anio` | Integer | Año de lanzamiento | 1900-2030 |
| `duracion_min` | Integer | Duración en minutos | >= 0 |
| `calificacion` | Float | Rating 0-10 | 0.0-10.0 |
| `idioma` | String | Idioma original | ISO 639-1 |
| `pais` | String | País de origen | NOT NULL |

**Schema JSON (MongoDB):**
```json
{
  "_id": "content_001",
  "titulo": "Stranger Things",
  "tipo": "serie",
  "genero": "Sci-Fi",
  "anio": 2016,
  "duracion_min": 50,
  "calificacion": 8.7,
  "idioma": "en",
  "pais": "USA",
  "metadata": {
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-03-20T15:45:00Z"
  }
}
```

---

### 2. Usuarios (`usuarios`)

| Campo | Tipo | Descripción | Restricciones |
|-------|------|-------------|---------------|
| `user_id` | String | Identificador único | PK, NOT NULL |
| `nombre` | String | Nombre completo | NOT NULL |
| `email` | String | Correo electrónico | UNIQUE, NOT NULL |
| `pais` | String | País de residencia | NOT NULL |
| `plan` | Enum | "basico", "estandar", "premium" | NOT NULL |
| `fecha_registro` | Date | Fecha de alta | NOT NULL |
| `edad` | Integer | Edad del usuario | 13-120 |
| `dispositivo_principal` | String | Dispositivo preferido | - |

**Schema JSON (MongoDB):**
```json
{
  "_id": "user_001",
  "nombre": "Juan Pérez",
  "email": "juan.perez@email.com",
  "pais": "Mexico",
  "plan": "premium",
  "fecha_registro": "2023-06-15",
  "edad": 28,
  "dispositivo_principal": "Smart TV",
  "preferencias": {
    "generos_favoritos": ["Action", "Sci-Fi"],
    "idioma_subtitulos": "es"
  }
}
```

---

### 3. Visualizaciones (`visualizaciones`)

| Campo | Tipo | Descripción | Restricciones |
|-------|------|-------------|---------------|
| `view_id` | String | Identificador único | PK, NOT NULL |
| `user_id` | String | Referencia a usuario | FK -> usuarios |
| `content_id` | String | Referencia a contenido | FK -> catalogo |
| `fecha` | DateTime | Fecha/hora de visualización | NOT NULL |
| `duracion_vista_min` | Integer | Minutos vistos | >= 0 |
| `completado` | Boolean | ¿Terminó el contenido? | NOT NULL |
| `dispositivo` | String | Dispositivo usado | NOT NULL |

**Relaciones:**
- `user_id` → `usuarios.user_id` (N:1)
- `content_id` → `catalogo.id` (N:1)

**Schema JSON (MongoDB):**
```json
{
  "_id": "view_001",
  "user_id": "user_001",
  "content_id": "content_001",
  "fecha": "2024-03-15T20:30:00Z",
  "duracion_vista_min": 45,
  "completado": true,
  "dispositivo": "Smart TV",
  "calidad_stream": "4K",
  "ubicacion": {
    "pais": "Mexico",
    "ciudad": "CDMX"
  }
}
```

---

### 4. Valoraciones (`valoraciones`)

| Campo | Tipo | Descripción | Restricciones |
|-------|------|-------------|---------------|
| `rating_id` | String | Identificador único | PK, NOT NULL |
| `user_id` | String | Referencia a usuario | FK -> usuarios |
| `content_id` | String | Referencia a contenido | FK -> catalogo |
| `puntuacion` | Integer | Rating del usuario | 1-5 |
| `comentario` | String | Reseña opcional | max 1000 |
| `fecha` | DateTime | Fecha de valoración | NOT NULL |

**Schema JSON (MongoDB):**
```json
{
  "_id": "rating_001",
  "user_id": "user_001",
  "content_id": "content_001",
  "puntuacion": 5,
  "comentario": "Excelente serie, muy recomendada",
  "fecha": "2024-03-16T10:00:00Z",
  "helpful_votes": 15
}
```

---

### 5. Eventos de Streaming (`eventos` - Kafka)

| Campo | Tipo | Descripción | Restricciones |
|-------|------|-------------|---------------|
| `event_id` | UUID | Identificador único | PK, NOT NULL |
| `event_type` | Enum | Tipo de evento | NOT NULL |
| `timestamp` | DateTime | Momento del evento | NOT NULL |
| `user_id` | String | Usuario que genera el evento | NOT NULL |
| `content_id` | String | Contenido relacionado | - |
| `device` | String | Dispositivo | NOT NULL |
| `country` | String | País | NOT NULL |

**Tipos de Eventos:**
- `PLAY` - Inicia reproducción
- `PAUSE` - Pausa reproducción
- `STOP` - Detiene reproducción
- `RATE` - Califica contenido
- `SEARCH` - Búsqueda
- `LOGIN` - Inicio de sesión
- `ERROR` - Error del sistema

---

## Implementación de Propiedades ACID

### 1. Atomicity (Atomicidad)

**Implementación en Spark:**
```python
# Checkpoint para garantizar atomicidad
spark.sparkContext.setCheckpointDir("/checkpoints")

# Transacción atómica con try-except
try:
    df_transformed = df.transform(pipeline)
    df_transformed.write.mode("overwrite").parquet("/output")
except Exception as e:
    # Rollback - no se guardan cambios parciales
    logger.error(f"Transaction failed: {e}")
    raise
```

**Implementación en MongoDB:**
```python
# Transacciones MongoDB
with client.start_session() as session:
    with session.start_transaction():
        collection1.insert_one(doc1, session=session)
        collection2.update_one(filter, update, session=session)
        # Si falla cualquier operación, se revierte todo
```

### 2. Consistency (Consistencia)

**Validación de Schema en Spark:**
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Schema estricto
catalogo_schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("titulo", StringType(), nullable=False),
    StructField("tipo", StringType(), nullable=False),
    StructField("calificacion", FloatType(), nullable=True),
])

# Lectura con validación
df = spark.read.schema(catalogo_schema).json("/datos/catalogo.json")
```

**Validación en MongoDB:**
```javascript
// Validación de schema en MongoDB
db.createCollection("catalogo", {
   validator: {
      $jsonSchema: {
         bsonType: "object",
         required: ["titulo", "tipo", "genero"],
         properties: {
            calificacion: {
               bsonType: "double",
               minimum: 0,
               maximum: 10
            }
         }
      }
   }
})
```

### 3. Isolation (Aislamiento)

**Particionamiento en Spark:**
```python
# Particionamiento para aislamiento
df.write \
    .partitionBy("pais", "fecha") \
    .mode("append") \
    .parquet("/output")

# Cada partición se procesa de forma aislada
```

**Particiones en Kafka:**
```python
# Topics con múltiples particiones
# Cada partición es procesada por un consumidor independiente
# Garantiza orden dentro de la partición

topic_config = {
    "num_partitions": 3,
    "replication_factor": 1
}
```

### 4. Durability (Durabilidad)

**HDFS Replication:**
```xml
<!-- hdfs-site.xml -->
<property>
    <name>dfs.replication</name>
    <value>3</value>  <!-- 3 copias de cada bloque -->
</property>
```

**MongoDB Write Concern:**
```python
# Write concern para durabilidad
client = MongoClient(
    "mongodb://...",
    w="majority",  # Escribe en mayoría de nodos
    journal=True   # Garantiza escritura en journal
)
```

---

## Tipos de Archivos Utilizados

| Formato | Uso | Ventajas | Ubicación |
|---------|-----|----------|-----------|
| **CSV** | Datos tabulares | Legible, universal | `/datos/*.csv` |
| **JSON** | Datos anidados | Flexible, schema-less | `/datos/*.json` |
| **Parquet** | Almacenamiento optimizado | Columnar, comprimido | `/resultados/parquet/` |
| **TXT** | Logs | Simple, streaming | `/datos/logs_*.txt` |

### Comparación de Formatos:

```
┌────────────────────────────────────────────────────────────────┐
│                   COMPARACIÓN DE FORMATOS                      │
├──────────┬─────────────┬──────────────┬───────────────────────┤
│ Formato  │ Tamaño      │ Velocidad    │ Caso de Uso           │
├──────────┼─────────────┼──────────────┼───────────────────────┤
│ CSV      │ Medio       │ Lenta        │ Intercambio de datos  │
│ JSON     │ Grande      │ Media        │ APIs, documentos      │
│ Parquet  │ Pequeño     │ Rápida       │ Analytics, Spark      │
│ Avro     │ Pequeño     │ Rápida       │ Kafka, streaming      │
└──────────┴─────────────┴──────────────┴───────────────────────┘
```

---

## Diagrama Entidad-Relación

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│    USUARIOS     │       │ VISUALIZACIONES │       │    CATALOGO     │
├─────────────────┤       ├─────────────────┤       ├─────────────────┤
│ PK user_id      │──┐    │ PK view_id      │    ┌──│ PK id           │
│    nombre       │  │    │ FK user_id      │────┘  │    titulo       │
│    email        │  └───>│ FK content_id   │<──────│    tipo         │
│    pais         │       │    fecha        │       │    genero       │
│    plan         │       │    duracion     │       │    calificacion │
│    edad         │       │    completado   │       │    pais         │
└─────────────────┘       └─────────────────┘       └─────────────────┘
         │                                                   │
         │                ┌─────────────────┐                │
         │                │  VALORACIONES   │                │
         │                ├─────────────────┤                │
         └───────────────>│ PK rating_id    │<───────────────┘
                          │ FK user_id      │
                          │ FK content_id   │
                          │    puntuacion   │
                          │    comentario   │
                          └─────────────────┘
```

---

## Índices y Optimización

### Índices en MongoDB:

```javascript
// Índices para consultas frecuentes
db.catalogo.createIndex({ "genero": 1 })
db.catalogo.createIndex({ "tipo": 1, "calificacion": -1 })
db.usuarios.createIndex({ "email": 1 }, { unique: true })
db.visualizaciones.createIndex({ "user_id": 1, "fecha": -1 })
db.visualizaciones.createIndex({ "content_id": 1 })

// Índice compuesto para joins frecuentes
db.valoraciones.createIndex({ "content_id": 1, "puntuacion": -1 })
```

### Particionamiento en Spark:

```python
# Particionamiento óptimo por columnas frecuentes
df.repartition(4, "pais") \
  .write \
  .partitionBy("tipo", "genero") \
  .parquet("/output")
```

---

## Resumen del Modelo

| Aspecto | Implementación |
|---------|----------------|
| **Atomicidad** | Checkpoints en Spark, Transacciones MongoDB |
| **Consistencia** | Schemas estrictos, Validadores |
| **Aislamiento** | Particionamiento, Topics Kafka |
| **Durabilidad** | Replicación HDFS (3x), Write Concern MongoDB |
| **Formato Principal** | Parquet (analytics), JSON (documentos) |
| **Base de Datos** | MongoDB 7.0 (NoSQL documental) |
| **Relaciones** | Referencias por ID (desnormalizado) |
