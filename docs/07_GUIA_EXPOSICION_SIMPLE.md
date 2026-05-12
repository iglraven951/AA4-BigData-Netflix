# Guía de Exposición - AA4 Big Data
## Netflix Analytics Platform

**Duración Total**: 17 minutos

---

# INTEGRANTE 1: Caso, Problema, Objetivos y Datos (3 min)

## Qué Decir:

### Introducción (30 seg)
"Buenas tardes, somos el grupo X y vamos a presentar nuestra plataforma de analytics inspirada en Netflix. Netflix es una empresa de streaming con más de 230 millones de usuarios en el mundo, y genera millones de datos cada día."

### El Problema (1 min)
"El problema principal que identificamos es que las empresas de streaming tienen MUCHA información pero no pueden analizarla rápido. Imaginen que Netflix detecta un error en su plataforma 3 días después de que pasó - eso significa miles de usuarios molestos.

Los problemas específicos son:
- Los datos están en diferentes formatos: algunos en CSV, otros en JSON, otros en archivos de texto
- No hay análisis en tiempo real, todo se hace manual
- Los reportes tardan semanas en generarse
- No pueden detectar problemas automáticamente"

### Objetivos (45 seg)
"Nuestro objetivo es crear una plataforma que pueda:
1. Leer datos de cualquier formato
2. Limpiar y transformar esos datos automáticamente
3. Generar reportes y métricas de negocio
4. Procesar eventos en tiempo real
5. Detectar anomalías y generar alertas
6. Guardar todo en una base de datos confiable"

### Datos Utilizados (45 seg)
"Para este proyecto usamos 4 fuentes de datos:
- Un CSV con el catálogo de Netflix: casi 9 mil películas y series
- Un JSON con información de usuarios
- Un archivo de texto con el historial de reproducciones
- Y eventos en tiempo real que simulamos: reproducciones, pausas, búsquedas y errores"

---

# INTEGRANTE 2: Arquitectura Big Data y Flujo del Dato (3 min)

## Qué Decir:

### La Arquitectura (1 min)
"Nuestra arquitectura tiene 3 capas principales:

**Primera capa - Ingesta**: Aquí es donde entran los datos. Usamos HDFS de Hadoop para guardar archivos grandes y Kafka para recibir eventos en tiempo real.

**Segunda capa - Procesamiento**: Aquí usamos Apache Spark, que es el motor que procesa todos los datos. Spark puede procesar archivos guardados (batch) y también eventos en tiempo real (streaming).

**Tercera capa - Almacenamiento**: Los resultados se guardan en MongoDB, que es una base de datos NoSQL, y también exportamos a diferentes formatos como Parquet y JSON."

### Infraestructura Docker (1 min)
"Todo esto corre en Docker, que es como tener varias computadoras virtuales trabajando juntas. En total tenemos 11 contenedores:

- 5 contenedores para Hadoop: el sistema de archivos distribuido
- 2 contenedores para Spark: el motor de procesamiento
- 3 contenedores para Kafka: el sistema de mensajería en tiempo real
- 2 contenedores para MongoDB: la base de datos

Con un solo comando levantamos toda esta infraestructura."

### Flujo del Dato (1 min)
"El dato sigue dos caminos:

**Camino Batch**: Los archivos CSV, JSON y TXT entran a HDFS, luego Spark los lee, los limpia, los transforma, y genera reportes que se guardan en MongoDB o en archivos Parquet.

**Camino Streaming**: Los eventos de usuario llegan a Kafka en tiempo real, Spark Streaming los procesa cada 10 segundos, detecta anomalías, y genera alertas inmediatas."

---

# INTEGRANTE 3: Procesamiento Batch con Spark (4 min)

## Qué Decir:

### Por qué Spark (30 seg)
"Usamos Apache Spark porque es hasta 100 veces más rápido que tecnologías anteriores como MapReduce. La razón es que Spark procesa los datos en memoria RAM en lugar de escribir al disco todo el tiempo."

### RDD - La Base de Spark (1 min)
"RDD significa Resilient Distributed Dataset, y es la forma más básica de trabajar con datos en Spark.

Imaginen que tienen una lista gigante de eventos. Con RDD pueden:
- **Map**: Transformar cada elemento, por ejemplo extraer solo el tipo de evento
- **Filter**: Quedarse solo con algunos elementos, por ejemplo solo los errores
- **Reduce**: Combinar elementos, por ejemplo contar cuántos eventos hay de cada tipo

Lo interesante es que Spark no ejecuta nada hasta que realmente necesitas el resultado. Esto se llama 'lazy evaluation' y hace todo más eficiente."

### DataFrames - Datos Estructurados (1 min)
"DataFrames es como trabajar con una tabla de Excel pero distribuida en varios servidores.

Con DataFrames hicimos:
- **Lectura**: Leímos archivos CSV, JSON y TXT con una sola línea de código
- **Limpieza**: Eliminamos registros vacíos, duplicados y datos raros
- **Transformación**: Cambiamos nombres de columnas, convertimos tipos de datos, creamos columnas nuevas
- **Integración**: Unimos las diferentes tablas usando JOINs, igual que en SQL"

### Spark SQL (1 min)
"Spark SQL nos permite escribir consultas SQL normales sobre los DataFrames.

Usamos funciones avanzadas como:
- **CTEs**: Para organizar consultas complejas en pasos
- **Window Functions**: Para hacer rankings y cálculos que dependen de filas anteriores
- **Agregaciones**: Para calcular totales, promedios, máximos por grupos"

### Resultados (30 seg)
"Al final generamos KPIs como: total de títulos, rating promedio, género más popular, y país con más contenido. Todo esto se exporta en 3 formatos: CSV para reportes, JSON para APIs, y Parquet para análisis futuros."

---

# INTEGRANTE 4: MongoDB (3 min)

## Qué Decir:

### Por qué MongoDB (45 seg)
"Elegimos MongoDB porque es una base de datos NoSQL, lo que significa que no tiene tablas rígidas como MySQL.

En MongoDB guardamos documentos JSON, que son muy flexibles. Si mañana queremos agregar un campo nuevo, simplemente lo agregamos sin cambiar toda la estructura.

Además, MongoDB escala horizontalmente: si necesitamos más capacidad, agregamos más servidores en lugar de comprar uno más grande."

### Propiedades ACID (45 seg)
"Aunque MongoDB es NoSQL, cumple con las propiedades ACID que garantizan que los datos sean confiables:

- **Atomicidad**: Las operaciones se completan totalmente o no se hacen
- **Consistencia**: Los datos siempre cumplen las reglas definidas
- **Aislamiento**: Las operaciones no se interfieren entre sí
- **Durabilidad**: Una vez guardado, el dato no se pierde"

### Colecciones y Modelo (45 seg)
"Creamos 5 colecciones principales:
- **catalogo**: Todas las películas y series
- **usuarios**: Información de cada usuario
- **visualizaciones**: Historial de qué vio cada quien
- **valoraciones**: Los ratings que dan los usuarios
- **eventos_streaming**: Los eventos en tiempo real

Cada documento tiene campos como ID, título, tipo, país, fecha, y metadatos de control."

### Carga y Consultas (45 seg)
"Desde Spark cargamos los DataFrames procesados directamente a MongoDB con el conector oficial.

Luego podemos hacer consultas como:
- Contar cuántas películas vs series hay
- Ver los 5 países con más contenido
- Buscar todo el contenido de un género específico
- Ver los errores de los últimos 5 minutos

También creamos índices para que estas consultas sean rápidas."

---

# INTEGRANTE 5: Kafka, Visualizaciones, GitHub y Conclusiones (4 min)

## Qué Decir:

### Apache Kafka (1.5 min)
"Kafka es el sistema que usamos para procesar eventos en tiempo real.

Funciona así:
1. Un **Productor** genera eventos simulados: reproducciones, pausas, búsquedas, valoraciones y errores
2. Estos eventos van a **Topics** de Kafka, que son como canales de mensajes
3. Un **Consumidor** (Spark Streaming) lee estos eventos y los procesa

Nuestro productor genera 5 eventos por segundo, simulando la actividad de usuarios reales.

Spark Streaming procesa estos eventos en ventanas de tiempo. Por ejemplo, cada 30 segundos cuenta cuántos eventos hubo de cada tipo. Si detecta algo raro, como un usuario con más de 50 eventos por minuto o muchos errores seguidos, genera una alerta."

### Visualizaciones (1 min)
"Para monitorear todo esto tenemos 3 interfaces web:

**Kafka UI** en el puerto 8083: Aquí vemos los topics creados, los mensajes que pasan en tiempo real, y cuántos mensajes se han procesado.

**Spark UI** en el puerto 8080: Muestra los trabajos que están corriendo, cuánto tardan, y si hay errores.

**Mongo Express** en el puerto 8082: Nos permite ver las colecciones de MongoDB y los documentos guardados."

### GitHub y Versionamiento (45 seg)
"Todo el código está versionado con Git y subido a GitHub.

Organizamos el código en carpetas: spark-apps para los scripts de Python, datos para los archivos de entrada, scripts para los comandos de ejecución, y docs para la documentación.

Usamos una convención de commits clara: 'feat' para nuevas funciones, 'fix' para correcciones, 'docs' para documentación.

También configuramos GitHub Actions para que cada vez que subimos código, automáticamente se valide que no tenga errores."

### Conclusiones (45 seg)
"En resumen, logramos:

1. Procesar casi 9 mil títulos en menos de 30 segundos
2. Procesar eventos en tiempo real con ventanas de 30 segundos
3. Detectar anomalías automáticamente
4. Generar alertas cuando hay muchos errores
5. Almacenar todo con garantías ACID

Las tecnologías que dominamos fueron: Spark para procesamiento, Kafka para streaming, MongoDB para almacenamiento, Docker para infraestructura, y GitHub para versionamiento.

Como trabajo futuro, se podría agregar machine learning para predecir qué usuarios van a cancelar su suscripción.

¿Alguna pregunta?"

---

# TIPS PARA LA PRESENTACIÓN

1. **Hablen con sus propias palabras**, no lean el documento
2. **Miren al público**, no a la pantalla
3. **Si hacen demo**, practiquen antes para que no falle
4. **Si les preguntan algo que no saben**, digan "buena pregunta, tendríamos que investigarlo"
5. **Respiren**, 17 minutos pasan rápido

---

# ANTES DE EXPONER

- [ ] Docker encendido
- [ ] `docker-compose up -d` ejecutado
- [ ] Las 3 interfaces web abiertas (Kafka UI, Spark UI, Mongo Express)
- [ ] Terminales listas si van a hacer demo

---

*AA4 Big Data - CERTUS - Mayo 2026*
