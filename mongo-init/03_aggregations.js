// =============================================================================
// AGREGACIONES Y CONSULTAS AVANZADAS EN MONGODB
// =============================================================================
// Este script demuestra el uso del framework de agregacion de MongoDB
// para generar estadisticas y metricas del ecosistema Netflix Analytics
// =============================================================================

db = db.getSiblingDB('admin');
db.auth('admin', 'admin123');
db = db.getSiblingDB('netflix_analytics');

print('='.repeat(60));
print('EJECUTANDO AGREGACIONES EN MONGODB');
print('='.repeat(60));

// =============================================================================
// AGREGACION 1: Estadisticas de Catalogo por Genero y Tipo
// =============================================================================
print('\n[1] ESTADISTICAS DE CATALOGO POR GENERO Y TIPO');
print('-'.repeat(50));

const catalogoStats = db.catalogo.aggregate([
    {
        $group: {
            _id: { genero: "$genero", tipo: "$tipo" },
            cantidad: { $sum: 1 },
            calificacion_promedio: { $avg: "$calificacion" },
            duracion_promedio: { $avg: "$duracion_min" },
            calificacion_maxima: { $max: "$calificacion" },
            calificacion_minima: { $min: "$calificacion" }
        }
    },
    {
        $project: {
            _id: 0,
            genero: "$_id.genero",
            tipo: "$_id.tipo",
            cantidad: 1,
            calificacion_promedio: { $round: ["$calificacion_promedio", 2] },
            duracion_promedio: { $round: ["$duracion_promedio", 0] },
            calificacion_maxima: 1,
            calificacion_minima: 1
        }
    },
    { $sort: { cantidad: -1 } }
]).toArray();

// Guardar en coleccion
db.catalogo_stats.drop();
db.catalogo_stats.insertMany(catalogoStats);
print('    Estadisticas guardadas en catalogo_stats: ' + catalogoStats.length + ' registros');

// Mostrar resultados
catalogoStats.slice(0, 10).forEach(doc => {
    print('    ' + doc.genero + ' (' + doc.tipo + '): ' + doc.cantidad + ' titulos, Rating: ' + doc.calificacion_promedio);
});

// =============================================================================
// AGREGACION 2: Metricas de Usuarios por Pais y Plan
// =============================================================================
print('\n[2] METRICAS DE USUARIOS POR PAIS Y PLAN');
print('-'.repeat(50));

const usuariosMetricas = db.usuarios.aggregate([
    {
        $group: {
            _id: { pais: "$pais", plan: "$plan" },
            total_usuarios: { $sum: 1 },
            edad_promedio: { $avg: "$edad" },
            edad_minima: { $min: "$edad" },
            edad_maxima: { $max: "$edad" }
        }
    },
    {
        $project: {
            _id: 0,
            pais: "$_id.pais",
            plan: "$_id.plan",
            total_usuarios: 1,
            edad_promedio: { $round: ["$edad_promedio", 1] },
            edad_minima: 1,
            edad_maxima: 1
        }
    },
    { $sort: { total_usuarios: -1 } }
]).toArray();

// Guardar en coleccion
db.usuarios_metricas.drop();
db.usuarios_metricas.insertMany(usuariosMetricas);
print('    Metricas guardadas en usuarios_metricas: ' + usuariosMetricas.length + ' registros');

// Mostrar resultados
usuariosMetricas.forEach(doc => {
    print('    ' + doc.pais + ' (' + doc.plan + '): ' + doc.total_usuarios + ' usuarios, Edad prom: ' + doc.edad_promedio);
});

// =============================================================================
// AGREGACION 3: Engagement por Contenido (con Lookup)
// =============================================================================
print('\n[3] ENGAGEMENT POR CONTENIDO');
print('-'.repeat(50));

const engagement = db.visualizaciones.aggregate([
    {
        $group: {
            _id: "$content_id",
            total_vistas: { $sum: 1 },
            completadas: { $sum: { $cond: ["$completado", 1, 0] } },
            duracion_total: { $sum: "$duracion_vista_min" },
            dispositivos: { $addToSet: "$dispositivo" }
        }
    },
    {
        $lookup: {
            from: "catalogo",
            localField: "_id",
            foreignField: "id",
            as: "contenido"
        }
    },
    { $unwind: "$contenido" },
    {
        $lookup: {
            from: "valoraciones",
            localField: "_id",
            foreignField: "content_id",
            as: "ratings"
        }
    },
    {
        $project: {
            _id: 0,
            content_id: "$_id",
            titulo: "$contenido.titulo",
            tipo: "$contenido.tipo",
            genero: "$contenido.genero",
            total_vistas: 1,
            completadas: 1,
            tasa_completado: {
                $round: [{ $multiply: [{ $divide: ["$completadas", "$total_vistas"] }, 100] }, 2]
            },
            duracion_promedio: { $round: [{ $divide: ["$duracion_total", "$total_vistas"] }, 1] },
            num_valoraciones: { $size: "$ratings" },
            rating_promedio: {
                $round: [{ $avg: "$ratings.puntuacion" }, 2]
            },
            dispositivos: 1
        }
    },
    { $sort: { total_vistas: -1 } }
]).toArray();

// Guardar en coleccion
db.engagement.drop();
db.engagement.insertMany(engagement);
print('    Engagement guardado: ' + engagement.length + ' registros');

// Mostrar Top 10
print('\n    TOP 10 CONTENIDO MAS VISTO:');
engagement.slice(0, 10).forEach((doc, i) => {
    print('    ' + (i+1) + '. ' + doc.titulo + ': ' + doc.total_vistas + ' vistas, ' + doc.tasa_completado + '% completado, Rating: ' + (doc.rating_promedio || 'N/A'));
});

// =============================================================================
// AGREGACION 4: Preferencias por Pais
// =============================================================================
print('\n[4] PREFERENCIAS POR PAIS');
print('-'.repeat(50));

const preferencias = db.usuarios.aggregate([
    {
        $lookup: {
            from: "visualizaciones",
            localField: "user_id",
            foreignField: "user_id",
            as: "vistas"
        }
    },
    { $unwind: "$vistas" },
    {
        $lookup: {
            from: "catalogo",
            localField: "vistas.content_id",
            foreignField: "id",
            as: "contenido"
        }
    },
    { $unwind: "$contenido" },
    {
        $group: {
            _id: { pais: "$pais", genero: "$contenido.genero" },
            total_vistas: { $sum: 1 },
            usuarios_unicos: { $addToSet: "$user_id" }
        }
    },
    {
        $project: {
            _id: 0,
            pais: "$_id.pais",
            genero: "$_id.genero",
            total_vistas: 1,
            usuarios_unicos: { $size: "$usuarios_unicos" }
        }
    },
    { $sort: { pais: 1, total_vistas: -1 } }
]).toArray();

print('    Preferencias por pais calculadas: ' + preferencias.length + ' combinaciones');

// Mostrar genero favorito por pais
const paisesProcesados = new Set();
preferencias.forEach(doc => {
    if (!paisesProcesados.has(doc.pais)) {
        print('    ' + doc.pais + ' -> Genero favorito: ' + doc.genero + ' (' + doc.total_vistas + ' vistas)');
        paisesProcesados.add(doc.pais);
    }
});

// =============================================================================
// AGREGACION 5: Reporte de KPIs
// =============================================================================
print('\n[5] REPORTE DE KPIs DEL SISTEMA');
print('-'.repeat(50));

// Total usuarios
const totalUsuarios = db.usuarios.countDocuments();
print('    Total Usuarios: ' + totalUsuarios);

// Usuarios por plan
const usuariosPorPlan = db.usuarios.aggregate([
    { $group: { _id: "$plan", count: { $sum: 1 } } }
]).toArray();
usuariosPorPlan.forEach(p => print('      - ' + p._id + ': ' + p.count));

// Total contenido
const totalContenido = db.catalogo.countDocuments();
print('\n    Total Contenido: ' + totalContenido);

// Contenido por tipo
const contenidoPorTipo = db.catalogo.aggregate([
    { $group: { _id: "$tipo", count: { $sum: 1 } } }
]).toArray();
contenidoPorTipo.forEach(c => print('      - ' + c._id + ': ' + c.count));

// Total visualizaciones
const totalVisualizaciones = db.visualizaciones.countDocuments();
print('\n    Total Visualizaciones: ' + totalVisualizaciones);

// Tasa de completado general
const completados = db.visualizaciones.countDocuments({ completado: true });
const tasaCompletado = ((completados / totalVisualizaciones) * 100).toFixed(2);
print('    Tasa de Completado: ' + tasaCompletado + '%');

// Total valoraciones
const totalValoraciones = db.valoraciones.countDocuments();
print('\n    Total Valoraciones: ' + totalValoraciones);

// Rating promedio general
const ratingPromedio = db.valoraciones.aggregate([
    { $group: { _id: null, avg: { $avg: "$puntuacion" } } }
]).toArray();
print('    Rating Promedio: ' + (ratingPromedio[0]?.avg?.toFixed(2) || 'N/A'));

// =============================================================================
// RESUMEN FINAL
// =============================================================================
print('\n' + '='.repeat(60));
print('AGREGACIONES COMPLETADAS');
print('='.repeat(60));

print('\nCOLECCIONES DE AGREGACION GENERADAS:');
print('  - catalogo_stats: ' + db.catalogo_stats.countDocuments() + ' documentos');
print('  - usuarios_metricas: ' + db.usuarios_metricas.countDocuments() + ' documentos');
print('  - engagement: ' + db.engagement.countDocuments() + ' documentos');
