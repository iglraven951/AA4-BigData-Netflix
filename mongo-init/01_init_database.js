// =============================================================================
// INICIALIZACION DE BASE DE DATOS MONGODB
// =============================================================================
// Este script se ejecuta automaticamente cuando el contenedor de MongoDB inicia
// Crea la base de datos, colecciones e indices para el ecosistema Netflix Analytics
// =============================================================================

// Autenticacion como admin
db = db.getSiblingDB('admin');
db.auth('admin', 'admin123');

// Crear/seleccionar la base de datos
db = db.getSiblingDB('netflix_analytics');

print('='.repeat(60));
print('INICIALIZANDO BASE DE DATOS: netflix_analytics');
print('='.repeat(60));

// =============================================================================
// CREAR COLECCIONES CON VALIDACION DE ESQUEMA
// =============================================================================

// 1. Coleccion: catalogo
print('\n[1] Creando coleccion: catalogo');
db.createCollection('catalogo', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['id', 'titulo', 'tipo', 'genero'],
            properties: {
                id: { bsonType: 'int', description: 'ID unico del contenido' },
                titulo: { bsonType: 'string', description: 'Titulo del contenido' },
                tipo: { enum: ['serie', 'pelicula'], description: 'Tipo de contenido' },
                genero: { bsonType: 'string', description: 'Genero del contenido' },
                anio: { bsonType: 'int', description: 'Año de lanzamiento' },
                duracion_min: { bsonType: 'int', description: 'Duracion en minutos' },
                calificacion: { bsonType: 'double', description: 'Calificacion promedio' },
                idioma: { bsonType: 'string', description: 'Idioma original' },
                pais: { bsonType: 'string', description: 'Pais de origen' }
            }
        }
    }
});

// 2. Coleccion: usuarios
print('[2] Creando coleccion: usuarios');
db.createCollection('usuarios', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['user_id', 'nombre', 'email', 'plan'],
            properties: {
                user_id: { bsonType: 'string', description: 'ID unico del usuario' },
                nombre: { bsonType: 'string', description: 'Nombre completo' },
                email: { bsonType: 'string', description: 'Correo electronico' },
                pais: { bsonType: 'string', description: 'Pais de residencia' },
                plan: { enum: ['basico', 'estandar', 'premium'], description: 'Plan de suscripcion' },
                fecha_registro: { bsonType: 'string', description: 'Fecha de registro' },
                edad: { bsonType: 'int', description: 'Edad del usuario' },
                dispositivo_principal: { bsonType: 'string', description: 'Dispositivo preferido' }
            }
        }
    }
});

// 3. Coleccion: visualizaciones
print('[3] Creando coleccion: visualizaciones');
db.createCollection('visualizaciones', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['view_id', 'user_id', 'content_id'],
            properties: {
                view_id: { bsonType: 'string', description: 'ID de visualizacion' },
                user_id: { bsonType: 'string', description: 'ID del usuario' },
                content_id: { bsonType: 'int', description: 'ID del contenido' },
                fecha: { bsonType: 'string', description: 'Fecha de visualizacion' },
                duracion_vista_min: { bsonType: 'int', description: 'Minutos vistos' },
                completado: { bsonType: 'bool', description: 'Si completo el contenido' },
                dispositivo: { bsonType: 'string', description: 'Dispositivo utilizado' }
            }
        }
    }
});

// 4. Coleccion: valoraciones
print('[4] Creando coleccion: valoraciones');
db.createCollection('valoraciones', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['rating_id', 'user_id', 'content_id', 'puntuacion'],
            properties: {
                rating_id: { bsonType: 'string', description: 'ID de valoracion' },
                user_id: { bsonType: 'string', description: 'ID del usuario' },
                content_id: { bsonType: 'int', description: 'ID del contenido' },
                puntuacion: { bsonType: 'int', minimum: 1, maximum: 5, description: 'Puntuacion 1-5' },
                comentario: { bsonType: 'string', description: 'Comentario del usuario' },
                fecha: { bsonType: 'string', description: 'Fecha de valoracion' }
            }
        }
    }
});

// 5. Coleccion: catalogo_stats (agregaciones)
print('[5] Creando coleccion: catalogo_stats');
db.createCollection('catalogo_stats');

// 6. Coleccion: usuarios_metricas (agregaciones)
print('[6] Creando coleccion: usuarios_metricas');
db.createCollection('usuarios_metricas');

// 7. Coleccion: engagement (agregaciones)
print('[7] Creando coleccion: engagement');
db.createCollection('engagement');

// =============================================================================
// CREAR INDICES PARA OPTIMIZAR CONSULTAS
// =============================================================================

print('\n[8] Creando indices...');

// Indices para catalogo
db.catalogo.createIndex({ id: 1 }, { unique: true });
db.catalogo.createIndex({ tipo: 1 });
db.catalogo.createIndex({ genero: 1 });
db.catalogo.createIndex({ pais: 1 });
db.catalogo.createIndex({ calificacion: -1 });
db.catalogo.createIndex({ genero: 1, tipo: 1 });

// Indices para usuarios
db.usuarios.createIndex({ user_id: 1 }, { unique: true });
db.usuarios.createIndex({ email: 1 }, { unique: true });
db.usuarios.createIndex({ pais: 1 });
db.usuarios.createIndex({ plan: 1 });
db.usuarios.createIndex({ pais: 1, plan: 1 });

// Indices para visualizaciones
db.visualizaciones.createIndex({ view_id: 1 }, { unique: true });
db.visualizaciones.createIndex({ user_id: 1 });
db.visualizaciones.createIndex({ content_id: 1 });
db.visualizaciones.createIndex({ fecha: 1 });
db.visualizaciones.createIndex({ user_id: 1, content_id: 1 });

// Indices para valoraciones
db.valoraciones.createIndex({ rating_id: 1 }, { unique: true });
db.valoraciones.createIndex({ user_id: 1 });
db.valoraciones.createIndex({ content_id: 1 });
db.valoraciones.createIndex({ puntuacion: 1 });

print('\n' + '='.repeat(60));
print('INICIALIZACION COMPLETADA');
print('='.repeat(60));

// Mostrar resumen
print('\nCOLECCIONES CREADAS:');
db.getCollectionNames().forEach(function(collection) {
    print('  - ' + collection);
});
