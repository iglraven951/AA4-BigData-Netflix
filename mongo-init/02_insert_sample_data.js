// =============================================================================
// INSERCION DE DATOS DE MUESTRA EN MONGODB
// =============================================================================
// Este script inserta los datos iniciales en las colecciones de MongoDB
// =============================================================================

db = db.getSiblingDB('admin');
db.auth('admin', 'admin123');
db = db.getSiblingDB('netflix_analytics');

print('='.repeat(60));
print('INSERTANDO DATOS DE MUESTRA');
print('='.repeat(60));

// =============================================================================
// INSERTAR CATALOGO
// =============================================================================
print('\n[1] Insertando catalogo de contenido...');

const catalogo = [
    {id: 1, titulo: "La Casa de Papel", tipo: "serie", genero: "drama", anio: 2017, duracion_min: 55, calificacion: 8.5, idioma: "espanol", pais: "Espana"},
    {id: 2, titulo: "Stranger Things", tipo: "serie", genero: "ciencia_ficcion", anio: 2016, duracion_min: 50, calificacion: 8.7, idioma: "ingles", pais: "USA"},
    {id: 3, titulo: "El Juego del Calamar", tipo: "serie", genero: "drama", anio: 2021, duracion_min: 60, calificacion: 8.0, idioma: "coreano", pais: "Corea"},
    {id: 4, titulo: "Breaking Bad", tipo: "serie", genero: "drama", anio: 2008, duracion_min: 47, calificacion: 9.5, idioma: "ingles", pais: "USA"},
    {id: 5, titulo: "Narcos", tipo: "serie", genero: "drama", anio: 2015, duracion_min: 50, calificacion: 8.8, idioma: "espanol", pais: "Colombia"},
    {id: 6, titulo: "The Crown", tipo: "serie", genero: "drama", anio: 2016, duracion_min: 58, calificacion: 8.6, idioma: "ingles", pais: "UK"},
    {id: 7, titulo: "Ozark", tipo: "serie", genero: "drama", anio: 2017, duracion_min: 60, calificacion: 8.5, idioma: "ingles", pais: "USA"},
    {id: 8, titulo: "Black Mirror", tipo: "serie", genero: "ciencia_ficcion", anio: 2011, duracion_min: 60, calificacion: 8.8, idioma: "ingles", pais: "UK"},
    {id: 9, titulo: "Wednesday", tipo: "serie", genero: "comedia", anio: 2022, duracion_min: 45, calificacion: 8.1, idioma: "ingles", pais: "USA"},
    {id: 10, titulo: "You", tipo: "serie", genero: "thriller", anio: 2018, duracion_min: 45, calificacion: 7.7, idioma: "ingles", pais: "USA"},
    {id: 11, titulo: "Emily in Paris", tipo: "serie", genero: "comedia", anio: 2020, duracion_min: 30, calificacion: 6.9, idioma: "ingles", pais: "Francia"},
    {id: 12, titulo: "Lucifer", tipo: "serie", genero: "fantasia", anio: 2016, duracion_min: 42, calificacion: 8.1, idioma: "ingles", pais: "USA"},
    {id: 13, titulo: "Peaky Blinders", tipo: "serie", genero: "drama", anio: 2013, duracion_min: 60, calificacion: 8.8, idioma: "ingles", pais: "UK"},
    {id: 14, titulo: "The Witcher", tipo: "serie", genero: "fantasia", anio: 2019, duracion_min: 60, calificacion: 8.2, idioma: "ingles", pais: "USA"},
    {id: 15, titulo: "Bridgerton", tipo: "serie", genero: "romance", anio: 2020, duracion_min: 60, calificacion: 7.3, idioma: "ingles", pais: "UK"},
    {id: 16, titulo: "Dark", tipo: "serie", genero: "ciencia_ficcion", anio: 2017, duracion_min: 60, calificacion: 8.7, idioma: "aleman", pais: "Alemania"},
    {id: 17, titulo: "Elite", tipo: "serie", genero: "drama", anio: 2018, duracion_min: 50, calificacion: 7.5, idioma: "espanol", pais: "Espana"},
    {id: 18, titulo: "Lupin", tipo: "serie", genero: "drama", anio: 2021, duracion_min: 45, calificacion: 7.5, idioma: "frances", pais: "Francia"},
    {id: 19, titulo: "Arcane", tipo: "serie", genero: "animacion", anio: 2021, duracion_min: 40, calificacion: 9.0, idioma: "ingles", pais: "USA"},
    {id: 20, titulo: "The Queen Gambit", tipo: "serie", genero: "drama", anio: 2020, duracion_min: 60, calificacion: 8.6, idioma: "ingles", pais: "USA"},
    {id: 21, titulo: "Titanic", tipo: "pelicula", genero: "romance", anio: 1997, duracion_min: 195, calificacion: 7.9, idioma: "ingles", pais: "USA"},
    {id: 22, titulo: "El Padrino", tipo: "pelicula", genero: "drama", anio: 1972, duracion_min: 175, calificacion: 9.2, idioma: "ingles", pais: "USA"},
    {id: 23, titulo: "Inception", tipo: "pelicula", genero: "ciencia_ficcion", anio: 2010, duracion_min: 148, calificacion: 8.8, idioma: "ingles", pais: "USA"},
    {id: 24, titulo: "Pulp Fiction", tipo: "pelicula", genero: "drama", anio: 1994, duracion_min: 154, calificacion: 8.9, idioma: "ingles", pais: "USA"},
    {id: 25, titulo: "The Matrix", tipo: "pelicula", genero: "ciencia_ficcion", anio: 1999, duracion_min: 136, calificacion: 8.7, idioma: "ingles", pais: "USA"},
    {id: 26, titulo: "Forrest Gump", tipo: "pelicula", genero: "drama", anio: 1994, duracion_min: 142, calificacion: 8.8, idioma: "ingles", pais: "USA"},
    {id: 27, titulo: "El Senor de los Anillos", tipo: "pelicula", genero: "fantasia", anio: 2001, duracion_min: 178, calificacion: 8.9, idioma: "ingles", pais: "Nueva_Zelanda"},
    {id: 28, titulo: "Interstellar", tipo: "pelicula", genero: "ciencia_ficcion", anio: 2014, duracion_min: 169, calificacion: 8.6, idioma: "ingles", pais: "USA"},
    {id: 29, titulo: "Gladiator", tipo: "pelicula", genero: "accion", anio: 2000, duracion_min: 155, calificacion: 8.5, idioma: "ingles", pais: "USA"},
    {id: 30, titulo: "The Dark Knight", tipo: "pelicula", genero: "accion", anio: 2008, duracion_min: 152, calificacion: 9.0, idioma: "ingles", pais: "USA"},
    {id: 31, titulo: "Avatar", tipo: "pelicula", genero: "ciencia_ficcion", anio: 2009, duracion_min: 162, calificacion: 7.9, idioma: "ingles", pais: "USA"},
    {id: 32, titulo: "Jurassic Park", tipo: "pelicula", genero: "aventura", anio: 1993, duracion_min: 127, calificacion: 8.2, idioma: "ingles", pais: "USA"},
    {id: 33, titulo: "Toy Story", tipo: "pelicula", genero: "animacion", anio: 1995, duracion_min: 81, calificacion: 8.3, idioma: "ingles", pais: "USA"},
    {id: 34, titulo: "Coco", tipo: "pelicula", genero: "animacion", anio: 2017, duracion_min: 105, calificacion: 8.4, idioma: "espanol", pais: "Mexico"},
    {id: 35, titulo: "Roma", tipo: "pelicula", genero: "drama", anio: 2018, duracion_min: 135, calificacion: 7.7, idioma: "espanol", pais: "Mexico"},
    {id: 36, titulo: "Y tu mama tambien", tipo: "pelicula", genero: "drama", anio: 2001, duracion_min: 106, calificacion: 7.6, idioma: "espanol", pais: "Mexico"},
    {id: 37, titulo: "Amores Perros", tipo: "pelicula", genero: "drama", anio: 2000, duracion_min: 154, calificacion: 8.1, idioma: "espanol", pais: "Mexico"},
    {id: 38, titulo: "El Laberinto del Fauno", tipo: "pelicula", genero: "fantasia", anio: 2006, duracion_min: 118, calificacion: 8.2, idioma: "espanol", pais: "Espana"},
    {id: 39, titulo: "Parasite", tipo: "pelicula", genero: "thriller", anio: 2019, duracion_min: 132, calificacion: 8.5, idioma: "coreano", pais: "Corea"},
    {id: 40, titulo: "Oldboy", tipo: "pelicula", genero: "thriller", anio: 2003, duracion_min: 120, calificacion: 8.4, idioma: "coreano", pais: "Corea"},
    {id: 41, titulo: "Your Name", tipo: "pelicula", genero: "animacion", anio: 2016, duracion_min: 106, calificacion: 8.4, idioma: "japones", pais: "Japon"},
    {id: 42, titulo: "Spirited Away", tipo: "pelicula", genero: "animacion", anio: 2001, duracion_min: 125, calificacion: 8.6, idioma: "japones", pais: "Japon"},
    {id: 43, titulo: "Amelie", tipo: "pelicula", genero: "comedia", anio: 2001, duracion_min: 122, calificacion: 8.3, idioma: "frances", pais: "Francia"},
    {id: 44, titulo: "La Vida es Bella", tipo: "pelicula", genero: "drama", anio: 1997, duracion_min: 116, calificacion: 8.6, idioma: "italiano", pais: "Italia"},
    {id: 45, titulo: "Cinema Paradiso", tipo: "pelicula", genero: "drama", anio: 1988, duracion_min: 155, calificacion: 8.5, idioma: "italiano", pais: "Italia"},
    {id: 46, titulo: "Cidade de Deus", tipo: "pelicula", genero: "drama", anio: 2002, duracion_min: 130, calificacion: 8.6, idioma: "portugues", pais: "Brasil"},
    {id: 47, titulo: "El Secreto de sus Ojos", tipo: "pelicula", genero: "thriller", anio: 2009, duracion_min: 129, calificacion: 8.2, idioma: "espanol", pais: "Argentina"},
    {id: 48, titulo: "Relatos Salvajes", tipo: "pelicula", genero: "comedia", anio: 2014, duracion_min: 122, calificacion: 8.1, idioma: "espanol", pais: "Argentina"},
    {id: 49, titulo: "Sin Novedad en el Frente", tipo: "pelicula", genero: "drama", anio: 2022, duracion_min: 148, calificacion: 7.8, idioma: "aleman", pais: "Alemania"},
    {id: 50, titulo: "Dont Look Up", tipo: "pelicula", genero: "comedia", anio: 2021, duracion_min: 138, calificacion: 7.2, idioma: "ingles", pais: "USA"}
];

db.catalogo.insertMany(catalogo);
print('    Insertados ' + catalogo.length + ' documentos en catalogo');

// =============================================================================
// INSERTAR USUARIOS
// =============================================================================
print('\n[2] Insertando usuarios...');

const usuarios = [
    {user_id: "U001", nombre: "Carlos Martinez", email: "carlos.martinez@email.com", pais: "Mexico", plan: "premium", fecha_registro: "2022-01-15", edad: 28, dispositivo_principal: "smart_tv"},
    {user_id: "U002", nombre: "Maria Garcia", email: "maria.garcia@email.com", pais: "Espana", plan: "estandar", fecha_registro: "2021-06-20", edad: 34, dispositivo_principal: "laptop"},
    {user_id: "U003", nombre: "Juan Rodriguez", email: "juan.rodriguez@email.com", pais: "Argentina", plan: "premium", fecha_registro: "2020-03-10", edad: 42, dispositivo_principal: "smart_tv"},
    {user_id: "U004", nombre: "Ana Lopez", email: "ana.lopez@email.com", pais: "Colombia", plan: "basico", fecha_registro: "2023-02-28", edad: 25, dispositivo_principal: "celular"},
    {user_id: "U005", nombre: "Pedro Sanchez", email: "pedro.sanchez@email.com", pais: "Peru", plan: "estandar", fecha_registro: "2022-08-05", edad: 31, dispositivo_principal: "tablet"},
    {user_id: "U006", nombre: "Laura Fernandez", email: "laura.fernandez@email.com", pais: "Chile", plan: "premium", fecha_registro: "2021-11-12", edad: 29, dispositivo_principal: "smart_tv"},
    {user_id: "U007", nombre: "Diego Torres", email: "diego.torres@email.com", pais: "Mexico", plan: "estandar", fecha_registro: "2023-01-20", edad: 22, dispositivo_principal: "celular"},
    {user_id: "U008", nombre: "Sofia Ruiz", email: "sofia.ruiz@email.com", pais: "Espana", plan: "premium", fecha_registro: "2020-07-18", edad: 38, dispositivo_principal: "laptop"},
    {user_id: "U009", nombre: "Miguel Herrera", email: "miguel.herrera@email.com", pais: "Venezuela", plan: "basico", fecha_registro: "2022-04-30", edad: 27, dispositivo_principal: "celular"},
    {user_id: "U010", nombre: "Carmen Diaz", email: "carmen.diaz@email.com", pais: "Ecuador", plan: "estandar", fecha_registro: "2021-09-25", edad: 33, dispositivo_principal: "tablet"},
    {user_id: "U011", nombre: "Roberto Morales", email: "roberto.morales@email.com", pais: "Mexico", plan: "premium", fecha_registro: "2019-12-01", edad: 45, dispositivo_principal: "smart_tv"},
    {user_id: "U012", nombre: "Patricia Jimenez", email: "patricia.jimenez@email.com", pais: "Colombia", plan: "estandar", fecha_registro: "2022-06-14", edad: 30, dispositivo_principal: "laptop"},
    {user_id: "U013", nombre: "Andres Vargas", email: "andres.vargas@email.com", pais: "Peru", plan: "basico", fecha_registro: "2023-03-08", edad: 24, dispositivo_principal: "celular"},
    {user_id: "U014", nombre: "Elena Castro", email: "elena.castro@email.com", pais: "Chile", plan: "premium", fecha_registro: "2020-10-22", edad: 36, dispositivo_principal: "smart_tv"},
    {user_id: "U015", nombre: "Fernando Gomez", email: "fernando.gomez@email.com", pais: "Argentina", plan: "estandar", fecha_registro: "2021-04-17", edad: 40, dispositivo_principal: "tablet"},
    {user_id: "U016", nombre: "Lucia Ortiz", email: "lucia.ortiz@email.com", pais: "Mexico", plan: "premium", fecha_registro: "2022-02-09", edad: 26, dispositivo_principal: "smart_tv"},
    {user_id: "U017", nombre: "Alejandro Ramirez", email: "alejandro.ramirez@email.com", pais: "Espana", plan: "basico", fecha_registro: "2023-05-11", edad: 21, dispositivo_principal: "celular"},
    {user_id: "U018", nombre: "Isabel Mendoza", email: "isabel.mendoza@email.com", pais: "Colombia", plan: "estandar", fecha_registro: "2021-08-03", edad: 35, dispositivo_principal: "laptop"},
    {user_id: "U019", nombre: "Ricardo Flores", email: "ricardo.flores@email.com", pais: "Peru", plan: "premium", fecha_registro: "2020-05-29", edad: 43, dispositivo_principal: "smart_tv"},
    {user_id: "U020", nombre: "Daniela Perez", email: "daniela.perez@email.com", pais: "Chile", plan: "estandar", fecha_registro: "2022-11-16", edad: 28, dispositivo_principal: "tablet"},
    {user_id: "U021", nombre: "Oscar Navarro", email: "oscar.navarro@email.com", pais: "Mexico", plan: "basico", fecha_registro: "2023-04-02", edad: 23, dispositivo_principal: "celular"},
    {user_id: "U022", nombre: "Monica Reyes", email: "monica.reyes@email.com", pais: "Ecuador", plan: "premium", fecha_registro: "2021-01-28", edad: 37, dispositivo_principal: "smart_tv"},
    {user_id: "U023", nombre: "Gabriel Aguilar", email: "gabriel.aguilar@email.com", pais: "Argentina", plan: "estandar", fecha_registro: "2022-07-21", edad: 32, dispositivo_principal: "laptop"},
    {user_id: "U024", nombre: "Valentina Silva", email: "valentina.silva@email.com", pais: "Venezuela", plan: "basico", fecha_registro: "2023-06-05", edad: 20, dispositivo_principal: "celular"},
    {user_id: "U025", nombre: "Enrique Rojas", email: "enrique.rojas@email.com", pais: "Colombia", plan: "premium", fecha_registro: "2020-09-14", edad: 41, dispositivo_principal: "smart_tv"},
    {user_id: "U026", nombre: "Natalia Cruz", email: "natalia.cruz@email.com", pais: "Mexico", plan: "estandar", fecha_registro: "2022-03-27", edad: 29, dispositivo_principal: "tablet"},
    {user_id: "U027", nombre: "Pablo Medina", email: "pablo.medina@email.com", pais: "Peru", plan: "premium", fecha_registro: "2021-12-08", edad: 34, dispositivo_principal: "smart_tv"},
    {user_id: "U028", nombre: "Andrea Guerrero", email: "andrea.guerrero@email.com", pais: "Chile", plan: "basico", fecha_registro: "2023-01-14", edad: 26, dispositivo_principal: "celular"},
    {user_id: "U029", nombre: "Martin Espinoza", email: "martin.espinoza@email.com", pais: "Espana", plan: "estandar", fecha_registro: "2022-05-19", edad: 39, dispositivo_principal: "laptop"},
    {user_id: "U030", nombre: "Camila Vega", email: "camila.vega@email.com", pais: "Argentina", plan: "premium", fecha_registro: "2020-11-07", edad: 31, dispositivo_principal: "smart_tv"}
];

db.usuarios.insertMany(usuarios);
print('    Insertados ' + usuarios.length + ' documentos en usuarios');

// =============================================================================
// INSERTAR VISUALIZACIONES
// =============================================================================
print('\n[3] Insertando visualizaciones...');

const visualizaciones = [
    {view_id: "V001", user_id: "U001", content_id: 1, fecha: "2024-01-15", duracion_vista_min: 55, completado: true, dispositivo: "smart_tv"},
    {view_id: "V002", user_id: "U001", content_id: 2, fecha: "2024-01-16", duracion_vista_min: 45, completado: false, dispositivo: "smart_tv"},
    {view_id: "V003", user_id: "U002", content_id: 3, fecha: "2024-01-15", duracion_vista_min: 60, completado: true, dispositivo: "laptop"},
    {view_id: "V004", user_id: "U003", content_id: 5, fecha: "2024-01-17", duracion_vista_min: 50, completado: true, dispositivo: "smart_tv"},
    {view_id: "V005", user_id: "U004", content_id: 10, fecha: "2024-01-18", duracion_vista_min: 30, completado: false, dispositivo: "celular"},
    {view_id: "V006", user_id: "U005", content_id: 21, fecha: "2024-01-19", duracion_vista_min: 195, completado: true, dispositivo: "tablet"},
    {view_id: "V007", user_id: "U006", content_id: 16, fecha: "2024-01-20", duracion_vista_min: 60, completado: true, dispositivo: "smart_tv"},
    {view_id: "V008", user_id: "U007", content_id: 17, fecha: "2024-01-15", duracion_vista_min: 50, completado: true, dispositivo: "celular"},
    {view_id: "V009", user_id: "U008", content_id: 23, fecha: "2024-01-21", duracion_vista_min: 148, completado: true, dispositivo: "laptop"},
    {view_id: "V010", user_id: "U009", content_id: 34, fecha: "2024-01-22", duracion_vista_min: 105, completado: true, dispositivo: "celular"},
    {view_id: "V011", user_id: "U010", content_id: 1, fecha: "2024-01-23", duracion_vista_min: 55, completado: true, dispositivo: "tablet"},
    {view_id: "V012", user_id: "U011", content_id: 4, fecha: "2024-01-24", duracion_vista_min: 47, completado: true, dispositivo: "smart_tv"},
    {view_id: "V013", user_id: "U012", content_id: 8, fecha: "2024-01-25", duracion_vista_min: 60, completado: true, dispositivo: "laptop"},
    {view_id: "V014", user_id: "U013", content_id: 19, fecha: "2024-01-26", duracion_vista_min: 40, completado: true, dispositivo: "celular"},
    {view_id: "V015", user_id: "U014", content_id: 30, fecha: "2024-01-27", duracion_vista_min: 152, completado: true, dispositivo: "smart_tv"},
    {view_id: "V016", user_id: "U015", content_id: 47, fecha: "2024-01-28", duracion_vista_min: 129, completado: true, dispositivo: "tablet"},
    {view_id: "V017", user_id: "U016", content_id: 2, fecha: "2024-01-29", duracion_vista_min: 50, completado: true, dispositivo: "smart_tv"},
    {view_id: "V018", user_id: "U017", content_id: 11, fecha: "2024-01-30", duracion_vista_min: 20, completado: false, dispositivo: "celular"},
    {view_id: "V019", user_id: "U018", content_id: 39, fecha: "2024-02-01", duracion_vista_min: 132, completado: true, dispositivo: "laptop"},
    {view_id: "V020", user_id: "U019", content_id: 42, fecha: "2024-02-02", duracion_vista_min: 125, completado: true, dispositivo: "smart_tv"},
    {view_id: "V021", user_id: "U020", content_id: 3, fecha: "2024-02-03", duracion_vista_min: 55, completado: false, dispositivo: "tablet"},
    {view_id: "V022", user_id: "U001", content_id: 13, fecha: "2024-02-04", duracion_vista_min: 60, completado: true, dispositivo: "smart_tv"},
    {view_id: "V023", user_id: "U002", content_id: 25, fecha: "2024-02-05", duracion_vista_min: 136, completado: true, dispositivo: "laptop"},
    {view_id: "V024", user_id: "U003", content_id: 28, fecha: "2024-02-06", duracion_vista_min: 169, completado: true, dispositivo: "smart_tv"},
    {view_id: "V025", user_id: "U004", content_id: 33, fecha: "2024-02-07", duracion_vista_min: 81, completado: true, dispositivo: "celular"},
    {view_id: "V026", user_id: "U005", content_id: 6, fecha: "2024-02-08", duracion_vista_min: 58, completado: true, dispositivo: "tablet"},
    {view_id: "V027", user_id: "U006", content_id: 7, fecha: "2024-02-09", duracion_vista_min: 60, completado: true, dispositivo: "smart_tv"},
    {view_id: "V028", user_id: "U007", content_id: 9, fecha: "2024-02-10", duracion_vista_min: 45, completado: true, dispositivo: "celular"},
    {view_id: "V029", user_id: "U008", content_id: 12, fecha: "2024-02-11", duracion_vista_min: 42, completado: true, dispositivo: "laptop"},
    {view_id: "V030", user_id: "U009", content_id: 14, fecha: "2024-02-12", duracion_vista_min: 60, completado: true, dispositivo: "celular"}
];

db.visualizaciones.insertMany(visualizaciones);
print('    Insertados ' + visualizaciones.length + ' documentos en visualizaciones');

// =============================================================================
// INSERTAR VALORACIONES
// =============================================================================
print('\n[4] Insertando valoraciones...');

const valoraciones = [
    {rating_id: "R001", user_id: "U001", content_id: 1, puntuacion: 5, comentario: "Excelente serie muy emocionante", fecha: "2024-01-16"},
    {rating_id: "R002", user_id: "U002", content_id: 3, puntuacion: 4, comentario: "Muy buena pero algo violenta", fecha: "2024-01-16"},
    {rating_id: "R003", user_id: "U003", content_id: 5, puntuacion: 5, comentario: "Narcos es increible la recomiendo", fecha: "2024-01-18"},
    {rating_id: "R004", user_id: "U004", content_id: 10, puntuacion: 3, comentario: "Regular no me atrapo mucho", fecha: "2024-01-19"},
    {rating_id: "R005", user_id: "U005", content_id: 21, puntuacion: 5, comentario: "Clasico del cine romantico", fecha: "2024-01-20"},
    {rating_id: "R006", user_id: "U006", content_id: 16, puntuacion: 5, comentario: "Dark es una obra maestra", fecha: "2024-01-21"},
    {rating_id: "R007", user_id: "U007", content_id: 17, puntuacion: 4, comentario: "Elite es entretenida para jovenes", fecha: "2024-01-16"},
    {rating_id: "R008", user_id: "U008", content_id: 23, puntuacion: 5, comentario: "Inception te vuela la mente", fecha: "2024-01-22"},
    {rating_id: "R009", user_id: "U009", content_id: 34, puntuacion: 5, comentario: "Coco me hizo llorar hermosa pelicula", fecha: "2024-01-23"},
    {rating_id: "R010", user_id: "U010", content_id: 1, puntuacion: 4, comentario: "La Casa de Papel muy buena pero larga", fecha: "2024-01-24"},
    {rating_id: "R011", user_id: "U011", content_id: 4, puntuacion: 5, comentario: "Breaking Bad la mejor serie", fecha: "2024-01-25"},
    {rating_id: "R012", user_id: "U012", content_id: 8, puntuacion: 4, comentario: "Black Mirror te hace pensar", fecha: "2024-01-26"},
    {rating_id: "R013", user_id: "U013", content_id: 19, puntuacion: 5, comentario: "Arcane es arte visual increible", fecha: "2024-01-27"},
    {rating_id: "R014", user_id: "U014", content_id: 30, puntuacion: 5, comentario: "The Dark Knight mejor Batman", fecha: "2024-01-28"},
    {rating_id: "R015", user_id: "U015", content_id: 47, puntuacion: 5, comentario: "El Secreto de sus Ojos excelente", fecha: "2024-01-29"},
    {rating_id: "R016", user_id: "U016", content_id: 2, puntuacion: 5, comentario: "Stranger Things nostalgia pura", fecha: "2024-01-30"},
    {rating_id: "R017", user_id: "U017", content_id: 11, puntuacion: 2, comentario: "Emily in Paris muy superficial", fecha: "2024-01-31"},
    {rating_id: "R018", user_id: "U018", content_id: 39, puntuacion: 5, comentario: "Parasite merecido Oscar", fecha: "2024-02-01"},
    {rating_id: "R019", user_id: "U019", content_id: 42, puntuacion: 5, comentario: "Spirited Away magia de Ghibli", fecha: "2024-02-02"},
    {rating_id: "R020", user_id: "U020", content_id: 3, puntuacion: 4, comentario: "El Juego del Calamar muy intensa", fecha: "2024-02-03"}
];

db.valoraciones.insertMany(valoraciones);
print('    Insertados ' + valoraciones.length + ' documentos en valoraciones');

// =============================================================================
// RESUMEN FINAL
// =============================================================================
print('\n' + '='.repeat(60));
print('INSERCION DE DATOS COMPLETADA');
print('='.repeat(60));

print('\nRESUMEN DE COLECCIONES:');
print('  - catalogo: ' + db.catalogo.countDocuments() + ' documentos');
print('  - usuarios: ' + db.usuarios.countDocuments() + ' documentos');
print('  - visualizaciones: ' + db.visualizaciones.countDocuments() + ' documentos');
print('  - valoraciones: ' + db.valoraciones.countDocuments() + ' documentos');
