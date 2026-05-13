"""
=============================================================================
SCRIPT 0: GENERADOR DE DATOS MASIVOS
=============================================================================
Genera datasets con minimo 10,000 registros para cumplir requisitos AA4.

Requisitos:
- 5 archivos historicos minimo
- 3 formatos diferentes (CSV, JSON, TXT)
- Archivo principal con minimo 10,000 registros
- Relacion entre archivos mediante IDs

Proyecto: Netflix Analytics Platform - AA4 Big Data
=============================================================================
"""

import csv
import json
import random
from datetime import datetime, timedelta
import os

# Configuracion
OUTPUT_DIR = "/datos"
SEED = 42
random.seed(SEED)

# =============================================================================
# DATOS BASE
# =============================================================================
GENEROS = ["Drama", "Comedy", "Action", "Horror", "Sci-Fi", "Documentary",
           "Thriller", "Romance", "Animation", "Crime", "Fantasy", "Mystery"]
PAISES = ["Mexico", "USA", "Spain", "Argentina", "Colombia", "Peru", "Chile",
          "Brazil", "UK", "Germany", "France", "Italy", "Japan", "Korea"]
PLANES = ["Basico", "Estandar", "Premium"]
DISPOSITIVOS = ["Smart TV", "Mobile", "Tablet", "Laptop", "Desktop", "Gaming Console"]
ACCIONES = ["LOGIN", "PLAY", "PAUSE", "STOP", "SEARCH", "RATE", "LOGOUT", "BROWSE", "DOWNLOAD", "SHARE"]
CALIDADES = ["SD", "HD", "Full HD", "4K", "HDR"]

# Titulos de contenido simulados
TITULOS_PELICULAS = [
    "The Last Kingdom", "Dark Paradise", "Echoes of Tomorrow", "Silent Waters",
    "Breaking Dawn", "The Hidden Truth", "Midnight Sun", "Lost in Time",
    "The Final Chapter", "Beyond the Horizon", "City of Dreams", "The Forgotten",
    "Shadow Games", "Wild Hearts", "The Return", "Eternal Flame", "Northern Lights",
    "Deep Blue", "Rising Storm", "Golden Age", "The Outsider", "Dark Waters",
    "Broken Promises", "The Last Stand", "Secret Garden", "Night Watch"
]

TITULOS_SERIES = [
    "Crime Stories", "The Dynasty", "Modern Life", "Dark Secrets", "The Office Club",
    "Medical Center", "Legal Eagles", "Tech World", "Family Matters", "Street Life",
    "Historical Tales", "Sci-Fi Adventures", "Comedy Hour", "Drama Queens",
    "Action Heroes", "Mystery Files", "Romance Stories", "Documentary Series"
]

# =============================================================================
# FUNCIONES GENERADORAS
# =============================================================================

def generate_content_id(index):
    """Generar ID de contenido"""
    return f"CNT{index:05d}"

def generate_user_id(index):
    """Generar ID de usuario"""
    return f"USR{index:05d}"

def random_date(start_year=2020, end_year=2024):
    """Generar fecha aleatoria"""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)

def random_datetime_str(start_year=2023, end_year=2024):
    """Generar datetime aleatorio como string"""
    dt = random_date(start_year, end_year)
    dt = dt.replace(
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59)
    )
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# =============================================================================
# GENERADOR 1: CATALOGO (CSV) - 500 registros
# =============================================================================
def generate_catalogo(filename, count=500):
    """Generar catalogo de contenido"""
    print(f"[1/5] Generando {filename} ({count} registros)...")

    data = []
    for i in range(1, count + 1):
        tipo = random.choice(["Pelicula", "Serie"])
        titulo = random.choice(TITULOS_PELICULAS if tipo == "Pelicula" else TITULOS_SERIES)
        titulo = f"{titulo} {i}" if random.random() > 0.3 else titulo

        record = {
            "content_id": generate_content_id(i),
            "titulo": titulo,
            "tipo": tipo,
            "genero": random.choice(GENEROS),
            "año": random.randint(2015, 2024),
            "duracion_min": random.randint(90, 180) if tipo == "Pelicula" else random.randint(30, 60),
            "rating_promedio": round(random.uniform(3.0, 5.0), 2),
            "temporadas": random.randint(1, 8) if tipo == "Serie" else 0,
            "pais_origen": random.choice(PAISES),
            "idioma": random.choice(["Español", "Ingles", "Frances", "Aleman", "Japones"]),
            "clasificacion": random.choice(["G", "PG", "PG-13", "R", "NC-17"]),
            "fecha_agregado": random_date().strftime("%Y-%m-%d")
        }
        data.append(record)

    # Guardar CSV
    with open(f"{OUTPUT_DIR}/{filename}", 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    print(f"    [OK] {count} registros guardados")
    return data

# =============================================================================
# GENERADOR 2: USUARIOS (CSV) - 1,000 registros
# =============================================================================
def generate_usuarios(filename, count=1000):
    """Generar usuarios"""
    print(f"[2/5] Generando {filename} ({count} registros)...")

    nombres = ["Ana", "Carlos", "Maria", "Juan", "Laura", "Pedro", "Sofia", "Diego",
               "Lucia", "Miguel", "Elena", "David", "Carmen", "Jose", "Isabel"]
    apellidos = ["Garcia", "Rodriguez", "Martinez", "Lopez", "Gonzalez", "Hernandez",
                 "Perez", "Sanchez", "Ramirez", "Torres", "Flores", "Rivera"]

    data = []
    for i in range(1, count + 1):
        nombre = f"{random.choice(nombres)} {random.choice(apellidos)}"
        edad = random.randint(18, 70)

        record = {
            "user_id": generate_user_id(i),
            "nombre": nombre,
            "email": f"user{i}@email.com",
            "edad": edad,
            "pais": random.choice(PAISES),
            "plan": random.choice(PLANES),
            "fecha_registro": random_date(2018, 2024).strftime("%Y-%m-%d"),
            "dispositivo_preferido": random.choice(DISPOSITIVOS),
            "idioma_preferido": random.choice(["Español", "Ingles"]),
            "perfiles": random.randint(1, 5),
            "activo": random.choice([True, True, True, False])  # 75% activos
        }
        data.append(record)

    # Guardar CSV
    with open(f"{OUTPUT_DIR}/{filename}", 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    print(f"    [OK] {count} registros guardados")
    return data

# =============================================================================
# GENERADOR 3: VISUALIZACIONES (JSON) - 15,000 registros (PRINCIPAL)
# =============================================================================
def generate_visualizaciones(filename, count=15000, users=None, catalogo=None):
    """Generar visualizaciones - ARCHIVO PRINCIPAL con 15,000+ registros"""
    print(f"[3/5] Generando {filename} ({count} registros) - ARCHIVO PRINCIPAL...")

    user_ids = [u["user_id"] for u in users] if users else [generate_user_id(i) for i in range(1, 1001)]
    content_ids = [c["content_id"] for c in catalogo] if catalogo else [generate_content_id(i) for i in range(1, 501)]

    data = []
    for i in range(1, count + 1):
        duracion_total = random.randint(30, 180)
        minutos_vistos = random.randint(5, duracion_total)

        record = {
            "view_id": f"VW{i:06d}",
            "user_id": random.choice(user_ids),
            "content_id": random.choice(content_ids),
            "fecha_hora": random_datetime_str(),
            "dispositivo": random.choice(DISPOSITIVOS),
            "pais": random.choice(PAISES),
            "duracion_total_min": duracion_total,
            "minutos_vistos": minutos_vistos,
            "porcentaje_visto": round((minutos_vistos / duracion_total) * 100, 1),
            "completado": minutos_vistos >= duracion_total * 0.9,
            "calidad": random.choice(CALIDADES),
            "velocidad_conexion_mbps": round(random.uniform(5, 100), 1),
            "pausas": random.randint(0, 10),
            "rebobinados": random.randint(0, 5),
            "subtitulos": random.choice([True, False]),
            "idioma_audio": random.choice(["Original", "Español", "Ingles"]),
            "sesion_id": f"SES{random.randint(100000, 999999)}"
        }
        data.append(record)

        if i % 5000 == 0:
            print(f"    ... {i}/{count} registros generados")

    # Guardar JSON
    with open(f"{OUTPUT_DIR}/{filename}", 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"    [OK] {count} registros guardados")
    return data

# =============================================================================
# GENERADOR 4: VALORACIONES (JSON) - 5,000 registros
# =============================================================================
def generate_valoraciones(filename, count=5000, users=None, catalogo=None):
    """Generar valoraciones"""
    print(f"[4/5] Generando {filename} ({count} registros)...")

    user_ids = [u["user_id"] for u in users] if users else [generate_user_id(i) for i in range(1, 1001)]
    content_ids = [c["content_id"] for c in catalogo] if catalogo else [generate_content_id(i) for i in range(1, 501)]

    comentarios = [
        "Excelente pelicula!", "Me encanto la serie", "Muy aburrida",
        "Recomendada", "No me gusto el final", "Increible actuacion",
        "Historia muy predecible", "Efectos especiales increibles",
        "Gran banda sonora", "Demasiado larga", "Perfecta para el fin de semana",
        "La mejor que he visto", "Esperaba mas", "Muy emotiva",
        None, None, None  # Algunos sin comentario
    ]

    data = []
    for i in range(1, count + 1):
        record = {
            "rating_id": f"RT{i:06d}",
            "user_id": random.choice(user_ids),
            "content_id": random.choice(content_ids),
            "fecha": random_date(2023, 2024).strftime("%Y-%m-%d"),
            "puntuacion": random.randint(1, 5),
            "tipo_reaccion": random.choice(["thumbs_up", "thumbs_down", "love", "wow", "sad"]),
            "comentario": random.choice(comentarios),
            "util_count": random.randint(0, 50),
            "reportado": random.random() < 0.02  # 2% reportados
        }
        data.append(record)

    # Guardar JSON
    with open(f"{OUTPUT_DIR}/{filename}", 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"    [OK] {count} registros guardados")
    return data

# =============================================================================
# GENERADOR 5: LOGS DE ACTIVIDAD (TXT) - 12,000 registros
# =============================================================================
def generate_logs(filename, count=12000, users=None):
    """Generar logs de actividad"""
    print(f"[5/5] Generando {filename} ({count} registros)...")

    user_ids = [u["user_id"] for u in users] if users else [generate_user_id(i) for i in range(1, 1001)]

    logs = []
    for i in range(count):
        timestamp = random_datetime_str()
        user = random.choice(user_ids)
        action = random.choice(ACCIONES)
        device = random.choice(DISPOSITIVOS)
        country = random.choice(PAISES)

        # Generar detalles segun la accion
        if action == "PLAY":
            details = f"content={generate_content_id(random.randint(1,500))},quality={random.choice(CALIDADES)}"
        elif action == "SEARCH":
            terms = ["action movies", "comedy series", "new releases", "top rated", "spanish films"]
            details = f"query='{random.choice(terms)}',results={random.randint(0,150)}"
        elif action == "RATE":
            details = f"content={generate_content_id(random.randint(1,500))},rating={random.randint(1,5)}"
        elif action == "LOGIN":
            details = f"ip=192.168.{random.randint(1,255)}.{random.randint(1,255)},success=True"
        elif action == "LOGOUT":
            details = f"session_duration={random.randint(5,180)}min"
        else:
            details = f"status=OK"

        # Ocasionalmente agregar errores
        if random.random() < 0.03:  # 3% errores
            action = "ERROR"
            error_types = ["PLAYBACK_FAILED", "NETWORK_TIMEOUT", "AUTH_EXPIRED", "CONTENT_UNAVAILABLE"]
            details = f"type={random.choice(error_types)},severity={random.choice(['LOW','MEDIUM','HIGH','CRITICAL'])}"

        log_line = f"{timestamp}|{user}|{action}|{device}|{country}|{details}"
        logs.append(log_line)

        if (i + 1) % 4000 == 0:
            print(f"    ... {i+1}/{count} registros generados")

    # Guardar TXT
    with open(f"{OUTPUT_DIR}/{filename}", 'w', encoding='utf-8') as f:
        f.write("timestamp|user_id|action|device|country|details\n")
        f.write("\n".join(logs))

    print(f"    [OK] {count} registros guardados")
    return logs

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("""
=============================================================================
        GENERADOR DE DATOS MASIVOS - NETFLIX ANALYTICS AA4
=============================================================================

Generando datasets que cumplen los requisitos:
- 5 archivos historicos minimo
- 3 formatos diferentes (CSV, JSON, TXT)
- Archivo principal con minimo 10,000 registros
- Relacion entre archivos mediante IDs

=============================================================================
    """)

    # Verificar directorio
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"[INIT] Directorio creado: {OUTPUT_DIR}")

    # Generar datos
    catalogo = generate_catalogo("catalogo_completo.csv", 500)
    usuarios = generate_usuarios("usuarios_completo.csv", 1000)
    visualizaciones = generate_visualizaciones("visualizaciones_historico.json", 15000, usuarios, catalogo)
    valoraciones = generate_valoraciones("valoraciones_historico.json", 5000, usuarios, catalogo)
    logs = generate_logs("logs_actividad_completo.txt", 12000, usuarios)

    # Resumen
    print("""
=============================================================================
                        RESUMEN DE GENERACION
=============================================================================

ARCHIVOS GENERADOS:
┌─────────────────────────────────────┬──────────┬─────────────────┐
│ Archivo                             │ Formato  │ Registros       │
├─────────────────────────────────────┼──────────┼─────────────────┤
│ catalogo_completo.csv               │ CSV      │     500         │
│ usuarios_completo.csv               │ CSV      │   1,000         │
│ visualizaciones_historico.json      │ JSON     │  15,000 ★       │
│ valoraciones_historico.json         │ JSON     │   5,000         │
│ logs_actividad_completo.txt         │ TXT      │  12,000         │
└─────────────────────────────────────┴──────────┴─────────────────┘

★ ARCHIVO PRINCIPAL (cumple requisito de 10,000+ registros)

RELACIONES ENTRE ARCHIVOS:
- user_id: usuarios <-> visualizaciones <-> valoraciones <-> logs
- content_id: catalogo <-> visualizaciones <-> valoraciones

FORMATOS: 3 (CSV, JSON, TXT) ✓
ARCHIVOS: 5 ✓
REGISTROS PRINCIPALES: 15,000 ✓
RELACIONES: Por IDs ✓

=============================================================================
                    GENERACION COMPLETADA
=============================================================================
    """)

if __name__ == "__main__":
    main()
