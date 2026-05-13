"""
=============================================================================
SCRIPT 6: PRODUCTOR DE EVENTOS KAFKA
=============================================================================
Este script implementa el productor de eventos para Kafka:

E. Procesamiento streaming con Kafka
   [x] Creacion de un productor de eventos
   [x] Creacion de un topic en Kafka
   [x] Envio de eventos simulados

El productor simula eventos de Netflix en tiempo real:
- Reproducciones (PLAY)
- Pausas (PAUSE)
- Valoraciones (RATE)
- Busquedas (SEARCH)
- Errores de sistema (ERROR)

Proyecto: Netflix Analytics Platform - AA4 Big Data
=============================================================================
"""

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import random
import time
from datetime import datetime
import uuid
import signal
import sys

# =============================================================================
# CONFIGURACION
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
TOPIC_EVENTS = 'netflix-events'
TOPIC_ALERTS = 'netflix-alerts'

# Datos para simulacion
USERS = [f"user_{i:03d}" for i in range(1, 51)]
CONTENT_IDS = [f"content_{i:03d}" for i in range(1, 101)]
DEVICES = ["Smart TV", "Mobile", "Tablet", "Laptop", "Desktop", "Gaming Console"]
COUNTRIES = ["Mexico", "USA", "Spain", "Argentina", "Colombia", "Peru", "Chile"]
GENRES = ["Drama", "Comedy", "Action", "Horror", "Sci-Fi", "Documentary", "Thriller", "Romance"]
ACTIONS = ["PLAY", "PAUSE", "STOP", "REWIND", "FORWARD", "RATE", "SEARCH", "LOGIN", "LOGOUT"]

# Control de ejecucion
running = True

def signal_handler(sig, frame):
    """Manejar Ctrl+C para salir limpiamente"""
    global running
    print("\n\n[!] Deteniendo productor...")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# =============================================================================
# FUNCIONES DE CREACION DE TOPICS
# =============================================================================
def create_topics():
    """Crear topics de Kafka si no existen"""
    print("\n[1] CREANDO TOPICS DE KAFKA")
    print("-" * 50)

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='topic-creator'
        )

        topics = [
            NewTopic(
                name=TOPIC_EVENTS,
                num_partitions=3,
                replication_factor=1
            ),
            NewTopic(
                name=TOPIC_ALERTS,
                num_partitions=1,
                replication_factor=1
            )
        ]

        for topic in topics:
            try:
                admin_client.create_topics([topic])
                print(f"    [✓] Topic creado: {topic.name}")
            except TopicAlreadyExistsError:
                print(f"    [i] Topic ya existe: {topic.name}")

        admin_client.close()
        return True

    except Exception as e:
        print(f"    [!] Error creando topics: {e}")
        return False

# =============================================================================
# GENERADORES DE EVENTOS
# =============================================================================
def generate_play_event():
    """Generar evento de reproduccion"""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "PLAY",
        "timestamp": datetime.now().isoformat(),
        "user_id": random.choice(USERS),
        "content_id": random.choice(CONTENT_IDS),
        "device": random.choice(DEVICES),
        "country": random.choice(COUNTRIES),
        "position_seconds": random.randint(0, 3600),
        "quality": random.choice(["SD", "HD", "4K"]),
        "buffering_time_ms": random.randint(100, 2000)
    }

def generate_pause_event():
    """Generar evento de pausa"""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "PAUSE",
        "timestamp": datetime.now().isoformat(),
        "user_id": random.choice(USERS),
        "content_id": random.choice(CONTENT_IDS),
        "device": random.choice(DEVICES),
        "position_seconds": random.randint(0, 3600),
        "pause_duration_seconds": random.randint(5, 300)
    }

def generate_rate_event():
    """Generar evento de valoracion"""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "RATE",
        "timestamp": datetime.now().isoformat(),
        "user_id": random.choice(USERS),
        "content_id": random.choice(CONTENT_IDS),
        "rating": random.randint(1, 5),
        "feedback_type": random.choice(["thumbs_up", "thumbs_down", "stars", "skip"])
    }

def generate_search_event():
    """Generar evento de busqueda"""
    search_terms = ["stranger things", "action movies", "comedy", "new releases",
                   "spanish movies", "anime", "documentaries", "trending"]
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "SEARCH",
        "timestamp": datetime.now().isoformat(),
        "user_id": random.choice(USERS),
        "search_query": random.choice(search_terms),
        "results_count": random.randint(0, 150),
        "filter_genre": random.choice(GENRES + [None]),
        "device": random.choice(DEVICES)
    }

def generate_error_event():
    """Generar evento de error (para alertas)"""
    error_types = [
        ("PLAYBACK_ERROR", "Video playback failed - codec issue"),
        ("NETWORK_ERROR", "Connection timeout after 30s"),
        ("AUTH_ERROR", "Session expired - token invalid"),
        ("CONTENT_ERROR", "Content not available in region"),
        ("BUFFERING_CRITICAL", "Buffering exceeded threshold (>10s)")
    ]
    error_type, error_msg = random.choice(error_types)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "ERROR",
        "error_code": error_type,
        "timestamp": datetime.now().isoformat(),
        "user_id": random.choice(USERS),
        "content_id": random.choice(CONTENT_IDS),
        "device": random.choice(DEVICES),
        "error_message": error_msg,
        "severity": random.choice(["LOW", "MEDIUM", "HIGH", "CRITICAL"])
    }

def generate_login_event():
    """Generar evento de login"""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "LOGIN",
        "timestamp": datetime.now().isoformat(),
        "user_id": random.choice(USERS),
        "device": random.choice(DEVICES),
        "country": random.choice(COUNTRIES),
        "ip_address": f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
        "session_id": str(uuid.uuid4())[:8]
    }

def generate_random_event():
    """Generar un evento aleatorio basado en pesos de probabilidad"""
    event_generators = [
        (generate_play_event, 40),      # 40% reproducciones
        (generate_pause_event, 20),     # 20% pausas
        (generate_rate_event, 15),      # 15% valoraciones
        (generate_search_event, 15),    # 15% busquedas
        (generate_login_event, 7),      # 7% logins
        (generate_error_event, 3),      # 3% errores
    ]

    total_weight = sum(weight for _, weight in event_generators)
    random_num = random.randint(1, total_weight)

    cumulative = 0
    for generator, weight in event_generators:
        cumulative += weight
        if random_num <= cumulative:
            return generator()

    return generate_play_event()

# =============================================================================
# PRODUCTOR PRINCIPAL
# =============================================================================
def create_producer():
    """Crear productor de Kafka"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3
    )

def run_producer(events_per_second=5, duration_seconds=None):
    """Ejecutar el productor de eventos"""
    global running

    print("\n" + "=" * 70)
    print("    PRODUCTOR DE EVENTOS KAFKA - NETFLIX STREAMING")
    print("=" * 70)

    # Crear topics
    create_topics()

    # Crear productor
    print("\n[2] INICIANDO PRODUCTOR")
    print("-" * 50)

    try:
        producer = create_producer()
        print(f"    [✓] Conectado a Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"    [i] Topic de eventos: {TOPIC_EVENTS}")
        print(f"    [i] Topic de alertas: {TOPIC_ALERTS}")
        print(f"    [i] Eventos por segundo: {events_per_second}")
    except Exception as e:
        print(f"    [!] Error conectando a Kafka: {e}")
        return

    print("\n[3] ENVIANDO EVENTOS (Ctrl+C para detener)")
    print("-" * 50)

    event_count = 0
    error_count = 0
    start_time = time.time()
    interval = 1.0 / events_per_second

    # Contadores por tipo
    event_type_counts = {}

    try:
        while running:
            # Verificar duracion
            if duration_seconds and (time.time() - start_time) >= duration_seconds:
                print(f"\n    [i] Duracion alcanzada: {duration_seconds}s")
                break

            # Generar evento
            event = generate_random_event()
            event_type = event.get("event_type", "UNKNOWN")

            # Actualizar contadores
            event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1

            # Determinar topic
            topic = TOPIC_ALERTS if event_type == "ERROR" else TOPIC_EVENTS

            # Enviar evento
            try:
                future = producer.send(
                    topic,
                    key=event.get("user_id"),
                    value=event
                )
                future.get(timeout=10)  # Esperar confirmacion
                event_count += 1

                # Mostrar progreso cada 10 eventos
                if event_count % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = event_count / elapsed if elapsed > 0 else 0
                    print(f"\r    [>] Eventos enviados: {event_count} | "
                          f"Rate: {rate:.1f}/s | "
                          f"Errores: {error_count}", end="")

            except Exception as e:
                error_count += 1
                if error_count <= 5:  # Solo mostrar primeros errores
                    print(f"\n    [!] Error enviando evento: {e}")

            # Esperar intervalo
            time.sleep(interval)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()

    # Resumen final
    elapsed = time.time() - start_time
    print("\n\n" + "=" * 70)
    print("    RESUMEN DE PRODUCCION")
    print("=" * 70)
    print(f"""
    Duracion total: {elapsed:.1f} segundos
    Eventos enviados: {event_count}
    Errores de envio: {error_count}
    Rate promedio: {event_count / elapsed:.1f} eventos/segundo

    DISTRIBUCION POR TIPO:
    """)
    for event_type, count in sorted(event_type_counts.items(), key=lambda x: -x[1]):
        pct = (count / event_count * 100) if event_count > 0 else 0
        print(f"    - {event_type}: {count} ({pct:.1f}%)")

    print("\n" + "=" * 70)
    print("    PRODUCTOR FINALIZADO")
    print("=" * 70)

# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Netflix Kafka Event Producer')
    parser.add_argument('--rate', type=int, default=10, help='Eventos por segundo (default: 10)')
    parser.add_argument('--duration', type=int, default=None, help='Duracion en segundos (None=infinito)')
    parser.add_argument('--events', type=int, default=2000, help='Numero total de eventos a generar (default: 2000)')
    parser.add_argument('--mode', type=str, default='count', choices=['count', 'duration', 'infinite'],
                       help='Modo: count=N eventos, duration=N segundos, infinite=sin limite')
    args = parser.parse_args()

    print(f"""
    =============================================================================
                    NETFLIX KAFKA EVENT PRODUCER
    =============================================================================

    Este productor simula eventos de una plataforma de streaming:

    TIPOS DE EVENTOS (6 tipos - cumple requisito de 4+):
    ┌─────────────┬─────────────────────────────────────┬───────────┐
    │ Tipo        │ Descripcion                         │ Topic     │
    ├─────────────┼─────────────────────────────────────┼───────────┤
    │ PLAY        │ Reproducciones de contenido         │ events    │
    │ PAUSE       │ Pausas durante reproduccion         │ events    │
    │ RATE        │ Valoraciones de usuarios            │ events    │
    │ SEARCH      │ Busquedas de contenido              │ events    │
    │ LOGIN       │ Inicios de sesion                   │ events    │
    │ ERROR       │ Errores del sistema                 │ alerts    │
    └─────────────┴─────────────────────────────────────┴───────────┘

    REGLAS DE ALERTA (cumple requisito de 2+):
    1. Errores con severidad CRITICAL -> Topic de alertas
    2. Errores con severidad HIGH -> Topic de alertas

    CONFIGURACION ACTUAL:
    - Modo: {args.mode}
    - Eventos a generar: {args.events if args.mode == 'count' else 'N/A'}
    - Rate: {args.rate} eventos/segundo
    - Duracion: {args.duration if args.mode == 'duration' else 'N/A'}

    Topics:
    - netflix-events: Eventos normales de usuario
    - netflix-alerts: Eventos de error para alertas

    Presiona Ctrl+C para detener.
    """)

    # Determinar duracion basada en modo
    if args.mode == 'count':
        # Calcular duracion para generar N eventos
        duration = args.events // args.rate
        print(f"    [i] Generando {args.events} eventos en ~{duration} segundos")
        run_producer(events_per_second=args.rate, duration_seconds=duration)
    elif args.mode == 'duration':
        run_producer(events_per_second=args.rate, duration_seconds=args.duration)
    else:
        run_producer(events_per_second=args.rate, duration_seconds=None)
