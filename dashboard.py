import pika
import json
import time
import numpy as np
import os
from collections import defaultdict

# --- Configuración de Colas ---
RABBITMQ_HOST = '192.168.3.52'
SCENARIOS_QUEUE = 'scenarios_queue'
RESULTS_QUEUE = 'results_queue'

# --- Almacenamiento de Estadísticas ---
results_list = []
worker_stats = defaultdict(int)
total_processed = 0

def clear_console():
    """Limpia la terminal."""
    os.system('cls' if os.name == 'nt' else 'clear')

def print_dashboard(stats):
    """Imprime el panel de estadísticas."""
    clear_console()
    print("--- Dashboard Simulación Montecarlo (En tiempo real) ---")
    print("\n--- Estado de Colas ---")
    print(f"Escenarios Pendientes: {stats.get('scenarios_pending', 'N/A')}")
    print(f"Resultados Procesados: {stats.get('results_processed', 'N/A')} (Total: {total_processed})")
    
    print("\n--- Estadísticas de Resultados ---")
    if results_list:
        mean = np.mean(results_list)
        std = np.std(results_list)
        p5 = np.percentile(results_list, 5)
        p95 = np.percentile(results_list, 95)
        print(f"  Media        : {mean:.4f}")
        print(f"  Std. Dev     : {std:.4f}")
        print(f"  Percentil 5  : {p5:.4f}")
        print(f"  Percentil 95 : {p95:.4f}")
    else:
        print("  (Esperando primeros resultados...)")

    print("\n--- Actividad de Workers ---")
    if not worker_stats:
        print("  (Esperando actividad...)")
    else:
        # Mostrar los 5 workers más activos
        sorted_workers = sorted(worker_stats.items(), key=lambda item: item[1], reverse=True)
        for worker, count in sorted_workers[:5]:
            print(f"  {worker}: {count} tareas")

def get_queue_stats(channel):
    """Obtiene el número de mensajes en las colas."""
    try:
        # passive=True nos da el estado de la cola sin alterarla
        scenarios_info = channel.queue_declare(queue=SCENARIOS_QUEUE, passive=True)
        results_info = channel.queue_declare(queue=RESULTS_QUEUE, passive=True)
        
        return {
            "scenarios_pending": scenarios_info.method.message_count,
            "results_processed": results_info.method.message_count
        }
    except Exception as e:
        print(f"Error al obtener stats (la cola puede no existir aún): {e}")
        return {}

def main():
    global total_processed
    print("Iniciando Dashboard... (Presiona CTRL+C para salir)")
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue=RESULTS_QUEUE, durable=True)

    last_stats_check = 0

    try:
        while True:
            # --- 1. Consumir un resultado (sin bloqueo) ---
            method_frame, header_frame, body = channel.basic_get(queue=RESULTS_QUEUE)
            
            if method_frame:
                data = json.loads(body.decode())
                results_list.append(data['result'])
                worker_stats[data['worker']] += 1
                total_processed += 1
                
                # Acusar recibo
                channel.basic_ack(method_frame.delivery_tag)
            
            # --- 2. Actualizar estadísticas de colas (cada 1s) ---
            current_time = time.time()
            if current_time - last_stats_check > 1:
                stats = get_queue_stats(channel)
                print_dashboard(stats)
                last_stats_check = current_time

            # Si no hay mensajes, espera un poco para no saturar la CPU
            if not method_frame:
                time.sleep(0.01)

    except KeyboardInterrupt:
        print("\nCerrando dashboard.")
    finally:
        connection.close()

if __name__ == '__main__':
    main()