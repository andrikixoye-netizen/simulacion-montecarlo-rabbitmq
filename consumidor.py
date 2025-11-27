import pika
import json
import time
import math # Para funciones matemáticas seguras en eval
import os

# --- Configuración de Colas ---
RABBITMQ_HOST = '172.18.192.1'
MODEL_QUEUE = 'model_queue'
SCENARIOS_QUEUE = 'scenarios_queue'
RESULTS_QUEUE = 'results_queue'

# Variable global para almacenar el modelo
CURRENT_MODEL_FUNCTION = None

def get_model(channel):
    """Obtiene el modelo de la cola. Bloqueante."""
    global CURRENT_MODEL_FUNCTION
    
    print("Intentando obtener el modelo...")
    while CURRENT_MODEL_FUNCTION is None:
        method_frame, header_frame, body = channel.basic_get(queue=MODEL_QUEUE)
        if method_frame:
            CURRENT_MODEL_FUNCTION = body.decode()
            print(f"Modelo adquirido: {CURRENT_MODEL_FUNCTION}")
            # No hacemos 'ack'. Si el consumidor falla, el modelo
            # sigue en la cola para otros (o para él mismo al reiniciar).
            # NOTA: basic_get en modo auto_ack=True (default) lo consume.
            # Para re-publicarlo para otros, hacemos:
            channel.basic_publish(
                exchange='',
                routing_key=MODEL_QUEUE,
                body=body,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            # Y acusamos recibo del nuestro
            channel.basic_ack(method_frame.delivery_tag)
        else:
            print("... Modelo aún no disponible. Reintentando en 2s...")
            time.sleep(2)

def execute_model(model_function, scenario_data):
    """
    Ejecuta de forma SEGURA la función del modelo.
    Usa eval() pero restringe el entorno.
    """
    
    # Definimos un entorno seguro para eval()
    # Solo permitimos variables del escenario y funciones matemáticas.
    safe_globals = {
        "__builtins__": None, # Bloquea built-ins peligrosos
        "math": math # Permite usar 'math.sqrt()', 'math.pow()', etc.
    }
    
    # ADVERTENCIA: eval() es poderoso. Este enfoque es 
    # razonablemente seguro, pero en producción se 
    # recomienda usar un parser dedicado si la seguridad es crítica.
    try:
        # scenario_data actúa como el 'scope' local
        result = eval(model_function, safe_globals, scenario_data)
        return result
    except Exception as e:
        print(f"Error evaluando el modelo: {e}")
        return None

def main():
    worker_id = f"worker-{os.getpid()}"
    print(f"Iniciando Consumidor: {worker_id}")

    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()

    # --- Declarar Colas ---
    channel.queue_declare(queue=MODEL_QUEUE, durable=True)
    channel.queue_declare(queue=SCENARIOS_QUEUE, durable=True, arguments={'x-max-length': 2000000 })
    channel.queue_declare(queue=RESULTS_QUEUE, durable=True)

    # --- 1. Obtener el Modelo ---
    get_model(channel)

    # --- 2. Definir el Callback de Procesamiento ---
    def callback(ch, method, properties, body):
        scenario_data = json.loads(body.decode())
        
        # Ejecutar simulación
        result = execute_model(CURRENT_MODEL_FUNCTION, scenario_data)
        
        if result is not None:
            # Publicar resultado
            ch.basic_publish(
                exchange='',
                routing_key=RESULTS_QUEUE,
                body=json.dumps({"worker": worker_id, "result": result}),
                properties=pika.BasicProperties(delivery_mode=2)
            )
        
        # Acusar recibo del escenario
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # --- 3. Suscribirse a la Cola de Escenarios ---
    # prefetch_count=1 asegura que este worker no tome más de 
    # un mensaje a la vez, permitiendo balanceo de carga.
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=SCENARIOS_QUEUE,
        on_message_callback=callback
    )

    print(f"[{worker_id}] Esperando escenarios. (Presiona CTRL+C para salir)")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Cerrando consumidor.")
        connection.close()

if __name__ == '__main__':
    main()