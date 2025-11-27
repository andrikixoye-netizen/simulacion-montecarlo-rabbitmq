import pika
import json
import json5 
import numpy as np
import time

# --- Configuración de Colas ---
RABBITMQ_HOST = 'localhost'
MODEL_QUEUE = 'model_queue'
SCENARIOS_QUEUE = 'scenarios_queue'
MODEL_FILE = 'modelo.jsonc'

def get_random_value(config):
    """Genera un valor aleatorio basado en la configuración de distribución."""
    dist = config['distribution']
    params = config['params']
    
    if dist == 'normal':
        return np.random.normal(params['loc'], params['scale'])
    elif dist == 'uniform':
        return np.random.uniform(params['low'], params['high'])
    elif dist == 'poisson':
        return np.random.poisson(params['lam'])
    else:
        raise ValueError(f"Distribución desconocida: {dist}")

def main():
    print("Iniciando Productor...")
    
    # --- Cargar Modelo ---
    with open(MODEL_FILE, 'r') as f:
        model_config = json5.load(f)
        
    model_function = model_config['model_function']
    variables = model_config['variables']
    sim_count = model_config['simulation_count']

    # --- Conexión a RabbitMQ ---
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()

    # --- Declarar Colas (Idempotente) ---
    # Cola de modelo
    channel.queue_declare(queue=MODEL_QUEUE, durable=True) 
    # Cola de escenarios
    channel.queue_declare(queue=SCENARIOS_QUEUE, durable=True, arguments={
        'x-max-length': 2000000 # Evita que la cola crezca indefinidamente
    })
    # Cola de resultados (para que el dashboard la encuentre)
    channel.queue_declare(queue='results_queue', durable=True)

    # --- LIMPIEZA TOTAL DE SIMULACIÓN ANTERIOR ---
    print("Purgando cola de resultados anterior...")
    channel.queue_purge(queue='results_queue')
    print("Purgando cola de escenarios anterior...")
    channel.queue_purge(queue=SCENARIOS_QUEUE)
    # --- FIN DE LÍNEAS NUEVAS ---

    # --- 1. Publicar el Modelo ---
    # Purgamos la cola para cumplir con la "caducidad"
    print(f"Purgando cola de modelo anterior: {MODEL_QUEUE}")
    channel.queue_purge(queue=MODEL_QUEUE)
    
    # Publicamos el nuevo modelo
    channel.basic_publish(
        exchange='',
        routing_key=MODEL_QUEUE,
        body=model_function,
        properties=pika.BasicProperties(
            delivery_mode=2, # Hacer mensaje persistente
        )
    )
    print(f"Nuevo modelo publicado en {MODEL_QUEUE}: {model_function}")

    # --- 2. Generar y Publicar Escenarios ---
    print(f"Iniciando generación de {sim_count} escenarios...")
    start_time = time.time()
    
    for i in range(sim_count):
        scenario = {}
        for var_name, config in variables.items():
            scenario[var_name] = get_random_value(config)
            
        message_body = json.dumps(scenario)
        
        channel.basic_publish(
            exchange='',
            routing_key=SCENARIOS_QUEUE,
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2, # Persistente
            )
        )
        
        if (i + 1) % 10000 == 0:
            print(f"  ... {i+1} escenarios publicados.")

    end_time = time.time()
    print(f"Generación completa. {sim_count} escenarios en {end_time - start_time:.2f}s")
    
    connection.close()

if __name__ == '__main__':
    main()