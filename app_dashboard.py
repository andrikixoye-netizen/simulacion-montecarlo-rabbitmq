import pika
import json
import numpy as np
import threading
import os
import time
from collections import deque, defaultdict

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go

# --- Configuración de RabbitMQ ---
# ¡IMPORTANTE! Usa la IP del servidor RabbitMQ si es remoto
RABBITMQ_HOST = '172.18.192.1' 
SCENARIOS_QUEUE = 'scenarios_queue'
RESULTS_QUEUE = 'results_queue'

# --- Almacenamiento de Datos (Thread-Safe) ---
# Usamos un deque con 'maxlen' para el histograma, 
# para no agotar la memoria. 5000 muestras son suficientes.
results_sample = deque(maxlen=5000)
worker_stats = defaultdict(int)
total_processed = 0
sum_of_results = 0.0
sum_of_squares = 0.0

# El 'Lock' es crucial para evitar que el hilo de Dash
# lea los datos mientras el hilo de Pika los está escribiendo.
data_lock = threading.Lock()

# --- 1. Lógica del Consumidor (Hilo de fondo) ---

def process_message(ch, method, properties, body):
    """Callback que se ejecuta por cada resultado recibido."""
    global total_processed, sum_of_results, sum_of_squares, worker_stats, results_sample
    
    data = json.loads(body.decode())
    result = data['result']
    
    # Adquirimos el lock para actualizar los datos globales
    with data_lock:
        results_sample.append(result)
        worker_stats[data['worker']] += 1
        total_processed += 1
        sum_of_results += result
        sum_of_squares += (result ** 2)
        
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_consumer_thread():
    """Inicia la conexión y consumo de RabbitMQ en un hilo separado."""
    print("Iniciando hilo consumidor de resultados...")
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
            channel = connection.channel()
            
            channel.queue_declare(queue=RESULTS_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=100) # Optimización: consume 100 a la vez
            channel.basic_consume(
                queue=RESULTS_QUEUE,
                on_message_callback=process_message
            )
            print("Consumidor de resultados listo y esperando.")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Error de conexión: {e}. Reintentando en 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"Error inesperado en consumidor: {e}. Reiniciando...")
            time.sleep(5)

# --- 2. Lógica del Dashboard (Hilo principal) ---

def get_queue_stats():
    """Obtiene el estado de las colas. Es una operación costosa."""
    # Nota: Abrir y cerrar conexiones es ineficiente,
    # pero es la forma más simple de hacerlo en un callback de Dash.
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(RABBITMQ_HOST, heartbeat=10)
        )
        channel = connection.channel()
        
        # passive=True: consulta el estado sin modificar
        scenarios_info = channel.queue_declare(queue=SCENARIOS_QUEUE, passive=True)
        results_info = channel.queue_declare(queue=RESULTS_QUEUE, passive=True)
        
        stats = {
            "scenarios": scenarios_info.method.message_count,
            "results": results_info.method.message_count
        }
        connection.close()
        return stats
    except Exception:
        # Si RabbitMQ no está, evitamos que el dashboard falle
        return {"scenarios": "N/A", "results": "N/A"}

# --- 3. Inicialización y Layout de la App Dash ---

app = dash.Dash(__name__)

app.layout = html.Div(style={'fontFamily': 'Arial, sans-serif'}, children=[
    html.H1("Dashboard de Simulación Montecarlo"),
    html.Hr(),

    # Contenedor de KPIs
    html.Div(style={'display': 'flex', 'justifyContent': 'space-around'}, children=[
        html.Div([html.H3("Escenarios Pendientes"), html.H2(id='kpi-scenarios')], style={'textAlign': 'center'}),
        html.Div([html.H3("Resultados en Cola"), html.H2(id='kpi-results-q')], style={'textAlign': 'center'}),
        html.Div([html.H3("Resultados Totales"), html.H2(id='kpi-total')], style={'textAlign': 'center'}),
        html.Div([html.H3("Media del Resultado"), html.H2(id='kpi-media')], style={'textAlign': 'center'}),
        html.Div([html.H3("Desv. Estándar"), html.H2(id='kpi-std')], style={'textAlign': 'center'}),
    ]),
    
    html.Hr(),

    # Contenedor de Gráficos
    html.Div(style={'display': 'flex'}, children=[
        # Histograma de Resultados
        html.Div(dcc.Graph(id='histograma-resultados'), style={'width': '50%'}),
        
        # Gráfico de Workers
        html.Div(dcc.Graph(id='grafico-workers'), style={'width': '50%'}),
    ]),

    # Componente de intervalo para actualizaciones automáticas
    dcc.Interval(
        id='interval-update',
        interval=1 * 1000,  # 1 segundo (en milisegundos)
        n_intervals=0
    )
])

# --- 4. Callbacks (La magia de Dash) ---

@app.callback(
    [Output('kpi-scenarios', 'children'),
     Output('kpi-results-q', 'children'),
     Output('kpi-total', 'children'),
     Output('kpi-media', 'children'),
     Output('kpi-std', 'children'),
     Output('histograma-resultados', 'figure'),
     Output('grafico-workers', 'figure')],
    [Input('interval-update', 'n_intervals')]
)
def update_graphs(n):
    # 1. Obtener estadísticas de colas
    queue_stats = get_queue_stats()

    # 2. Copiar los datos compartidos de forma segura
    with data_lock:
        local_total = total_processed
        local_sum = sum_of_results
        local_sum_sq = sum_of_squares
        
        # Copiamos la muestra para el histograma
        hist_data = np.array(list(results_sample))
        
        # Copiamos las estadísticas de workers
        local_workers = dict(worker_stats)

    # 3. Preparar KPIs
    if local_total > 0:
        media = local_sum / local_total
        # Varianza = E[X^2] - (E[X])^2
        variance = (local_sum_sq / local_total) - (media ** 2)
        # Evitar raíz negativa por errores de precisión
        std_dev = np.sqrt(max(0, variance)) 
        
        kpi_total_str = f"{local_total:,}"
        kpi_media_str = f"{media:.3f}"
        kpi_std_str = f"{std_dev:.3f}"
    else:
        kpi_total_str = "0"
        kpi_media_str = "N/A"
        kpi_std_str = "N/A"

    # 4. Crear Figura: Histograma
    fig_hist = go.Figure(
        data=[go.Histogram(x=hist_data, nbinsx=50)],
        layout=go.Layout(
            title="Distribución de Resultados (últimas 5000 muestras)",
            xaxis_title="Valor del Resultado",
            yaxis_title="Frecuencia"
        )
    )

    # 5. Crear Figura: Gráfico de Barras de Workers
    worker_names = list(local_workers.keys())
    worker_counts = list(local_workers.values())
    
    fig_bar = go.Figure(
        data=[go.Bar(x=worker_names, y=worker_counts)],
        layout=go.Layout(
            title="Actividad por Worker",
            xaxis_title="Worker ID",
            yaxis_title="Escenarios Procesados"
        )
    )
    
    return (
        f"{queue_stats['scenarios']:,}",
        f"{queue_stats['results']:,}",
        kpi_total_str,
        kpi_media_str,
        kpi_std_str,
        fig_hist,
        fig_bar
    )


# --- 5. Ejecución ---

if __name__ == '__main__':
    # Inicia el hilo de Pika en modo 'daemon' para que se cierre
    # automáticamente cuando el hilo principal (Dash) se detenga.
    print("Iniciando hilo consumidor...")
    consumer_thread = threading.Thread(target=start_consumer_thread, daemon=True)
    consumer_thread.start()
    
    # --- 5. Ejecución ---

    # Inicia el servidor web de Dash
    print("Iniciando servidor Dash en http://127.0.0.1:8050/")
    app.run(debug=True, port=8050)