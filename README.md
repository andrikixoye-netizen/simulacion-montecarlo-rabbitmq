[README.md](https://github.com/user-attachments/files/23783575/README.md)
NOTA - Cambia la variable RABBITMQ_HOST por la ip de tu computadora

#Instalaciones
pip install pika numpy json5 dash plotly

#Ejecucion
1. Iniciar el rabbitmq en docker

2. Iniciar el dashboard
	python app_dashboard.py
3. Inicia el productor
	python productor.py  
4. Inicia el consumidor
	python consumidor.py   

#Linea de comando para iniciar el rabbit en Docker
docker run -d --hostname my-rabbit --name some-rabbit \
    -p 5672:5672 \
    -p 15672:15672 \
    rabbitmq:3-management
