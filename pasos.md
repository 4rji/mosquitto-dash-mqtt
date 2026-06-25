# Guía básica del proyecto

## ¿De qué trata?

Este proyecto es un panel web para observar en tiempo real los mensajes que envían routers Digi mediante MQTT.

MQTT es un sistema de comunicación sencillo: los dispositivos publican mensajes en diferentes temas y un servidor llamado **broker Mosquitto** los recibe y distribuye. El panel se conecta a ese broker para mostrar la información de forma ordenada.

## ¿Cómo funciona?

El recorrido básico de la información es:

1. Un router publica un mensaje MQTT.
2. Mosquitto recibe el mensaje.
3. La aplicación escrita en Python recoge el mensaje.
4. El mensaje se guarda temporalmente y también puede almacenarse en una base de datos SQLite.
5. La página web se actualiza automáticamente, sin tener que recargarla.

La aplicación intenta identificar cada dispositivo usando la primera parte del tema. Por ejemplo, un mensaje con el tema `router01/status` se relaciona con el dispositivo `router01`.



# 1. Instalar Mosquitto
usar un paquete como apt pacman etc  mosquitto

# 2. Revisar/editar la configuración
sudo nano /etc/mosquitto/mosquitto.conf

# 3. Ejecutar Mosquitto en modo verbose para depuración
mosquitto -v

# 4. Verificar el servicio
ctl mosquitto
# (posiblemente alias de: systemctl status mosquitto)

# 5. Monitorear logs del servicio
sudo journalctl -u mosquitto -f

# 6. Verificar tráfico MQTT en el puerto 1883
tcpdump -i any port 1883
sudo tcpdump -i any port 1883

# 7. Definir el broker MQTT
export MQTT_HOST=localhost

# 8. Suscribirse a todos los tópicos para validar mensajes
mosquitto_sub -h localhost -t '#' -v



Despues de la instalacion y verificar que recibimos logs poner el dashboard en marcha:



## ¿Qué muestra el panel?

El panel permite consultar:

- Estado de conexión con Mosquitto.
- Mensajes recibidos en tiempo real.
- Cantidad de mensajes, temas y dispositivos.
- Contenido de cada mensaje.
- Fecha y hora de la última comunicación.
- Datos de sistema como carga, memoria RAM y discos.
- Historial reciente guardado en SQLite.


incluir screenshots:
img/mosquitto/*

## Partes principales

- `app.py`: inicia la aplicación web.
- `mqtt_client.py`: conecta con Mosquitto.
- `dashboard_state.py`: organiza los mensajes y estadísticas.
- `message_store.py`: guarda el historial en SQLite.
- `templates/` y `static/`: contienen la interfaz visual.
- `tests/`: contiene pruebas para comprobar el funcionamiento.

## ¿Cómo se ejecuta?

Primero se instalan las dependencias:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Después se configura la dirección del broker y se inicia:

```bash
export MQTT_HOST=10.10.65.42 (or localhost this ip is the mosquitto host IP)
python app.py
```

Finalmente, se abre `http://localhost:5000` en el navegador.

También se puede desplegar en un contenedor Docker para ejecutarlo de forma aislada y más fácil de trasladar a otro equipo.
