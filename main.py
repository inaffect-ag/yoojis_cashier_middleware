import os, asyncio, websockets, requests
import time

from paho.mqtt import client as mqtt_client
from configparser import ConfigParser
from urllib.parse import urlparse


config = ConfigParser()
connected_websockets = set()


# Read config.cfg and define the environment variables
config.read(os.path.join(os.getcwd(), "config.cfg"))
mqtturl_parsed = urlparse(config["MQTT"].get("url"))
MQTT_BROKER = mqtturl_parsed.hostname
MQTT_PORT = mqtturl_parsed.port
MQTT_USERNAME = mqtturl_parsed.username
MQTT_PASSWORD = mqtturl_parsed.password
MQTT_TOPIC = config["MQTT"].get("topic")
POST_URL = config["API"].get("url")


def send_post_request(data):
    try:
        response = requests.post(POST_URL, json=data)
        response.raise_for_status()
        print(f"POST request successful: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"POST request failed: {e}")

async def websocket_handler(websocket, path):
    connected_websockets.add(websocket)
    try:
        async for message in websocket:
            pass
    except websockets.ConnectionClosed:
        pass
    finally:
        connected_websockets.remove(websocket)
        send_post_request({'event': 'websocket_disconnected'})

async def send_to_websockets(message):
    tasks = [ws.send(message) for ws in connected_websockets] 
    await asyncio.gather(*tasks)

def on_connect(client, userdata, flags, reason_code, properties):
    print("Connected with the MQTT Broke")
    client.subscribe(MQTT_TOPIC)
    print(f"Linked to the topic {MQTT_TOPIC}")

def on_disconnect(client, userdata, rc):
    print("MQTT connection aborted")
    send_post_request({'event': 'mqtt_disconnected'})

def on_message(client, userdata, msg):
    message = msg.payload.decode('utf-8')
    print(f"Receive message: {message}")
    asyncio.run(send_to_websockets(message))

def start_mqtt_client():
    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

async def heartbeat():
    while True:
        req = requests.get(f'https://www.christianbachmann.ch/heartbeats/log/{MQTT_TOPIC}')
        # print('HTTP STatus:', req.status_code)
        await asyncio.sleep(30)    

async def main():
    start_mqtt_client()
    asyncio.create_task(heartbeat())

    async with websockets.serve(websocket_handler, "localhost", 8765):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())