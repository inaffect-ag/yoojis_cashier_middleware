import os
import asyncio
import websockets
import requests
from datadog import initialize, api
from paho.mqtt import client as mqtt_client
from configparser import ConfigParser
from urllib.parse import urlparse

config = ConfigParser()
connected_websockets = set()

# Initialize Datadog
options = {
    "api_key": "bd4ce12d3afbe392476f0c7fedc515fd",
    "app_key": "f4f2658d67fca7752eac96255672fba47a354bf8",
}
initialize(**options)

# Read config.cfg and define the environment variables
config.read(os.path.join(os.getcwd(), "config.cfg"))
mqtturl_parsed = urlparse(config["MQTT"].get("url"))
MQTT_BROKER = mqtturl_parsed.hostname
MQTT_PORT = mqtturl_parsed.port
MQTT_USERNAME = mqtturl_parsed.username
MQTT_PASSWORD = mqtturl_parsed.password
MQTT_TOPIC = config["MQTT"].get("topic")
POST_URL = config["API"].get("url")
HEARTBEAT_URL = config["API"].get("heartbeat")


def send_post_request(data):
    try:
        response = requests.post(POST_URL, json=data)
        response.raise_for_status()
        print(f"POST request successful: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"POST request failed: {e}")
        api.Event.create(
            title="post_request_failure",
            text=f"post_request_failure: {e}",
            tags=["yoojis_middleware"],
        )


async def websocket_handler(websocket, path):
    connected_websockets.add(websocket)
    api.Event.create(
        title="websocket_connected",
        text="websocket_connected",
        tags=["yoojis_middleware"],
    )
    try:
        async for message in websocket:
            pass
    except websockets.ConnectionClosed:
        pass
    finally:
        connected_websockets.remove(websocket)
        send_post_request({"event": "websocket_disconnected"})
        api.Event.create(
            title="websocket_disconnected",
            text="websocket_disconnected",
            tags=["yoojis_middleware"],
        )


async def send_to_websockets(message):
    tasks = [ws.send(message) for ws in connected_websockets]
    await asyncio.gather(*tasks)


def on_connect(client, userdata, flags, reason_code, properties):
    print("Connected with the MQTT Broker")
    client.subscribe(MQTT_TOPIC)
    print(f"Linked to the topic {MQTT_TOPIC}")
    api.Event.create(
        title="mqtt_connected",
        text="mqtt_connected",
        tags=["yoojis_middleware"],
    )


def on_disconnect(client, userdata, rc):
    print("MQTT connection aborted")
    send_post_request({"event": "mqtt_disconnected"})
    api.Event.create(
        title="mqtt_disconnected",
        text="mqtt_disconnected",
        tags=["yoojis_middleware"],
    )


def on_message(client, userdata, msg):
    message = msg.payload.decode("utf-8")
    print(f"Received message: {message}")
    asyncio.run(send_to_websockets(message))
    api.Event.create(
        title="message_received",
        text="message_received",
        tags=["yoojis_middleware"],
    )


def start_mqtt_client():
    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()


async def heartbeat():
    while True:
        try:
            req = requests.get(f"{HEARTBEAT_URL}/{MQTT_TOPIC}")
            print("HTTP Status:", req.status_code)

        except requests.exceptions.RequestException as e:
            print(f"Heartbeat failed: {e}")
            api.Event.create(
                title="heartbeat failure",
                text=f"heartbeat status code {req.status_code} - {e}",
                tags=["yoojis_middleware"],
            )

        await asyncio.sleep(30)


async def main():
    start_mqtt_client()
    asyncio.create_task(heartbeat())

    async with websockets.serve(websocket_handler, "localhost", 8765):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
