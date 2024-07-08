import asyncio
import logging
import json
import paho.mqtt.client as mqtt
from configparser import ConfigParser
from urllib.parse import urlparse
import os
import websockets

logger = logging.getLogger(__name__)
config = ConfigParser()

async def main():
    async with websockets.serve(websocket_server, "localhost", 8765):
        print("WebSocket server started on ws://localhost:8765")
        await asyncio.Future()  # Run forever

async def websocket_server(websocket, path):
    config.read(os.path.join(os.getcwd(), "config.cfg"))

    # MQTT settings
    mqtturl = config["MQTT"].get("url")
    mqtturl_parsed = urlparse(mqtturl)

    broker = mqtturl_parsed.hostname
    port = mqtturl_parsed.port
    username = mqtturl_parsed.username
    password = mqtturl_parsed.password

    topic = config["MQTT"].get("topic")

    client = mqtt.Client()
    client.username_pw_set(username, password)

    def on_connect(client, userdata, flags, reason_code, properties=None):
        print(f"Connected with reason code {reason_code}")
        client.publish(
            "from_cashier",
            json.dumps({"cashier": topic, "status": "ws_connected"}),
        )
        client.subscribe(topic)

    def on_message(client, userdata, msg):
        payload = msg.payload.decode()
        topic = msg.topic
        logger.info(f"Received `{payload}` from `{topic}` topic")

        if "ping" in payload:
            client.publish(
                "from_cashier",
                json.dumps({"cashier": topic, "status": "ws_connected"}),
            )
        else:
            # immediately forward to websocket
            asyncio.run_coroutine_threadsafe(
                websocket.send(payload), asyncio.get_running_loop()
            )

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(broker, port, 60)
    client.loop_start()

    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
