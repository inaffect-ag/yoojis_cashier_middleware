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

    # start websocket server

    while True:
        async with websockets.serve(websocket_server, "localhost", 8765):
            print("websocket server")
            await asyncio.Future()


async def pouet(N, websocket):
    for i in range(N):
        logger.info(f"pouet: {i}")
    logger.info(websocket)


async def websocket_server(websocket, path):

    logger.info("from websocket server func")
    logger.info(websocket)

    config.read(os.path.join(os.getcwd(), "config.cfg"))
    logger.info(config.sections())

    # MQTT settings
    mqtturl = config["MQTT"].get("url")
    mqtturl_parsed = urlparse(mqtturl)

    broker = mqtturl_parsed.hostname
    port = mqtturl_parsed.port
    username = mqtturl_parsed.username
    password = mqtturl_parsed.password

    # topic_filter = config["MQTT"].get("topic_filter")
    topic = config["MQTT"].get("topic")

    client = mqtt.Client()
    client.username_pw_set(username, password)

    def on_connect(client, userdata, flags, rc):
        print(f"Connected with result code {rc}")
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
            logger.info("forwarding")
            # asyncio.get_event_loop().run_until_complete(websocket.send(payload))
            asyncio.run_coroutine_threadsafe(
                websocket.send(payload), asyncio.get_running_loop()
            )
            logger.info("done")
            # asyncio.run(websocket.send(msg))
            # websocket.send(payload)

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(broker, port, 60)

    while True:
        client.loop()
        await asyncio.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
