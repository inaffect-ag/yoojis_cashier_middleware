import asyncio
import logging
import json
from asyncio_mqtt import Client as mqtt_client
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


async def websocket_server(websocket, path):

    config.read(os.path.join(os.getcwd(), "config.cfg"))
    logger.info(config.sections())

    # MQTT settings
    mqtturl = config["MQTT"].get("url")
    mqtturl_parsed = urlparse(mqtturl)

    broker = mqtturl_parsed.hostname
    port = mqtturl_parsed.port
    username = mqtturl_parsed.username
    password = mqtturl_parsed.password

    topic_filter = config["MQTT"].get("topic_filter")
    topic = config["MQTT"].get("topic")

    async for message in websocket:

        async with mqtt_client(
            hostname=broker,
            port=port,
            client_id="",
            username=username,
            password=password,
        ) as client:

            await client.publish(
                "from_cashier",
                json.dumps({"cashier": topic, "status": "ws_connected"}),
            )

            async with client.filtered_messages(topic_filter) as messages:
                logger.info("client connected")

                await client.subscribe(topic)

                logger.info(f"subscribed to {topic}")

                async for mqtt_message in messages:
                    payload = mqtt_message.payload.decode()
                    topic = mqtt_message.topic
                    logger.info(f"Received `{payload}` from `{topic}` topic")

                    if "ping" in payload:
                        await client.publish(
                            "from_cashier",
                            json.dumps(
                                {"cashier": topic, "status": "ws_connected"}
                            ),
                        )
                    else:
                        # immediately forward to websocket
                        await websocket.send(payload)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
