import logging
import json
from configparser import ConfigParser
from urllib.parse import urlparse
import os
from paho.mqtt import client as mqtt_client
from paho.mqtt.enums import CallbackAPIVersion
from websocket_server import WebsocketServer

logger = logging.getLogger(__name__)
config = ConfigParser()


def connect_mqtt(client, username, password, broker, port):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(CallbackAPIVersion.VERSION1, "")

    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client, topic):
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
            server.send_message_to_all(payload)

    client.subscribe(topic)
    client.on_message = on_message


# Called for every client connecting (after handshake)
def new_client(client, server):
    print(f"New client connected and was given id {client['id']}")

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

    client = connect_mqtt(client, username, password, broker, port)

    subscribe(client, topic)
    client.publish(
        "from_cashier",
        json.dumps({"cashier": topic, "status": "ws_connected"}),
    )
    client.loop_forever()


# Called for every client disconnecting
def client_left(client, server):
    print(f"Client({client['id']}) disconnected")


# Called when a client sends a message
def message_received(client, server, message):
    if len(message) > 200:
        message = message[:200] + ".."
    print(f"Client({client['id']}) said: {message}")
    # server.send_message_to_all(f"Client({client['id']}) said: {message}")


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    PORT = 8765
    server = WebsocketServer(port=PORT, host="0.0.0.0")
    server.set_fn_new_client(new_client)
    server.set_fn_client_left(client_left)
    server.set_fn_message_received(message_received)

    server.run_forever()
