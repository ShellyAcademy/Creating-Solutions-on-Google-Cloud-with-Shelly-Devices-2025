import base64
import functions_framework
import paho.mqtt.client as mqtt
import json
import logging
import time
import os

logger = logging.getLogger()

MQTT_HOST = os.environ.get("MQTT_HOST", "35.233.92.84")  # change to your broker IP
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "device_username")  # set if you use auth
MQTT_PASS = os.environ.get("MQTT_PASS", "device_password")

@functions_framework.cloud_event
def pubsub_to_mqtt(event):
    logger.error("entering mqtt send")
    logger.error(event)
    """Triggered from a message on a Cloud Pub/Sub topic."""
    decoded_data = base64.b64decode(event.data["message"]["data"]).decode("utf-8")
    logger.error(f"Decoded data: {decoded_data}")
    try:
        data = json.loads(decoded_data)
    except:
        logger.error("Error decoding data to JSON")
        return

    topic = data["topic"]
    pubsub_message = json.dumps(data["payload"])
    
    # Publish to MQTT
    client = mqtt.Client()
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=10)
    client.publish(topic, pubsub_message, qos=0)
    client.loop()
    time.sleep(0.3)
    client.disconnect()

    logger.error(f"Forwarded: {pubsub_message} to {topic}")


