import base64
import json
import os
import logging

from cloudevents.http import CloudEvent
from google.cloud import pubsub_v1

logger = logging.getLogger()
logger.setLevel(logging.ERROR)

MQTT_COMMAND_TOPIC_PATH = os.environ.get(
    "MQTT_COMMAND_TOPIC_PATH",
    "projects/creating-solutions-gcp-shelly/topics/device_outbound",
)

TARGET_MQTT_TOPIC = os.environ.get(
    "TARGET_MQTT_TOPIC",
    "shelly/rpc"
)

publisher = pubsub_v1.PublisherClient()


def handle_shelly_button(cloudevent: CloudEvent) -> str:
    logger.error("=== handle_shelly_button invoked ===")

    data = cloudevent.data

    if isinstance(data, (bytes, bytearray)):
        data = json.loads(data.decode("utf-8"))

    if not isinstance(data, dict) or "message" not in data:
        logger.error("Unexpected CloudEvent.data: %s", data)
        return "Invalid CloudEvent"

    encoded = data["message"].get("data")
    if not encoded:
        logger.error("Missing Pub/Sub data field")
        return "Invalid Pub/Sub message"

    shelly_raw = base64.b64decode(encoded).decode("utf-8")
    logger.error("Shelly raw JSON: %s", shelly_raw)

    try:
        shelly = json.loads(shelly_raw)
    except Exception:
        logger.error("Failed to parse Shelly JSON")
        return "Invalid Shelly payload"

    events = shelly.get("params", {}).get("events", []) or []
    if not events:
        logger.error("No events in Shelly payload")
        return "OK (no events)"

    for ev in events:
        ev_type = ev.get("event")
        component = ev.get("component")

        logger.error("Event: type=%s, component=%s", ev_type, component)

        if not (isinstance(component, str) and component.startswith("bthomedevice:")):
            continue

        if ev_type not in ("single_push", "double_push"):
            continue

        desired_on = ev_type == "single_push"

        wrapper = {
            "topic": TARGET_MQTT_TOPIC,
            "payload": {
                "id": 0,
                "src": "gcp-function",
                "dst": "shellyproem50-08f9e0e7fb78",
                "method": "switch.set",
                "params": {
                    "id": 0,
                    "on": desired_on,
                },
            },
        }

        logger.error(
            "Publishing MQTT command to %s: %s",
            MQTT_COMMAND_TOPIC_PATH,
            wrapper,
        )

        publisher.publish(
            MQTT_COMMAND_TOPIC_PATH,
            data=json.dumps(wrapper).encode("utf-8"),
        )
        break

    return "OK"
