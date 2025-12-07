import base64
import json
import os
import logging

from flask import Request
from google.cloud import pubsub_v1

logger = logging.getLogger()
logger.setLevel(logging.ERROR)

# Pub/Sub topic that the MQTT sender function listens to
OUTBOUND_TOPIC_PATH = os.environ.get(
    "MQTT_COMMAND_TOPIC_PATH",
    "projects/creating-solutions-gcp-shelly/topics/device_outbound",
)

TARGET_MQTT_TOPIC = os.environ.get(
    "TARGET_MQTT_TOPIC",
    "shelly/rpc"
)

publisher = pubsub_v1.PublisherClient()


def virtual_button_mass_rpc(request: Request):
    logger.error("=== virtual_button_mass_rpc invoked ===")

    envelope = request.get_json(silent=True) or {}
    message = envelope.get("message", {})
    data_b64 = message.get("data")

    if not data_b64:
        logger.error("Missing Pub/Sub message.data")
        return ("Invalid Pub/Sub message", 400)

    shelly_raw = base64.b64decode(data_b64).decode("utf-8")
    logger.error("Shelly raw JSON: %s", shelly_raw)

    try:
        shelly = json.loads(shelly_raw)
    except Exception:
        logger.error("Failed to parse Shelly JSON")
        return ("Invalid Shelly payload", 400)

    if shelly.get("method") != "NotifyEvent":
        logger.error("Not a NotifyEvent, skipping.")
        return ("OK (not a NotifyEvent)", 200)

    events = shelly.get("params", {}).get("events", []) or []
    if not events:
        logger.error("NotifyEvent.params.events is empty.")
        return ("OK (no events)", 200)

    chosen_type = None

    for ev in events:
        ev_type = ev.get("event")
        component = ev.get("component")
        logger.error("Event: type=%s, component=%s", ev_type, component)

        if component != "button:200":
            continue
        if ev_type not in ("single_push", "double_push"):
            continue

        chosen_type = ev_type
        break

    if not chosen_type:
        logger.error("No matching BLU button event found.")
        return ("OK (no matching event)", 200)

    desired_on = chosen_type == "single_push"

    wrapper = {
        "topic": TARGET_MQTT_TOPIC,
        "payload": {
            "id": 0,
            "src": "cloud-scene",
            "method": "switch.set",
            "params": {
                "id": 0,
                "on": desired_on,
            },
        },
    }

    logger.error(
        "Publishing outbound command to %s: %s",
        OUTBOUND_TOPIC_PATH,
        wrapper,
    )

    publisher.publish(
        OUTBOUND_TOPIC_PATH,
        data=json.dumps(wrapper).encode("utf-8"),
    )

    return (f"Commands executed for event '{chosen_type}'", 200)
