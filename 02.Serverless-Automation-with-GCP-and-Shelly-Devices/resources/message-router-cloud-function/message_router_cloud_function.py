import base64
import json
import os
import logging
 
from cloudevents.http import CloudEvent
from google.cloud import pubsub_v1
 
logger = logging.getLogger()
logger.setLevel(logging.ERROR)
 
PROJECT_ID = os.environ.get("GCP_PROJECT", "creating-solutions-gcp-shelly")
 
STATUS_TOPIC_PATH = os.environ.get(
    "STATUS_TOPIC_PATH",
    f"projects/{PROJECT_ID}/topics/status-topic",
)
 
EVENTS_TOPIC_PATH = os.environ.get(
    "EVENTS_TOPIC_PATH",
    f"projects/{PROJECT_ID}/topics/events-topic",
)
 
publisher = pubsub_v1.PublisherClient()
 
 
def message_router(cloudevent: CloudEvent) -> str:
    logger.error("=== message_router invoked ===")
 
    data = cloudevent.data
    if isinstance(data, (bytes, bytearray)):
        data = json.loads(data.decode("utf-8"))
 
    if not isinstance(data, dict) or "message" not in data:
        logger.error("Unexpected CloudEvent.data: %s", data)
        return "Invalid CloudEvent"
 
    encoded = data["message"].get("data")
    if not encoded:
        logger.error("Missing Pub/Sub message.data")
        return "Invalid Pub/Sub message"
 
    raw = base64.b64decode(encoded).decode("utf-8")
    logger.error("Raw MQTT JSON: %s", raw)
 
    try:
        msg = json.loads(raw)
    except Exception:
        logger.error("Failed to parse Shelly JSON")
        return "Invalid Shelly payload"
 
    method = msg.get("method")
    device = msg.get("src", "unknown")
 
    if method == "NotifyStatus":
        target_topic = STATUS_TOPIC_PATH
        msg_type = "status"
    elif method == "NotifyEvent":
        target_topic = EVENTS_TOPIC_PATH
        msg_type = "event"
    else:
        logger.error("Unknown method '%s', skipping.", method)
        return "OK (ignored)"
 
    attrs = {
        "device": device,
        "type": msg_type,
    }
 
    params = msg.get("params", {})
    switch0 = params.get("switch:0") or {}
    if "apower" in switch0:
        attrs["has_apower"] = "true"
 
    logger.error(
        "Routing message method=%s device=%s to %s attrs=%s",
        method,
        device,
        target_topic,
        attrs,
    )
 
    publisher.publish(
        target_topic,
        data=raw.encode("utf-8"),
        **attrs,
    )
 
    return f"OK (routed to {msg_type})"