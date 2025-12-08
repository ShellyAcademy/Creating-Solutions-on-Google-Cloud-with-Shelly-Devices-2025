import base64
import json
import logging
 
from cloudevents.http import CloudEvent
from google.cloud import pubsub_v1
 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
 
CLEAN_TOPIC_PATH = (
    "projects/creating-solutions-gcp-shelly/topics/shelly-clean-status"
)
 
TEMP_ALERT_THRESHOLD = 60      # °C
POWER_ALERT_THRESHOLD = 1500   # W
 
publisher = pubsub_v1.PublisherClient()
 
 
def normalize_shelly_status(cloudevent: CloudEvent) -> str:
    logger.info("=== normalize_shelly_status invoked ===")

    try:
        data = json.loads(cloudevent.data.decode("utf-8"))
    except Exception:
        logger.error("Failed to decode CloudEvent bytes")
        return "Invalid CloudEvent"
 
    encoded = data["message"].get("data")
    if not encoded:
        logger.error("Missing Pub/Sub message.data")
        return "Invalid Pub/Sub message"
 
    raw = base64.b64decode(encoded).decode("utf-8")
    logger.info("Shelly raw JSON: %s", raw)
    msg = json.loads(raw)
 
    if msg.get("method") != "NotifyStatus":
        logger.info("Not a NotifyStatus message, skipping.")
        return "OK (not NotifyStatus)"
 
    params = msg.get("params", {})
    switch0 = params.get("switch:0", {}) or {}
 
    output = switch0.get("output")
    apower = switch0.get("apower")
    temp_c = (switch0.get("temperature") or {}).get("tC")
    ts = params.get("ts")
    device = msg.get("src", "unknown")
 
    logger.info(
        "Parsed status | device=%s | output=%s | apower=%s | tempC=%s | ts=%s",
        device, output, apower, temp_c, ts,
    )
 
    overheat = isinstance(temp_c, (int, float)) and temp_c > TEMP_ALERT_THRESHOLD
    high_power = isinstance(apower, (int, float)) and apower > POWER_ALERT_THRESHOLD
 
    if not (overheat or high_power):
        logger.info("No alert detected.")
        return "OK (no alert)"
 
    if overheat and high_power:
        alert = "overheat_and_high_power"
    elif overheat:
        alert = "overheat"
    else:
        alert = "high_power"
 
    reasons = []
    if overheat:
        reasons.append(f"tempC={temp_c} > {TEMP_ALERT_THRESHOLD}")
    if high_power:
        reasons.append(f"apower={apower} > {POWER_ALERT_THRESHOLD}")
 
    clean_event = {
        "device": device,
        "method": msg.get("method"),
        "output": output,
        "apower": apower,
        "temperature_c": temp_c,
        "ts": ts,
        "alert": alert,
        "alert_reasons": reasons,
    }
 
    attrs = {"device": device, "alert": alert}
    if output is not None:
        attrs["output"] = "true" if output else "false"
 
    logger.info("Publishing alert to %s: %s | attrs=%s",
                 CLEAN_TOPIC_PATH, clean_event, attrs)
 
    publisher.publish(
        CLEAN_TOPIC_PATH,
        data=json.dumps(clean_event).encode("utf-8"),
        **attrs,
    )
 
    return "OK (alert published)"