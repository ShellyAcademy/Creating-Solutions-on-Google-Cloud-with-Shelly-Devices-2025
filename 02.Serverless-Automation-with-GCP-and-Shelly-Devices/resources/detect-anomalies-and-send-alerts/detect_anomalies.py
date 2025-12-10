import functions_framework
import logging
import os
import base64
from google.cloud import pubsub_v1
import json
import smtplib
from email.message import EmailMessage
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("anomaly-alerts")

SHELLY_API_KEY = os.getenv("API_KEY")
logger.info(f"SHELLY_API_KEY={SHELLY_API_KEY}")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
logger.info(f"DISCORD_WEBHOOK_URL={DISCORD_WEBHOOK_URL}")
GOOGLE_APP_KEY = os.getenv("GOOGLE_APP_KEY")
logger.info(f"GOOGLE_APP_KEY={GOOGLE_APP_KEY}")
ALERT_PUBSUB_TOPIC = os.getenv("ALERT_PUBSUB_TOPIC")
logger.info(f"ALERT_PUBSUB_TOPIC={ALERT_PUBSUB_TOPIC}")

POWER_THRESHOLD = 30

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = "shelly.softuni@gmail.com"

publisher = pubsub_v1.PublisherClient()

def publish_message(data):
    payload = {
        "device_id": data["device_id"],
        "alert_message": "This is an alert",
        "event": data["event"]
    }
    logger.info(f"Publishing {payload}")
    publisher.publish(ALERT_PUBSUB_TOPIC, data=json.dumps(payload).encode("UTF-8"))

def send_discord_message(data):
    payload = {
        "content": f"This is an alert. Event {data["event"]}",
        "username": data["device_id"]
    }

    logger.info(f"Sending discord message: {payload}")

    requests.post(
        DISCORD_WEBHOOK_URL,
        json=payload,
        headers={"Content-Type": "application/json"}
    )

def send_email(data):
    msg = EmailMessage()
    msg.set_content(f"This is an alert. Event: {data["event"]}")
    msg["Subject"] = f"Alert from {data["device_id"]}"
    msg["From"] = SENDER_EMAIL
    msg["To"] = SENDER_EMAIL

    logger.info("Sending email")

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls() #start secure connection
        server.login(SENDER_EMAIL, GOOGLE_APP_KEY)  #login
        server.send_message(msg)  #send the email


@functions_framework.cloud_event
def detect_anomalies(cloud_event):
    payload_msg = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
    payload = json.loads(payload_msg)

    if payload["method"] != "NotifyStatus":
        return
    
    params = payload["params"]

    device_id = payload["src"]

    #check if apower was changed
    if "switch:0" in params and "apower" in params["switch:0"]:
        apower = params["switch:0"]["apower"]
        logger.info(f"Power consumption of {device_id} changed to {apower}")
        if apower > POWER_THRESHOLD:
            logger.info(f"Anomaly detected for {device_id}. Sending alerts")
            alert_data = {
                "device_id": device_id,
                "event": payload["params"]
            }            

            publish_message(alert_data)
            send_discord_message(alert_data)
            send_email(alert_data)
