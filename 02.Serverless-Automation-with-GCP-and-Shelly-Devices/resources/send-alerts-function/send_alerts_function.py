import functions_framework
import logging
import os
from google.cloud import pubsub_v1
import json
import smtplib
from email.message import EmailMessage
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("alerts-logger")

SHELLY_API_KEY = os.getenv("API_KEY")
logger.info(f"SHELLY_API_KEY={SHELLY_API_KEY}")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
logger.info(f"DISCORD_WEBHOOK_URL={DISCORD_WEBHOOK_URL}")
GOOGLE_APP_KEY = os.getenv("GOOGLE_APP_KEY")
logger.info(f"GOOGLE_APP_KEY={GOOGLE_APP_KEY}")
ALERT_PUBSUB_TOPIC = os.getenv("ALERT_PUBSUB_TOPIC")
logger.info(f"ALERT_PUBSUB_TOPIC={ALERT_PUBSUB_TOPIC}")


SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = "shelly.softuni@gmail.com"

publisher = pubsub_v1.PublisherClient()

def publish_message(data):
    payload = {
        "device_id": data["dev_info"]["id"],
        "alert_message": "This is an alert",
        "device_status": data["dev_status"]
    }
    logger.info(f"Publishing {payload}")
    publisher.publish(ALERT_PUBSUB_TOPIC, data=json.dumps(payload).encode("UTF-8"))

def send_discord_message(data):
    payload = {
        "content": f"This is an alert. Device status {data["dev_status"]}",
        "username": data["dev_info"]["id"]
    }

    logger.info(f"Sending discord message: {payload}")

    requests.post(
        DISCORD_WEBHOOK_URL,
        json=payload,
        headers={"Content-Type": "application/json"}
    )

def send_email(data):
    msg = EmailMessage()
    msg.set_content(f"This is an alert.<br><br>Device status:<br>{data["dev_status"]}")
    msg["Subject"] = f"Alert from {data["dev_info"]["id"]}"
    msg["From"] = SENDER_EMAIL
    msg["To"] = SENDER_EMAIL

    logger.info("Sending email")

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls() #start secure connection
        server.login(SENDER_EMAIL, GOOGLE_APP_KEY)  #login
        server.send_message(msg)  #send the email

@functions_framework.http
def process_alert(request):
    api_key = request.args.get("api_key", "")

    if api_key != SHELLY_API_KEY:
        return "Forbidden", 403

    if request.method != "GET":
        return "Wrong request", 400

    dev_info = json.loads(request.args.get("dev_info", "{}"))
    dev_status = json.loads(request.args.get("dev_status", "{}"))

    data = {
        "dev_info": dev_info,
        "dev_status": dev_status
    }

    logger.info(f"Request data is {data}")

    publish_message(data)
    send_discord_message(data)
    send_email(data)

    return {"result": "Alerts sent"}

