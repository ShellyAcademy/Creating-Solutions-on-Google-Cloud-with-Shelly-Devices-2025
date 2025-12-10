import functions_framework
import os
from flask import Request
import requests
import smtplib
from email.message import EmailMessage

SHELLY_API_KEY = os.getenv("API_KEY")
WEBHOOK_URL = ""

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587 # Important! Port 25 is blocked. 465 or 587 work.
SENDER_EMAIL = ""
APP_PASSWORD = ""

@functions_framework.http
def hello_http(request:Request):

    #check api key
    request_api_key = request.headers.get("x-api-key")
    if request_api_key != SHELLY_API_KEY:
        return "Forbidden", 403

    request_json = request.get_json(silent=True)

    payload = {
        "content": "Alert!",
        "username": "shelly"
    }

    requests.post(
        WEBHOOK_URL,
        json=payload,
        headers={"Content-Type": "application/json"}
    )

# 1. Create the email content
    msg = EmailMessage()
    msg.set_content("This is an automated alert from your Cloud Function.")
    msg["Subject"] = "Shelly Device Alert"
    msg["From"] = SENDER_EMAIL
    msg["To"] = "shelly.softuni@gmail.com"

    try:
        # 2. Connect to Gmail via TLS (Port 587)
        # Note: We use a context manager (with) to ensure connection closes
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls() # Secure the connection
            server.login(SENDER_EMAIL, APP_PASSWORD)
            server.send_message(msg)
            
        return "Email sent successfully!", 200

    except Exception as e:
        return f"Failed to send email: {e}", 500

    return "sent"
