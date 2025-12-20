import os
import base64
import requests


def send_notification(topic: str, title: str, message: str, priority: int = 3):
    username = os.getenv("NTFY_USERNAME")
    password = os.getenv("NTFY_PASSWORD")
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    headers = {
        "Title": title,
        "Priority": str(priority),
        "Authorization": f"Basic {encoded_credentials}"
    }

    host = os.getenv("NTFY_HOST")
    ntfy_url = f"{host}/{topic}"
    response = requests.post(ntfy_url, data=message.encode("utf-8"), headers=headers)
    
    if response.status_code != 200:
        print(f"Failed to send notification: {response.status_code} - {response.text}")
        raise Exception("Failed to send notification")
  