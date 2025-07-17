import requests
import base64
import dotenv
import os


def send_kp_index_notification(value):

    username = os.getenv("NTFY_USERNAME")
    password = os.getenv("NTFY_PASSWORD")
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    host = os.getenv("NTFY_HOST")
    topic = "kp_index"
    ntfy_url = f"{host}/{topic}"

    headers = {
        "Title": "Kp Index Alert",
        "Priority": "3",
        "Authorization": f"Basic {encoded_credentials}"
    }

    message = f"Kp Index: {value}"

    requests.post(ntfy_url, data=message.encode("utf-8"), headers=headers)


def main():
    dotenv.load_dotenv()

    api_endpoint = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
    data = requests.get(api_endpoint, timeout=5).json()
    kp_index = float(data[-1][1])

    if kp_index >= 5:
        send_kp_index_notification(kp_index)


if __name__ == "__main__":
    main()
