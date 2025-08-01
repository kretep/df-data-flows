from dotenv import load_dotenv
load_dotenv()

from prefect import flow, task
import os
from common.api_utils import fetch_json
from common.ntfy_utils import send_notification


@task
def get_kp_index() -> int:
    url = os.getenv("KP_INDEX_URL")
    data = fetch_json(url)
    kp_index = float(data[-1][1])
    return kp_index


@task
def send_kp_index_notification(value):
    send_notification("kp_index", "Kp Index Alert", f"Kp Index: {value}")


@flow
def kp_index_notify():
    kp_index = get_kp_index()
    if kp_index >= 5:
        send_kp_index_notification(kp_index)


if __name__ == "__main__":
    kp_index_notify()
