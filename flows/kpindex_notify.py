from dotenv import load_dotenv
load_dotenv()

from prefect import flow, task
import requests
from common.ntfy_utils import send_notification


@task
def get_kp_index() -> int:
    api_endpoint = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
    data = requests.get(api_endpoint, timeout=5).json()
    kp_index = float(data[-1][1])
    return kp_index


@task
def send_kp_index_notification(value):
    send_notification("kp_index", "Kp Index Alert", f"Kp Index: {value}")


@flow
def main():
    kp_index = get_kp_index()
    if kp_index >= 1:
        send_kp_index_notification(kp_index)


if __name__ == "__main__":
    main()
