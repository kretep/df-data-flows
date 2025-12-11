from dotenv import load_dotenv
load_dotenv()

import os
from prefect import flow, task
from common.api_utils import fetch_text


@task
def fetch_buienradar_data() -> str:
    url = os.getenv("BR_URL")
    lat = os.getenv("BR_LATITUDE")
    lon = os.getenv("BR_LONGITUDE")
    if lat is None or lon is None:
        return ''
    url = f'{url}?lat={lat}&lon={lon}'
    return fetch_text(url)

@task
def process_data(data: str) -> list[tuple[str, str]]:
    # Split lines, filter empty values
    lines = list(filter(None, data.split('\n')))
    tuples = [(line.split('|')[1].strip(), line.split('|')[0].strip().replace(",", ".")) for line in lines]
    return tuples

@flow
def fetch_and_process_buienradar_data() -> list[tuple[str, str]]:
    raw_data = fetch_buienradar_data()
    processed_data = process_data(raw_data)
    return processed_data

if __name__ == "__main__":
    data = fetch_and_process_buienradar_data()
    print(data)
