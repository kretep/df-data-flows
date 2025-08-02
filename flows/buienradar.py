from dotenv import load_dotenv
load_dotenv()

from prefect import task
import os
from common.api_utils import fetch_text


@task
def fetch_buienradar_data() -> str:
    url = os.getenv("BR_URL")
    lat = os.getenv("EIDASH_LATITUDE")
    lon = os.getenv("EIDASH_LONGITUDE")
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


if __name__ == "__main__":
    data = fetch_buienradar_data()
    processed_data = process_data(data)
    print(processed_data)
