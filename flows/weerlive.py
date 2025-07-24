from dotenv import load_dotenv
load_dotenv()

from prefect import flow, task
from datetime import timedelta
import os
from common.api_utils import fetch_json


@task(
    persist_result=True,
    cache_key_fn=lambda _1, parameters: f"weerlive_data_{parameters.get('location', 'default')}",
    cache_expiration=timedelta(15 * 60),
    retries=3,
    retry_delay_seconds=5,
)
def get_weerlive_data(location=None) -> dict:
    wl_key = os.environ['WL_KEY']
    wl_location = os.environ['WL_LOCATION'] if location is None else location
    query = f'http://weerlive.nl/api/json-data-10min.php?key={wl_key}&locatie={wl_location}'
    data = fetch_json(query)
    return data['liveweer'][0]


@flow
def main():
    data = get_weerlive_data("Berg en Dal")
    print(data)

if __name__ == "__main__":
    main()
