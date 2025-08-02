from dotenv import load_dotenv
load_dotenv()

from prefect import flow, task
from prefect.cache_policies import TASK_SOURCE, INPUTS
import os
from datetime import datetime, timedelta, timezone
from common.api_utils import fetch_json
from common.database_utils import write_to_database
from common.prefect_utils import maybe_invalidate_cache


def cache_key_fn(task, args):
    return "weerlive_data_" + args.get('location', 'default')

@task(
    persist_result=True,
    cache_key_fn=cache_key_fn,
    cache_expiration=timedelta(minutes=10),
    cache_policy=TASK_SOURCE + INPUTS,
    retries=3,
    retry_delay_seconds=5,
)
def get_weerlive_data(location=None) -> dict:
    print(f"Fetching Weerlive data for location: {location}")
    wl_key = os.environ['WL_KEY']
    wl_location = os.environ['WL_LOCATION'] if location is None else location
    query = f'http://weerlive.nl/api/json-data-10min.php?key={wl_key}&locatie={wl_location}'
    data = fetch_json(query)
    return data['liveweer'][0]

@task
def process_weerlive_data(data: dict) -> dict:
    # Filter the keys that we want
    keys = ['temp', 'gtemp', 'samenv', 'lv', 'windr', 'winds', 'luchtd', 'dauwp', 'zicht', 'image']
    processed_data = {key: data[key] for key in keys}
    processed_data['datetime'] = datetime.fromtimestamp(int(data['timestamp']), tz=timezone.utc)
    return processed_data

@task
def store_weerlive_data(data: dict):
    connection = os.getenv("DATABASE_CONNECTION")
    table_name = os.getenv("WL_TABLE")
    write_to_database(connection, table_name, data, ignore_unique_error=True)


@flow
def weerlive_data_etl():
    location = "Berg en Dal"
    data = get_weerlive_data(location)
    print(data)
    date_result = datetime.strptime(data.get('time'), '%d-%m-%Y %H:%M')
    cache_key = cache_key_fn(None, {'location': location})
    if maybe_invalidate_cache(date_result, cache_key, 720):
        data = get_weerlive_data(location)  # Re-fetch
        print(data)
    processed_data = process_weerlive_data(data)
    store_weerlive_data(processed_data)

if __name__ == "__main__":
    weerlive_data_etl()
