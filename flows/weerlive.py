from dotenv import load_dotenv
load_dotenv()

from prefect import flow, task
from prefect.cache_policies import TASK_SOURCE, INPUTS
from datetime import datetime, timedelta
import os
from common.api_utils import fetch_json
from common.database_utils import write_to_database


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
    return { key: data[key] for key in keys }

@task
def store_weerlive_data(data: dict):
    connection = os.getenv("DATABASE_CONNECTION")
    table_name = os.getenv("WL_TABLE")
    write_to_database(connection, table_name, data)


def maybe_invalidate_cache(location: str, data: dict) -> bool:
    """
    Custom logic for invalidating the cache based on the data's timestamp.
    Scenario:
        result time  21:45
        cache time   21:53
        current time 21:57
    Cache is not outdated, but results are stale, so we invalidate the cache.
    Invalidation is done by removing the cache file.
    """
    date_result = datetime.strptime(data.get('time'), '%d-%m-%Y %H:%M')
    date_now = datetime.now()
    print(f"Age of result: {date_now - date_result} seconds")
    if (date_now - date_result).total_seconds() > 720:
        # Invalidating the cache 12 minutes (not 10) after the result time,
        # as the data takes some time to be updated.
        print("Cache is outdated, invalidating...")
        cache_filename = cache_key_fn(None, {'location': location})
        cache_dir = os.getenv("PREFECT_CACHE_DIR")
        if not cache_dir:
            print("PREFECT_CACHE_DIR is not set, cannot invalidate cache.")
            return False
        path = os.path.join(cache_dir, cache_filename)
        if os.path.exists(path):
            os.remove(path)
            print(f"Cache file {path} removed.")
        else:
            print(f"Cache file {path} does not exist.")
        return True
    return False

@flow
def weerlive_data_etl():
    location = "Berg en Dal"
    data = get_weerlive_data(location)
    print(data)
    if maybe_invalidate_cache(location, data):
        data = get_weerlive_data(location)  # Re-fetch
        print(data)
    processed_data = process_weerlive_data(data)
    store_weerlive_data(processed_data)

if __name__ == "__main__":
    weerlive_data_etl()
