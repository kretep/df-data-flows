from dotenv import load_dotenv
load_dotenv()

from prefect import flow, task
from datetime import datetime, timedelta
import os
from common.api_utils import fetch_json
from common.prefect_utils import maybe_invalidate_cache

def cache_key_fn(task, args):
    return "nightscout_data"

@task(
    persist_result=True,
    cache_key_fn=cache_key_fn,
    cache_expiration=timedelta(minutes=5),
    retries=3,
    retry_delay_seconds=5,
)
def fetch_entries() -> list[dict]:
    host = os.environ['NS_URL']
    query_url = f'{host}/api/v1/entries/sgv.json?count=10'
    data = fetch_json(query_url)
    if type(data) == list and (len(data) == 0 or type(data[0]) == dict):
        return data
    raise ValueError(f"Invalid data: {data}")

def maybe_convert_units(mgdl):
    #TODO: make  config/env var
    return round(mgdl / 18.0182, 1) if True else mgdl

def filter_bgs(entries):
    bgs = [e.copy() for e in entries if 'sgv' in e]
    for bg in bgs:
        bg['sgv'] = int(bg['sgv'])
    return bgs

def get_minutes_ago(timestamp):
    return int((datetime.timestamp(datetime.now()) - timestamp / 1000) / 60)

def get_direction(entry):
    return {
        'DoubleUp': u'⇈',
        'SingleUp': u'↑',
        'FortyFiveUp': u'↗',
        'Flat': u'→',
        'FortyFiveDown': u'↘',
        'SingleDown': u'↓',
        'DoubleDown': u'⇊',
    }.get(entry.get('direction'), '-')

def get_delta(last, second_to_last):
    if (last['date'] - second_to_last['date']) / 1000 > 1000:
        return '?'
    return ('+' if last['sgv'] >= second_to_last['sgv'] else u'−') + \
        str(abs(maybe_convert_units(last['sgv'] - second_to_last['sgv'])))

@task
def extract_current_value(entries: list[dict]) -> dict:
    minutes_ago = -1
    bgs = filter_bgs(entries)
    if len(bgs) > 1:
        last, second_to_last = bgs[0:2]
        minutes_ago = get_minutes_ago(last['date'])

    return {
        "sgv": maybe_convert_units(last['sgv']),
        "direction": get_direction(last),
        "minutes_ago": minutes_ago,
        "delta": get_delta(last, second_to_last),
        "date_string": last['dateString']
    }

@flow(name="Nightscout data ETL")
def get_nightscout_data() -> dict:
    entries = fetch_entries() # Fetch the latest entries, potentially from cache
    
    # Sort entries by date in descending order
    entries.sort(key=lambda x: x['date'], reverse=True)

    # Check how old the results are and invalidate cache if necessary
    results_date = datetime.fromtimestamp(entries[0]['date'] / 1000)
    if maybe_invalidate_cache(results_date, cache_key_fn(None, {}), 300):
        entries = fetch_entries() # Re-fetch

    processed_data = extract_current_value(entries)
    return processed_data

if __name__ == "__main__":
    data = get_nightscout_data()
    # data = get_data()
    print(data)
