import datetime
from dotenv import load_dotenv
load_dotenv()

from prefect import flow, task
import os
from sqlalchemy.exc import IntegrityError
from common.api_utils import fetch_json
from common.database_utils import write_to_database

@task
def get_enphase_data():
    host = os.getenv("EN_HOST")
    token = os.getenv("EN_TOKEN")
    url = f"http://{host}/production.json"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    response = fetch_json(url, headers=headers, verify_ssl=False)
    return response

@task
def process_data(data: dict) -> dict:
    data = data['production'][0]
    timestamp = data['readingTime']
    if timestamp > 0:
        readingTime = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
    else:
        # For some reason, readingTime will be 0 when no power is being produced,
        # but the wh_lifetime is still valid.
        readingTime = datetime.datetime.now(tz=datetime.timezone.utc).replace(microsecond=0)
    return {
        'datetime': readingTime,
        'w_now': data['wNow'],
        'wh_lifetime': data['whLifetime']
    }

@task
def store_data(data):
    database_connection = os.getenv("DATABASE_CONNECTION")
    table_name = os.getenv("EN_DB_TABLE")
    try:
        write_to_database(database_connection, table_name, data)
    except IntegrityError as e:
        # Check if it's a unique constraint violation
        if "unique constraint" in str(e.orig).lower() or "duplicate key" in str(e.orig).lower():
            # Silently ignore unique constraint violations
            print("Warning: entry already exists")
        else:
            # Re-raise other integrity errors
            raise

@flow(name="Enphase data ETL")
def enphase_data_etl():
    data = get_enphase_data()
    print(f"Fetched data: {data}")
    processed_data = process_data(data)
    store_data(processed_data)


def main():
    data = get_enphase_data()
    print(data)

if __name__ == "__main__":
    enphase_data_etl()
