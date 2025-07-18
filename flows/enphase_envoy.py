import datetime
from dotenv import load_dotenv
load_dotenv()

from prefect import flow, task
import os
from api_utils import fetch_json
from database_utils import write_to_database

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
    readingTime = datetime.datetime.fromtimestamp(data['readingTime'], tz=datetime.timezone.utc)
    return {
        'datetime': readingTime,
        'w_now': data['wNow'],
        'wh_lifetime': data['whLifetime']
    }

@task
def store_data(data):
    database_connection = os.getenv("DATABASE_CONNECTION")
    table_name = os.getenv("EN_DB_TABLE")
    write_to_database(database_connection, table_name, data)

@flow(name="Enphase data ETL")
def enphase_data_etl():
    data = get_enphase_data()
    processed_data = process_data(data)
    store_data(processed_data)


def main():
    data = get_enphase_data()
    print(data)

if __name__ == "__main__":
    enphase_data_etl()
