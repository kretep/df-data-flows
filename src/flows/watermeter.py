import os
from dotenv import load_dotenv
load_dotenv()
from prefect import flow, task
from common.database_utils import write_to_database
from common.api_utils import fetch_json


@task
def fetch_data():
    url = os.getenv("HW_WATER_ENDPOINT")
    return fetch_json(url)

@task
def process_data(data):
    keys = ['total_liter_m3']
    return {key: data[key] for key in keys if key in data}

@task
def store_data(data):
    database_connection = os.getenv("DATABASE_CONNECTION")
    table_name = os.getenv("HW_WATER_TABLE")
    write_to_database(database_connection, table_name, data)

@flow(name="Water Meter ETL")
def watermeter_etl():
    data = fetch_data()
    processed_data = process_data(data)
    store_data(processed_data)


if __name__ == "__main__":
    watermeter_etl()
