from dotenv import load_dotenv
load_dotenv()

from prefect import flow, task
import os
from common.api_utils import fetch_json
from common.database_utils import write_to_database


@task
def fetch_p1_meter_data():
    url = os.getenv("HW_P1_ENDPOINT")
    data = fetch_json(url)
    return data

@task
def process_p1_meter_data(data):
    # Filter the keys that we want
    keys = [
        'active_tariff',
        'total_power_import_kwh',
        'total_power_import_t1_kwh',
        'total_power_import_t2_kwh',
        'total_power_export_kwh',
        'total_power_export_t1_kwh',
        'total_power_export_t2_kwh',
        'active_power_w',
        'active_power_l1_w',
        'active_power_l2_w',
        'active_power_l3_w',
    ]
    data = { key: data[key] for key in keys }
    return data

@task
def store_p1_meter_data(data):
    # Write the data to the database
    connection = os.getenv("DATABASE_CONNECTION")
    table_name = os.getenv("HW_P1_TABLE")
    write_to_database(connection, table_name, data)

@flow(name="p1_meter data ETL")
def p1_meter_etl():
    data = fetch_p1_meter_data()
    processed_data = process_p1_meter_data(data)
    store_p1_meter_data(processed_data)

if __name__ == "__main__":
    p1_meter_etl()
