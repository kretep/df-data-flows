from dotenv import load_dotenv
load_dotenv()

import os
from prefect import flow, task
from common.api_utils import fetch_json
from common.database_utils import write_to_database


@task
def fetch_homewizard_data():
    url = os.getenv("HW_P1_ENDPOINT")
    data = fetch_json(url)
    return data

@task
def process_homewizard_data(data):
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
def store_homewizard_data(data):
    # Write the data to the database
    connection = os.getenv("DATABASE_CONNECTION")
    table_name = os.getenv("HW_P1_TABLE")
    write_to_database(connection, table_name, data)

@flow(name="Homewizard data ETL")
def homewizard_etl():
    data = fetch_homewizard_data()
    processed_data = process_homewizard_data(data)
    store_homewizard_data(processed_data)

if __name__ == "__main__":
    homewizard_etl()
