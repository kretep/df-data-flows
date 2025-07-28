from datetime import datetime
from sqlalchemy import (MetaData, create_engine, insert)
import pandas as pd

def write_to_database(connection: str, table_name: str, row: dict):
    engine = create_engine(connection)
    with engine.begin() as conn:
        # Get table from database
        metadata = MetaData()
        metadata.reflect(bind=conn, only=[table_name])
        table = metadata.tables[table_name]

        # Create & execute query
        query = insert(table).values(**row)
        conn.execute(query)
        print(datetime.now(), 'Data written to database')

def write_dataframe_to_database(connection: str, table_name: str, df: pd.DataFrame):
    engine = create_engine(connection)
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(datetime.now(), 'Data written to database')
