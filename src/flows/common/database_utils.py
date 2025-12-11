from datetime import datetime
from sqlalchemy import MetaData, create_engine, insert
from sqlalchemy.exc import IntegrityError
import pandas as pd

def write_to_database(connection: str, table_name: str, row: dict, ignore_unique_error: bool = False):
    engine = create_engine(connection)
    with engine.begin() as conn:
        # Get table from database
        metadata = MetaData()
        metadata.reflect(bind=conn, only=[table_name])
        table = metadata.tables[table_name]

        # Create & execute query
        try:
            query = insert(table).values(**row)
            conn.execute(query)
            print(datetime.now(), 'Data written to database')
        except IntegrityError as e:
            # Check if it's a unique constraint violation
            if ignore_unique_error and ("unique constraint" in str(e.orig).lower() or "duplicate key" in str(e.orig).lower()):
                # Silently ignore unique constraint violations
                print("Warning: entry already exists")
            else:
                # Re-raise other integrity errors
                raise

def write_dataframe_to_database(connection: str, table_name: str, df: pd.DataFrame):
    engine = create_engine(connection)
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(datetime.now(), 'Data written to database')
