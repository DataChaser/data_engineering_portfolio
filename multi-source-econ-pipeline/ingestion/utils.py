import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse="ECON_WH",
        database="ECON_DB",
        schema="RAW"
    )

def execute_query(conn, query, data=None):
    cursor = conn.cursor()
    if data:
        cursor.executemany(query, data)
    else:
        cursor.execute(query)
    conn.commit()
    cursor.close()