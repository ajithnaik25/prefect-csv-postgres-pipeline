from prefect import task, get_run_logger
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import execute_batch
import pandas as pd
from config.config import DB_CONFIG, DB_TABLE


@task(name="load-data", retries=2, retry_delay_seconds=5)
def load(df: pd.DataFrame):
    logger = get_run_logger()
    logger.info("Connecting to database")

    conn: connection = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    logger.info("Preparing bulk data")

    data = df[["name", "email", "age", "city"]].values.tolist()

    logger.info(f"Inserting data into table: {DB_TABLE}")

    try:
        query = f"""
            INSERT INTO {DB_TABLE} (name, email, age, city)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING
        """

        execute_batch(cursor, query, data)

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting data: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

    logger.info("Data Inserted Successfully")
