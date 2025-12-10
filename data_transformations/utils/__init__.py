import os
from dagster import get_dagster_logger

logger = get_dagster_logger()


def attach_schema(conn, database: str):
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    try:
        conn.sql(
            f"""
        CREATE SCHEMA IF NOT EXISTS {database};
        CALL postgres_attach('postgresql://{DB_USERNAME}:{DB_PASSWORD}@{POSTGRES_HOST}/{database}',
                              source_schema='{database}',
                              filter_pushdown='true',
                              sink_schema='{database}');
        """
        )
        logger.info(f"Attached schema: {database}")
    except Exception as e:
        logger.error(f"Failed to attach schema: {database} - {e}")
