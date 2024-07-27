import requests
from datetime import datetime
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    asset
)
from dagster_duckdb import DuckDBResource
from . import constants

@asset(
    group_name="raw_data",
    metadata={"data_provider": "NYC Open Data", "data_license": "todo"}
)
def taxi_zones_file(context: AssetExecutionContext) -> MaterializeResult:
    """
    The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    # https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc
    zones = requests.get(
        f"https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD"
    )

    with open(constants.NYC_TAXI_ZONES_FILE_PATH, "wb") as f:
        f.write(zones.content)
    
    return MaterializeResult(
        metadata={
            "materialization_date": datetime.today().strftime('%Y-%m-%d')
        }
    )

@asset(
    deps=["taxi_zones_file"],
    group_name="duckdb"
)
def taxi_zones_table(
    context: AssetExecutionContext,
    database: DuckDBResource
) -> MaterializeResult:
    """
    The taxi zones, loaded into a DuckDB database.
    """
    query = f"""
        CREATE OR REPLACE TABLE {constants.NYC_TAXI_ZONES_TABLE} AS (
            SELECT
                LocationID as zone_id,
                zone,
                borough,
                the_geom as geometry
            FROM '{constants.NYC_TAXI_ZONES_FILE_PATH}'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)
