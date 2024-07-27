import os
import shutil
import pandas as pd
from datetime import datetime
from dagster import (
    AssetExecutionContext,
    AssetMaterialization,
    MaterializeResult,
    PipesSubprocessClient,
    asset,
    file_relative_path,
)
from dagster_duckdb import DuckDBResource
from . import constants

@asset(
    group_name="raw_data",
    metadata={"data_provider": "NYC Open Data", "data_license": "todo"}
)
def service_requests_file(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
) -> MaterializeResult:
    """
    The raw CSV file for the [NYC 311 service requests dataset](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9/about_data).
    Sourced from the NYC Open Data portal.
    """
    asset_key = "service_requests_file"

    context.log.info(f"check whether path {constants.DATASET_FILE_PATH} exists")
    if os.path.exists(constants.DATASET_FILE_PATH):
        context.log_event(
            AssetMaterialization(
                asset_key=asset_key,
                description=f"{constants.DATASET_FILE_PATH} found on filesystem, so we mark it as already materialized",
                metadata={
                    "path": constants.DATASET_FILE_PATH,
                    "materialization_date": datetime.today().strftime('%Y-%m-%d')
                },
                tags={
                    "file_format": "csv",
                    "color": "red"
                }
            )
        )
        return MaterializeResult(
            # asset_key=asset_key,
            metadata={
                "materialization_date": datetime.today().strftime('%Y-%m-%d')
            }
        )

    context.log.info(f"{constants.DATASET_FILE_PATH} not found. Download it now using aria2")
    # https://docs.dagster.io/concepts/dagster-pipes/subprocess/create-subprocess-asset
    cmd = [
        shutil.which("aria2c"),
        # https://docs.dagster.io/_apidocs/utilities#dagster.file_relative_path
        # f"--out={file_relative_path(__file__, '../../data/raw/311_service_requests.csv')}",
        f"--out={constants.DATASET_FILE_PATH}",
        "--split=4",
        constants.NYC_311_SERVICE_REQUESTS_CSV_FILE_URL
    ]

    return pipes_subprocess_client.run(command=cmd, context=context).get_materialize_result()

@asset(
    deps=["service_requests_file"],
    group_name="duckdb",
)
def service_requests_table(
    context: AssetExecutionContext,
    database: DuckDBResource
) -> MaterializeResult:
    """
    DuckDB table created from the CSV file of 311 service requests.
    """

    table = constants.NYC_311_SERVICE_REQUESTS_TABLE
    chunk_size = 1000  # number of rows per chunk

    context.log.info(f"create table {table} in DuckDB if does not exist")

    # DuckDB General-Purpose Data Types
    # https://duckdb.org/docs/sql/data_types/overview.html

    # Conversion between DuckDB and Python
    # https://duckdb.org/docs/api/python/conversion.html

    # https://dagster.io/blog/duckdb-data-lake

    # mapping between CSV columns and DuckDB columns
    columns = {
        "Unique Key": "unique_key",
        "Created Date": "created_date",
        "Closed Date": "closed_date",
        "Agency": "agency",
        "Agency Name": "agency_name",
        "Complaint Type": "complaint_type",
        "Descriptor": "descriptor",
        "Location Type": "location_type",
        "Incident Zip": "incident_zip",
        "Incident Address": "incident_address",
        "City": "city",
        "Status": "status",
        "Borough": "borough",
        "X Coordinate (State Plane)": "x_coord",
        "Y Coordinate (State Plane)": "y_coord",
        "Latitude": "latitude",
        "Longitude": "longitude",
        "Location": "location"
    }
    
    with database.get_connection() as conn:
        # Read the CSV file in chunks, selecting only the required columns
        chunk_iter = pd.read_csv(constants.DATASET_FILE_PATH, usecols=columns.keys(), chunksize=chunk_size)
        
        for i, df in enumerate(chunk_iter):
            # TODO: to be removed 
            if i >= 20: break

            df.rename(columns=columns, inplace=True)
            df['created_date'] = pd.to_datetime(df['created_date'], format='%m/%d/%Y %I:%M:%S %p')
            df['closed_date'] = pd.to_datetime(df['closed_date'], format='%m/%d/%Y %I:%M:%S %p')

            context.log.info(f"ingest chunk {i} into DuckDB table {table} (chunk_size: {chunk_size} rows)")
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df")
            conn.execute(f"INSERT INTO {table} SELECT * FROM df")

@asset(
    deps=["service_requests_table"],
    group_name="outputs",
)
def service_requests_stats(
    context: AssetExecutionContext,
    database: DuckDBResource
) -> MaterializeResult:
    """
    JSON file created from the DuckDB table of 311 service requests.
    """
    query = f"""
        SELECT
          agency_name,
          COUNT(complaint_type) as complaints,
        FROM {constants.NYC_311_SERVICE_REQUESTS_TABLE}
        GROUP BY agency_name
        ORDER BY complaints DESC;
    """
    with database.get_connection() as conn:
        stats = conn.execute(query).fetch_df()

    context.log.info(f"stats\n{stats.to_string()}")

    with open(constants.STATS_FILE_PATH, 'w') as f:
        f.write(stats.to_json())

    return MaterializeResult(
        metadata={
            "materialization_date": datetime.today().strftime('%Y-%m-%d')
        }
    )
