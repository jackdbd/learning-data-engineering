import requests
from dagster import asset
from . import constants

@asset
def service_requests_file():
    """
    The raw CSV file for the NYC 311 service requests dataset. Sourced from the NYC Open Data portal.
    """
    rows = requests.get(
        f"https://data.cityofnewyork.us/api/views/erm2-nwe9/rows.csv?accessType=DOWNLOAD"
    )

    with open(constants.DATASET_FILE_PATH, "wb") as output_file:
        output_file.write(rows.content)
