import shutil
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    PipesSubprocessClient,
    asset,
    file_relative_path,
)
from . import constants

@asset
def service_requests_file(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
) -> MaterializeResult:
    """
    The raw CSV file for the [NYC 311 service requests dataset](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9/about_data).
    Sourced from the NYC Open Data portal.
    """

    # https://docs.dagster.io/concepts/dagster-pipes/subprocess/create-subprocess-asset
    cmd = [
        shutil.which("aria2c"),
        # https://docs.dagster.io/_apidocs/utilities#dagster.file_relative_path
        # f"--out={file_relative_path(__file__, '../../data/raw/311_service_requests.csv')}",
        "--out=./data/raw/311_service_requests.csv",
        "--split=4",
        constants.NYC_311_SERVICE_REQUESTS_CSV_FILE_URL
    ]

    return pipes_subprocess_client.run(command=cmd, context=context).get_materialize_result()
