from dagster import (
    EnvVar,
    PipesSubprocessClient
)
from dagster_duckdb import DuckDBResource

database = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE"),
    # connection_config={}
)

pipes_subprocess_client = PipesSubprocessClient()
