from dagster import Definitions, load_assets_from_modules
from .assets import service_requests
from .resources import (
    database,
    pipes_subprocess_client
)

service_requests_assets = load_assets_from_modules([service_requests])
all_jobs = []
all_schedules = []
all_sensors = []

defs = Definitions(
    assets=[*service_requests_assets],
    jobs=all_jobs,
    resources={
        "database": database,
        "pipes_subprocess_client": pipes_subprocess_client
    },
    schedules=all_schedules,
    sensors=all_sensors,
)
