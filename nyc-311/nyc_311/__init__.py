from dagster import Definitions, load_assets_from_modules
from .assets import service_requests, zones, outputs
from .resources import (
    database,
    pipes_subprocess_client
)

service_requests_assets = load_assets_from_modules([service_requests])
zones_assets = load_assets_from_modules([zones])
outputs_assets = load_assets_from_modules([outputs])

all_jobs = []
all_schedules = []
all_sensors = []

defs = Definitions(
    assets=[*service_requests_assets, *zones_assets, *outputs_assets],
    jobs=all_jobs,
    resources={
        "database": database,
        "pipes_subprocess_client": pipes_subprocess_client
    },
    schedules=all_schedules,
    sensors=all_sensors,
)
