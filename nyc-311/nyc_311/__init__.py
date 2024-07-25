from dagster import Definitions, load_assets_from_modules
from .assets import service_requests
from .resources import database_resource

service_requests_assets = load_assets_from_modules([service_requests])
all_jobs = []
all_schedules = []
all_sensors = []

defs = Definitions(
    assets=[*service_requests_assets],
    jobs=all_jobs,
    resources={
        "database": database_resource,
    },
    schedules=all_schedules,
    sensors=all_sensors,
)
