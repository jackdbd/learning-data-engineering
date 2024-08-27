import debugpy
from loguru import logger

from dlt_workshop_homework.spacex_pipeline import (
    extract,
    extract_loop_and_plot,
    print_env,
    run_loop_and_plot,
)


def some_function():
    a = 1 + 2
    return a


host = "localhost"
port = 5678
logger.debug(f"Start debug server on {host}:{port}")
debugpy.listen((host, 5678))
logger.debug("Waiting for debugger attach...")
debugpy.wait_for_client()  # This will block execution until the debugger is attached
logger.debug("Debugger attached.")

# functions that you actually want to debug
# print_env()
# extract()
# extract_loop_and_plot()
run_loop_and_plot()
