import debugpy
from loguru import logger

# from some_module import some_function


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
some_function()
