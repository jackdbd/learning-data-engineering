import itertools
import os
import sys
import time

import dlt
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dlt.sources.helpers.rest_client import RESTClient

# from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator
from loguru import logger

spacex_client = RESTClient(base_url="https://api.spacexdata.com/v4")
# paginator = SinglePagePaginator()


def print_env():
    print("Environment variables:")
    print(
        f"DATA_WRITER__BUFFER_MAX_ITEMS = {os.environ.get('DATA_WRITER__BUFFER_MAX_ITEMS', '')}"
    )
    print(f"EXTRACT__WORKERS = {os.environ.get('EXTRACT__WORKERS','')}")
    print(f"NORMALIZE__WORKERS = {os.environ.get('NORMALIZE__WORKERS', '')}")
    print(f"LOAD__WORKERS = {os.environ.get('LOAD__WORKERS', '')}")


@dlt.resource(table_name="launches", parallelized=True)
def launches():
    # https://github.com/r-spacex/SpaceX-API/blob/master/docs/launches/v4/all.md
    res = spacex_client.get("/launches")
    res.raise_for_status()
    yield res.json()


@dlt.resource(table_name="rockets", parallelized=True)
def rockets():
    # https://github.com/r-spacex/SpaceX-API/blob/master/docs/rockets/v4/all.md
    res = spacex_client.get("/rockets")
    res.raise_for_status()
    yield res.json()


@dlt.resource(table_name="crew", parallelized=True)
def crew():
    # https://github.com/r-spacex/SpaceX-API/blob/master/docs/crew/v4/all.md
    res = spacex_client.get("/crew")
    res.raise_for_status()
    yield res.json()


@dlt.transformer(table_name="payloads", parallelized=True)
def payloads(launches):
    for d in launches:
        launch_id = d["id"]
        flight_number = d["flight_number"]

        xs = []
        for i, payload_id in enumerate(d["payloads"]):
            xs.append(
                {
                    "id": payload_id,
                    "launch_id": launch_id,
                    "flight_number": d["flight_number"],
                }
            )

        yield xs


payloads_from_launches = launches | payloads


@dlt.source()
def spacex():
    return [launches(), rockets(), crew(), payloads_from_launches()]


# @dlt.resource(table_name="launches", parallelized=True, write_disposition="replace")
# def get_launches():
#     # https://github.com/r-spacex/SpaceX-API/blob/master/docs/launches/v4/all.md
#     params = {
#         "per_page": 100,
#         "sort": "date_utc",
#         "direction": "asc",
#     }
#     for i, page in enumerate(
#         spacex_client.paginate("/launches", params=params, paginator=paginator)
#     ):
#         yield page


def run_loop(
    extract_workers: list[int], normalize_workers: list[int], load_workers: list[int]
):
    param_grid = {
        "EXTRACT__WORKERS": extract_workers,
        "NORMALIZE__WORKERS": normalize_workers,
        "LOAD__WORKERS": load_workers,
    }

    param_combinations = list(
        itertools.product(
            param_grid["EXTRACT__WORKERS"],
            param_grid["NORMALIZE__WORKERS"],
            param_grid["LOAD__WORKERS"],
        )
    )

    results = []
    for i, params in enumerate(param_combinations):
        ew, nw, lw = params
        os.environ["EXTRACT__WORKERS"] = str(ew)
        os.environ["NORMALIZE__WORKERS"] = str(nw)
        os.environ["LOAD__WORKERS"] = str(lw)

        pipeline = dlt.pipeline(
            pipeline_name="spacex_pipeline",
            destination="duckdb",
            dataset_name="spacex_data",
            dev_mode=True,
        )

        logger.debug(
            f"run pipeline with parameters combination {i+1}/{len(param_combinations)} (extract: {ew}, normalize: {nw}, load: {lw})"
        )
        start_time = time.time()
        load_info = pipeline.run(spacex())
        end_time = time.time()

        results.append(
            {
                "EXTRACT__WORKERS": ew,
                "NORMALIZE__WORKERS": nw,
                "LOAD__WORKERS": lw,
                "execution_time": end_time - start_time,
            }
        )

    return pd.DataFrame(results)


def extract(workers: int = 5, buffer_max_items: int = 5_000, verbose=False) -> float:
    pipeline = dlt.pipeline(
        pipeline_name="spacex_pipeline",
        destination="duckdb",
        dataset_name="spacex_data",
        dev_mode=True,
        # progress="log",
    )

    os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = str(buffer_max_items)

    start_time = time.time()
    extract_info = pipeline.extract(
        spacex(), write_disposition="replace", workers=workers
    )
    end_time = time.time()
    elapsed_time = end_time - start_time

    # logger.debug(extract_info)

    load_id = pipeline.list_extracted_load_packages()[0]
    extracted_package = pipeline.get_load_package_info(load_id)
    extracted_jobs = extracted_package.jobs["new_jobs"]
    assert (
        len(extracted_jobs) == 5
    ), "The number of extracted jobs should be equal to the number of resources + 1 for the dlt pipeline state"

    if verbose:
        for i, job in enumerate(extracted_jobs):
            logger.debug(job)
            elapsed = job._asdict()["elapsed"]
            fname = job.job_file_info.file_name()
            table = job.job_file_info.table_name
            logger.debug(
                {
                    "job": i,
                    "intermediate_file": fname,
                    "destination_table": table,
                    "elapsed_seconds": elapsed,
                }
            )

    logger.debug(
        f"extract phase completed in {elapsed_time:.2f} seconds (workers: {workers}, buffer_max_items: {buffer_max_items})"
    )

    return elapsed_time


def extract_loop(
    workers: list[int] = [1, 2, 3, 4], buffer_max_items: int = 5_000
) -> list[float]:
    elapsed_times = []  # in seconds
    for w in workers:
        # os.environ["EXTRACT__WORKERS"] = str(w)
        elapsed_times.append(extract(workers=w, buffer_max_items=buffer_max_items))

    return elapsed_times


def run_loop_and_plot():
    # extract_workers = [2]
    # normalize_workers = [2]
    # load_workers = [2]
    extract_workers = [1, 2, 3, 4]
    normalize_workers = [1, 2, 3, 4]
    load_workers = [1, 2, 3, 4]

    buffer_max_items = os.environ.get("DATA_WRITER__BUFFER_MAX_ITEMS", "5000")

    df = run_loop(
        extract_workers=extract_workers,
        normalize_workers=normalize_workers,
        load_workers=load_workers,
    )

    cond = df["execution_time"] == df["execution_time"].min()
    filtered_df = df.where(cond, np.nan)
    min_indices = np.where(cond)
    i_best_combination = min_indices[0][0]

    print(
        f"Best combination of parameters for when DATA_WRITER__BUFFER_MAX_ITEMS = {buffer_max_items}"
    )
    print(df.iloc[i_best_combination])

    fig, ax = plt.subplots(figsize=(12, 6))
    plt.stem(df["execution_time"], linefmt="blue")
    plt.stem(filtered_df["execution_time"], linefmt="red")

    x_offset = 1
    y_offset = 0.5
    xy = (
        i_best_combination + x_offset,
        df.iloc[i_best_combination]["execution_time"] + y_offset,
    )
    bbox = dict(boxstyle="round", fc="0.7")
    ax.annotate(
        f"""
    EXTRACT__WORKERS = {df.iloc[i_best_combination]["EXTRACT__WORKERS"]}
    NORMALIZE__WORKERS = {df.iloc[i_best_combination]["NORMALIZE__WORKERS"]}
    LOAD__WORKERS = {df.iloc[i_best_combination]["LOAD__WORKERS"]}
    """,
        xy,
        bbox=bbox,
    )

    plt.title(
        f"Total execution time as the number of workers varies (buffer_max_items = {buffer_max_items})"
    )
    plt.xlabel("Combination (index)")
    plt.ylabel("Execution time")
    fpath = os.path.join(
        os.getcwd(),
        "assets",
        "images",
        f"run-times-times-by-workers-with-buffer_max_items-{buffer_max_items}.png",
    )
    plt.savefig(fpath)


def extract_loop_and_plot(
    workers: list[int] = [1, 2, 3, 4], buffer_max_items: int = 5_000
) -> None:
    # https://matplotlib.org/stable/gallery/lines_bars_and_markers/linestyles.html
    # https://matplotlib.org/stable/gallery/color/color_demo.html#sphx-glr-gallery-color-color-demo-py
    specs = [
        {
            "buffer_max_items": 1,
            "color": "blue",
            "marker": "o",
            "linestyle": "dotted",
        },
        {
            "buffer_max_items": 200,
            "color": "peachpuff",
            "marker": "o",
            "linestyle": "dashdot",
        },
        {
            "buffer_max_items": 5000,
            "color": "green",
            "marker": "o",
            "linestyle": "dashed",
        },
        {
            "buffer_max_items": 10000,
            "color": "red",
            "marker": "o",
            "linestyle": "solid",
        },
    ]

    fig, axes = plt.subplots(figsize=(12, 6))

    lines = []
    labels = []
    for d in specs:
        seconds = extract_loop(workers=workers, buffer_max_items=d["buffer_max_items"])

        (line,) = axes.plot(
            workers,
            seconds,
            color=d["color"],
            marker=d["marker"],
            linestyle=d["linestyle"],
        )
        lines.append(line)
        labels.append(d["buffer_max_items"])

    # buffer_max_items can be set with the environment variable DATA_WRITER__BUFFER_MAX_ITEMS
    axes.legend(lines, labels, title="buffer_max_items", loc="upper right")

    plt.xlabel("EXTRACT__WORKERS")
    plt.xticks(range(1, max(workers) + 1))
    plt.ylabel("Extract phase execution time (s)")
    fig.suptitle(f"Extract phase as workers and buffer_max_items vary")

    fpath = os.path.join(
        os.getcwd(),
        "assets",
        "images",
        f"extract-times-by-workers-and-buffer_max_items.png",
    )
    plt.savefig(fpath)
    print(f"Saved plot to {fpath}")


if __name__ == "__main__":
    # The first request will probably be not cached, while the other ones will
    # be cached. By making a single request now, we avoid this issue.
    extract()

    # extract(workers=5, buffer_max_items=1)
    # extract(workers=5, buffer_max_items=100)
    # extract(workers=5, buffer_max_items=200)
    # extract(workers=5, buffer_max_items=1_000)
    # extract(workers=5, buffer_max_items=5_000)
    # extract(workers=5, buffer_max_items=10_000)

    workers = [1, 2, 3, 4, 5, 6, 7, 8]
    buffer_max_items = 1000
    # extract_loop(workers=workers, buffer_max_items=buffer_max_items)
    extract_loop_and_plot(workers=workers, buffer_max_items=buffer_max_items)

    # run_loop_and_plot()
