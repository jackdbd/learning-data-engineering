import datetime
import os
import dlt
from dlt.sources.helpers import requests
import fire
from loguru import logger

pipeline_name = os.path.basename(__file__).replace('.py', '')
repo_root = os.path.abspath(os.path.join(__file__, '..', '..'))

# https://duckdb.org/docs/sql/meta/information_schema.html
duckdb_catalog_name = 'raw'
# When dlt connects to DuckDB, a dlt dataset refers to a DuckDB schema
duckdb_schema_name = 'nyc'
table_name = 'service_requests' # will be: raw.nyc.service_requests
db_filepath = os.path.join(repo_root, 'data', f'{duckdb_catalog_name}.duckdb')

def download(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return {'value': response.json()}
    except requests.HTTPError as e:
        # message = e.response.reason
        message = json.loads(e.response.text)['message']
        return {'error': message}

def run(date_start='2010-01-01', date_stop=datetime.datetime.now().strftime('%Y-%m-%d'), limit=50_000):
    """
    Run the pipeline that extracts the NYC 311 dataset and ingests it into a DuckDB database.
    """
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        dev_mode=False,
        # progress='log'
    )

    base_url = f'https://data.cityofnewyork.us/resource/erm2-nwe9.json'

    # TODO: use a Dagster partition to pass date_start and date_stop
    # The Socrata Open Data API supports querying dates as YYYY-MM-dd and YYYY-MM (and maybe some other formats)
    # https://dev.socrata.com/docs/queries/
    offset = 0
    where = f"created_date between '{date_start}' and '{date_stop}'"

    order  = 'created_date'
    offset = 0
    # https://dev.socrata.com/docs/queries/where
    qs = f'$limit={limit}&$offset={offset}&$order={order}&$where={where}'
    qs = qs.replace(' ', '%20').replace("'", '%27')
    url = f'{base_url}?{qs}'

    logger.debug({
        'pipeline_name': pipeline_name,
        'db_filepath': db_filepath,
        'date_start': date_start, 
        'date_stop': date_stop,
        'limit': limit,
        'url': url
    })

    data = []
    while True:
        result = download(url)

        if result.get('error', None):
            exit(1) # TODO: collect errors, do not exit nor raise exceptions
        
        data_chunk = result['value']
        data = data + data_chunk
        if len(data_chunk) < limit:
            logger.debug(f'fetched {len(data_chunk)} records this time ({len(data)} records in total); limit is {limit}, so there are no more records to fetch')
            break
        else:
            logger.debug(f'fetched {len(data_chunk)} records this time ({len(data)} records in total); limit is {limit}, so there are some other records to fetch')
            offset += limit

        # TODO: what's better?
        # 1. call pipeline.run once with all the data, or
        # 2. call pipeline.run multiple times with smaller chunks of data
        load_info = pipeline.run(
            data_chunk,
            destination=dlt.destinations.duckdb(db_filepath),
            dataset_name=duckdb_schema_name,
            table_name=table_name,
            # https://dlthub.com/docs/general-usage/incremental-loading
            write_disposition='merge',
            primary_key='unique_key'
        )

        load_info.raise_on_failed_jobs()
        logger.debug(load_info.asstr())

        # row_counts = pipeline.last_trace.last_normalize_info
        # logger.debug(f'row_counts {row_counts}')

        # print human friendly extract information
        logger.debug(pipeline.last_trace.last_extract_info)
        # print human friendly normalization information
        print(pipeline.last_trace.last_normalize_info)
    
    # response = requests.get(url)
    # response.raise_for_status()

    # data = response.json()
    # logger.debug(f'fetched {len(data)} records')
    
    # data = [
    #     {'unique_key': 123, 'foo': 'bar'},
    #     {'unique_key': 456, 'foo': 'baz'},
    #     {'unique_key': 123, 'foo': 'this will not be inserted'}
    # ]

    logger.debug(f'view this dlt pipeline as a Streamlit app:')
    logger.debug(f'dlt pipeline {pipeline_name} show')
    logger.debug('try running a query like the following in the SQL editor:')
    logger.debug(f'SELECT * FROM {duckdb_schema_name}.{table_name} LIMIT 3')
    return

if __name__ == '__main__':
  fire.Fire(run)
