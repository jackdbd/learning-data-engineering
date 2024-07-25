# nyc_311

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

## Setup & Installation

Duplicate the `.env.example` file, rename it to `.env`, and fill your environment variables.

There is nothing to install, since the developer environment is created and managed by devenv.

Create a directory for the raw data:

```sh
mkdir -p nyc-311/data/raw
```

Download the raw CSV dataset using a download tool (it's much faster than using Python requests). For example wget...

```sh
wget -O nyc-311/data/raw/311_service_requests.csv https://data.cityofnewyork.us/api/views/erm2-nwe9/rows.csv?accessType=DOWNLOAD
```

...or aria2:

```sh
aria2c --out nyc-311/data/raw/311_service_requests.csv --split 4 https://data.cityofnewyork.us/api/views/erm2-nwe9/rows.csv?accessType=DOWNLOAD
```

## Development

Start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

## Tests

Run all tests found in the `nyc_311_tests` directory:

```sh
pytest nyc_311_tests
```

## Deploy to Dagster+

Check out the [Dagster+ documentation](https://docs.dagster.io/dagster-plus).

## Other

Use [VisiData](https://www.visidata.org/) to inspect a few rows of the original CSV dataset:

```sh
head -n 100 nyc-311/data/raw/311_service_requests.csv | vd -f csv
```

Convert the CSV dataset to Parquet:

```sh
csv2parquet nyc-311/data/raw/311_service_requests.csv nyc-311/data/raw/311_service_requests.parquet
```
