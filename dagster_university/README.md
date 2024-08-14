# dagster_university

This is a [Dagster](https://dagster.io/) project made to accompany Dagster University coursework.

## Setup & Installation

Duplicate the `.env.example` file, rename it to `.env`, and fill your environment variables.

There is nothing to install, since the developer environment is created and managed by devenv.

## Development

Start the Dagster UI web server:

```sh
# from this directory
dagster dev

# from the repository root (using a devenv script)
dagster-dev
```

Visit http://localhost:3000 to see the Dagster UI.

Data lineage of the `default` [asset group](https://docs.dagster.io/concepts/assets/software-defined-assets#grouping-assets):

http://127.0.0.1:3000/locations/dagster_university/asset-groups/default

View the Manhattan map in the browser (you need to materialize it first):

```sh
python -m http.server -d dagster_university/data/outputs/ 8888
```

## Tests

Run all tests found in the `dagster_university_tests` directory:

```sh
# from this directory
pytest -v dagster_university_tests

# from the repository root (using a devenv script)
dagster-test
```

## Deploy to Dagster+

Check out the [Dagster+ documentation](https://docs.dagster.io/dagster-plus).
