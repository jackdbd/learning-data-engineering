# dagster_university

This is a [Dagster](https://dagster.io/) project made to accompany Dagster University coursework.

## Setup & Installation

Duplicate the `.env.example` file and rename it to `.env`.

There is nothing to install, since the developer environment is created and managed by devenv.

## Development

Start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

Data lineage of the `default` [asset group](https://docs.dagster.io/concepts/assets/software-defined-assets#grouping-assets):

http://127.0.0.1:3000/locations/dagster_university/asset-groups/default

View the Manhattan map in the browser (you need to materialize it first):

```sh
python -m http.server -d dagster_university/data/outputs/ 8888
```

## Deploy to Dagster+

Check out the [Dagster+ documentation](https://docs.dagster.io/dagster-plus).
