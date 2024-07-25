# nyc_311

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

## Setup & Installation

Duplicate the `.env.example` file, rename it to `.env`, and fill your environment variables.

There is nothing to install, since the developer environment is created and managed by devenv.

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
