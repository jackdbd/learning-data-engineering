# Learning data engineering

Exercises and projects to learn data engineering.

## Setup & Installation

This projects uses [Nix](https://nixos.org/) and [devenv](https://devenv.sh/) to define a reproducible developer environment.

You can install Nix and devenv by following the instructions in the [devenv documentation](https://devenv.sh/getting-started/).

If you have Nix and devenv installed, the developer environment will be automatically created and activated when you enter the repository root directory (thanks to direnv).

## Repository structure

### `dagster_university`

The `dagster_university` directory contains the [Dagster project](https://docs.dagster.io/guides/dagster/recommended-project-structure) for the course [Dagster Essentials](https://courses.dagster.io/courses/dagster-essentials). The project was scaffolded using the following command (do **not** run this command):

```sh
dagster project from-example \
  --example project_dagster_university_start \
  --name dagster_university
```

If you want to check the Dagster project developed in that course, enter the `dagster_university` directory and follow [the instructions](./dagster_university/README.md).

## Reference

- [Dagster Essentials](https://courses.dagster.io/courses/dagster-essentials)
- [Dagster & dbt](https://courses.dagster.io/courses/dagster-dbt)
- [dbt Learn](https://www.getdbt.com/dbt-learn)
