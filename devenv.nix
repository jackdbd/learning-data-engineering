{
  config,
  inputs,
  lib,
  pkgs,
  ...
}: {
  # https://devenv.sh/reference/options/

  # This seems NOT to work with Dagster projects. I need to set it in an .env
  # file for each Dagster project.
  # env.DUCKDB_DATABASE = "dagster_university/data/staging/data.duckdb";
  env.GREET = "devenv";

  languages.nix.enable = true;

  languages.python = {
    enable = true;
    venv.enable = true;
    venv.requirements = ''
      dagster>=1.7.16
      dagster-duckdb>=0.23.16
      dagster-webserver>=1.7.16
      dbt-core>=1.8.5,<2.0
      dbt-duckdb>=1.8.2,<2.0
      debugpy>=1.8.5
      loguru>=0.7
      kaleido==0.2.*
      pandas>=2.2.2
      plotly>=5.23.0
      pytest
      streamlit
    '';
    libraries = [];
  };

  packages = [
    pkgs.aria2 # download tool
    pkgs.csv2parquet # convert CSV files to Apache Parquet
    pkgs.duckdb
    pkgs.git
    pkgs.xsv # various tools for CSV files
    pkgs.visidata # interactive terminal multitool for tabular data
  ];

  enterShell = ''
    echo Hello ${config.env.GREET}
    say-hello
    python --version
  '';

  enterTest = ''
    echo "assert Python version is 3.11.*"
    python --version | ag "3\.11\.[0-9]+$"
    echo "assert Dagster version is 1.7"
    dagster --version | grep "1.7"
  '';

  pre-commit.hooks = {
    # Format Nix
    alejandra.enable = true;
    # Format Python. Black can conflict with other Python tools (e.g. isort)
    # https://black.readthedocs.io/en/stable/guides/using_black_with_other_tools.html
    # black.autoflake = true;
    black.enable = true;
    # Format Markdown.
    # markdownlint = true;
  };

  # https://devenv.sh/processes/
  # processes.ping.exec = "ping example.com";

  # https://devenv.sh/scripts/
  scripts = {
    dagster-dev.exec = "cd dagster_university && dagster dev";
    dagster-test.exec = "pushd . && cd dagster_university && pytest -v dagster_university_tests && popd";
    say-hello.exec = "echo \"Hello from $GREET\"";
    versions.exec = ''
      echo "Versions"
      dagster --version
      echo "DuckDB $(${pkgs.duckdb}/bin/duckdb --version)"
      python --version
    '';
  };

  # https://devenv.sh/services/
  # services.postgres.enable = true;
}
