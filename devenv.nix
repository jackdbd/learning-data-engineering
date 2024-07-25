{
  config,
  inputs,
  lib,
  pkgs,
  ...
}: {
  # https://devenv.sh/basics/
  # https://devenv.sh/reference/options/

  # This seems NOT to work with Dagster projects. I need to set it in an .env
  # file for each Dagster project.
  # env.DUCKDB_DATABASE = "dagster_university/data/staging/data.duckdb";
  env.GREET = "devenv";

  # https://devenv.sh/languages/
  languages.nix.enable = true;
  languages.python = {
    enable = true;
    venv.enable = true;
    venv.requirements = ''
      dagster~=1.7
      dagster-duckdb==0.23.*
      dagster-webserver~=1.7
      geopandas~=1.0.1
      kaleido==0.2.*
      pandas~=2.2.2
      plotly~=5.23.0
    '';
    # libraries = [pkgs.cairo];
  };

  # https://devenv.sh/packages/
  packages = [pkgs.git];

  # https://devenv.sh/scripts/
  scripts.hello.exec = "echo hello from $GREET";

  enterShell = ''
    echo Hello ${config.env.GREET}
    say-hello
    python --version
  '';

  # https://devenv.sh/tests/
  enterTest = ''
    echo "assert Python version is 3.11.8"
    python --version | grep "3.11.8"
    echo "assert Dagster version is 1.7"
    dagster --version | grep "1.7"
  '';

  # https://devenv.sh/pre-commit-hooks/
  # pre-commit.hooks.shellcheck.enable = true;

  # https://devenv.sh/processes/
  # processes.ping.exec = "ping example.com";

  scripts = {
    say-hello.exec = "echo \"Hello from $GREET\"";
  };

  # https://devenv.sh/services/
  # services.postgres.enable = true;
}
