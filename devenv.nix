{
  config,
  inputs,
  lib,
  pkgs,
  ...
}: {
  # https://devenv.sh/basics/
  # https://devenv.sh/reference/options/
  env.GREET = "devenv";

  # https://devenv.sh/languages/
  languages.nix.enable = true;
  languages.python = {
    enable = true;
    venv.enable = true;
    venv.requirements = ''
      dagster~=1.7
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
