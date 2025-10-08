{ pkgs ? import <nixpkgs> {} }:
let
  pythonEnv =
    pkgs.python3.withPackages (ps: [
      ps.pytest
      ps.pytest-cov
    ]);
in
pkgs.mkShell {
  buildInputs = [
    pythonEnv
    pkgs.stdenv.cc.cc.lib
  ];

  shellHook = ''
    export PYTEST_DISABLE_PLUGIN_AUTOLOAD=1
    export PYTHONPATH=$PWD${PYTHONPATH:+:$PYTHONPATH}
  '';
}
