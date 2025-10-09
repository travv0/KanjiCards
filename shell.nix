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
    export PYTHONPATH=$PWD${PYTHONPATH:+:$PYTHONPATH}
  '';
}
