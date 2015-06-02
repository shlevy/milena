{ nixpkgs ? <nixpkgs>, nixpkgsArgs ? {} }: let
  pkgs = import nixpkgs nixpkgsArgs;
in {
  build = pkgs.haskellPackages.callPackage ./. {};
}
