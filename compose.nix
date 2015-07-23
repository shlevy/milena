{ nixpkgs
}:
let
  pkgs = import nixpkgs { config.allowUnfree = true; };

  inherit (pkgs) haskellPackages;
in {
  build = haskellPackages.callPackage ./. {};
}
