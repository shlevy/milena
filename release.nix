{ src
, nixpkgs ? null
, bootstrap-nixpkgs
, nix-composition-lib
}@args: import (nix-composition-lib + "/hydra.nix") args
