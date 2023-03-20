{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    combinat.url = "github:jfeser/combinat";
    combinat.inputs.nixpkgs.follows = "nixpkgs";
  };
  outputs = { self, flake-utils, nixpkgs, combinat }@inputs:
    flake-utils.lib.eachSystem [ "aarch64-darwin" "x86_64-linux" ] (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ self.overlay.${system} combinat.overlay.${system} ];
        };
      in {
        overlay = self: super: {
          ocamlPackages = super.ocaml-ng.ocamlPackages_4_14.overrideScope'
            (self: super: {
              ocaml = super.ocaml.override { flambdaSupport = true; };
              castor = super.buildDunePackage rec {
                pname = "castor";
                version = "0.1";
                duneVersion = "3";
                minimalOCamlVersion = "4.13";
                propagatedBuildInputs = [
                  self.core
                  self.core_bench
                  self.core_unix
                  self.ppx_yojson_conv
                  self.menhir
                  self.fmt
                  self.yojson
                  self.gen
                  self.iter
                  self.bheap
                  self.logs
                  self.combinat
                  self.bitarray
                  self.vpt
                  self.base
                  self.fmt
                  self.sek
                ];
                src = ./.;
              };
            });
        };

        defaultPackage = pkgs.ocamlPackages.symetric;
        devShell = pkgs.mkShell {
          nativeBuildInputs = [ pkgs.ocamlformat ];
          inputsFrom = [ pkgs.ocamlPackages.symetric ];
        };
      });
}
