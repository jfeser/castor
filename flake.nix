{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    combinat.url = "github:jfeser/combinat";
    combinat.inputs.nixpkgs.follows = "nixpkgs";
  };
  outputs = { self, flake-utils, nixpkgs, combinat }@inputs:
    let
      overlay = final: prev:
        let llvmPackages = prev.llvmPackages_14;
        in {
          cmph = final.callPackage ./nix/cmph.nix { };
          ocamlPackages = prev.ocamlPackages.overrideScope' (ofinal: oprev: {
            llvm = ofinal.callPackage ./nix/ocaml_llvm.nix {
              libllvm = llvmPackages.libllvm;
            };

            castor = oprev.buildDunePackage rec {
              pname = "castor";
              version = "n/a";
              src = ./.;
              duneVersion = "3";
              propagatedBuildInputs = with ofinal; [
                menhir
                dune-site
                core
                core_unix
                ofinal.combinat
                ocamlgraph
                iter
                llvm
                postgresql
                bos
                logs
                hashcons
                lwt
                lwt_ppx
                ppx_compare
                ppx_sexp_conv
                ppx_hash
                ppx_let
                visitors
                yojson
                llvmPackages.libllvm
              ];
              doCheck = false;
            };

            sqlgg = oprev.buildDunePackage rec {
              pname = "sqlgg";
              version = "n/a";
              src = ./.;
              duneVersion = "3";
              propagatedBuildInputs = with ofinal; [
                menhir
                base
                stdio
                ppx_sexp_conv
                ppx_compare
              ];
              doCheck = false;
            };

            cmph = oprev.buildDunePackage rec {
              pname = "cmph";
              version = "n/a";
              src = ./.;
              duneVersion = "3";
              propagatedBuildInputs = with ofinal; [
                final.cmph
                ounit
                ctypes
                core
                ppx_sexp_conv
              ];
              doCheck = false;
            };

            genhash = oprev.buildDunePackage rec {
              pname = "genhash";
              version = "n/a";
              src = ./.;
              duneVersion = "3";
              propagatedBuildInputs = with ofinal; [
                ounit
                integers
                ppx_sexp_conv
                ppx_compare
                ppx_expect
              ];
              doCheck = false;
            };

            castor_test = oprev.buildDunePackage rec {
              pname = "castor_test";
              version = "n/a";
              src = ./.;
              duneVersion = "3";
              propagatedBuildInputs = with ofinal; [
                castor
                postgresql
                bos
                logs
                sexp_diff
                hashcons
                lwt
                lwt_ppx
                ppx_compare
                ppx_sexp_conv
                ppx_hash
                ppx_let
                visitors
                ounit
                expect_test_helpers_core
                final.python3Packages.sqlparse
              ];
              doCheck = false;
            };
          });
        };
    in flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          overlays = [ combinat.overlays.${system}.default overlay ];
          inherit system;
        };
        llvmPackages = pkgs.llvmPackages_14;
        ocamlPkgs = pkgs.ocaml-ng.ocamlPackages;

      in {
        packages = {
          castor = pkgs.ocamlPackages.castor;
          cmph = pkgs.ocamlPackages.cmph;
          genhash = pkgs.ocamlPackages.genhash;
          sqlgg = pkgs.ocamlPackages.sqlgg;
          castor_test = pkgs.ocamlPackages.castor_test;
        };
        defaultPackage = self.packages.castor.${system};
        devShell = (pkgs.mkShell # .override { stdenv = llvmPackages.stdenv; }
        ) {
          buildInputs = [
            pkgs.ocamlformat
            pkgs.opam
            pkgs.ocamlPackages.ocaml-lsp
            pkgs.llvmPackages_14.clang
          ];
          inputsFrom = [
            self.packages.${system}.castor
            self.packages.${system}.castor_test
            self.packages.${system}.cmph
            self.packages.${system}.genhash
            self.packages.${system}.sqlgg
          ];
          # shellHook = ''
          #   export LIBCLANG_PATH="${llvmPackages.libclang}/lib";
          # '';
        };
      });
}
