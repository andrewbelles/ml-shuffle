{
  description = "ml-shuffle dev/build env for full project. Restricted to linux systems";

  inputs = {
    nixpkgs.url      = "github:NixOS/nixpkgs/nixos-25.05";
    flake-utils.url  = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
    crane.url        = "github:ipetkov/crane";
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, crane, ... }:
    flake-utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" ] (system:
      let
        pkgs = import nixpkgs {
          inherit system; 
          overlays = [ rust-overlay.overlays.default ];
        };

        rustToolchain = pkgs.rust-bin.stable.latest.default; 
        craneLib = crane.mkLib pkgs;

        rsCrawlerSrc = craneLib.cleanCargoSource (
          craneLib.path ./services/rs-crawler
        );

        commonNative = [
          pkgs.pkg-config 
          pkgs.openssl
          pkgs.cmake 
          pkgs.clang 
        ];

        devTools = [
          rustToolchain
          pkgs.rust-analyzer 
          pkgs.sqlx-cli 
          pkgs.sqlite 
          pkgs.zstd  
          pkgs.just 
        ];
      in {
        devShells.default = pkgs.mkShell {
          packages = devTools ++ commonNative;
          env = {
            DATABASE_URL    = "sqlite:./services/data/raw.db";
            DATA_ROOT = "./services/data/"; 
            RUST_LOG  = "info,rs_crawler=debug,reqwest=warn";
            LIVE_HTTP = "0";
          };
          shellHook = ''
            mkdir -p ./services/data/ ./services/data/http-cache
          '';
        };

        packages.rs-crawler = craneLib.buildPackage {
          pname   = "rs-crawler";
          version = "0.1.0";
          src     = rsCrawlerSrc;

          nativeBuildInputs = commonNative;
          buildInputs       = [ pkgs.openssl ]; 
          OPENSSL_NO_VENDOR = "1";
        };

        packages.default = self.packages.${system}.rs-crawler; 

        apps.rs-crawler = {
          type    = "app";
          program = "${self.packages.${system}.rs-crawler}/bin/rs-crawler"; 
        };

        checks.rs-crawler-build = self.packages.${system}.rs-crawler;

        checks.rs-crawler-tests = 
          let 
            cargoArtifacts = craneLib.buildDepsOnly {
              pname = "rs-crawler-deps";
              src   = rsCrawlerSrc;
              nativeBuildInputs = commonNative;
              buildInputs = [ pkgs.openssl ]; 
              OPENSSL_NO_VENDOR = "1";
            };
          in 
          craneLib.cargoTest {
            pname = "rs-crawler-tests";
            src   = rsCrawlerSrc; 

            inherit cargoArtifacts; 
            nativeBuildInputs = commonNative; 
            buildInputs       = [ pkgs.openssl ];
            OPENSSL_NO_VENDOR = "1";

            cargoTestExtraArgs = "-- --nocapture";

            CARGO_TERM_COLOR = "always";
            RUST_LOG = "info,rs_crawler=debug,reqwest=warn";
            DATABASE_URL = "sqlite::memory:";
            LIVE_HTTP = "0";
          };

        formatter = pkgs.alejandra;
      }
    ); 
}
