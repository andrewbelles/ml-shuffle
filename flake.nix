{
  description = "ml-shuffle unified dev/build environment for Python, Rust, C++, & CUDA";

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
          config.allowUnfree = true; 
        };

        rustToolchain = pkgs.rust-bin.stable.latest.deafult.override {
          extensions = [ "rust-src" ];
        };

        craneLib = crane.mkLib pkgs; 

        trackCrawlerSrc = craneLib.cleanCargoSource (
          craneLib.path ./services/track-crawler
        );

        commonNative = [
          pkgs.pkg-config 
          pkgs.openssl 
          pkgs.cmake 
          pkgs.clang 
          pkgs.gcc 
          pkgs.binutils 
        ];

        devTools = [
          rustToolchain 
          pkgs.rust-analyzer
          pkgs.sqlx-cli 
          pkgs.just 
          pkgs.sqlite 
          pkgs.zstd 
          pkgs.python3Full 
          pkgs.pyright 
          pkgs.clang-tools
          pkgs.cudatoolkit
          pkgs.linuxPackages.nvidia_x11 
        ];
      in {
        devShells.default = pkgs.mkShell {
          packages = devTools ++ commonNative; 
          env = {
            DATABASE_URL = "sqlite:./services/data/raw.db";
            DATA_ROOL    = "./services/data/";
            RUST_LOG     = "info,track_crawler=debug,reqwest=warn";
            LIVE_HTTP    = "0";
            CUDA_PATH    = "${pkgs.cudatoolkit}";
          };
          shellHook = ''
          mkdir -p ./services/data/ ./services/data/http-cache 
          echo "ml-shuffle dev shell"
          echo "Rust toolchain: $(rustc --version), Python $(python --version)" 
          '';
        };

        packages."track_crawler" = craneLib.buildPackage {
          pname   = "track-crawler";
          version = "0.1.0";
          src     = trackCrawlerSrc; 
          nativeBuildInputs = commonNative; 
          buildInputs = [ pkgs.openssl ];
          OPENSSL_NO_VENDOR = "1";
        };

        packages.default = self.packages.${system}."track-crawler";

        apps."track-crawler" = {
          type = "app";
          program = "${self.packages.${system}.track-crawler}/bin/track-crawler";
        };

        checks."track-crawler-build" = self.packages.${system}."track-crawler";
        checks."track-crawler-tests" = craneLib.cargoTest {
          pname = "track-crawler-tests";
          src   = trackCrawlerSrc;
          cargoArtifacts = craneLib.buildDepsOnly {
            pname = "track-crawler-deps";
            src   = trackCrawlerSrc; 
            nativeBuildInputs = commonNative; 
            buildInputs = [ pkgs.openssl ];
            OPENSSL_NO_VENDOR = "1";
          };
          nativeBuildInputs = commonNative;
          buildInputs = [ pkgs.openssl ];
          OPENSSL_NO_VENDOR = "1";
          CARGO_TERM_COLOR = "always";
          RUST_LOG = "info,track_crawler=debug,reqwest=warn";
          DATABASE_URL = "sqlite::memory:";
          LIVE_HTTP = "0";
        };

        formatter = pkgs.alejandra; 
      }
    );
}
