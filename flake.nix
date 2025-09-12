{
  description = "ml-shuffle dev/build env for full project. Restricted to linux systems";

  inputs = {
    nixpkgs.url      = "github:NixOs/nixpkgs/nixos-24.05";
    flake-utils.url  = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
    crane.url        = "github:ipetkov/crane";
  };

  output = { self, nixpkgs, flake-utils, rust-overlay, crane, ... }:
    flake-utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" ] (system:
      let
        pkgs = import nixpkgs {
          inherit system; 
          overlays = [ rust-overlay.overlays.default ];
        };

        rustToolchain = pkgs.rust-bin.stable.latest.default; 
        craneLib = crane.lib.${system};

        rsIdLinkerSrc = craneLib.cleanCargoSource (
          craneLib.path ./services/rs-id-linker
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
            DB_URL    = "sqlite:./services/data/track.db";
            DATA_ROOT = "./services/data/"; 
            RUST_LOG  = "info,rs_id_linker=debug,reqwest=warn";
          };
          shellHook = ''
            mkdir -p ./services/data/raw ./services/data/http-cache
          '';
        };

        packages.rs-id-linker = craneLib.buildPackage {
          pname   = "rs-id-linker";
          version = "0.1.0";
          src     = rsIdLinkerSrc;

          nativeBuildInputs = commonNative;
          buildInputs       = [ pkgs.openssl ]; 
          OPENSSL_NO_VENDOR = "1";

          doCheck = true; 
          cargoTestExtraArgs = "-- --nocapture";
        };

        packages.default = self.packages.${system}.rs-id-linker; 

        app.rs-id-linker = {
          type    = "app";
          program = "${self.packages.${system}.rs-id-linker}/bin/rs-id-linker"; 
        };

        checks.rs-id-linker-build = self.packages.${system}.rs-id-linker;

        formatter = pkgs.alejandra;
      }
    ); 
}
