# Import the Nixpkgs library
with import <nixpkgs> {};

# Define the Nix expression for the app + its environment
stdenv.mkDerivation {
  name = "rust";

  # Specify the packages to be available in the environment
  buildInputs = [
    pkgs.cargo
    pkgs.rustc
    pkgs.rust-analyzer
    pkgs.clippy

    # openssl
    pkgs.openssl.dev
    pkgs.pkg-config

    # frontend
    pkgs.nodejs_24
  ];
  
  shellHook = ''
  '';
}
