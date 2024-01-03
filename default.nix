{ pkgs ? import <nixpkgs> {} }:

let
    openfhe = import ./common/nix/openfhe { inherit pkgs; };
    libpaseto = import ./common/nix/libpaseto { inherit pkgs; };
in

with pkgs;

gcc13Stdenv.mkDerivation rec {
    pname = "herdsman";
    version = "0.0.9";
    src = ./.;

    nativeBuildInputs = [
        pkg-config
        cmake
        libpaseto
    ];

    buildInputs = [
        libuuid
        sqlite
        openssl
        openfhe
        libsodium
        protobuf
        grpc
        curl
    ];
}
