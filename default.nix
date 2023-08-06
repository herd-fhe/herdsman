{ pkgs ? import <nixpkgs> {} }:

let
    openfhe = import ./common/nix/openfhe { inherit pkgs; };
in

with pkgs;

gcc13Stdenv.mkDerivation rec {
    pname = "herdsman";
    version = "0.0.2";
    src = ./.;

    nativeBuildInputs = [
        pkgconfig
        cmake
    ];

    buildInputs = [
        libuuid
        sqlite
        openssl
        openfhe
        libsodium
        protobuf
        grpc
    ];
}
