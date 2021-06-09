#!/bin/sh

mkdir -p ./build
pushd build
../configure
make
popd
