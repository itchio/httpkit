#!/bin/sh -xe

go version

rm -rf src pkg bin

export CURRENT_BUILD_PATH=$(pwd)
export GOPATH=$CURRENT_BUILD_PATH
export PKG=github.com/itchio/httpfile
export PATH=$PATH:$GOPATH/bin

mkdir -p src/$PKG
rsync -a --exclude 'src' . src/$PKG

go get -v -d -t $PKG
go test -cover $PKG
