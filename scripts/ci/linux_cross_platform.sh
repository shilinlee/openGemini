#!/usr/bin/env bash

set -e

sudo apt-get update
sudo apt-get install mingw-w64 -y

echo "success install mingw"

whereis x86-64-w64-mingw32-g++

whereis x86_64-w64-mingw32-gcc

whereis gcc-mingw-w64-x86-64

env CC=x86_64-w64-mingw32-gcc GOOS=windows GOARCH=amd64 CGO_ENABLED=1 go build ./app/ts-cli

ls -l