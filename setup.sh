#!/bin/sh
# Fetches go-ethereum v1.17.6 and applies the patch the engine needs.
set -e
cd "$(dirname "$0")"
[ -d third_party/go-ethereum ] && exit 0
git clone --quiet --depth 1 --branch v1.17.6 https://github.com/ethereum/go-ethereum third_party/go-ethereum
git -C third_party/go-ethereum apply ../go-ethereum-v1.17.6.patch
