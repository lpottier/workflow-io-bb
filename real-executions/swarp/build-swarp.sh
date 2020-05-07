#!/bin/bash -l
set -e

export SWARP_VERSION="2.38.0"

DIR="swarp-$SWARP_VERSION.tar.gz"

LINK_SWARP="https://www.astromatic.net/download/swarp/swarp-$SWARP_VERSION.tar.gz"
WGET=wget

if [ ! -f "$DIR" ]; then
    echo -n "swarp-$SWARP_VERSION.tar.gz not found. Downloading from $LINK_SWARP ..."
    $WGET $LINK_SWARP
    echo "done."
else
    echo "swarp-$SWARP_VERSION.tar.gz found"
fi

if [ ! -d "swarp-$SWARP_VERSION" ]; then
    echo -n "Extracting swarp-$SWARP_VERSION.tar.gz into swarp-$SWARP_VERSION/ ..."
    tar -zxf "swarp-$SWARP_VERSION.tar.gz"
    echo "done."
else
    echo "swarp-$SWARP_VERSION.tar.gz already extracted in swarp-$SWARP_VERSION/."
fi

INSTALL_DIR="$(pwd)"
cd "swarp-$SWARP_VERSION"

export CC=gcc

if [ $(/bin/arch) == "ppc64le" ]; then
	./configure --build=powerpc64le-unknown-linux-gnu --prefix=${INSTALL_DIR} 2>&1 | tee c.out
else
	module load gcc/7.3.0
    ./configure --prefix=${INSTALL_DIR} 2>&1 | tee c.out
fi
make 2>&1 | tee m.out
make check 2>&1 | tee mc.out
make install 2>&1 | tee mi.out

#rm -rf "${INSTALL_DIR}/swarp-$SWARP_VERSION"
