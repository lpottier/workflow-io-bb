#!/bin/bash -l
set -e

module load gcc/7.3.0

export SWARP_VERSION="2.38.0"

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
    tar -zvxf "swarp-$SWARP_VERSION.tar.gz"
    echo "done."
else
    echo "swarp-$SWARP_VERSION/ already exists."
fi

# Use wrapper flags for the compilers
export CC=gcc

install_dir="$(pwd)"
cd "swarp-$SWARP_VERSION"
./configure --prefix=${install_dir} 2>&1 | tee c.out

make 2>&1 | tee m.out
make check 2>&1 | tee mc.out
make install 2>&1 | tee mi.out

mv "${install_dir}/bin/swarp" "${install_dir}"
rm -rf "${install_dir}/bin" "${install_dir}/share" 
#rm -rf "${install_dir}/swarp-$SWARP_VERSION"

