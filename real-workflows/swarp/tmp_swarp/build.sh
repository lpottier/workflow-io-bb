#!/bin/bash -l
set -e

module load gcc/7.3.0

export SWARP_VERSION="2.38.0"

# Download source
# wget "https://www.astromatic.net/download/swarp/swarp-$SWARP_VERSION.tar.gz"
tar -zxf "swarp-$SWARP_VERSION.tar.gz"

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

