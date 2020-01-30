#!/bin/bash -l
set -e

export SWARP_VERSION="swarp-2.38.0"

module load gcc/7.3.0

# Download source
# wget "https://www.astromatic.net/download/swarp/$SWARP_VERSION.tar.gz"
tar -zxf "$SWARP_VERSION.tar.gz"

# Use wrapper flags for the compilers
export CC=gcc

install_dir="$(pwd)"
cd "$SWARP_VERSION"
./configure --prefix=${install_dir} 2>&1 | tee c.out

make 2>&1 | tee m.out
make check 2>&1 | tee mc.out
make install 2>&1 | tee mi.out

mv "${install_dir}/bin/swarp" "${install_dir}"
rm -rf "${install_dir}/bin" "${install_dir}/share" 
#rm -rf "${install_dir}/$SWARP_VERSION"

