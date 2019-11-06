#!/bin/bash -l
set -e

#module load gcc/7.4.0

# Download source
# wget https://www.astromatic.net/download/swarp/swarp-2.38.0.tar.gz
tar -zxf swarp-2.38.0.tar.gz

# Use wrapper flags for the compilers
export CC=gcc

install_dir="$(pwd)/swarp-2.38.0-install"
cd swarp-2.38.0
if [ $(/bin/arch) == "ppc64le" ]; then
	./configure --build=powerpc64le-unknown-linux-gnu --prefix=${install_dir} 2>&1 | tee c.out
else
	./configure --prefix=${install_dir} 2>&1 | tee c.out
fi
make 2>&1 | tee m.out
make check 2>&1 | tee mc.out
make install 2>&1 | tee mi.out
