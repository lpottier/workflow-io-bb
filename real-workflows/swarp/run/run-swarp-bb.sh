#!/bin/bash -l

#module load gcc/7.3.0

set -x
BASE=$(pwd)
#BASE="$SCRATCH/deep-sky"

EXE=${BASE}/swarp-2.38.0-install/bin/swarp
INPUT_DIR=${BASE}/input
OUPUT_DIR=${BASE}/ouput
CONFIG_DIR=${BASE}/config
RESAMPLE_CONFIG=${CONFIG_DIR}/resample.swarp
COMBINE_CONFIG=${CONFIG_DIR}/combine.swarp

FILE_PATTERN='PTF201111*'
IMAGE_PATTERN='PTF201111*.w.fits'
RESAMPLE_PATTERN='PTF201111*.w.resamp.fits'

mkdir -p ${OUPUT_DIR}
rm -f ${OUPUT_DIR}/* resample.xml combine.xml coadd.fits coadd.weight.fits PTF201111*.w.resamp*
ls

srun -n 1 -C "haswell" -c 10 --cpu-bind=cores \
     $EXE -c $RESAMPLE_CONFIG ${INPUT_DIR}/${IMAGE_PATTERN} --bbf=bbf.conf

srun -n 1 -C "haswell" -c 10 --cpu-bind=cores \
     $EXE -c $COMBINE_CONFIG ${OUPUT_DIR}/${RESAMPLE_PATTERN} --bbf=bbf.conf
