#!/bin/bash -l

#module load gcc/7.3.0

#module load allinea-reports

set -x
BASE=$(pwd)
#BASE="$SCRATCH/deep-sky"

EXE=${BASE}/bin/swarp
INPUT_DIR=${BASE}/input
export OUTPUT_DIR=${BASE}/output
CONFIG_DIR=${BASE}/config
RESAMPLE_CONFIG=${CONFIG_DIR}/resample.swarp
COMBINE_CONFIG=${CONFIG_DIR}/combine.swarp

FILE_PATTERN='PTF201111*'
IMAGE_PATTERN='PTF201111*.w.fits'
RESAMPLE_PATTERN='PTF201111*.w.resamp.fits'

export RESAMP_DIR=resamp_files

rm -rf ${RESAMP_DIR}

mkdir -p ${OUTPUT_DIR}
mkdir -p ${RESAMP_DIR}
rm -f ${OUTPUT_DIR}/* resample.xml combine.xml coadd.fits coadd.weight.fits PTF201111*.w.resamp*
ls

MONITORING="env OUTPUT_DIR=$OUTPUT_DIR RESAMP_DIR=$RESAMP_DIR pegasus-kickstart"

srun -n 1 -C "haswell" -c 10 --cpu-bind=cores \
     $MONITORING -l "$OUTPUT_DIR/stat-resample.xml" $EXE \
     -c $RESAMPLE_CONFIG ${INPUT_DIR}/${IMAGE_PATTERN}

srun -n 1 -C "haswell" -c 10 --cpu-bind=cores \
     $MONITORING -l "$OUTPUT_DIR/stat-combine.xml" $EXE \
     -c $COMBINE_CONFIG ${RESAMP_DIR}/${RESAMPLE_PATTERN}

