#!/bin/bash

set -x

IMAGE_PATTERN='PTF201111*.w.fits'
RESAMPLE_PATTERN='PTF201111*.w.resamp.fits'

EXE=$(pwd)/swarp-2.38.0-install/bin/swarp

CONFIG_DIR=$(pwd)/config
RESAMPLE_CONFIG=${CONFIG_DIR}/resample.swarp
COMBINE_CONFIG=${CONFIG_DIR}/combine.swarp

INPUT_DIR=${DW_JOB_STRIPED}input
export OUTPUT_DIR=${DW_JOB_STRIPED}output/

mkdir -p ${OUTPUT_DIR}
chmod 777 ${OUTPUT_DIR}

export RESAMP_DIR=${DW_JOB_STRIPED}/resamp
mkdir -p ${RESAMP_DIR}
chmod 777 ${RESAMP_DIR}

srun -N 1 -n 1 -C "haswell" -c 10 --cpu-bind=cores \
	-o "output.resample" \
	-e "error.resample" \
	$EXE -c $RESAMPLE_CONFIG ${INPUT_DIR}/${IMAGE_PATTERN}

srun -N 1 -n 1 -C "haswell" -c 10 --cpu-bind=cores \
	-o "output.coadd" \
	-e "error.coadd" \
	$EXE -c $COMBINE_CONFIG ${RESAMP_DIR}/${RESAMPLE_PATTERN}

