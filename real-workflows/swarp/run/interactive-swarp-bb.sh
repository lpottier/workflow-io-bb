#!/bin/bash

set -x

IMAGE_PATTERN='PTF201111*.w.fits'
IMAGE_WEIGHT_PATTERN='PTF201111*.w.weight.fits'
RESAMPLE_PATTERN='PTF201111*.w.resamp.fits'

EXE=$(pwd)/bin/swarp

CONFIG_DIR=$(pwd)/config
RESAMPLE_CONFIG=${CONFIG_DIR}/resample.swarp
COMBINE_CONFIG=${CONFIG_DIR}/combine.swarp

CONFIG_FILES="${RESAMPLE_CONFIG} ${COMBINE_CONFIG}"

INPUT_DIR=${DW_JOB_STRIPED}/input
export OUTPUT_DIR=${DW_JOB_STRIPED}/output/

mkdir -p ${OUTPUT_DIR}
chmod 777 ${OUTPUT_DIR}

export RESAMP_DIR=${DW_JOB_STRIPED}/resamp
mkdir -p ${RESAMP_DIR}
chmod 777 ${RESAMP_DIR}

MONITORING="pegasus-kickstart -z "

STAGE_FILES=files_to_stage.txt

rm -f ${STAGE_FILES}

for f in $(ls ${INPUT_DIR}/${IMAGE_PATTERN}); do
    echo $(pwd)/$f ${DW_JOB_STRIPED}/input >> ${STAGE_FILES}

for f in $(ls ${INPUT_DIR}/${IMAGE_WEIGHT_PATTERN}); do
    echo $(pwd)/$f ${DW_JOB_STRIPED}/input >> ${STAGE_FILES}

# for f in ${CONFIG_FILES}; do
#     echo $f ${DW_JOB_STRIPED}/config >> ${STAGE_FILES}

srun -N 1 -n 1 -C "haswell" -c 1 --cpu-bind=cores \
	-o "output.resample" \
	-e "error.resample" \
    $MONITORING $EXE -l $OUTPUT_DIR/stat.resample.xml \
	$EXE -c $RESAMPLE_CONFIG ${INPUT_DIR}/${IMAGE_PATTERN}

srun -N 1 -n 1 -C "haswell" -c 1 --cpu-bind=cores \
	-o "output.coadd" \
	-e "error.coadd" \
    $MONITORING $EXE -l $OUTPUT_DIR/stat.combine.xml \
	$EXE -c $COMBINE_CONFIG ${RESAMP_DIR}/${RESAMPLE_PATTERN}

