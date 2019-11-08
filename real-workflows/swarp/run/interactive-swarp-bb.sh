#!/bin/bash

#set -x

IMAGE_PATTERN='PTF201111*.w.fits'
IMAGE_WEIGHT_PATTERN='PTF201111*.w.weight.fits'
RESAMPLE_PATTERN='PTF201111*.w.resamp.fits'

BASE="/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/"

EXE=$BASE/bin/swarp
CORE_COUNT=1

CONFIG_DIR=$BASE/config
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

MONITORING="env OUTPUT_DIR=$OUTPUT_DIR RESAMP_DIR=$RESAMP_DIR CORE_COUNT=$CORE_COUNT pegasus-kickstart -z"

module load dws
sessID=$(dwstat sessions | grep $SLURM_JOBID | awk '{print $1}')
echo "session ID is: "${sessID}
instID=$(dwstat instances | grep $sessID | awk '{print $1}')
echo "instance ID is: "${instID}
echo "fragments list:"
echo "frag state instID capacity gran node"
dwstat fragments | grep ${instID}

du -sh $DW_JOB_STRIPED/
echo "Started resample..."

srun -N 1 -n 1 -C "haswell" -c $CORE_COUNT --cpu-bind=cores \
	-o "output.resample" \
	-e "error.resample" \
    	$MONITORING -l "$OUTPUT_DIR/stat.resample.xml" \
	$EXE -c $RESAMPLE_CONFIG ${INPUT_DIR}/${IMAGE_PATTERN}
echo "Started combine..."

srun -N 1 -n 1 -C "haswell" -c $CORE_COUNT --cpu-bind=cores \
	-o "output.coadd" \
	-e "error.coadd" \
    	$MONITORING -l "$OUTPUT_DIR/stat.combine.xml" \
	$EXE -c $COMBINE_CONFIG ${RESAMP_DIR}/${RESAMPLE_PATTERN}

du -sh $DW_JOB_STRIPED/

