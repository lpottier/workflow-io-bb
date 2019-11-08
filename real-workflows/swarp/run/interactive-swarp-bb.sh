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

rm -rf {error,output}.*

MONITORING="env OUTPUT_DIR=$OUTPUT_DIR RESAMP_DIR=$RESAMP_DIR CORE_COUNT=$CORE_COUNT pegasus-kickstart -z"

module load dws
sessID=$(dwstat sessions | grep $SLURM_JOBID | awk '{print $1}')
echo "session ID is: "${sessID}
instID=$(dwstat instances | grep $sessID | awk '{print $1}')
echo "instance ID is: "${instID}
echo "fragments list:"
echo "frag state instID capacity gran node"
dwstat fragments | grep ${instID}

echo "Starting STAGE_IN... $(date --rfc-3339=ns)"
t1=$(date +%s.%N)
cp -r $BASE/input ${DW_JOB_STRIPED}
t2=$(date +%s.%N)
tdiff1=$(echo "$t2 - $t1" | bc -l)
echo "TIME STAGE_IN $tdiff1"


du -sh $DW_JOB_STRIPED/
echo "Starting RESAMPLE... $(date --rfc-3339=ns)"
t1=$(date +%s.%N)

srun -N 1 -n 1 -C "haswell" -c $CORE_COUNT --cpu-bind=cores \
	-o "$OUTPUT_DIR/output.resample" \
	-e "$OUTPUT_DIR/error.resample" \
    	$MONITORING -l "$OUTPUT_DIR/stat.resample.xml" \
	$EXE -c $RESAMPLE_CONFIG ${INPUT_DIR}/${IMAGE_PATTERN}

t2=$(date +%s.%N)
tdiff2=$(echo "$t2 - $t1" | bc -l)
echo "TIME RESAMPLE $tdiff2"

echo "Starting combine... $(date --rfc-3339=ns)"
t1=$(date +%s.%N)

srun -N 1 -n 1 -C "haswell" -c $CORE_COUNT --cpu-bind=cores \
	-o "$OUTPUT_DIR/output.coadd" \
	-e "$OUTPUT_DIR/error.coadd" \
    	$MONITORING -l "$OUTPUT_DIR/stat.combine.xml" \
	$EXE -c $COMBINE_CONFIG ${RESAMP_DIR}/${RESAMPLE_PATTERN}

t2=$(date +%s.%N)
tdiff3=$(echo "$t2 - $t1" | bc -l)
echo "TIME COMBINE $tdiff3"

du -sh $DW_JOB_STRIPED/

echo "Starting STAGE_OUT... $(date --rfc-3339=ns)"
t1=$(date +%s.%N)
cp -r $OUTPUT_DIR $(pwd)
t2=$(date +%s.%N)
tdiff4=$(echo "$t2 - $t1" | bc -l)
echo "TIME STAGE_OUT $tdiff4"

echo "========"
tdiff=$(echo "$tdiff1 + $tdiff2 + $tdiff3 + $tdiff4" | bc -l)
echo "TIME TOTAL $tdiff"

