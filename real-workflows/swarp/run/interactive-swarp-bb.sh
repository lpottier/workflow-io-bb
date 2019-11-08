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
export OUTPUT_DIR=${DW_JOB_STRIPED}/output.$SLURM_JOB_ID.${CORE_COUNT}c/

OUTPUT_FILE=$(pwd)/output.$SLURM_JOB_ID.${CORE_COUNT}c.out

mkdir -p ${OUTPUT_DIR}
chmod 777 ${OUTPUT_DIR}

export RESAMP_DIR=${DW_JOB_STRIPED}/resamp
mkdir -p ${RESAMP_DIR}
chmod 777 ${RESAMP_DIR}

rm -rf {error,output}.*

echo "CORE $CORE_COUNT" | tee $OUTPUT_FILE

MONITORING="env OUTPUT_DIR=$OUTPUT_DIR RESAMP_DIR=$RESAMP_DIR CORE_COUNT=$CORE_COUNT pegasus-kickstart -z"

module load dws
sessID=$(dwstat sessions | grep $SLURM_JOBID | awk '{print $1}')
echo "session ID is: "${sessID} | tee $OUTPUT_FILE
instID=$(dwstat instances | grep $sessID | awk '{print $1}')
echo "instance ID is: "${instID} | tee $OUTPUT_FILE
echo "fragments list:" | tee $OUTPUT_FILE
echo "frag state instID capacity gran node" | tee $OUTPUT_FILE
dwstat fragments | grep ${instID} | tee $OUTPUT_FILE

echo "Starting STAGE_IN... $(date --rfc-3339=ns)" | tee $OUTPUT_FILE
t1=$(date +%s.%N)
cp -r $BASE/input $DW_JOB_STRIPED
cp -r $EXE $DW_JOB_STRIPED
cp -r $CONFIG_DIR $DW_JOB_STRIPED
t2=$(date +%s.%N)
tdiff1=$(echo "$t2 - $t1" | bc -l)
echo "TIME STAGE_IN $tdiff1" | tee $OUTPUT_FILE

#if we stge in executable
EXE=$DW_JOB_STRIPED/swarp

du -sh $DW_JOB_STRIPED/ | tee $OUTPUT_FILE
echo "Starting RESAMPLE... $(date --rfc-3339=ns)" | tee $OUTPUT_FILE
t1=$(date +%s.%N)

srun -N 1 -n 1 -C "haswell" -c $CORE_COUNT --cpu-bind=cores \
	-o "$OUTPUT_DIR/output.resample" \
	-e "$OUTPUT_DIR/error.resample" \
    	$MONITORING -l "$OUTPUT_DIR/stat.resample.xml" \
	$EXE -c $DW_JOB_STRIPED/config/resample.swarp ${INPUT_DIR}/${IMAGE_PATTERN}

t2=$(date +%s.%N)
tdiff2=$(echo "$t2 - $t1" | bc -l)
echo "TIME RESAMPLE $tdiff2" | tee $OUTPUT_FILE

echo "Starting combine... $(date --rfc-3339=ns)" | tee $OUTPUT_FILE
t1=$(date +%s.%N)

srun -N 1 -n 1 -C "haswell" -c $CORE_COUNT --cpu-bind=cores \
	-o "$OUTPUT_DIR/output.coadd" \
	-e "$OUTPUT_DIR/error.coadd" \
    	$MONITORING -l "$OUTPUT_DIR/stat.combine.xml" \
	$EXE -c $DW_JOB_STRIPED/config/combine.swarp ${RESAMP_DIR}/${RESAMPLE_PATTERN}

t2=$(date +%s.%N)
tdiff3=$(echo "$t2 - $t1" | bc -l)
echo "TIME COMBINE $tdiff3" | tee $OUTPUT_FILE

du -sh $DW_JOB_STRIPED/ | tee $OUTPUT_FILE

echo "Starting STAGE_OUT... $(date --rfc-3339=ns)" | tee $OUTPUT_FILE
t1=$(date +%s.%N)
cp -r $OUTPUT_DIR $(pwd)
t2=$(date +%s.%N)
tdiff4=$(echo "$t2 - $t1" | bc -l)
echo "TIME STAGE_OUT $tdiff4" | tee $OUTPUT_FILE

echo "========" | tee $OUTPUT_FILE
tdiff=$(echo "$tdiff1 + $tdiff2 + $tdiff3 + $tdiff4" | bc -l)
echo "TIME TOTAL $tdiff" | tee $OUTPUT_FILE

env | grep SLURM > $OUTPUT_DIR/slurm.env

