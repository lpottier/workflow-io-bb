#!/bin/bash -l

#module load gcc/7.3.0
#module load allinea-reports
#set -x

#module load perftools-base perftools
#BASE="/ccs/home/lpottier/workflow-io-bb/real-workflows/swarp/"
BASE=$(pwd)

#BASE=/gpfs/alpine/scratch/lpottier/csc355/
#BASE="$SCRATCH/deep-sky"

EXE=$BASE/bin/swarp
INPUT_DIR=$BASE/input
export OUTPUT_DIR=output
CONFIG_DIR=$BASE/config
RESAMPLE_CONFIG=$CONFIG_DIR/resample.swarp
COMBINE_CONFIG=$CONFIG_DIR/combine.swarp

FILE_PATTERN='PTF201111*'
IMAGE_PATTERN='PTF201111*.w.fits'
RESAMPLE_PATTERN='PTF201111*.w.resamp.fits'

export RESAMP_DIR=toto

export CORE_COUNT=1

mkdir -p $OUTPUT_DIR
mkdir -p $RESAMP_DIR

rm -f $OUTPUT_DIR/* resample.xml combine.xml coadd.fits coadd.weight.fits PTF201111*.w.resamp*
ls

echo "DIR          : $BASE"
echo "EXE          : $EXE"
echo "#CORES       : $CORE_COUNT"
echo "INPUT        : $INPUT_DIR"
echo "OUTPUT       : $OUTPUT_DIR"
echo "CONFIG       : $CONFIG_DIR"
echo "RESAMP FILES : $RESAMP_DIR"

#MONITORING="env OUTPUT_DIR=${OUTPUT_DIR} RESAMP_DIR=$RESAMP_DIR CORE_COUNT=$CORE_COUNT"
#MONITORING="env OUTPUT_DIR=$OUTPUT_DIR RESAMP_DIR=$RESAMP_DIR CORE_COUNT=$CORE_COUNT pegasus-kickstart -z"

jsrun -n 1 -a 1 -c $CORE_COUNT $EXE -c $RESAMPLE_CONFIG ${INPUT_DIR}/${IMAGE_PATTERN}

jsrun -n 1 -a 1 -c $CORE_COUNT $EXE -c $COMBINE_CONFIG ${RESAMP_DIR}/${RESAMPLE_PATTERN}

