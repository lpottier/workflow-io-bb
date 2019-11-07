#!/bin/bash

#set -x

BASE="/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/"

INPUT_DIR=${BASE}/input
IMAGE_PATTERN='PTF201111*.w.fits'
IMAGE_WEIGHT_PATTERN='PTF201111*.w.weight.fits'
STAGE_FILE=files_to_stage.txt

rm -f ${STAGE_FILES}

for f in $(ls ${INPUT_DIR}/${IMAGE_PATTERN}); do
    echo $f '$DW_JOB_STRIPED'/input >> $STAGE_FILE
done

for f in $(ls ${INPUT_DIR}/${IMAGE_WEIGHT_PATTERN}); do
    echo $f '$DW_JOB_STRIPED'/input >> $STAGE_FILE
done

#salloc -N 1 -C haswell -q interactive -t 01:00:00 --bbf=bbf.conf
