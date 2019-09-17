#!/bin/sh

PWD_REPO=~/research/usc-isi/projects/workflow-io-bb/simulation
DIR_OUTPUT_DEV=$PWD_REPO/output/dev-data

PLATFORM=test-cori.xml
WORKFLOW=genome.dax
TIMESTAMP=$(date "+%ss")

LOG_OUTPUT=$PWD_REPO/build/bbsimu-${WORKFLOW%%.*}-${PLATFORM%%.*}-$TIMESTAMP
mkdir -p $LOG_OUTPUT

$PWD_REPO/build/workflow-io-bb \
    $PWD_REPO/data/platform-files/$PLATFORM \
    $PWD_REPO/data/workflow-files/$WORKFLOW \
    $LOG_OUTPUT \
    2> $LOG_OUTPUT/err.log

echo ""
echo "Output files written in $LOG_OUTPUT directory"
echo ""
tail -n 3 $LOG_OUTPUT/err.log

# PLATFORM=test-summit.xml
# LOG_OUTPUT=$PWD_REPO/build/bbsimu-${WORKFLOW%%.*}-${PLATFORM%%.*}-$TIMESTAMP
# mkdir -p $LOG_OUTPUT

# $PWD_REPO/build/workflow-io-bb \
#     $PWD_REPO/data/platform-files/$PLATFORM \
#     $PWD_REPO/data/workflow-files/$WORKFLOW \
#         2> $LOG_OUTPUT

# echo "Log written in $LOG_OUTPUT"