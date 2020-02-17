#!/bin/bash

export CC=gcc-8
export CXX=g++-8

PWD=$(pwd)
DIR_OUTPUT=$PWD/output/
BUILD=$PWD/build/

KS_DIR_FILES="$PWD/data/trace-files/swarp/shared-cori/baseline-pfs/pipeline-1_cores-1/"


PLATFORM="test-cori.xml"
WORKFLOW="swarp-1.dax"
FILE_MAP="files_to_stage.txt"
OUTPUT_LOG="output.log"
TIMESTAMP=$(date "+%ss")
EXP_ID="simu-pfs-${WORKFLOW%%.*}-${PLATFORM%%.*}-$TIMESTAMP"

mkdir -p $DIR_OUTPUT

LOG_OUTPUT=$DIR_OUTPUT/$EXP_ID
mkdir -p $LOG_OUTPUT

## Generate WRENCH DAX
python3 kickstart-to-wrench.py \
    -x "$PWD/data/workflow-files/$WORKFLOW" \
    -k "$KS_DIR_FILES/stat.resample.27829588.1.xml" \
    -k "$KS_DIR_FILES/stat.combine.27829588.1.xml" \
    -i "$KS_DIR_FILES/stage-in-bb-global.csv" \
    -o "$LOG_OUTPUT/$WORKFLOW"


cd $BUILD/
cmake .. && make
cd ..

$PWD/build/workflow-io-bb \
    --platform="$PWD/data/platform-files/$PLATFORM" \
    --dax="$LOG_OUTPUT/$WORKFLOW" \
    --stage-file="$BUILD/$FILE_MAP" \
    --real-log="$BUILD/$OUTPUT_LOG" \
    --output="$LOG_OUTPUT" \
    2> $LOG_OUTPUT/err.log

echo ""
echo "Output files written in $LOG_OUTPUT directory."
echo "Log written: $EXP_ID/err.log"
#tail -n 3 $LOG_OUTPUT/err.log
