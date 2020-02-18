#!/bin/bash
 
export CC=gcc-8
export CXX=g++-8

#On mac OSX gdate, on Linux date
DATE=gdate
PYTHON="python3"

PWD=$(pwd)
DIR_OUTPUT=$PWD/output/
BUILD=$PWD/build/
KS_TO_DAX="kickstart-to-wrench.py"


echo "[$($DATE --rfc-3339=ns)] Building WRENCH simulator..."
cd $BUILD/
cmake .. && make
cd ..
echo "[$($DATE --rfc-3339=ns)] Done."

KS_DIR_FILES="$PWD/data/trace-files/swarp/shared-cori/baseline-pfs/pipeline-1_cores-1/"
MAIN_DIR="$PWD/data/trace-files/swarp/shared-cori/baseline-pfs/pipeline-1_cores-1/"
SWARP_FOLDER="swarp-premium-32C-200B-1W-0F-2-2-private"

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
echo "[$($DATE --rfc-3339=ns)] Generate a WRENCH compatible DAX..."

$PYTHON "$KS_TO_DAX" \
    -x "$PWD/data/workflow-files/$WORKFLOW" \
    -k "$KS_DIR_FILES/stat.resample.27829588.1.xml" \
    -k "$KS_DIR_FILES/stat.combine.27829588.1.xml" \
    -i "$KS_DIR_FILES/stage-in-bb-global.csv" \
    -o "$LOG_OUTPUT/$WORKFLOW"

echo "[$($DATE --rfc-3339=ns)] Done."

echo "[$($DATE --rfc-3339=ns)] Run the simulations..."
echo ""

$PWD/build/workflow-io-bb \
    --platform="$PWD/data/platform-files/$PLATFORM" \
    --dax="$LOG_OUTPUT/$WORKFLOW" \
    --stage-file="$BUILD/$FILE_MAP" \
    --real-log="$BUILD/$OUTPUT_LOG" \
    --output="$LOG_OUTPUT" \
    2> $LOG_OUTPUT/err.log

echo ""
echo "[$($DATE --rfc-3339=ns)] Done. Log written in $EXP_ID/err.log"

# echo ""
# echo "Output files written in $LOG_OUTPUT directory."
# echo "Log written: $EXP_ID/err.log"
#tail -n 3 $LOG_OUTPUT/err.log
