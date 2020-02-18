#!/bin/bash
 
#  0: No print, only simulation results
#  1: Basic print
#  2: Debug print

VERBOSE=0

export CC=gcc-8
export CXX=g++-8

#On mac OSX gdate, on Linux date
DATE=gdate
PYTHON="python3"

PWD=$(pwd)
DIR_OUTPUT=$PWD/output/
BUILD=$PWD/build/
KS_TO_DAX="kickstart-to-wrench.py"

FILE_MAP="files_to_stage.txt"
OUTPUT_LOG="output.log"
STAGEIN_CSV="stage-in-bb-global.csv"
RSMPL="stat.resample"
COMBINE="stat.combine"


PLATFORM="test-cori.xml"

### WORK ONLY WITH ONE PIPELINE
WORKFLOW="swarp-1.dax"

echo "[$($DATE --rfc-3339=ns)] Building WRENCH simulator..."

err_make_wrench="/dev/null"
if (( "$VERBOSE" >= 1 )); then
    err_ks_to_wrench=1
fi

cd $BUILD/
cmake .. > $err_make_wrench 2>&1
make > $err_make_wrench 2>&1
cd ..

echo "[$($DATE --rfc-3339=ns)] Done."

KS_DIR_FILES="$PWD/data/trace-files/swarp/shared-cori/baseline-pfs/pipeline-1_cores-1/"
MAIN_DIR="$PWD/data/trace-files/swarp/shared-cori/baseline-pfs/pipeline-1_cores-1/"
SWARP_FOLDER="swarp-premium-32C-200B-1W-0F-2-2-private"

EXP_DIR=$(echo $MAIN_DIR/$SWARP_FOLDER/swarp-run-*N-*F.*/swarp.*/)

echo "[$($DATE --rfc-3339=ns)] Target directory found: $(basename $EXP_DIR)"
echo ""

print_header=''

for run in $(find $EXP_DIR/* -maxdepth 0 -type d | sort -n); do
    if (( "$VERBOSE" >= 1 )); then
        echo "run found: $(basename $run)"
    fi
    for pipeline in $(find $run/* -maxdepth 0 -type d | sort -n); do
        if (( "$VERBOSE" >= 1 )); then
            echo "  pipeline found: $(basename $pipeline)"
        fi

        LOC_OUTPUTLOG="$(find $run -maxdepth 1 -type f -name $OUTPUT_LOG)"
        LOC_FILEMAP="$(find $pipeline -maxdepth 1 -type f -name $FILE_MAP)"

        LOC_STAGEIN="$(find $run -maxdepth 1 -type f -name $STAGEIN_CSV)"
        LOC_RSMPL="$(find $pipeline -maxdepth 1 -type f -name $RSMPL*)"
        LOC_COMBINE="$(find $pipeline -maxdepth 1 -type f -name $COMBINE*)"

        if (( "$VERBOSE" >= 2 )); then
            echo "  |-> $OUTPUT_LOG found: $(basename $LOC_OUTPUTLOG)"
            echo "  |-> $FILE_MAP found: $(basename $LOC_FILEMAP)"
            echo "  |-> $STAGEIN_CSV found: $(basename $LOC_STAGEIN)"
            echo "  |-> $RSMPL found: $(basename $LOC_RSMPL)"
            echo "  |-> $COMBINE found: $(basename $LOC_COMBINE)"
        fi
        ## Generate WRENCH DAX
        if (( "$VERBOSE" >= 1 )); then
            echo ""
            echo "  [$($DATE --rfc-3339=ns)] Generate a WRENCH compatible DAX from $(basename $WORKFLOW)..."
        fi
        #DAX="$pipeline/${WORKFLOW%%.*}-$(basename $pipeline).dax"

        err_ks_to_wrench="/dev/null"
        if (( "$VERBOSE" >= 1 )); then
            err_ks_to_wrench=1
        fi

        DAX="$pipeline/$WORKFLOW"
        $PYTHON "$KS_TO_DAX" \
            -x "$PWD/data/workflow-files/$WORKFLOW" \
            -k "$LOC_RSMPL" \
            -k "$LOC_COMBINE" \
            -i "$LOC_STAGEIN" \
            -o "$DAX" 2>$err_ks_to_wrench

        if (( "$VERBOSE" >= 1 )); then
            echo "  [$($DATE --rfc-3339=ns)] Done. Written in $DAX."
            echo ""

            echo "[$($DATE --rfc-3339=ns)] Run the simulations..."
            echo ""
        fi

        $PWD/build/workflow-io-bb \
            --id="$(basename $run)" \
            --pipeline="$(basename $pipeline)" \
            --platform="$PWD/data/platform-files/$PLATFORM" \
            --dax="$DAX" "$print_header" \
            --stage-file="$LOC_FILEMAP" \
            --real-log="$LOC_OUTPUTLOG" \
            --output="$pipeline" \
            2> $pipeline/wrench.err

        print_header="--no-header"

        if (( "$VERBOSE" >= 1 )); then
            echo ""
            echo "[$($DATE --rfc-3339=ns)] Done. Log written in $pipeline/wrench.err"
        fi
    done
done


echo ""
echo "[$($DATE --rfc-3339=ns)] Done."


# TIMESTAMP=$(date "+%ss")
# # EXP_ID="simu-pfs-${WORKFLOW%%.*}-${PLATFORM%%.*}-$TIMESTAMP"

# # mkdir -p $DIR_OUTPUT

# # LOG_OUTPUT=$DIR_OUTPUT/$EXP_ID
# # mkdir -p $LOG_OUTPUT




# echo "[$($DATE --rfc-3339=ns)] Run the simulations..."
# echo ""

# $PWD/build/workflow-io-bb \
#     --platform="$PWD/data/platform-files/$PLATFORM" \
#     --dax="$LOG_OUTPUT/$WORKFLOW" \
#     --stage-file="$BUILD/$FILE_MAP" \
#     --real-log="$BUILD/$OUTPUT_LOG" \
#     --output="$LOG_OUTPUT" \
#     2> $LOG_OUTPUT/err.log


# echo ""
# echo "Output files written in $LOG_OUTPUT directory."
# echo "Log written: $EXP_ID/err.log"
#tail -n 3 $LOG_OUTPUT/err.log
