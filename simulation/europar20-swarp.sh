#!/bin/bash
 
#  0: No print, only simulation results
#  1: Basic print
#  2: Debug print
VERBOSE=0

usage()
{
    echo "usage: $0 [[[-d=directory] [-c=csv] [-s print CSV header]] | [-h]]"
}

echo "WARNING: do not support GCC >= 9"
##### Main

HEADER_CSV=1

while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
            -d|--dir)
                MAIN_DIR="$2"
                shift # past argument]
                shift # value
            ;;
            -c|--csv)
                CSV_OUTPUT="$2"
                shift # past argument
                shift # value
            ;;
            -s|--no-header)
                HEADER_CSV=0
                shift # past argument=value
            ;;
            -h|--usage)
                usage
                exit
            ;;
            *)
            # unknown option
            usage
            exit
            ;;
    esac
done

if [ -z "$MAIN_DIR" ]; then

    response=

    echo -n "Enter name of directory to process> "
    read response
    if [ -n "$response" ]; then
        MAIN_DIR=$response
    fi
fi

if [ -z "$CSV_OUTPUT" ]; then
    CSV_OUTPUT="res.csv"
fi

export CC=gcc-8
export CXX=g++-8
export PYTHONPATH=$(pegasus-config --python)

#On mac OSX gdate, on Linux date
DATE=gdate
PYTHON="python3"

PWD=$(pwd)

DIR_OUTPUT=$PWD/output/
BUILD=$PWD/build/
KS_TO_DAX="kickstart-to-wrench.py"
DAXGEN_DIR="/Users/lpottier/research/usc-isi/projects/active/workflow-io-bb/real-executions/swarp/pegasus"

FILE_MAP="files_to_stage.txt"
OUTPUT_LOG="output.log"
STAGEIN_CSV="stage-in-bb-global.csv"
RSMPL="stat.resample"
COMBINE="stat.combine"

DIR_PLATFORM="/Users/lpottier/research/usc-isi/projects/active/workflow-io-bb/data/platforms/"
CORI_PLATFORM="$DIR_PLATFORM/cori-1N.xml"
SUMMIT_PLATFORM="$DIR_PLATFORM/summit-1N.xml"

### WORK ONLY WITH ONE PIPELINE
WORKFLOW="swarp.dax"

echo "[$($DATE --rfc-3339=ns)] Building WRENCH simulator..."

err_make_wrench="$(mktemp /tmp/build.wrench.XXXXX)"

cd $BUILD/
cmake .. > $err_make_wrench 2>&1
make >> $err_make_wrench 2>&1
cd ..

if (( "$VERBOSE" >= 1 )); then
    cat $err_make_wrench
fi

echo "[$($DATE --rfc-3339=ns)] Done."

#MAIN_DIR="$PWD/data/trace-files/swarp/shared-cori/baseline-pfs/pipeline-1_cores-1/"

# Generate the different folder to process
current=$(pwd)
# cd $MAIN_DIR
# SWARP_FOLDER="$(find swarp-* -maxdepth 0 -type d)"
# cd $current

print_header=''

# To print the header only for the first run
if (( "$HEADER_CSV" == 0 )); then
    print_header="--no-header"
fi

#for folder in $SWARP_FOLDER; do

EXP_DIR=$(echo $MAIN_DIR/swarp*.*/swarp*/)

max_pipeline=$(echo "$(basename $EXP_DIR)" | cut -f1 -d'.')
max_pipeline=$(echo "$max_pipeline" | cut -f2 -d'-')

#max_pipeline=1
 

core=$(echo "$(basename $EXP_DIR)" | cut -f3 -d'.')
core=${core%%c}

jobpid=$(echo "$(basename $EXP_DIR)" | cut -f5 -d'.')
bb_type=$(echo "$(basename $MAIN_DIR)" | cut -f8 -d'-')


# ##########
# # Old format
# max_pipeline=1
# bb_type=$(echo "$(basename $MAIN_DIR)" | cut -f9 -d'-')
# # ##########

fits=${MAIN_DIR##*-}
if (( "$VERBOSE" >= 1 )); then
    echo $(basename $EXP_DIR) $jobpid $max_pipeline $core $fits 
fi
echo "[$($DATE --rfc-3339=ns)] processing: $(basename $EXP_DIR)"

echo ""

for run in $(ls $EXP_DIR | sort -n); do
    if (( "$VERBOSE" >= 1 )); then
        echo "run found: $(basename $run)"
    fi
    for pipeline in $(find $EXP_DIR/$run/* -maxdepth 0 -type d | sort -n); do
        if (( "$VERBOSE" >= 1 )); then
            echo "  pipeline found: $(basename $pipeline) (be cautious: it is number of pipeline + 1)"
        fi

        LOC_OUTPUTLOG="$(find $EXP_DIR/$run -maxdepth 1 -type f -name $OUTPUT_LOG)"
        LOC_FILEMAP="$(find $pipeline -maxdepth 1 -type f -name $FILE_MAP)"

        # For summit the FILE_MAP is not located at the same place (dumb me..)
        if [ -z "$LOC_FILEMAP" ]; then
            LOC_FILEMAP="$(find $MAIN_DIR/swarp*.*/ -maxdepth 1 -type f -name $FILE_MAP)"
            # I forget to copy back the input_File with the real BB path
            sed -i -e "s|@INPUT@|/mnt/bb/lpottier/input/|g" $LOC_FILEMAP
        fi

        LOC_STAGEIN="$(find $EXP_DIR/$run -maxdepth 1 -type f -name $STAGEIN_CSV)"
        LOC_RSMPL="$(find $pipeline -maxdepth 1 -type f -name $RSMPL*)"
        LOC_COMBINE="$(find $pipeline -maxdepth 1 -type f -name $COMBINE*)"

        if (( "$VERBOSE" >= 2 )); then
            if [ ! -z "$LOC_OUTPUTLOG" ]; then
                echo "  |-> $OUTPUT_LOG found: $LOC_OUTPUTLOG"
            else
                echo "  |-> ERROR: $OUTPUT_LOG not found"
                exit 1
            fi
            if [ ! -z "$LOC_FILEMAP" ]; then
                echo "  |-> $FILE_MAP found: $LOC_FILEMAP"
            else
                echo "  |-> ERROR: $FILE_MAP not found neither in $pipeline nor $MAIN_DIR/swarp*.*/"
                exit 1
            fi
            if [ ! -z "$LOC_STAGEIN" ]; then
                echo "  |-> $STAGEIN_CSV found: $LOC_STAGEIN"
            else
                echo "  |-> ERROR: $STAGEIN_CSV not found"
                exit 1
            fi
            if [ ! -z "$LOC_RSMPL" ]; then
                echo "  |-> $RSMPL found: $LOC_RSMPL"
            else
                echo "  |-> ERROR: $RSMPL not found"
                exit 1
            fi
            if [ ! -z "$LOC_COMBINE" ]; then
                echo "  |-> $COMBINE found: $LOC_COMBINE"
            else
                echo "  |-> ERROR: $COMBINE not found"
                exit 1
            fi
        fi

        stagein=$(awk -F "\"* \"*" '{print $6}' $LOC_STAGEIN)
        stagein=$(echo $stagein | cut -d' ' -f2) 
        rsmpl=$(LC_ALL=C sed -n 's/^<invocation.*duration=\"\([0-9]*\.[0-9]*\)\".*>/\1/p' $LOC_RSMPL)
        combine=$(LC_ALL=C sed -n 's/^<invocation.*duration=\"\([0-9]*\.[0-9]*\)\".*>/\1/p' $LOC_COMBINE)
        mksp=$(awk "BEGIN { print $stagein + $rsmpl + $combine }")

        err_daxgen="/dev/null"
        if (( "$VERBOSE" >= 1 )); then
            err_daxgen=1
        fi

        DAX="$pipeline/${WORKFLOW%%.*}-$(basename $pipeline).dax"
        ## Generate Pegasus DAX

        $PYTHON $DAXGEN_DIR/daxgen.py \
            --dax-file "$DAX" \
            --scalability "$max_pipeline" \
            --stage-in \
            > $err_daxgen 2>&1

        ## Generate WRENCH DAX
        if (( "$VERBOSE" >= 1 )); then
            echo ""
            echo "  [$($DATE --rfc-3339=ns)] Generate a WRENCH compatible DAX from $(basename $WORKFLOW)..."
        fi
        # DAX="$pipeline/${WORKFLOW%%.*}-$(basename $pipeline).dax"

        err_ks_to_wrench="/dev/null"
        if (( "$VERBOSE" >= 1 )); then
            err_ks_to_wrench=1
        fi

        #DAX="$pipeline/$WORKFLOW"
        $PYTHON "$KS_TO_DAX" \
            --dax="$DAX" \
            --kickstart="$LOC_RSMPL" \
            --kickstart="$LOC_COMBINE" \
            --io="0.203" \
            --io="0.26" \
            --io-stagein="1" \
            --cores-stagein="$core" \
            --cores="$core" \
            --cores="$core" \
            --stagein="$LOC_STAGEIN" \
            -o "$DAX" \
            --debug \
            2>$err_ks_to_wrench
       
        if (( "$VERBOSE" >= 1 )); then
            echo "[$($DATE --rfc-3339=ns)] Written in $DAX"
            echo ""
            echo "[$($DATE --rfc-3339=ns)] Run the simulations..."
            echo ""
        fi

        err_wrench="$pipeline/wrench.err"
        if (( "$VERBOSE" >= 2 )); then
            err_wrench=1
        fi

        # nb_pipeline=0
        nb_pipeline=$(basename $pipeline)
        if (( $nb_pipeline == 0 )); then
            nb_pipeline=$(echo "$nb_pipeline + 1" | bc -l)
        fi

        PLATFORM=$CORI_PLATFORM

        if [[ "$bb_type" == "summit" ]]; then
            PLATFORM=$SUMMIT_PLATFORM
        fi

        if [[ "$fits" == "stagefits" ]]; then
            $PWD/build/workflow-io-bb \
                --jobid="$jobpid" \
                --id="$(basename $run)" \
                --pipeline="$nb_pipeline" \
                --max-pipeline="$max_pipeline" \
                --platform="$PLATFORM" \
                --dax="$DAX" \
                --bb-type="$bb_type" \
                --stage-file="$LOC_FILEMAP" \
                --makespan="$mksp" \
                --scheduler-log="$LOC_OUTPUTLOG" \
                --cores="$core" \
                --output="$pipeline" \
                --fits \
                --csv="$CSV_OUTPUT" \
                "$print_header" \
                2> $err_wrench
        else
            $PWD/build/workflow-io-bb \
                --jobid="$jobpid" \
                --id="$(basename $run)" \
                --pipeline="$nb_pipeline" \
                --max-pipeline="$max_pipeline" \
                --platform="$PLATFORM" \
                --dax="$DAX" \
                --bb-type="$bb_type" \
                --stage-file="$LOC_FILEMAP" \
                --makespan="$mksp" \
                --scheduler-log="$LOC_OUTPUTLOG" \
                --cores="$core" \
                --output="$pipeline" \
                --csv="$CSV_OUTPUT" \
                "$print_header" \
                2> $err_wrench
        fi

        print_header="--no-header"
        
        if (( "$VERBOSE" >= 1 )); then
            echo ""
            echo "[$($DATE --rfc-3339=ns)] Done. Log written in $err_wrench"
        fi

    done
done

echo ""
echo "[$($DATE --rfc-3339=ns)] Done."

