#!/bin/bash
 
#  0: No print, only simulation results
#  1: Basic print
#  2: Debug print
VERBOSE=0

usage()
{
    echo "usage: $0 [[[-d=directory] [-c=csv] [-s print CSV header]] | [-h]]"
}

##### Main

HEADER_CSV=1

for i in "$@"; do
    case $i in
            -d=*|--dir=*)
                MAIN_DIR="${i#*=}"
                shift # past argument=value
            ;;
            -c=*|--csv=*)
                CSV_OUTPUT="${i#*=}"
                shift # past argument=value
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
DAXGEN_DIR="/Users/lpottier/research/usc-isi/projects/workflow-io-bb/real-workflows/swarp/pegasus/"

FILE_MAP="files_to_stage.txt"
OUTPUT_LOG="output.log"
STAGEIN_CSV="stage-in-bb-global.csv"
RSMPL="stat.resample"
COMBINE="stat.combine"

PLATFORM="cori.xml"

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


core=$(echo "$(basename $EXP_DIR)" | cut -f3 -d'.')
core=${core%%c}

jobpid=$(echo "$(basename $EXP_DIR)" | cut -f5 -d'.')

fits=${MAIN_DIR##*-}

echo "[$($DATE --rfc-3339=ns)] processing: $(basename $EXP_DIR)"

echo ""

for run in $(ls $EXP_DIR | sort -n); do
    if (( "$VERBOSE" >= 1 )); then
        echo "run found: $(basename $run)"
    fi
    for pipeline in $(find $EXP_DIR/$run/* -maxdepth 0 -type d | sort -n); do
        if (( "$VERBOSE" >= 1 )); then
            echo "  pipeline found: $(basename $pipeline)"
        fi

        LOC_OUTPUTLOG="$(find $EXP_DIR/$run -maxdepth 1 -type f -name $OUTPUT_LOG)"
        LOC_FILEMAP="$(find $pipeline -maxdepth 1 -type f -name $FILE_MAP)"

        LOC_STAGEIN="$(find $EXP_DIR/$run -maxdepth 1 -type f -name $STAGEIN_CSV)"
        LOC_RSMPL="$(find $pipeline -maxdepth 1 -type f -name $RSMPL*)"
        LOC_COMBINE="$(find $pipeline -maxdepth 1 -type f -name $COMBINE*)"

        if (( "$VERBOSE" >= 2 )); then
            echo "  |-> $OUTPUT_LOG found: $(basename $LOC_OUTPUTLOG)"
            echo "  |-> $FILE_MAP found: $(basename $LOC_FILEMAP)"
            echo "  |-> $STAGEIN_CSV found: $(basename $LOC_STAGEIN)"
            echo "  |-> $RSMPL found: $(basename $LOC_RSMPL)"
            echo "  |-> $COMBINE found: $(basename $LOC_COMBINE)"
        fi


        stagein=$(awk -F "\"* \"*" '{print $6}' $LOC_STAGEIN)
        stagein=$(echo $stagein | cut -d' ' -f2) 
        rsmpl=$(sed -n 's/^<invocation.*duration=\"\([0-9]*\.[0-9]*\)\".*>/\1/p' $LOC_RSMPL)
        combine=$(sed -n 's/^<invocation.*duration=\"\([0-9]*\.[0-9]*\)\".*>/\1/p' $LOC_COMBINE)
        mksp=$(echo "$stagein + $rsmpl +$combine" | bc -l)

        err_daxgen="/dev/null"
        if (( "$VERBOSE" >= 1 )); then
            err_daxgen=1
        fi

        DAX="$pipeline/${WORKFLOW%%.*}-$(basename $pipeline).dax"
        ## Generate Pegasus DAX
        $PYTHON $DAXGEN_DIR/daxgen.py \
            --dax-file "$DAX" \
            --scalability "$(basename $pipeline)" \
            --stage-in > $err_daxgen 2>&1

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

        # TODO parse the folder name to get the number of cores + stage fits


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
            -o "$DAX" --debug 2>$err_ks_to_wrench

       
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

        nb_pipeline=$(basename $pipeline)
        if (( $nb_pipeline == 0 )); then
            nb_pipeline=$(echo "$nb_pipeline + 1" | bc -l)
        fi

        if [[ "$fits" == "stagefits" ]]; then
            $PWD/build/workflow-io-bb \
                --jobid="$jobpid" \
                --id="$(basename $run)" \
                --pipeline="$nb_pipeline" \
                --platform="$PWD/data/platform-files/$PLATFORM" \
                --dax="$DAX" \
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
                --platform="$PWD/data/platform-files/$PLATFORM" \
                --dax="$DAX" \
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
#done

echo ""
echo "[$($DATE --rfc-3339=ns)] Done."

