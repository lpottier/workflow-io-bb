#!/bin/bash -l
#SBATCH -p debug
#SBATCH --ntasks=2
#SBATCH -C haswell
#SBATCH --ntasks-per-node=2
#SBATCH --ntasks-per-socket=1
#SBATCH -t 0:30:00
#SBATCH -J swarp-scaling
#SBATCH -o output.%j
#SBATCH -e error.%j
#SBATCH --mail-user=lpottier@isi.edu
#SBATCH --mail-type=FAIL
#SBATCH --export=ALL
#DW jobdw capacity=50GB access_mode=striped type=scratch
#@STAGE@

module unload darshan
module load perftools-base perftools

usage()
{
    echo "usage: $0 [[[-f=file ] [-c=COUNT]] | [-h]]"
}
if [ -z "$DW_JOB_STRIPED" ]; then
    echo "Error: burst buffer allocation found. Run start_nostage.sh first"
    exit
fi

FILES_TO_STAGE="files_to_stage.txt"
COUNT=0
CURRENT_DIR=$(pwd)

# Test code to verify command line processing

if [ -f "$FILES_TO_STAGE" ]; then
    echo "File list used: $FILES_TO_STAGE"
else
    echo "$FILES_TO_STAGE does not seem to exist"
    exit
fi

if (( "$COUNT" < 0 )); then
    COUNT=$(cat $FILES_TO_STAGE | wc -l)
fi

echo "List of files used: $FILES_TO_STAGE"
echo "Number of files staged in BB: $COUNT"

IMAGE_PATTERN='PTF201111*.w.fits'
IMAGE_WEIGHT_PATTERN='PTF201111*.w.weight.fits'
RESAMPLE_PATTERN='PTF201111*.w.resamp.fits'

SWARP_DIR=workflow-io-bb/real-workflows/swarp
BASE="$SCRATCH/$SWARP_DIR/swarp-debug-16C-50B-1_4W-0F-16-12/"
LAUNCH="$SCRATCH/$SWARP_DIR/swarp-debug-16C-50B-1_4W-0F-16-12//wrapper.sh"
EXE=/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp//bin/swarp
COPY=/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp//copy.py
FILE_MAP=/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp//build_filemap.py

NODE_COUNT=2   # Number of compute nodes requested by sbatch
TASK_COUNT=$SLURM_NTASKS   # Number of tasks allocated
CORE_COUNT=16        # Number of cores used by both tasks

STAGE_EXEC=0        #0 no stage. 1 -> stage exec in BB
STAGE_CONFIG=0      #0 no stage. 1 -> stage config dir in BB
NB_AVG=2            # Number of identical runs

CONFIG_DIR=$BASE
if (( "$STAGE_CONFIG" == 1 )); then
    CONFIG_DIR=$DW_JOB_STRIPED/config
fi
RESAMPLE_CONFIG=${CONFIG_DIR}/resample.swarp
COMBINE_CONFIG=${CONFIG_DIR}/combine.swarp

CONFIG_FILES="${RESAMPLE_CONFIG} ${COMBINE_CONFIG}"

INPUT_DIR_PFS=$SCRATCH/$SWARP_DIR/input
INPUT_DIR=$DW_JOB_STRIPED/input

OUTPUT_DIR_NAME=$SLURM_JOB_NAME.batch.${CORE_COUNT}c.${COUNT}f.$SLURM_JOB_ID/
export GLOBAL_OUTPUT_DIR=$DW_JOB_STRIPED/$OUTPUT_DIR_NAME
mkdir -p $GLOBAL_OUTPUT_DIR
chmod 777 $GLOBAL_OUTPUT_DIR

mkdir -p $OUTPUT_DIR_NAME

for k in $(seq 1 1 $NB_AVG); do
    echo "#### Starting run $k... $(date --rfc-3339=ns)"
    rm -rf $INPUT_DIR

    export OUTPUT_DIR=$GLOBAL_OUTPUT_DIR/${k}
    mkdir -p $OUTPUT_DIR
    echo "OUTPUT_DIR -> $OUTPUT_DIR"

    #The local version
    mkdir -p $OUTPUT_DIR_NAME/${k}
    echo "LOCAL_OUTPUT_DIR -> $OUTPUT_DIR_NAME/${k}"

    OUTPUT_FILE=$OUTPUT_DIR/output.log
    BB_INFO=$OUTPUT_DIR/bb.log
    DU_RES=$OUTPUT_DIR/data-stagedin.log
    BB_ALLOC=$OUTPUT_DIR/bb_alloc.log

    mkdir -p $OUTPUT_DIR
    chmod 777 $OUTPUT_DIR

    export RESAMP_DIR=$OUTPUT_DIR/resamp

    mkdir -p $RESAMP_DIR
    chmod 777 $RESAMP_DIR

    #rm -f {error,output}.*

    for process in $(seq 1 ${TASK_COUNT}); do
        mkdir -p ${OUTPUT_DIR}/${process}
        mkdir -p $OUTPUT_DIR_NAME/${k}/${process}
    done
    #### To select file to stage
    ## To modify the lines 1 to 5 to keep 5 files on the PFS (by default they all go on the BB)
    cp $FILES_TO_STAGE $OUTPUT_DIR/
    LOC_FILES_TO_STAGE="$OUTPUT_DIR/$FILES_TO_STAGE"
    sed -i -e "s|@INPUT@|$INPUT_DIR|" "$LOC_FILES_TO_STAGE"
    echo "Number of files kept in PFS:$(echo "$COUNT*2" | bc)/$(cat $LOC_FILES_TO_STAGE | wc -l)" | tee $OUTPUT_FILE
    echo "NODE=$NODE_COUNT" | tee -a $OUTPUT_FILE
    echo "TASK=$TASK_COUNT" | tee -a $OUTPUT_FILE
    echo "CORE=$CORE_COUNT" | tee -a $OUTPUT_FILE
    echo "NTASKS_PER_NODE=$SLURM_NTASKS_PER_NODE" | tee -a $OUTPUT_FILE

    echo "Compute nodes: $(srun uname -n) " | tee -a $OUTPUT_FILE
    MONITORING="env OUTPUT_DIR=$OUTPUT_DIR RESAMP_DIR=$RESAMP_DIR CORE_COUNT=$CORE_COUNT pegasus-kickstart -z"

    module unload python3
    module load dws
    sessID=$(dwstat sessions | grep $SLURM_JOBID | awk '{print $1}')
    echo "session ID is: "${sessID} | tee $BB_INFO
    instID=$(dwstat instances | grep $sessID | awk '{print $1}')
    echo "instance ID is: "${instID} | tee -a $BB_INFO
    echo "fragments list:" | tee -a $BB_INFO
    echo "frag state instID capacity gran node" | tee -a $BB_INFO
    dwstat fragments | grep ${instID} | tee -a $BB_INFO

    bballoc=$(dwstat fragments | grep ${instID} | awk '{print $4}')
    echo "$bballoc" > $BB_ALLOC

    echo "Starting STAGE_IN... $(date --rfc-3339=ns)" | tee -a $OUTPUT_FILE
    t1=$(date +%s.%N)
    if [ -f "$LOC_FILES_TO_STAGE" ]; then
        $COPY -f $LOC_FILES_TO_STAGE -d $OUTPUT_DIR
    else
        $COPY -i $INPUT_DIR_PFS -o $INPUT_DIR -d $OUTPUT_DIR
    fi

    if (( "$STAGE_EXEC" == 1 )); then
        cp -r $EXE $DW_JOB_STRIPED
    fi

    if (( "$STAGE_CONFIG" == 1 )); then
        cp -r $CONFIG_DIR $DW_JOB_STRIPED
    fi

    t2=$(date +%s.%N)
    tdiff1=$(echo "$t2 - $t1" | bc -l)
    echo "TIME STAGE_IN $tdiff1" | tee -a $OUTPUT_FILE

    mkdir -p $INPUT_DIR

    #If we did not stage any input files
    if [[ -f "$(ls -A $INPUT_DIR)" ]]; then
        INPUT_DIR=$INPUT_DIR_PFS
        echo "No files staged in, INPUT_DIR set as $INPUT_DIR"
    fi

    if (( "$STAGE_EXEC" == 1 )); then
        EXE=$DW_JOB_STRIPED/swarp
    fi

    RESAMPLE_FILES="$OUTPUT_DIR/resample_files.txt"
    $FILE_MAP -I $INPUT_DIR_PFS -B $INPUT_DIR -O $RESAMPLE_FILES -R $IMAGE_PATTERN  | tee -a $OUTPUT_FILE

    dsize=$(du -sh $INPUT_DIR | awk '{print $1}')
    nbfiles=$(ls -al $INPUT_DIR | grep '^-' | wc -l)
    echo "$nbfiles $dsize" | tee $DU_RES

    echo "Starting RESAMPLE... $(date --rfc-3339=ns)" | tee -a $OUTPUT_FILE
    for process in $(seq 1 ${TASK_COUNT}); do
        echo "Launching RESAMPLE process ${process} at:$(date --rfc-3339=ns) ... " | tee -a $OUTPUT_FILE
        cd ${OUTPUT_DIR}/${process}
        srun --cpus-per-task=$CORE_COUNT -o "output.resample.%j.${process}" -e "error.resample.%j.${process}" $MONITORING -l "stat.resample.$SLURM_JOB_ID.${process}.xml" $EXE -c $RESAMPLE_CONFIG $(cat $RESAMPLE_FILES) &
        cd ..
        echo "done"
        echo ""
    done

    t1=$(date +%s.%N)
    wait
    t2=$(date +%s.%N)
    tdiff2=$(echo "$t2 - $t1" | bc -l)
    echo "TIME RESAMPLE $tdiff2" | tee -a $OUTPUT_FILE

    echo "Starting COMBINE... $(date --rfc-3339=ns)" | tee -a $OUTPUT_FILE

    ###
    ## TODO: Copy back from the PFS the resamp files so we an play also with the alloc there
    ###

    for process in $(seq 1 ${TASK_COUNT}); do
        echo "Launching COMBINE process ${process} at:$(date --rfc-3339=ns) ... " | tee -a $OUTPUT_FILE
        cd ${OUTPUT_DIR}/${process}
        srun --cpus-per-task=$CORE_COUNT -o "output.combine.%j.${process}" -e "error.combine.%j.${process}" $MONITORING -l "stat.combine.xml.$SLURM_JOB_ID.${process}" $EXE -c $COMBINE_CONFIG ${RESAMP_DIR}/${RESAMPLE_PATTERN} &
        cd ..
        echo "done"
        echo ""
    done

    t1=$(date +%s.%N)
    wait
    t2=$(date +%s.%N)
    tdiff3=$(echo "$t2 - $t1" | bc -l)
    echo "TIME COMBINE $tdiff3" | tee -a $OUTPUT_FILE

    du -sh $DW_JOB_STRIPED/ | tee -a $OUTPUT_FILE

    env | grep SLURM > $OUTPUT_DIR/slurm.env

    echo "Starting STAGE_OUT... $(date --rfc-3339=ns)" | tee -a $OUTPUT_FILE
    for process in $(seq 1 ${TASK_COUNT}); do
        echo "Launching STAGEOUT process ${process} at:$(date --rfc-3339=ns) ... " | tee -a $OUTPUT_FILE
        $COPY -i "${process}/" -o "$CURRENT_DIR/$OUTPUT_DIR_NAME/${k}/${process}/" -a "stage-out" -d "$CURRENT_DIR/$OUTPUT_DIR_NAME/${k}/${process}/" &
        echo "done"
        echo ""
    done
    t1=$(date +%s.%N)
    wait
    $COPY -i "${OUTPUT_DIR}/" -o "$CURRENT_DIR/$OUTPUT_DIR_NAME/${k}/" -a "stage-out" -d "$CURRENT_DIR/$OUTPUT_DIR_NAME/${k}/"
    t2=$(date +%s.%N)
    tdiff4=$(echo "$t2 - $t1" | bc -l)

    OUTPUT_FILE=$CURRENT_DIR/$OUTPUT_DIR_NAME/${k}/output.log
    rm -rf "$CURRENT_DIR/$OUTPUT_DIR_NAME/${k}/*/*.fits"
    echo "TIME STAGE_OUT $tdiff4" | tee -a $OUTPUT_FILE

    echo "========" | tee -a $OUTPUT_FILE
    tdiff=$(echo "$tdiff1 + $tdiff2 + $tdiff3 + $tdiff4" | bc -l)
    echo "TIME TOTAL $tdiff" | tee -a $OUTPUT_FILE


    echo "#### Ending run $k... $(date --rfc-3339=ns)"
done
