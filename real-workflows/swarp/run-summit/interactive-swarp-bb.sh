#!/bin/bash

#set -x

usage()
{
    echo "usage: $0 [[[-f=file ] [-c=COUNT]] | [-h]]"
}

#if [ -z "$DW_JOB_STRIPED" ]; then
#	echo "Error: burst buffer allocation found. Run start_nostage.sh first"
#	exit
#fi

##### Main

cd $MEMBERWORK/csc355/workflow-io-bb/real-workflows/swarp/run-summit

DW_JOB_STRIPED="/mnt/bb/$USER/"
CMD_BB="jsrun -n 1 -c 1 -a 1"
JOB_ID=$LSB_JOBID

FILES_TO_STAGE="files_to_stage.txt"
COUNT=0

for i in "$@"; do
	case $i in
    		-f=*|--file=*)
    			FILES_TO_STAGE="${i#*=}"
    			shift # past argument=value
    		;;
    		-c=*|--count=*)
    			COUNT="${i#*=}"
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

echo $COUNT

IMAGE_PATTERN='PTF201111*.w.fits'
IMAGE_WEIGHT_PATTERN='PTF201111*.w.weight.fits'
RESAMPLE_PATTERN='PTF201111*.w.resamp.fits'

BASE="$MEMBERWORK/csc355/workflow-io-bb/real-workflows/swarp/"

EXE=$BASE/bin/swarp
COPY=$BASE/copy.py
FILE_MAP=$BASE/build_filemap.py

NODE_COUNT=1		# Number of compute nodes requested by srun
TASK_COUNT=1		# Number of tasks allocated by srun
export CORE_COUNT=1		# Number of cores used by both tasks

FILES_TO_STAGE="files_to_stage.txt"
STAGE_EXEC=0 		#0 no stage. 1 -> stage exec in BB
STAGE_CONFIG=0 		#0 no stage. 1 -> stage config dir in BB


CONFIG_DIR=$BASE/config
if [ "$STAGE_CONFIG" = 1 ]; then
	CONFIG_DIR=$DW_JOB_STRIPED/config
fi
RESAMPLE_CONFIG=$CONFIG_DIR/resample.swarp
COMBINE_CONFIG=$CONFIG_DIR/combine.swarp

CONFIG_FILES="${RESAMPLE_CONFIG} ${COMBINE_CONFIG}"

INPUT_DIR_PFS=$BASE/input
INPUT_DIR=$DW_JOB_STRIPED/input
export OUTPUT_DIR=$DW_JOB_STRIPED/output.interactive.${CORE_COUNT}c.${COUNT}f.$JOB_ID/

echo $OUTPUT_DIR

OUTPUT_FILE=$OUTPUT_DIR/output.log

$CMD_BB mkdir -p $OUTPUT_DIR
#chmod 777 $OUTPUT_DIR

export RESAMP_DIR=$DW_JOB_STRIPED/resamp
$CMD_BB mkdir -p $RESAMP_DIR
#chmod 777 $RESAMP_DIR

rm -f {error,output}.*

#### To select file to stage
## To modify the lines 1 to 5 to keep 5 files on the PFS (by default they all go on the BB)
$CMD_BB cp $FILES_TO_STAGE $OUTPUT_DIR/$FILES_TO_STAGE
FILES_TO_STAGE=$OUTPUT_DIR/$FILES_TO_STAGE
#sed -i -e "1,${COUNT}s|\(\$DW_JOB_STRIPED\/\)|${BASE}|" $FILES_TO_STAGE
#We want to unstage the w.fits and the corresponding w.weight.fits
if (( "$COUNT" > 0 )); then
	$CMD_BB sed -i -e "1,${COUNT}s|\(\$DW_JOB_STRIPED\/\)\(.*w.fits\)|${BASE}\2|" $FILES_TO_STAGE
	## TODO: Fix this, only work if files are sorted w.fits first and with 16 files....
	x=$(echo "$COUNT+16" | bc)
	$CMD_BB sed -i -e "16,${x}s|\(\$DW_JOB_STRIPED\/\)\(.*w.weight.fits\)|${BASE}\2|" $FILES_TO_STAGE
fi

echo "Number of files kept in PFS: $(echo "$COUNT*2" | bc)/$($CMD_BB cat $FILES_TO_STAGE | wc -l)" | $CMD_BB tee $OUTPUT_FILE
echo "NODE $NODE_COUNT" | $CMD_BB tee -a $OUTPUT_FILE
echo "TASK $TASK_COUNT" | $CMD_BB tee -a $OUTPUT_FILE
echo "CORE $CORE_COUNT" | $CMD_BB tee -a $OUTPUT_FILE

MONITORING="env OUTPUT_DIR=$OUTPUT_DIR RESAMP_DIR=$RESAMP_DIR CORE_COUNT=$CORE_COUNT pegasus-kickstart -z"

$CMD_BB df -h $DW_JOB_STRIPED/ | $CMD_BB tee -a $OUTPUT_FILE

echo "Starting STAGE_IN... $(date --rfc-3339=ns)" | $CMD_BB tee -a $OUTPUT_FILE
t1=$(date +%s.%N)
if [ -f "$FILES_TO_STAGE" ]; then
	$CMD_BB $COPY -f $FILES_TO_STAGE
else
	$CMD_BB $COPY -i $INPUT_DIR_PFS -o $INPUT_DIR
fi
if [ "$STAGE_EXEC" = 1 ]; then
	$CMD_BB cp -r $EXE $DW_JOB_STRIPED
fi
if [ "$STAGE_CONFIG" = 1 ]; then
	$CMD_BB cp -r $CONFIG_DIR $DW_JOB_STRIPED
fi
t2=$(date +%s.%N)
tdiff1=$(echo "$t2 - $t1" | bc -l)
echo "TIME STAGE_IN $tdiff1" | $CMD_BB tee -a $OUTPUT_FILE

#If we did not stage any input files
if ( ! $CMD_BB test -d "$INPUT_DIR" || test -f "$($CMD_BB ls -A $INPUT_DIR)" ); then
	INPUT_DIR=$INPUT_DIR_PFS
	echo "INPUT_DIR set as $INPUT_DIR (no input in the BB)"
fi

#if we stage in executable
if (( "$STAGE_EXEC" == 1 )); then
	EXE=$DW_JOB_STRIPED/swarp
fi

RESAMPLE_FILES="$OUTPUT_DIR/resample_files.txt"
$CMD_BB $FILE_MAP -I $INPUT_DIR_PFS -B $INPUT_DIR -O $RESAMPLE_FILES -R $IMAGE_PATTERN  | $CMD_BB tee -a $OUTPUT_FILE
RFILES=$($CMD_BB cat $RESAMPLE_FILES)
$CMD_BB du -sh $DW_JOB_STRIPED/ | $CMD_BB tee -a $OUTPUT_FILE

echo "Starting RESAMPLE... $(date --rfc-3339=ns)" | $CMD_BB tee -a $OUTPUT_FILE
t1=$(date +%s.%N)

jsrun -n $NODE_COUNT -a $TASK_COUNT -c $CORE_COUNT \
	-o "output.resample" \
	-k "error.resample" \
    $MONITORING -l "$OUTPUT_DIR/stat.resample.xml"  $EXE -c $RESAMPLE_CONFIG $RFILES
#	$EXE -c $DW_JOB_STRIPED/config/resample.swarp ${INPUT_DIR}/${IMAGE_PATTERN}
#    $MONITORING -l "$OUTPUT_DIR/stat.resample.xml" \

t2=$(date +%s.%N)
tdiff2=$(echo "$t2 - $t1" | bc -l)
echo "TIME RESAMPLE $tdiff2" | $CMD_BB tee -a $OUTPUT_FILE

echo "Starting COMBINE... $(date --rfc-3339=ns)" | $CMD_BB tee -a $OUTPUT_FILE
t1=$(date +%s.%N)

###
## TODO: Copy back from the PFS the resamp files so we an play also with the alloc there
###

RESAMPLE_FITS=$($CMD_BB bash -c "ls ${RESAMP_DIR}/${RESAMPLE_PATTERN}")

jsrun -n $NODE_COUNT -a $TASK_COUNT -c $CORE_COUNT \
	-o "output.coadd" \
	-k "error.coadd" \
    $MONITORING -l "$OUTPUT_DIR/stat.combine.xml" $EXE -c $COMBINE_CONFIG ${RESAMPLE_FITS}
#    	$MONITORING -l "$OUTPUT_DIR/stat.combine.xml" \

t2=$(date +%s.%N)
tdiff3=$(echo "$t2 - $t1" | bc -l)
echo "TIME COMBINE $tdiff3" | $CMD_BB tee -a $OUTPUT_FILE

$CMD_BB du -sh $DW_JOB_STRIPED/ | $CMD_BB tee -a $OUTPUT_FILE

#env | grep LSF > $OUTPUT_DIR/lsf.env

echo "Starting STAGE_OUT... $(date --rfc-3339=ns)" | $CMD_BB tee -a $OUTPUT_FILE
t1=$(date +%s.%N)
$CMD_BB cp -r $OUTPUT_DIR $(pwd)
t2=$(date +%s.%N)
tdiff4=$(echo "$t2 - $t1" | bc -l)
echo "TIME STAGE_OUT $tdiff4" | $CMD_BB tee -a $OUTPUT_FILE

echo "========" | $CMD_BB tee -a $OUTPUT_FILE
tdiff=$(echo "$tdiff1 + $tdiff2 + $tdiff3 + $tdiff4" | bc -l)
echo "TIME TOTAL $tdiff" | $CMD_BB tee -a $OUTPUT_FILE

rm -rf $(pwd)/$OUTPUT_DIR/*.fits

