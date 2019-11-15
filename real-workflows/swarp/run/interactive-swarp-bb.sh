#!/bin/bash

#set -x

usage()
{
    echo "usage: $0 [[[-f=file ] [-c=COUNT]] | [-h]]"
}

if [ -z "$DW_JOB_STRIPED" ]; then
	echo "Error: burst buffer allocation found. Run start_nostage.sh first"
	exit
fi

##### Main

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

echo $FILES_TO_STAGE
echo $COUNT

IMAGE_PATTERN='PTF201111*.w.fits'
IMAGE_WEIGHT_PATTERN='PTF201111*.w.weight.fits'
RESAMPLE_PATTERN='PTF201111*.w.resamp.fits'

BASE="/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/"

EXE=$BASE/bin/swarp
COPY=$BASE/copy.py
FILE_MAP=$BASE/build_filemap.py

NODE_COUNT=1		# Number of compute nodes requested by srun
TASK_COUNT=1		# Number of tasks allocated by srun
CORE_COUNT=1		# Number of cores used by both tasks

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
export OUTPUT_DIR=$DW_JOB_STRIPED/output.interactive.${CORE_COUNT}c.${COUNT}f.$SLURM_JOB_ID/

echo $OUTPUT_DIR

OUTPUT_FILE=$OUTPUT_DIR/output.log

rm -rf $DW_JOB_STRIPED/*

mkdir -p $OUTPUT_DIR
chmod 777 $OUTPUT_DIR

export RESAMP_DIR=$DW_JOB_STRIPED/resamp
mkdir -p $RESAMP_DIR
chmod 777 $RESAMP_DIR

rm -f {error,output}.*

#### To select file to stage
## To modify the lines 1 to 5 to keep 5 files on the PFS (by default they all go on the BB)
cp $FILES_TO_STAGE $OUTPUT_DIR/$FILES_TO_STAGE
FILES_TO_STAGE=$OUTPUT_DIR/$FILES_TO_STAGE
#sed -i -e "1,${COUNT}s|\(\$DW_JOB_STRIPED\/\)|${BASE}|" $FILES_TO_STAGE
#We want to unstage the w.fits and the corresponding w.weight.fits
if (( "$COUNT" > 0 )); then
	sed -i -e "1,${COUNT}s|\(\$DW_JOB_STRIPED\/\)\(.*w.fits\)|${BASE}\2|" $FILES_TO_STAGE
	## TODO: Fix this, only work if files are sorted w.fits first and with 16 files....
	x=$(echo "$COUNT+16" | bc)
	sed -i -e "16,${x}s|\(\$DW_JOB_STRIPED\/\)\(.*w.weight.fits\)|${BASE}\2|" $FILES_TO_STAGE
fi

echo "Number of files kept in PFS: $(echo "$COUNT*2" | bc)/$(cat $FILES_TO_STAGE | wc -l)" | tee $OUTPUT_FILE
echo "NODE $NODE_COUNT" | tee -a $OUTPUT_FILE
echo "TASK $TASK_COUNT" | tee -a $OUTPUT_FILE
echo "CORE $CORE_COUNT" | tee -a $OUTPUT_FILE

MONITORING="env OUTPUT_DIR=$OUTPUT_DIR RESAMP_DIR=$RESAMP_DIR CORE_COUNT=$CORE_COUNT pegasus-kickstart -z"

module load dws
sessID=$(dwstat sessions | grep $SLURM_JOBID | awk '{print $1}')
echo "session ID is: "${sessID} | tee -a $OUTPUT_FILE
instID=$(dwstat instances | grep $sessID | awk '{print $1}')
echo "instance ID is: "${instID} | tee -a $OUTPUT_FILE
echo "fragments list:" | tee -a $OUTPUT_FILE
echo "frag state instID capacity gran node" | tee -a $OUTPUT_FILE
dwstat fragments | grep ${instID} | tee -a $OUTPUT_FILE

echo "Starting STAGE_IN... $(date --rfc-3339=ns)" | tee -a $OUTPUT_FILE
t1=$(date +%s.%N)
if [ -f "$FILES_TO_STAGE" ]; then
	$COPY -f $FILES_TO_STAGE
else
	$COPY -i $INPUT_DIR_PFS -o $INPUT_DIR
fi
if [ "$STAGE_EXEC" = 1 ]; then
	cp -r $EXE $DW_JOB_STRIPED
fi
if [ "$STAGE_CONFIG" = 1 ]; then
	cp -r $CONFIG_DIR $DW_JOB_STRIPED
fi
t2=$(date +%s.%N)
tdiff1=$(echo "$t2 - $t1" | bc -l)
echo "TIME STAGE_IN $tdiff1" | tee -a $OUTPUT_FILE

#If we did not stage nay input files
if [[ ! -d "$INPUT_DIR" || -f "$(ls -A $INPUT_DIR)" ]]; then
	INPUT_DIR=$INPUT_DIR_PFS
	echo "INPUT_DIR set as $INPUT_DIR (no input in the BB)"
fi

#if we stge in executable
if [ "$STAGE_EXEC" = 1 ]; then
	EXE=$DW_JOB_STRIPED/swarp
fi

RESAMPLE_FILES="$OUTPUT_DIR/resample_files.txt"
$FILE_MAP -I $INPUT_DIR_PFS -B $INPUT_DIR -O $RESAMPLE_FILES -R $IMAGE_PATTERN  | tee -a $OUTPUT_FILE

du -sh $DW_JOB_STRIPED/ | tee -a $OUTPUT_FILE
echo "Starting RESAMPLE... $(date --rfc-3339=ns)" | tee -a $OUTPUT_FILE
t1=$(date +%s.%N)

srun -N $NODE_COUNT -n $TASK_COUNT -C "haswell" -c $CORE_COUNT --cpu-bind=cores \
	-o "$OUTPUT_DIR/output.resample" \
	-e "$OUTPUT_DIR/error.resample" \
    	$MONITORING -l "$OUTPUT_DIR/stat.resample.xml" \
	$EXE -c $RESAMPLE_CONFIG $(cat $RESAMPLE_FILES)
#	$EXE -c $DW_JOB_STRIPED/config/resample.swarp ${INPUT_DIR}/${IMAGE_PATTERN}

t2=$(date +%s.%N)
tdiff2=$(echo "$t2 - $t1" | bc -l)
echo "TIME RESAMPLE $tdiff2" | tee -a $OUTPUT_FILE

echo "Starting combine... $(date --rfc-3339=ns)" | tee -a $OUTPUT_FILE
t1=$(date +%s.%N)

###
## TODO: Copy back from the PFS the resamp files so we an play also with the alloc there
###

srun -N $NODE_COUNT -n $TASK_COUNT -C "haswell" -c $CORE_COUNT --cpu-bind=cores \
	-o "$OUTPUT_DIR/output.coadd" \
	-e "$OUTPUT_DIR/error.coadd" \
    	$MONITORING -l "$OUTPUT_DIR/stat.combine.xml" \
	$EXE -c $COMBINE_CONFIG ${RESAMP_DIR}/${RESAMPLE_PATTERN}

t2=$(date +%s.%N)
tdiff3=$(echo "$t2 - $t1" | bc -l)
echo "TIME COMBINE $tdiff3" | tee -a $OUTPUT_FILE

du -sh $DW_JOB_STRIPED/ | tee -a $OUTPUT_FILE

env | grep SLURM > $OUTPUT_DIR/slurm.env

echo "Starting STAGE_OUT... $(date --rfc-3339=ns)" | tee -a $OUTPUT_FILE
t1=$(date +%s.%N)
cp -r $OUTPUT_DIR $(pwd)
t2=$(date +%s.%N)
tdiff4=$(echo "$t2 - $t1" | bc -l)
echo "TIME STAGE_OUT $tdiff4" | tee -a $OUTPUT_FILE

echo "========" | tee -a $OUTPUT_FILE
tdiff=$(echo "$tdiff1 + $tdiff2 + $tdiff3 + $tdiff4" | bc -l)
echo "TIME TOTAL $tdiff" | tee -a $OUTPUT_FILE

rm -rf $(pwd)/$OUTPUT_DIR/*.fits

