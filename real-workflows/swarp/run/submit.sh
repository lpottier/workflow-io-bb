#!/bin/bash

#set -x

usage()
{
    echo "usage: $0 [ [-q=queue | --qos=queue] | [-h]]"
}

WHO=$(whoami)
EXEC="batch-swarp-bb.sh"
START=0
STEP=2
LOCAL=8 # if debug (only 5 jobs max so 0 -> 8 with a step=2 -> 5 jobs)
TOTAL=16
MAX_SUBMIT_JOBS=5
MAX_RUNNING_JOBS=2
SEC=30

QUEUE="debug"

for i in "$@"; do
        case $i in
                -q=*|--qos=*)
                        QUEUE="${i#*=}"
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

if [[ "$QUEUE" == "regular" ]]; then
	MAX_SUBMIT_JOBS=5000
fi
echo "Queue: $QUEUE"

for i in $(seq $START $STEP $LOCAL); do
	echo -n "Run $i..."
	sbatch --partition="$QUEUE" "$EXEC" -f="files_to_stage.txt" -c="$i"
done

echo "Jobs launched..."

echo -n "waiting for an empty queue"
until (( $(squeue -p $QUEUE -u $WHO -o "%A" -h | wc -l) == 0  )); do
    sleep $SEC
    echo -n "."
done

echo " $QUEUE queue is empty, start new batch of jobs"

LOCAL=$(echo "$LOCAL + $STEP" | bc -l)

for i in $(seq $LOCAL $STEP $TOTAL); do
	echo -n "Run $i..."
    sbatch --partition="$QUEUE" "$EXEC" -f="files_to_stage.txt" -c="$i"
done

