#!/bin/bash

#set -x

usage()
{
    echo "usage: $0 [ [-q=queue | --qos=queue] | [-h]]"
}

WHO=$(whoami)
EXEC="batch-swarp-bb.sh"
LOCAL=6 # if debug (only 5 jobs max so 0 -> 8 with a step=2 -> 5 jobs)
TOTAL=2
MAX_JOBS=5
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
	MAX_JOBS=10
fi
echo "Queue: $QUEUE"

for i in $(seq 0 2 $LOCAL); do
	echo "Run $i"
	sbatch --partition="$QUEUE" "$EXEC" -f="files_to_stage.txt" -c="$i"
done

echo "Jobs launched..."

until (( $(echo $(squeue -p $QUEUE -u $WHO -o "%A" -h) | wc -l) > 0  )); do
	sleep $SEC
done

LOCAL=$(echo "$LOCAL + 2" | bc -l)

for i in $(seq $LOCAL 2 $TOTAL); do
	echo "Run $i"
        sbatch --partition="$QUEUE" "$EXEC" -f="files_to_stage.txt" -c="$i"
done

