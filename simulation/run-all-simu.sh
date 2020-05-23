#!/bin/bash
 
#  0: No print, only simulation results
#  1: Basic print
#  2: Debug print
verbose=0

##### Main


## Change to $@
DIRS="$@"

#On mac OSX gdate, on Linux date
DATE=gdate

PWD=$(pwd)

DIR_OUTPUT=$PWD/output/
CSV_OUTPUT="simu.csv"

SCRIPT=europar20-swarp.sh

HEADER=0

for dir in $DIRS; do
    echo "[$($DATE --rfc-3339=ns)] processing: $(basename $dir)..."
    if [[ -d "$dir" && "$(ls -A $dir)" ]]; then
        if (( "$HEADER" == 0 )); then
            VERBOSE="$verbose" bash $SCRIPT --dir "$dir" --csv "$CSV_OUTPUT"
        else
            VERBOSE="$verbose" bash $SCRIPT --dir "$dir" --csv "$CSV_OUTPUT" --no-header
        fi
        HEADER=1
    else
        echo "$(basename $dir) does not exist."
    fi
done

echo "[$($DATE --rfc-3339=ns)] Done."

