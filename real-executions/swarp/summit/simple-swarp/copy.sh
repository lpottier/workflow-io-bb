#!/bin/bash

#set -x

while getopts 'f:' opt; do
    case $opt in
        f) INPUT_FILE=$OPTARG ;;
    esac
done

BB_DIR="/mnt/bb/$USER"

#$WRAPPER ls -alh $BB_DIR
mkdir -p $BB_DIR/input

cp "$INPUT_FILE" "$BB_DIR"
INPUT="$BB_DIR/$INPUT_FILE"

start_time=$(date +%s.%N)
i=1
while IFS=' ' read -r src dst
do
	#src=$(echo $line | cut -d' ' -f1)
	#dst=$(echo $line | cut -d' ' -f2)
	
	#echo "$i:$(basename $src) -> $(dirname $dst)/$(basename $src)"
	#$WRAPPER mkdir -p $(eval $(dirname $dst) )
    i=$(echo "$i + 1" | bc -l)
    cp "$src" "$dst" &
done < "$INPUT"

wait

end_time=$(date +%s.%N)

time_stagein=$(echo "$end_time - $start_time" | bc -l)

SIZE_BB=$($WRAPPER du -d1 $BB_DIR/input/ | awk '{print $1}')

nbline=$(cat $INPUT_FILE | wc -l) 
nbline_after=$($WRAPPER ls $BB_DIR/input | wc -l)

if (( $nbline == $nbline_after )); then
    #echo "CHECK OK"
    echo "$time_stagein"
else
    #echo "CHECK FAIL ($nbline != $nbline_after)"
    echo "-1"
fi

#echo "SIZE(B) $SIZE_BB"
#echo "TIME(S) STAGE_IN $time_stagein"
#echo "BANDWIDTH(MB/S) $(echo "$time_stagein / ( $SIZE_BB / ( 1024 * 1024 ) )" | bc -l)"

#$WRAPPER ls -alh $BB_DIR/input


