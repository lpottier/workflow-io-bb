#!/bin/bash

#set -x

WRAPPER="jsrun -n 1 "
BB_DIR="/mnt/bb/$USER"
INPUT_FILE="files_to_stage.txt"

$WRAPPER ls -alh $BB_DIR
$WRAPPER mkdir -p $BB_DIR/input

while IFS= read -r line
do
	src=$(echo $line | cut -d' ' -f1)
	dst=$(echo $line | cut -d' ' -f2)
	
	echo "$(basename $src) ->  $(dirname $dst)"
	#$WRAPPER mkdir -p $(eval $(dirname $dst) )
	#$WRAPPER cp "$src" "$dst"
done < "$INPUT_FILE"

$WRAPPER ls -alh $BB_DIR/input


