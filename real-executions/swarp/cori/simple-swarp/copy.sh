#!/bin/bash

WRAPPER="jsrun -n1"

input="files_to_stage.txt"

while IFS= read -r line
do
	src=$(echo $line | cut -d' ' -f1)
	dst=$(echo $line | cut -d' ' -f2)
	
	#We expand variables
	#src=$(echo $src)
	#dst=$(echo $dst)
	#echo "$dst"
	echo "$(basename $src) ->  $(dirname  $dst)"
	$WRAPPER cp -r "$(eval $src)" "$(eval $(dirname $dst) )" 
done < "$input"

