#!/bin/bash

for i in $(seq 2 2 2); 
do
	echo "Run $i"
	./interactive-swarp-bb.sh -c=$i
	#rm -rf $DW_JOB_STRIPED/*
done

