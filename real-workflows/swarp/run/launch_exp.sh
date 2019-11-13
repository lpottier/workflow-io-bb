#!/bin/bash

for i in $(seq 0 2 16);
do
	echo "Run $i"
	./interactive-swarp-bb.sh -c=$i
	#rm -rf $DW_JOB_STRIPED/*
done

