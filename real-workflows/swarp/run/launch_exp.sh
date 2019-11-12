#!/bin/bash

for i in $(seq 0 2 16); 
do
	echo "Run $i"
	sbatch batch-run-swar-bb -c=$i
	#rm -rf $DW_JOB_STRIPED/*
done

