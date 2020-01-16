#!/bin/bash

PROC=1

../generate_scripts.py -b 50 -p $PROC -w 1 2 4 8 16 -r 5 -t 2:00:00 -q premium -c 0

../generate_scripts.py -b 50 -p $PROC -w 1 2 4 8 16 -r 5 -t 2:00:00 -q regular -c 2

../generate_scripts.py -b 50 -p $PROC -w 1 2 4 8 16 -r 5 -t 2:00:00 -q regular -c 4

../generate_scripts.py -b 50 -p $PROC -w 1 2 4 8 16 -r 5 -t 2:00:00 -q regular -c 8

../generate_scripts.py -b 50 -p $PROC -w 1 2 4 8 16 -r 5 -t 2:00:00 -q regular -c 16

../generate_scripts.py -b 50 -p $PROC -w 1 2 4 8 16 -r 5 -t 2:00:00 -q regular -c 32
