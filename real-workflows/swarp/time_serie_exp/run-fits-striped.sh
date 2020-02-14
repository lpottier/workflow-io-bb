#!/bin/bash -l

source /global/homes/l/lpottier/.bashrc

PROC=32
AVG=15
BB=200
TIME="2:00:00"
PIPELINE="1"

DIR="/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/"
cd "$DIR/time_serie_exp"

module restore swarp-run

### No Fits -> done
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 0
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 8
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 16
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 24
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 32

### Fits -> Done
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 0 -z
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 8 -z
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 16 -z
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 24 -z
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 32 -z

### Fits + Striped ->
python3 $DIR/generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 0 -z -s
python3 $DIR/generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 8 -z -s
python3 $DIR/generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 16 -z -s
python3 $DIR/generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 24 -z -s
python3 $DIR/generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 32 -z -s

### No Fits + Striped
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 0 -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 8 -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 16 -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 24 -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 32 -s


