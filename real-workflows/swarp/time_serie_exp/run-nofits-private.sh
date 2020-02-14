#!/bin/bash -l

source /global/homes/l/lpottier/.bashrc

PROC=32
AVG=15
BB=200
TIME="2:00:00"
PIPELINE="1"

DIR="/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/"

module restore swarp-run

cd "$DIR/time_serie_exp"

### No Fits -> done
python3 $DIR/generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 0
python3 $DIR/generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 8
python3 $DIR/generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 16
python3 $DIR/generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 24
python3 $DIR/generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 32

### Fits -> Done
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 0 -z
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 8 -z
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 16 -z
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 24 -z
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 32 -z


### Fits + Striped ->
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 0 -z -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 8 -z -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 16 -z -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 24 -z -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 32 -z -s

### No Fits + Striped
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 0 -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 8 -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 16 -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 24 -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 32 -s


