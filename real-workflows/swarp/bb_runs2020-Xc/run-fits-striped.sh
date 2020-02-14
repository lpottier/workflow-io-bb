#!/bin/bash -l

source /global/homes/l/lpottier/.bashrc

PROC=32
AVG=15
BB=200
TIME="2:00:00"
PIPELINE="1"
FILES=32

DIR="/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/"
cd "$DIR/bb_runs2020-Xc"

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
python3 $DIR/generate_scripts.py -S -b $BB -p 1 -w $PIPELINE -r $AVG -t $TIME -q premium -c $FILES -z -s
python3 $DIR/generate_scripts.py -S -b $BB -p 4 -w $PIPELINE -r $AVG -t $TIME -q premium -c $FILES -z -s
python3 $DIR/generate_scripts.py -S -b $BB -p 8 -w $PIPELINE -r $AVG -t $TIME -q premium -c $FILES -z -s
python3 $DIR/generate_scripts.py -S -b $BB -p 16 -w $PIPELINE -r $AVG -t $TIME -q premium -c $FILES -z -s
python3 $DIR/generate_scripts.py -S -b $BB -p 32 -w $PIPELINE -r $AVG -t $TIME -q premium -c $FILES -z -s

### No Fits + Striped
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 0 -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 8 -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 16 -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 24 -s
#../generate_scripts.py -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q premium -c 32 -s


