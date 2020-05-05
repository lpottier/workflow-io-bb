#!/bin/bash -l

source /global/homes/l/lpottier/.bashrc

PROC=1
AVG=1
BB=50
TIME="0:30:00"
PIPELINE="1"
FILES=32
QUEUE="debug"

DIR="/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/"

RUN="python3 $DIR/generate_scripts.py"

#You must do `module save swarp-run` from the login node first !

module restore swarp-run

#Order matters here! module restore and then source
source /global/homes/l/lpottier/.bashrc.ext

cd "$DIR/"

### No Fits -> done
$RUN -S -b $BB -p $PROC -w $PIPELINE -r $AVG -t $TIME -q $QUEUE -c $FILES

# Then in crontab -e -> "MM HH * * * bash /global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/test_cronjob/example-cron-job.sh"
