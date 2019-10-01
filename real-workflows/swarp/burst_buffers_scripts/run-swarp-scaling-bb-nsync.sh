#!/bin/bash -l
#SBATCH -p debug
#SBATCH -N @NODES@
#SBATCH -C haswell
#SBATCH -t 00:10:00
#SBATCH -J swarp-scaling
#SBATCH -o output.%j
#SBATCH -e error.%j
#SBATCH --mail-user=lpottier@isi.edu
#SBATCH --mail-type=END,FAIL
#SBATCH --export=ALL
#SBATCH -d singleton
#DW jobdw capacity=15GB access_mode=striped type=scratch
#@STAGE@

use_bb=true
module unload darshan
module load perftools-base perftools
#module load ipm/2.0.3-git_serial-io-preload

set -x
SWARP_DIR=workflow-io-bb/real-workflows/swarp
LAUNCH="pegasus-kickstart -z"

# export CONTROL_FILE="$SCRATCH/control_file.txt"

#export | grep SLURM

export CORE_COUNT=1
CONFIG_DIR=$SCRATCH/$SWARP_DIR/config  # -numa
RESAMPLE_CONFIG=${CONFIG_DIR}/resample-bb.swarp
COMBINE_CONFIG=${CONFIG_DIR}/combine-bb.swarp
EXE=$SCRATCH/$SWARP_DIR/bin/swarp

FILE_PATTERN='PTF201111*'
IMAGE_PATTERN='PTF201111*.w.fits'
RESAMPLE_PATTERN='PTF201111*.w.resamp.fits'

echo "NUM NODES ${SLURM_JOB_NUM_NODES}"
echo "STAMP PREPARATION $(date --rfc-3339=ns)"


# Create the final output directory and run directory
#outdir=$(mktemp --directory --tmpdir=$(/bin/pwd) swarp-run-${SLURM_JOB_NUM_NODES}N.XXXXXX)
outdir="$(pwd)/output"; mkdir ${outdir}
if [ $use_bb = true ]; then
    ./bbinfo.sh
    rundir=$DW_JOB_STRIPED/swarp-run
    mkdir $rundir
else
    indir="../input" # The input data is already on OST 1
    #lfs setstripe -c 1 -o 1 ${outdir}
    rundir=$outdir
fi
# Create a output and run directory for each SWarp process
for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do
    mkdir -p ${rundir}/${process}
    mkdir -p ${outdir}/${process}
done


cd ${rundir}
echo "STAMP RESAMPLE PREP $(date --rfc-3339=ns)"
for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do
    echo "Launching resample process ${process}"
    indir="$DW_JOB_STRIPED/input/${process}" # This data has already been staged in
    cd ${process}
    #LAUNCH="env OUTPUT_DIR=$OUTPUT_DIR RESAMP_DIR=$RESAMP_DIR CORE_COUNT=$CORE_COUNT pegasus-kickstart -z"
    srun \
    -N 1 \
    -n 1 \
    -c ${CORE_COUNT} \
    -o "output.resample.%j.${process}" \
    -e "error.resample.%j.${process}" \
    $LAUNCH -l "stat.resample.%j.${process}.xml" \
    $EXE -c $RESAMPLE_CONFIG ${indir}/${IMAGE_PATTERN} &
    cd ..
done

echo "STAMP RESAMPLE $(date --rfc-3339=ns)"
t1=$(date +%s.%N)
wait
t2=$(date +%s.%N)
tdiff=$(echo "$t2 - $t1" | bc -l)
echo "TIME RESAMPLE $tdiff"

# Copy the stdout, stderr, SWarp XML files and IPM XML file
for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do
    cp -n -v ${process}/{output*,error*,*.xml} ${outdir}/${process}
done


echo "STAMP COMBINE PREP $(date --rfc-3339=ns)"
for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do
    echo "Launching coadd process ${process}"
    cd ${process}
    srun \
    -N 1 \
    -n 1 \
    -c ${CORE_COUNT} \
    -o "output.coadd.%j.${process}" \
    -e "error.coadd.%j.${process}" \
    $LAUNCH -l "stat.coadd.%j.${process}.xml" \
    $EXE -c $COMBINE_CONFIG ${RESAMPLE_PATTERN} &
    cd ..
done

echo "STAMP COMBINE $(date --rfc-3339=ns)"
t1=$(date +%s.%N)
wait
t2=$(date +%s.%N)
tdiff=$(echo "$t2 - $t1" | bc -l)
echo "TIME COMBINE $tdiff"

# Copy the stdout, stderr, SWarp XML files and IPM XML file
for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do
    ls -lh ${rundir}/${process}/*.fits $DW_JOB_STRIPED/input/${process}/*.fits > ${outdir}/${process}/list_of_files.out
    cp -n -v ${process}/{output*,error*,*.xml} ${outdir}/${process}
done
du -sh $DW_JOB_STRIPED/input ${rundir} > ${outdir}/disk_usage.out


echo "STAMP CLEANUP $(date --rfc-3339=ns)"
for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do
    rm -v ${process}/*.fits
done
echo "STAMP DONE $(date --rfc-3339=ns)"
