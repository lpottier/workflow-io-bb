#!/bin/bash -l
#SBATCH -p debug
#SBATCH -N @NODES@
#SBATCH -C haswell
#SBATCH -t 00:10:00
#SBATCH -J swarp-scaling
#SBATCH -o output.%j
#SBATCH -e error.%j
#SBATCH --mail-user=lpottier@isi.edu
#SBATCH --mail-type=FAIL
#SBATCH --export=ALL
#SBATCH -d singleton
#DW jobdw capacity=150GB access_mode=striped type=scratch
#@STAGE@

use_bb=true
module unload darshan
#module load ipm/2.0.3-git_serial-io-preload

set -x
SWARP_DIR=workflow-io-bb/real-workflows/swarp
LAUNCH="$SCRATCH/$SWARP_DIR/burst_buffers_scripts/sync_launch.sh"
export CONTROL_FILE="$SCRATCH/control_file.txt"

#export | grep SLURM

CORES_PER_PROCESS=16
CONFIG_DIR=$SCRATCH/$SWARP_DIR/config  # -numa
RESAMPLE_CONFIG=${CONFIG_DIR}/resample.swarp
COMBINE_CONFIG=${CONFIG_DIR}/combine.swarp
EXE=$SCRATCH/$SWARP_DIR/swarp/swarp-2.38.0-install/bin/swarp

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
    srun \
	-N 1 \
	-n 1 \
	-c ${CORES_PER_PROCESS} \
	-o "output.resample.%j.${process}" \
	-e "error.resample.%j.${process}" \
	$LAUNCH $EXE -c $RESAMPLE_CONFIG ${indir}/${IMAGE_PATTERN} &
    cd ..
done
sleep 10
touch $CONTROL_FILE
echo "STAMP RESAMPLE $(date --rfc-3339=ns)"
t1=$(date +%s.%N)
wait
rm $CONTROL_FILE
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
	-c ${CORES_PER_PROCESS} \
	-o "output.coadd.%j.${process}" \
	-e "error.coadd.%j.${process}" \
	$LAUNCH $EXE -c $COMBINE_CONFIG ${RESAMPLE_PATTERN} &
    cd ..
done
sleep 10
touch $CONTROL_FILE
echo "STAMP COMBINE $(date --rfc-3339=ns)"
t1=$(date +%s.%N)
wait
rm $CONTROL_FILE
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
