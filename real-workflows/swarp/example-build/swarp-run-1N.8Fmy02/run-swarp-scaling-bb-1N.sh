#!/bin/bash -l
#SBATCH -p debug
#SBATCH -N 1
#SBATCH -C haswell
#SBATCH -t 00:15:00
#SBATCH -J swarp-scaling
#SBATCH -o output.%j
#SBATCH -e error.%j
#SBATCH --mail-user=lpottier@isi.edu
#SBATCH --mail-type=FAIL
#SBATCH --export=ALL
#SBATCH -d singleton
#DW jobdw capacity=100GB access_mode=striped type=scratch
#@STAGE@
#DW stage_in source=/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp//input destination=$DW_JOB_STRIPED/input/1 type=directory

module unload darshan
module load perftools-base perftools

set -x
SWARP_DIR=workflow-io-bb/real-workflows/swarp
LAUNCH="$SCRATCH/$SWARP_DIR/build/wrapper.sh"
EXE=$SCRATCH/$SWARP_DIR/bin/swarp
export CONTROL_FILE="$SCRATCH/control_file.txt"

CORES_PER_PROCESS=1
CONFIG_DIR=$SCRATCH/$SWARP_DIR/config
RESAMPLE_CONFIG=${CONFIG_DIR}/resample-orig.swarp
COMBINE_CONFIG=${CONFIG_DIR}/combine-orig.swarp
FILE_PATTERN='PTF201111*'
IMAGE_PATTERN='PTF201111*.w.fits'
RESAMPLE_PATTERN='PTF201111*.w.resamp.fits'
echo "NUM NODES ${SLURM_JOB_NUM_NODES}"
echo "STAMP PREPARATION $(date --rfc-3339=ns)"

# Create the final output directory and run directory
outdir="$(pwd)/output"; mkdir ${outdir}
sh bbinfo.sh
rundir=$DW_JOB_STRIPED/swarp-run
mkdir $rundir
# Create a output and run directory for each SWarp process
for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do
    mkdir -p ${rundir}/${process}
    mkdir -p ${outdir}/${process}
done

cd ${rundir}
du -sh $DW_JOB_STRIPED/input ${rundir} > ${outdir}/du_init.out
echo "STAMP RESAMPLE PREP $(date --rfc-3339=ns)"
for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do
    echo "Launching resample process ${process}"
    indir="$DW_JOB_STRIPED/input/${process}" # This data has already been staged in
    cd ${process}
    srun -N 1 -n 1 -c ${CORES_PER_PROCESS} -o "output.resample.%j.${process}" -e "error.resample.%j.${process}" pegasus-kickstart -z -l stat.resample.xml $LAUNCH $EXE -c $RESAMPLE_CONFIG ${indir}/${IMAGE_PATTERN} & 
    cd ..
done
echo "STAMP RESAMPLE $(date --rfc-3339=ns)"

echo "STAMP RESAMPLE $(date --rfc-3339=ns)"
t1=$(date +%s.%N)
wait
t2=$(date +%s.%N)
tdiff=$(echo "$t2 - $t1" | bc -l)
echo "TIME RESAMPLE $tdiff"
du -sh $DW_JOB_STRIPED/input ${rundir} > ${outdir}/du_resample.out

# Copy the stdout, stderr, SWarp XML files and IPM XML file
for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do
    cp -n -v ${process}/{output*,error*,*.xml} ${outdir}/${process}
done

echo "STAMP COMBINE PREP $(date --rfc-3339=ns)"
for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do
    echo "Launching coadd process ${process}"
    cd ${process}
    srun -N 1 -n 1 -c ${CORES_PER_PROCESS} -o "output.coadd.%j.${process}" -e "error.coadd.%j.${process}" pegasus-kickstart -z -l stat.combine.xml $LAUNCH $EXE -c -c $COMBINE_CONFIG ${RESAMPLE_PATTERN} &
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
