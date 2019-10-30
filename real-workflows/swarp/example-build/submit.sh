#!/bin/bash
#set -x
for i in 1; do
    outdir=$(mktemp --directory --tmpdir=$(/bin/pwd) swarp-run-${i}N.XXXXXX)
    script="run-swarp-scaling-bb-${i}N.sh"
    echo $outdir
    echo $script
    sed "s/@NODES@/${i}/" "run-swarp-scaling-bb.sh" > ${outdir}/${script}
    for j in $(seq ${i} -1 1); do
       stage_in="#DW stage_in source=/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp//input destination=\$DW_JOB_STRIPED/input/${j} type=directory"
       sed -i "s|@STAGE@|@STAGE@\n${stage_in}|" ${outdir}/${script}
    done
    cp "bbinfo.sh" "wrapper.sh" "${outdir}"
    cd "${outdir}"
    sbatch ${script}
    cd ..
done
