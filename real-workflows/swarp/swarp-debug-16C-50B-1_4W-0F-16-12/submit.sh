#!/bin/bash
#set -x
for i in 1 2 4; do
    outdir=$(mktemp --directory --tmpdir=$(/bin/pwd) swarp-run-${i}N-0F.XXXXXX)
    script="run-swarp-scaling-bb-${i}N.sh"
    echo $outdir
    echo $script
    sed "s/@NODES@/${i}/g" "run-swarp-scaling-bb.sh" > ${outdir}/${script}
    cp /global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp//copy.py /global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp//build_filemap.py files_to_stage.txt "bbinfo.sh" "wrapper.sh" "${outdir}"
    cd "${outdir}"
    sbatch ${script}
    cd ..
done
