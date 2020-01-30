#!/bin/bash
#set -Eexo pipefail

module unload namd
module swap craype-${CRAY_CPU_TARGET} craype-haswell
#module load namd

WRAPPER="pegasus-kickstart -z -l $OUTPUT_DIR/$SLURM_JOB_NAME.$SLURM_JOB_ID.stat.xml"

srun -n 1 -N 1 -c 1 $WRAPPER /global/homes/l/lpottier/workflow-io-bb/real-workflows/sns-workflow/bin/namd2 "$@"

