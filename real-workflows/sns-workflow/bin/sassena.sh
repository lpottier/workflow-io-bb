#!/bin/bash
set -Eexo pipefail

#module unload namd
module swap craype-${CRAY_CPU_TARGET} craype-haswell
#module load namd

#if [ -z "${PEGASUS_CORES}" ]; then
#        PEGASUS_CORES=1
#fi

if [ ! -z "${PEGASUS_SCRATCH_DIR}" ]; then
        cd $PEGASUS_SCRATCH_DIR
fi

srun --cpu-bind=cores /global/homes/l/lpottier/workflow-io-bb/real-workflows/sns-workflow/bin/sassena "$@"

