#!/bin/bash
set -Eexo pipefail

#module unload namd
module swap craype-${CRAY_CPU_TARGET} craype-haswell
module load amber

if [ ! -z "${PEGASUS_SCRATCH_DIR}" ]; then
        cd $PEGASUS_SCRATCH_DIR
fi

srun cpptraj "$@"

