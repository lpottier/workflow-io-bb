#!/bin/bash
set -Eexo pipefail

#module unload namd
module swap craype-${CRAY_CPU_TARGET} craype-haswell
#module load namd

srun --cpu-bind=cores /global/homes/l/lpottier/workflow-io-bb/real-workflows/sns-workflow/bin/sassena "$@"

