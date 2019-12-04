#!/bin/bash
set -Eexo pipefail

#module unload namd
module swap craype-${CRAY_CPU_TARGET} craype-haswell
module load amber

srun --cpu-bind=cores cpptraj "$@"

