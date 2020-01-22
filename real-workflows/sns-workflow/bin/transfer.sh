#!/bin/bash
#set -Eexo pipefail

module swap craype-${CRAY_CPU_TARGET} craype-haswell

WRAPPER="pegasus-kickstart -z -l $OUTPUT_DIR/stat.$SLURM_JOB_NAME.$SLURM_JOB_ID.xml"

$WRAPPER "$@"

