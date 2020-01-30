#!/bin/bash
#set -Eexo pipefail

set -x

module swap craype-${CRAY_CPU_TARGET} craype-haswell

COPY="cp -p -f"
WRAPPER="pegasus-kickstart -z -l $OUTPUT_DIR/$SLURM_JOB_NAME.$SLURM_JOB_ID.stat.xml"

$WRAPPER $COPY "$@"

