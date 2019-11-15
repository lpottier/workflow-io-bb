#!/bin/bash

echo "=============================================="
echo "Job:$SLURM_ARRAY_JOB_ID $SLURM_ARRAY_TASK_ID/$SLURM_ARRAY_TASK_COUNT"
echo "Burst Buffers info:"
sh bbinfo.sh
echo "=============================================="

