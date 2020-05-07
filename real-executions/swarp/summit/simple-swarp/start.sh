#!/bin/bash

#set -x

PROJECT="CSC355"

# -alloc_flags "NVME" will create a directory /mnt/bb/lpottier/

bsub -Is -P $PROJECT -nnodes 1 -W 1:30 -alloc_flags "NVME" $SHELL

