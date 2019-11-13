#!/bin/bash

#set -x

PROJECT="CSC355"

bsub -Is -P $PROJECT -nnodes 1 -W 0:30 $SHELL

