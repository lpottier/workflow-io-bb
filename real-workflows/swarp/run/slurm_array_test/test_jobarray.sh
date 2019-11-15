#!/bin/bash
#SBATCH -N 1
#SBATCH -p debug
#SBATCH -C haswell
#SBATCH -J test_array
#SBATCH -t 00:05:00
#SBATCH --array=1-3

srun ./jkeg.sh

