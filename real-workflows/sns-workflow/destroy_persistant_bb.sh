#!/bin/bash
#SBATCH -p debug
#SBATCH -t 00:05:00
#SBATCH -N 1
#SBATCH --constraint=haswell
#BB destroy_persistent name=lpottier_bb

