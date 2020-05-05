#!/bin/bash
#SBATCH --qos=premium
#SBATCH --time=00:01:00
#SBATCH --nodes=0
#SBATCH --constraint=haswell
#BB create_persistent name=lpottier_swarp capacity=300GB access_mode=striped type=scratch

echo $DW_PERSISTENT_STRIPED_lpottier_swarp/
exit 0
