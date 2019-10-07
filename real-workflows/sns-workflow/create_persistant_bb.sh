#!/bin/bash
#SBATCH --qos=debug
#SBATCH --time=1
#SBATCH --nodes=1
#SBATCH --constraint=haswell
#BB create_persistent name=sns_workflow capacity=200GB access_mode=striped type=scratch

echo $DW_PERSISTENT_STRIPED_sns_workflow/

