#!/bin/bash
#SBATCH -p debug
#SBATCH -t 00:05:00
#SBATCH -N 1
#SBATCH --constraint=haswell
#BB create_persistent name=lpottier_bb capacity=50GB access_mode=striped type=scratch
#DW persistentdw name=lpottier_bb

ls $DW_PERSISTENT_STRIPED_lpottier_bb/
mkdir $DW_PERSISTENT_STRIPED_lpottier_bb/test_create
ls $DW_PERSISTENT_STRIPED_lpottier_bb/

