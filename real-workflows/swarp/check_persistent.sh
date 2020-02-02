#!/bin/bash
#SBATCH --qos=debug
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --constraint=haswell
#DW persistentdw name=swarp


echo $DW_PERSISTENT_PRIVATE_swarp/
ls $DW_PERSISTENT_PRIVATE_swarp/
sh bbinfo.sh

