#!/bin/bash
#SBATCH --qos=debug
#SBATCH --time=1
#SBATCH --nodes=1
#SBATCH --constraint=haswell
#BB create_persistent name=swarp capacity=300GB access_mode=private type=scratch
