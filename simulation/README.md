# Simulation

## Reproducibility
For the paper we used WRENCH 1.4 and SimGrid 3.24 compiled with GCC 8.3.0.
The runs have been done on macOS 10.15.3.

## Build
The simulator is based on WRENCH so first you need to install WRENCH see: [https://wrench-project.org/wrench/1.4/user/install.html](How to install WRENCH).

## How to run the simulator

## NERSC Cori
To reproduce our results you will have to use the `data/platform/cori.xml` as
platform file input for SimGrid.
To run all simulation on a set of directories in a `data` folder:
```
./run-all-simu.sh $(ls -d data/* )
```
Note that, those directories must have been generated by the real runs.
They have to follow some patterns.
