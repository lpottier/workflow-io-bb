# SWarp workflow

The SWarp workflow is used by the Dark Energy Camera Legacy Survey (DECaLS) 
project to produce high resolution images  (FITS format) of the northern hemisphere sky. 
One SWarp pipeline consists into two tasks:
 1. the _Resample_ (_rsmpl_) takes 16 raw images (32 MiB each) and 16 weight-map images
 (16 MiB each) as input and produces one resampled image for each input 
 (so 32 output files). The total input size of the pipeline is around 770 MiB.
 2. the _Combine_ (_coadd_) takes the resampled images and  weight-map images as input and combines them into a single image and a single weight-map.

Note that, both tasks are implemented by the same executable `swarp`, so 
both steps could be done as one bigger step. `swarp` is a C multi-threaded (POSIX thread) code.

For instance, with 12 threads on one Intel(R) Xeon(R) CPU E5-2698 v3, each step takes approximately 30s to complete.

## How to run the one pipeline?

Has been tested with GCC 7.3.0. Find [SWarp manual](https://www.astromatic.net/pubsvn/software/swarp/trunk/doc/swarp.pdf)

### Without Burst Buffers

Run `run/run-swarp.sh` . Make sure to modify the path regarding your install of SWarp and where are the `input/`.

The configuration files in `config` directory control the problem.
For efficiency, set the following variables according to the DRAM available on your machine:
```
VMEM_MAX               31744           # Maximum amount of virtual memory (MiB)
MEM_MAX                31744           # Maximum amount of usable RAM (MiB)
COMBINE_BUFSIZE        24576           # RAM dedicated to co-addition (MiB)
```

Note that those files are set such that each stage run on 12 threads and both stage are run separately. 
```
COMBINE                N               # Combine resampled images (Y/N)?
RESAMPLE               Y               # Resample input images (Y/N)?
```
If both values are set to `Y` then both stages are executed one after the other in the same task.

The `NTHREADS` controls the number of threads.
```
NTHREADS               12               # No. threads
```
If `NTHREADS = 0` or if `NTHREADS` is not defined then automatically the number of threads equals to the number of cores.

### With Burst Buffers