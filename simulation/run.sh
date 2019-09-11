#!/bin/sh

PWD_REPO=~/research/usc-isi/projects/workflow-io-bb/simulation/
DIR_OUTPUT_DEV=$PWD_REPO/output/dev-data


$PWD_REPO/build/workflow-io-bb \
    $PWD_REPO/data/platform-files/test-cori.xml \
    $PWD_REPO/data/workflow-files/genome.dax \
        2> $PWD_REPO/build/simu.log
