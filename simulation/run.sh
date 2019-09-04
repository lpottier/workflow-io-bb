#!/bin/sh

PWD_REPO=~/research/usc-isi/projects/workflow-io-bb/simulation/

$PWD_REPO/build/workflow-io-bb \
    $PWD_REPO/data/platform-files/hosts.xml \
    $PWD_REPO/data/workflow-files/genome.dax
