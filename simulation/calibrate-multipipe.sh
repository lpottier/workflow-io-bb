#!/bin/bash

npipe="$1"

private=../data/traces/swarp/shared-cori/bb_runs2020-multipipeline-1C/swarp-regular-1C-300B-${npipe}W-32F-*-private-stagefits
striped=../data/traces/swarp/shared-cori/bb_runs2020-multipipeline-1C/swarp-regular-1C-300B-${npipe}W-32F-*-striped-stagefits
summit=../data/traces/swarp/private-summit/runs-multi-pipelines-1c-clean/swarp-batch-1C-1400B-${npipe}W-32F-*-summit-stagefits

./europar20-swarp.sh -d "$private" --csv test-mp-private.csv
./europar20-swarp.sh -d "$striped" --csv test-mp-striped.csv
./europar20-swarp.sh -d "$summit" --csv test-mp-summit.csv
