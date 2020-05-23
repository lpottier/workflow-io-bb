#!/bin/bash

nfile="$1"

private=../data/traces/swarp/shared-cori/bb-runs2020-32c/swarp-premium-32C-200B-1W-${nfile}F-*-private-stagefits
striped=../data/traces/swarp/shared-cori/bb-runs2020-32c/swarp-premium-32C-200B-1W-${nfile}F-*-striped-stagefits
summit=../data/traces/swarp/private-summit/runs-input-files-32c-1w/swarp-batch-32C-1400B-1W-${nfile}F-*-summit-stagefits

./europar20-swarp.sh -d "$private" --csv test-private.csv
./europar20-swarp.sh -d "$striped" --csv test-striped.csv
./europar20-swarp.sh -d "$summit" --csv test-summit.csv
