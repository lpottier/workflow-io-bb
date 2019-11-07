#!/bin/bash

set -x

salloc -N 1 -C haswell -q interactive -t 01:00:00 --bbf=run/bbf.conf
