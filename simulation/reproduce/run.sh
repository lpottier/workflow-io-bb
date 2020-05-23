#!/bin/bash

../build/workflow-io-bb  --fits --platform cori-1N.xml --stage-file files_to_stage.txt --dax "$1"

#../build/workflow-io-bb --fits --platform cori-1N.xml --stage-file files_to_stage.txt --dax run-32c/swarp-multiply.dax

#../build/workflow-io-bb --fits --platform cori-1N.xml --stage-file files_to_stage.txt --dax run-32c/swarp-normal.dax
