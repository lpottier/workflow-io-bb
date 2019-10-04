#!/usr/bin/env python3
import os
import pwd
import sys
import time
import glob
from Pegasus.DAX3 import *

##################### BEGIN PARAMETERS #####################
INPUTS_DIR="input/"
CONFIG_DIR="config/"

RESAMPLE="resample"
COMBINE="combine"

RESAMPLE_CONF="resample.swarp"
COMBINE_CONF="combine.swarp"

RESAMPLE_OUTPUT=["resample.xml"]
COMBINE_OUTPUT=["combine.xml", "coadd.fits", "coadd.weight.fits"]

FILE_PATTERN="PTF201111*"
IMAGE_PATTERN="*.w.fits"
WEIGHTMAP_PATTERN=".weight.fits"
RESAMPLE_PATTERN=".w.resamp.fits"

###################### END PARAMETERS ######################



# The name of the DAX file is the first argument
if len(sys.argv) != 2:
    sys.stderr.write(" Usage: %s DAXFILE\n" % (sys.argv[0]))
    sys.exit(1)

daxfile = sys.argv[1]

USER = pwd.getpwuid(os.getuid())[0]

# Create a abstract dag
print(" Creating SWarp ADAG...")
swarp = ADAG("swarp-workflow")

# Add some workflow-level metadata
swarp.metadata("creator", "%s@%s" % (USER, os.uname()[1]))
swarp.metadata("created", time.ctime())


print (" Add resample tasks...")

resample = Job(name=RESAMPLE)
input_files = glob.glob(os.getcwd()+ "/" + INPUTS_DIR + IMAGE_PATTERN)
resample_output_files = []

resample.addArguments("-c", RESAMPLE_CONF)
resample.uses(File(RESAMPLE_CONF), link=Link.INPUT)

for in_file in input_files:
    resample.uses(File("{0}".format(os.path.basename(in_file))), link=Link.INPUT)

    output_name = os.path.basename(in_file).split(".w.")[0] + RESAMPLE_PATTERN
    resample_output = File(output_name)
    resample_output_files.append(resample_output)

    resample.uses(resample_output, link=Link.OUTPUT, transfer=True, register=True)
    resample.addArguments(os.path.basename(in_file))

for output in RESAMPLE_OUTPUT:
    resample.uses(File(output), link=Link.OUTPUT, transfer=True, register=True)

swarp.addJob(resample)

print (" Add combine tasks...")

combine = Job(name=COMBINE)
combine.addArguments("-c", COMBINE_CONF)
combine.uses(File(COMBINE_CONF), link=Link.INPUT)

for resamp_file in resample_output_files:
    combine.uses(resamp_file, link=Link.INPUT)
    combine.addArguments(resamp_file.name)

for output in COMBINE_OUTPUT:
    combine.uses(File(output), link=Link.OUTPUT, transfer=True, register=True)

swarp.addJob(combine)

print (" Add dependencies tasks...")

swarp.addDependency(Dependency(parent=resample, child=combine))

# Write the DAX to stdout
print(" Writing %s" % daxfile)

with open(daxfile, "w") as f:
    swarp.writeXML(f)
