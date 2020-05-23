#!/usr/bin/env python3
import os
import pwd
import sys
import time
import glob
import argparse
from Pegasus.DAX3 import *

##################### BEGIN PARAMETERS #####################
INPUTS_DIR="/Users/lpottier/research/usc-isi/projects/active/workflow-io-bb/real-executions/swarp/input/"

STAGEIN="stagein"
RESAMPLE="resample"
COMBINE="combine"

RESAMPLE_CONF="resample.swarp"
COMBINE_CONF="combine.swarp"

RESAMPLE_OUTPUT=["resample.xml"]
COMBINE_OUTPUT=["combine.xml", "coadd.fits", "coadd.weight.fits"]

FILE_PATTERN="PTF201111*"
IMAGE_PATTERN="*.w.fits"
WEIGHTMAP_PATTERN=".w.weight.fits"
RESAMPLE_PATTERN=".w.resamp.fits"

###################### END PARAMETERS ######################

def main():
    parser = argparse.ArgumentParser(description='Create the DAX for the SWarp workflow')
    parser.add_argument('--dax-file', '-o', dest='dax', type=str, 
                        default='swarp.dax', help='DAX output file', 
                        required=False)
    parser.add_argument('--scalability', '-n', dest='scalability', type=int, 
                        default=1, help='Number of parallel pipelines', 
                        required=False)
    parser.add_argument('--private-input', '-p', dest='private', 
                        action='store_true', help='(NOT IMPLEMENTED) If given, the input files are replicated (so private) for each pipeline', 
                        required=False)
    parser.add_argument('--stage-in', '-s', dest='stagein', 
                        action='store_true', help='Add one staging task from PFS to BB', 
                        required=False)
    # parser.add_argument('--sum', dest='accumulate', action='store_const',
    #                     const=sum, default=max,
    #                     help='sum the integers (default: find the max)')

    args = parser.parse_args()

    #print(args.dax, args.scalability, args.private)

    USER = pwd.getpwuid(os.getuid())[0]

    # Create a abstract dag
    print(" Creating SWarp ADAG...")
    swarp = ADAG("swarp-workflow")

    # Add some workflow-level metadata
    swarp.metadata("creator", "%s@%s" % (USER, os.uname()[1]))
    swarp.metadata("created", time.ctime())

    input_files = glob.glob(INPUTS_DIR + IMAGE_PATTERN)
    #print(input_files)

    if args.scalability <= 0:
        print ("[WARNING] having " + str(args.scalability) + " pipelines is not possible, generating a workflow with 1 pipeline")
        args.scalability = 1

    if args.stagein:
        print (" Add a stage-in task...")

        stagein = Job(name=STAGEIN)
        stagein_output_files = []

        # COPY TASK
        for in_file in input_files:
            # # .fits file
            # stagein.uses(File("{0}".format(os.path.basename(in_file))), link=Link.INPUT)
            # # weight map
            # stagein.uses(File("{0}".format(os.path.basename(in_file).split(".w.")[0] + WEIGHTMAP_PATTERN)), link=Link.INPUT)

            # .fits file
            stagein.uses(File("{0}".format(os.path.basename(in_file))), link=Link.OUTPUT)
            # weight map
            stagein.uses(File("{0}".format(os.path.basename(in_file).split(".w.")[0] + WEIGHTMAP_PATTERN)), link=Link.OUTPUT)

        swarp.addJob(stagein)

    for i in range(args.scalability):
        print (" Add resample tasks...")

        resample = Job(name=RESAMPLE)
        # input_files = glob.glob(os.getcwd()+ "/" + INPUTS_DIR + IMAGE_PATTERN)
        resample_output_files = []

        resample.addArguments("-c", RESAMPLE_CONF)
        resample.uses(File(RESAMPLE_CONF), link=Link.INPUT)

        for in_file in input_files:
            # .fits file
            resample.uses(File("{0}".format(os.path.basename(in_file))), link=Link.INPUT)
            # weight map
            resample.uses(File("{0}".format(os.path.basename(in_file).split(".w.")[0] + WEIGHTMAP_PATTERN)), link=Link.INPUT)

            output_name = 'W'+str(i)+'-'+os.path.basename(in_file).split(".w.")[0] + RESAMPLE_PATTERN
            resample_output = File(output_name)
            resample_output_files.append(resample_output)

            resample.uses(resample_output, link=Link.OUTPUT, transfer=True, register=True)
            resample.addArguments(os.path.basename(in_file))

        for output in RESAMPLE_OUTPUT:
            resample.uses(File('W'+str(i)+'-'+output), link=Link.OUTPUT, transfer=True, register=True)

        swarp.addJob(resample)

        print (" Add combine tasks...")

        combine = Job(name=COMBINE)
        combine.addArguments("-c", COMBINE_CONF)
        combine.uses(File(COMBINE_CONF), link=Link.INPUT)

        for resamp_file in resample_output_files:
            combine.uses(resamp_file, link=Link.INPUT)

            combine.addArguments(resamp_file.name)

        for output in COMBINE_OUTPUT:
            combine.uses(File('W'+str(i)+'-'+output), link=Link.OUTPUT, transfer=True, register=True)

        swarp.addJob(combine)

        print (" Add dependencies between tasks...")

        if args.stagein:
            swarp.addDependency(Dependency(parent=stagein, child=resample))

        swarp.addDependency(Dependency(parent=resample, child=combine))

    # Write the DAX to stdout
    print(" Writing %s" % args.dax)

    with open(args.dax, "w") as f:
        swarp.writeXML(f)


if __name__ == '__main__':
    main()

