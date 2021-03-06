#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import stat
import platform
import time
import tempfile
import subprocess
import argparse

from enum import Enum,unique,auto

PROJID="csc355"
USER="lpottier"
HOME="/ccs/home/{}".format(USER)
SCRATCH="/gpfs/alpine/scratch/"

SWARP_DIR = "{0}/{1}/{2}/workflow-io-bb/real-executions/swarp/".format(SCRATCH, USER, PROJID)
WRAPPER = "wrapper.sh" 

INPUT_ONE_RUN = 769 #Input size in MB
SIZE_ONE_PIPELINE = 2048 #Disk Usage for one run in MiB -> 1688. We take 2GiB for safety

@unique
class TaskType(Enum):
    RESAMPLE = auto()
    COMBINE = auto()
    BOTH = auto()

class SwarpWorkflowConfig:

    def __init__(self, task_type, 
                    nthreads, 
                    resample_dir='.', 
                    vmem_max=31744, 
                    mem_max=31744, 
                    combine_bufsize=24576, 
                    existing_file=None,
                    output_dir=None
                ):
        self.task_type = task_type
        self.nthreads = nthreads
        self.resample_dir = resample_dir
        self.vmem_max = vmem_max
        self.mem_max = mem_max
        self.combine_bufsize = combine_bufsize
        self.existing_file = existing_file
        self.output_dir = output_dir

        if not isinstance(self.task_type, TaskType):
            raise ValueError("Bad task type: must be a TaskType")

        if self.output_dir:
            self.file = self.output_dir + "/" + self.task_type.name.lower() + ".swarp"
        else:
            self.file = self.task_type.name.lower() + ".swarp"

    def output(self):
        string = "# Default configuration file for SWarp 2.17.1\n"
        string += "# EB 2008-08-25\n"
        string += "#\n"
        string += "#----------------------------------- Output -----------------------------------\n"

        string += "HEADER_ONLY            N               # Only a header as an output file (Y/N)?\n"
        string += "HEADER_SUFFIX          .head           # Filename extension for additional headers\n"
        return string

    def weight(self):
        string = "#------------------------------- Input Weights --------------------------------\n"
        string += "WEIGHT_TYPE            MAP_WEIGHT      # BACKGROUND,MAP_RMS,MAP_VARIANCE\n"
        string += "                                       # or MAP_WEIGHT\n"
        string += "WEIGHT_SUFFIX          .weight.fits    # Suffix to use for weight-maps\n"
        string += "                                       # (all or for each weight-map)\n"
        string += "WEIGHT_THRESH          0.1             # weight threshold[s] for bad pixels\n"
        return string

    def coaddition(self):
        string = "#------------------------------- Co-addition ----------------------------------\n"

        if self.task_type == TaskType.RESAMPLE:
            string += "COMBINE                N               # Combine resampled images (Y/N)?\n"
        else:
            string += "COMBINE                Y               # Combine resampled images (Y/N)?\n"
        string += "COMBINE_TYPE           MEDIAN          # MEDIAN,AVERAGE,MIN,MAX,WEIGHTED,CHI2\n"
        string += "                                       # or SUM\n"
        return string

    def astronometry(self):
        string = "#-------------------------------- Astrometry ----------------------------------\n"

        string += "CELESTIAL_TYPE         NATIVE          # NATIVE, PIXEL, EQUATORIAL,\n"
        string += "                                       # GALACTIC,ECLIPTIC, or SUPERGALACTIC\n"
        string += "PROJECTION_TYPE        TAN             # Any WCS projection code or NONE\n"
        string += "PROJECTION_ERR         0.001           # Maximum projection error (in output\n"
        string += "                                       # pixels), or 0 for no approximation\n"
        string += "CENTER_TYPE            MANUAL          # MANUAL, ALL or MOST\n"
        string += "CENTER                 210.25,54.25    # Coordinates of the image center\n"
        string += "PIXELSCALE_TYPE        MANUAL          # MANUAL,FIT,MIN,MAX or MEDIAN\n"
        string += "PIXEL_SCALE            1.0             # Pixel scale\n"
        string += "IMAGE_SIZE             3600            # Image size (0 = AUTOMATIC)\n"

        return string

    def resampling(self):
        string = "#-------------------------------- Resampling ----------------------------------\n"
        if self.task_type == TaskType.COMBINE:
            string += "RESAMPLE               N               # Resample input images (Y/N)?\n"
        else:
            string += "RESAMPLE               Y               # Resample input images (Y/N)?\n"

        string += "RESAMPLE_DIR           {}               # Directory path for resampled images\n".format(self.resample_dir)
        string += "RESAMPLE_SUFFIX        .resamp.fits    # filename extension for resampled images\n\n"

        string += "RESAMPLING_TYPE        LANCZOS4        # NEAREST,BILINEAR,LANCZOS2,LANCZOS3\n"
        string += "                                       # or LANCZOS4 (1 per axis)\n"
        string += "OVERSAMPLING           0               # Oversampling in each dimension\n"
        string += "                                       # (0 = automatic)\n"
        string += "INTERPOLATE            N               # Interpolate bad input pixels (Y/N)?\n"
        string += "                                       # (all or for each image)\n\n"

        string += "FSCALASTRO_TYPE        FIXED           # NONE,FIXED, or VARIABLE\n"
        string += "FSCALE_KEYWORD         FLXSCALE        # FITS keyword for the multiplicative\n"
        string += "                               # factor applied to each input image\n"
        string += "FSCALE_DEFAULT         1.0             # Default FSCALE value if not in header\n\n"

        string += "GAIN_KEYWORD           GAIN            # FITS keyword for effect. gain (e-/ADU)\n"
        string += "GAIN_DEFAULT           4.0             # Default gain if no FITS keyword found\n"

        string += "BLANK_BADPIXELS        Y\n"

        return string


    def background(self):
        string = "#--------------------------- Background subtraction ---------------------------\n"

        string += "SUBTRACT_BACK          Y               # Subtraction sky background (Y/N)?\n"
        string += "                                       # (all or for each image)\n\n"

        string += "BACK_TYPE              AUTO            # AUTO or MANUAL\n"
        string += "                                       # (all or for each image)\n"
        string += "BACK_DEFAULT           0.0             # Default background value in MANUAL\n"
        string += "                                       # (all or for each image)\n"
        string += "BACK_SIZE              128             # Background mesh size (pixels)\n"
        string += "                                       # (all or for each image)\n"
        string += "BACK_FILTERSIZE        3               # Background map filter range (meshes)\n"
        string += "                                       # (all or for each image)\n"

        return string

    def memory(self):
        string = "#------------------------------ Memory management -----------------------------\n"

        string += "VMEM_DIR               .               # Directory path for swap files\n"
        string += "VMEM_MAX               {}           # Maximum amount of virtual memory (MiB)\n".format(self.vmem_max)
        string += "MEM_MAX                {}           # Maximum amount of usable RAM (MiB)\n".format(self.mem_max)
        string += "COMBINE_BUFSIZE        {}           # RAM dedicated to co-addition (MiB)\n".format(self.combine_bufsize)
        return string

    def misc(self):
        string = "#------------------------------ Miscellaneous ---------------------------------\n"

        string += "DELETE_TMPFILES        Y               # Delete temporary resampled FITS files\n"
        string += "                                       # (Y/N)?\n"
        string += "COPY_KEYWORDS          OBJECT          # List of FITS keywords to propagate\n"
        string += "                                       # from the input to the output headers\n"
        string += "WRITE_FILEINFO         N               # Write information about each input\n"
        string += "                                       # file in the output image header?\n"
        string += "WRITE_XML              Y               # Write XML file (Y/N)?\n"

        if self.task_type == TaskType.RESAMPLE:
            string += "XML_NAME               resample.xml    # Filename for XML output\n"
        elif self.task_type == TaskType.COMBINE:
            string += "XML_NAME               combine.xml     # Filename for XML output\n"
        else:
            string += "XML_NAME               both.xml        # Filename for XML output\n"

        string += "VERBOSE_TYPE           FULL            # QUIET,NORMAL or FULL\n"

        string += "NTHREADS               {}               # No. threads\n".format(self.nthreads)
        return string


    def write(self, file=None, overide=False):
        if file == None:
            file = self.file
        if not overide and os.path.exists(file):
            raise FileNotFoundError("file {} already exists".format(file))

        if self.existing_file != None and os.path.exists(self.existing_file):
            sys.stderr.write(" === workflow: file {} already exists and will be used.\n".format(self.existing_file))

        if os.path.exists(file):
            sys.stderr.write(" === workflow: file {} already exists and will be re-written.\n".format(file))

        with open(file, 'w') as f:
            f.write(self.output())
            f.write(self.weight())
            f.write(self.coaddition())
            f.write(self.astronometry())
            f.write(self.resampling())
            f.write(self.background())
            f.write(self.memory())
            f.write(self.misc())


class SwarpSchedulerConfig:
    def __init__(self, num_nodes, num_cores, queue, timeout, slurm_options=None):
        self.num_nodes = num_nodes #Number of nodes requested
        self.num_cores = num_cores #Cores per nodes
        self._queue = queue #Execution queue
        self._timeout = timeout #Timeout HH:MM:SS

    def nodes(self):
        return self.num_nodes
    
    def timeout(self):
        return self._timeout
    
    def queue(self):
        return self._queue

    def cores(self):
        return self.num_cores

class SwarpBurstBufferConfig:
    def __init__(self, size_bb, stage_input_dirs, stage_input_files, stage_output_dirs, access_mode, bbtype):
        self.size_bb = size_bb
        self.stage_input_dirs = stage_input_dirs #List of input dirs
        self.stage_output_dirs = stage_output_dirs #List of output dirs (usually one)
        self.access_mode = access_mode
        self.bbtype = bbtype
        self.stage_input_files = stage_input_files #List of files to stages

    def size(self):
        return self.size_bb

    def indirs(self):
        return self.stage_input_dirs

    def infiles(self):
        return self.stage_input_files

    def outdirs(self):
        return self.stage_output_dirs

    def mode(self):
        return self.access_mode

    def type(self):
        return self.bbtype

class SwarpInstance:
    def __init__(self, script_dir, resample_config, combine_config, sched_config, bb_config, nb_files_on_bb, stagein_fits, nb_avg=1, input_sharing=True, standalone=True, no_stagein=True):
        self.standalone = standalone
        self.no_stagein = no_stagein

        self.resample_config = resample_config
        self.combine_config = combine_config

        self.bb_config = bb_config
        self.sched_config = sched_config
        self.script_dir = script_dir
        self.nb_avg = nb_avg
        self.input_sharing = input_sharing
        # TODO: Fix this -> make a loop to automatically create experiments for each number of files
        self.nb_files_on_bb = nb_files_on_bb[0]
        self.stagein_fits = stagein_fits # if True all the resamp.fits are staged in

    def scheduler_header(self):
        string = "#!/bin/bash\n"
        string += "#BSUB -P {}\n".format(PROJID)
        
        # or use ln_slots (where one slot is the smallest unit (one sequential task use 1 slots)
        # One node is 43 slots

        if self.standalone:
            string += "#BSUB -nnodes 1\n"
            # string += "#BSUB -ln_slots @NODES@\n"
        else:
            string += "#BSUB -nnodes 1\n"
            # string += "#BSUB -ln_slots {}\n".format(self.sched_config.nodes())


        # All processors allocated to this job must be on the same hosts
        # Each slot maps to one core. LSF tries to pack n cores as close as possible on single NUMA or socket.
        #Cannot be use with -nnodes (easy mode)
        #string += "#BSUB -R \"span[hosts=1] affinity[core(1):distribute=pack]\"\n"

        string += "#BSUB -W {}\n".format(self.sched_config.timeout())
        string += "#BSUB -J swarp-@NODES@\n"
        string += "#BSUB -o output.%J\n"
        string += "#BSUB -e error.%J\n"
        # string += "#SBATCH --export=ALL\n"
        
        # Send email when the job ends
        #string += "#BSUB -N\n"
    
        # Exclusive usage of the node
        #string += "#BSUB -x\n"

        # Disable SMT and activate private burst buffers access
        string += "#BSUB -alloc_flags \"smt1\"\n"
        string += "#BSUB -alloc_flags \"nvme\"\n\n"

        return string

    def script_modules(self):
        string = ""
        return string

    def script_headers(self, interactive=False):
        s = ''
        if interactive:
            s += "#!/bin/bash\n\n"
            #s += "set -x\n\n"

        s += "usage()\n"
        s += "{\n"
        s += "    echo \"usage: $0 [[[-f=file ] [-c=COUNT]] | [-h]]\"\n"
        s += "}\n"

        s += "BBDIR=/mnt/bb/{}/\n\n".format(USER)

        s += "\n"
        s += "CURRENT_DIR=$(pwd)\n"

        #BASE="/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/"
        s += "SWARP_DIR=workflow-io-bb/real-executions/swarp\n"
        # s += "BASE=\"$SCRATCH/$SWARP_DIR/{}\"\n".format(self.script_dir)
        s += "BASE=\"$CURRENT_DIR\"\n"
        s += "LAUNCH=\"$CURRENT_DIR/{}\"\n".format(WRAPPER)
        s += "EXE={}/bin/swarp\n".format(SWARP_DIR)
        s += "COPY={}/tools/copy.py\n".format(SWARP_DIR)
        s += "FILE_MAP={}/tools/build_filemap.py\n".format(SWARP_DIR)
        s += "\n"

        if interactive:
            s += "NODE_COUNT=@NODES@   # Number of compute nodes requested\n"
            s += "TASK_COUNT=@NODES@   # Number of tasks allocated\n"
        else:
            s += "NODE_COUNT=@NODES@   # Number of compute nodes requested\n"
            s += "TASK_COUNT=@NODES@   # Number of tasks allocated\n"
        s += "CORE_COUNT={}        # Number of cores used by both tasks\n".format(self.sched_config.cores())
        s += "\n"

        #s += echo "SLURM NODES: ${SLURM_JOB_NUM_NODES}"
        s += "STAGE_EXEC=0        #0 no stage. 1 -> stage exec in BB\n"
        s += "STAGE_CONFIG=0      #0 no stage. 1 -> stage config dir in BB\n"
        s += "NB_AVG={}           # Number of identical runs\n".format(self.nb_avg)
        s += "\n"
        s += "ALLOC=swarp\n"
        s += "WRAPPER=\"jsrun -n 1 -a 1 -c 1 \"\n"
        s += "JSRUN=\"jsrun\"\n"
        s += "\n"
        s += "echo \"JSRUN -> $JSRUN\"\n"

        s += "\n"
        s += "TASK_COUNT=$(echo \"$TASK_COUNT - 1\" | bc -l)\n"

        s += "FILES_TO_STAGE=\"files_to_stage.txt\"\n"
        s += "COUNT={}\n".format(self.nb_files_on_bb)
        if self.stagein_fits:
            # We stage .resamp.fits in BB
            s += "STAGE_FITS=1\n"
        else:
            # We stage .resamp.fits in PFS
            s += "STAGE_FITS=0\n"
        
        s += "\n"
        s += "# Test code to verify command line processing\n\n"
        s += "if [ -f \"$BASE/$FILES_TO_STAGE\" ]; then\n"
        s += "    echo \"List of files used: $FILES_TO_STAGE\"\n"
        s += "else\n"
        s += "    echo \"$FILES_TO_STAGE does not seem to exist in $BASE\"\n"
        s += "    exit\n"
        s += "fi\n"
        s += "\n"

        s += "if (( \"$COUNT\" < 0 )); then\n"
        s += "    COUNT=$(cat $FILES_TO_STAGE | wc -l)\n"
        s += "fi\n"
        s += "\n"

        s += "echo \"Number of files staged in BB: $COUNT\"\n"
        s += "\n"

        s += "IMAGE_PATTERN='PTF201111*.w.fits'\n"
        s += "IMAGE_WEIGHT_PATTERN='PTF201111*.w.weight.fits'\n"
        s += "RESAMPLE_PATTERN='PTF201111*.w.resamp.fits'\n"
        s += "\n"

        s += "CONFIG_DIR=$BASE\n"
        s += "if (( \"$STAGE_CONFIG\" == 1 )); then\n"
        s += "    CONFIG_DIR=$BBDIR/config\n"
        s += "fi\n"
        s += "RESAMPLE_CONFIG=${CONFIG_DIR}/resample.swarp\n"
        s += "COMBINE_CONFIG=${CONFIG_DIR}/combine.swarp\n"
        s += "\n"

        s += "if [ -f \"$RESAMPLE_CONFIG\" ]; then\n"
        s += "    echo \"$RESAMPLE_CONFIG exist\"\n"
        s += "else\n"
        s += "    echo \"$RESAMPLE_CONFIG does not exist\"\n"
        s += "    exit\n"
        s += "fi\n"
        s += "\n"

        s += "if [ -f \"$COMBINE_CONFIG\" ]; then\n"
        s += "    echo \"$COMBINE_CONFIG exist\"\n"
        s += "else\n"
        s += "    echo \"$COMBINE_CONFIG does not exist\"\n"
        s += "    exit\n"
        s += "fi\n"
        s += "\n"

        s += "CONFIG_FILES=\"${RESAMPLE_CONFIG} ${COMBINE_CONFIG}\"\n"
        s += "\n"
        if interactive:
            s += "OUTPUT_DIR_NAME=swarp.interactive.${CORE_COUNT}c.${COUNT}f.$LSB_JOBID/\n"
        else:
            s += "OUTPUT_DIR_NAME=$LSB_JOBNAME.batch.${CORE_COUNT}c.${COUNT}f.$LSB_JOBID/\n"
        s += "export BB_OUTPUT_DIR=$BBDIR/$OUTPUT_DIR_NAME\n"
        s += "$WRAPPER mkdir -p $BB_OUTPUT_DIR\n"
        s += "$WRAPPER chmod 777 $BB_OUTPUT_DIR\n"
        # s += " lfs setstripe -c 1 -o 1 $BB_OUTPUT_DIR\n"
       
        s += "\n"
        s += "INPUT_DIR_PFS=$MEMBERWORK/csc355/$SWARP_DIR/input\n"
        #s += "INPUT_DIR=$DW_JOB_STRIPED/input\n"
        s += "INPUT_DIR=$BB_OUTPUT_DIR/input\n"
        s += "\n"

        s += "mkdir -p $OUTPUT_DIR_NAME\n"


        s += "echo \"Warming BB... $(date --rfc-3339=ns)\"\n"
        s += "$WRAPPER cp \"$BASE/$FILES_TO_STAGE\" \"$BB_OUTPUT_DIR/\"\n"
        s += "TMP_FILES_TO_STAGE=\"$BB_OUTPUT_DIR/$FILES_TO_STAGE\"\n"
        s += "$WRAPPER sed -i -e \"s|@INPUT@|$INPUT_DIR|g\" \"$TMP_FILES_TO_STAGE\"\n"

        s += "$WRAPPER $COPY -f $TMP_FILES_TO_STAGE -d $BB_OUTPUT_DIR\n"
        s += "\n"
        s += "echo \"Warming done. $(date --rfc-3339=ns)\"\n"
        s += "$WRAPPER bash -c \"rm -rf $BB_OUTPUT_DIR/*\"\n"
        s += "\n"
        return s

    def average_loop(self):
        s = ''
        s += "for k in $(seq 1 1 $NB_AVG); do\n"
        s += "\n"

        s += "    export OUTPUT_DIR=$BB_OUTPUT_DIR/${k}\n"
        s += "    $WRAPPER mkdir -p $OUTPUT_DIR\n"
        s += "    echo \"OUTPUT_DIR -> $OUTPUT_DIR\"\n"
        s += "\n"

        s += "    #The local version\n"
        s += "    export LOCAL_OUTPUT_DIR=$BASE/$OUTPUT_DIR_NAME/${k}/\n"
        s += "    mkdir -p $LOCAL_OUTPUT_DIR\n"
        # s += "    lfs setstripe -c 1 -o 1 ${LOCAL_OUTPUT_DIR}\n"
        s += "    echo \"LOCAL_OUTPUT_DIR -> $LOCAL_OUTPUT_DIR\"\n"
        s += "\n"

        s += "    OUTPUT_FILE=$OUTPUT_DIR/output.log\n"
        s += "    BB_INFO=$OUTPUT_DIR/bb.log\n"
        s += "    DU_RES=$OUTPUT_DIR/data-stagedin.log\n"
        s += "    DU_RESAMP=$OUTPUT_DIR/data-resamp.log\n"
        s += "    BB_ALLOC=$OUTPUT_DIR/bb_alloc.log\n"
        # s += "    SQUEUE_START=$OUTPUT_DIR/squeue_start.log\n"
        # s += "    SQUEUE_END=$OUTPUT_DIR/squeue_end.log\n"
        # s += "    SCONTROL_START=$OUTPUT_DIR/scontrol_start.log\n"
        # s += "    SCONTROL_END=$OUTPUT_DIR/scontrol_end.log\n"
        s += "\n"
        s += "    echo \"######################################################\" | $WRAPPER tee -a $OUTPUT_FILE\n"
        s += "    echo \"Starting run $k ... $(date --rfc-3339=ns)\" | $WRAPPER tee -a $OUTPUT_FILE\n"

        s += "    $WRAPPER mkdir -p $OUTPUT_DIR\n"
        s += "    $WRAPPER chmod 777 $OUTPUT_DIR\n"
        s += "\n"

        # s += "    export RESAMP_DIR=$OUTPUT_DIR/resamp\n"
        # s += "\n"

        # s += "    mkdir -p $RESAMP_DIR\n"
        # s += "    chmod 777 $RESAMP_DIR\n"
        # s += "\n"

        # s += "    for process in $(seq 1 ${TASK_COUNT}); do\n"
        # #s += "        mkdir -p ${rundir}/${process}\n"
        # s += "        mkdir -p ${OUTPUT_DIR}/${process}\n"
        # s += "        mkdir -p $OUTPUT_DIR_NAME/${k}/${process}\n"
        # #s += "        cp $FILES_TO_STAGE ${OUTPUT_DIR}/${process}\n""
        # s += "    done\n"

        s += "    RESAMP_DIR=\"resamp\"\n"
        s += "\n"

        s += "    start=$(date --rfc-3339=seconds)\n"
        s += "    rsmpl_dir=$(mktemp -d -t resamp-XXXXXXX --tmpdir=$MEMBERWORK/csc355/)\n"
        # s += "    echo $start > $SCONTROL_START\n"
        # s += "    echo $start > $SQUEUE_START\n"
        # s += "    scontrol show burst --local -o -d >> $SCONTROL_START\n"
        # s += "    squeue -t RUNNING --noconvert --format=%all >> $SQUEUE_START\n"

        s += "\n"

        s += "    for process in $(seq 0 ${TASK_COUNT}); do\n"
        s += "        $WRAPPER mkdir -p ${OUTPUT_DIR}/${process}\n"
        s += "        mkdir -p ${LOCAL_OUTPUT_DIR}/${process}\n"
        s += "        $WRAPPER mkdir -p ${OUTPUT_DIR}/${process}/$RESAMP_DIR\n"
        s += "        mkdir -p $rsmpl_dir/${process}/\n"
        

        s += "\n"
        s += "        $WRAPPER cp $CONFIG_FILES ${OUTPUT_DIR}/${process}/\n"
        s += "        LOC_RESAMPLE_CONF=${OUTPUT_DIR}/${process}/resample.swarp\n"
        s += "        LOC_COMBINE_CONF=${OUTPUT_DIR}/${process}/combine.swarp\n"

        s += "        if (( \"$STAGE_FITS\" == \"0\" )); then\n"
        # We stage .resamp.fits in PFS
        s += "            $WRAPPER sed -i -e \"s|@DIR@|$rsmpl_dir/${process}/|g\" \"$LOC_RESAMPLE_CONF\"\n"
        s += "            $WRAPPER sed -i -e \"s|@DIR@|$rsmpl_dir/${process}/|g\" \"$LOC_COMBINE_CONF\"\n"
        s += "        else\n"
        # We stage .resamp.fits in BB
        s += "            $WRAPPER sed -i -e \"s|@DIR@|${OUTPUT_DIR}/${process}/$RESAMP_DIR|g\" \"$LOC_RESAMPLE_CONF\"\n"
        s += "            $WRAPPER sed -i -e \"s|@DIR@|${OUTPUT_DIR}/${process}/$RESAMP_DIR|g\" \"$LOC_COMBINE_CONF\"\n"
        s += "        fi\n"

        s += "\n"
        s += "        $WRAPPER cp \"$BASE/$FILES_TO_STAGE\" \"$OUTPUT_DIR/${process}/\"\n"
        s += "        LOC_FILES_TO_STAGE=\"$OUTPUT_DIR/${process}/$FILES_TO_STAGE\"\n"
        s += "        $WRAPPER sed -i -e \"s|@INPUT@|$INPUT_DIR|g\" \"$LOC_FILES_TO_STAGE\"\n"
        s += "    done\n"

        s += "    echo \"Number of files kept in PFS:$(echo \"$COUNT\" | bc)/$($WRAPPER cat $LOC_FILES_TO_STAGE | wc -l)\" | $WRAPPER tee $OUTPUT_FILE\n"
        #s += "    echo \"NODE=$NODE_COUNT\" | tee -a $OUTPUT_FILE\n"
        #s += "    echo \"TASK=$SLURM_NTASKS\" | tee -a $OUTPUT_FILE\n"
        s += "    echo \"CORE=$CORE_COUNT\" | $WRAPPER tee -a $OUTPUT_FILE\n"
        #s += "    echo \"NTASKS_PER_NODE=$SLURM_NTASKS_PER_NODE\" | tee -a $OUTPUT_FILE\n"
        s += "\n"
        s += "    echo \"Compute nodes: $($WRAPPER uname -n) \" | $WRAPPER tee -a $OUTPUT_FILE\n"

        s += "    MONITORING=\"pegasus-kickstart -z\"\n"

        s += "\n"
        s += "    echo \"1.4TB\" | $WRAPPER tee $BB_ALLOC\n"
        s += "\n"

        s += "    echo \"Starting STAGE_IN... $(date --rfc-3339=ns)\" | $WRAPPER tee -a $OUTPUT_FILE\n"
        s += "    t1=$(date +%s.%N)\n"
        s += "    if $WRAPPER test -f \"$LOC_FILES_TO_STAGE\" ; then\n"
        s += "        $WRAPPER $COPY -f $LOC_FILES_TO_STAGE -d $OUTPUT_DIR\n"
        s += "    else\n"
        s += "        $WRAPPER $COPY -i $INPUT_DIR_PFS -o $INPUT_DIR -d $OUTPUT_DIR\n"
        s += "    fi\n"
        s += "\n"

        s += "    if (( \"$STAGE_EXEC\" == 1 )); then\n"
        s += "        $WRAPPER cp -r $EXE $BBDIR\n"
        s += "    fi\n"
        s += "\n"

        s += "    if (( \"$STAGE_CONFIG\" == 1 )); then\n"
        s += "        $WRAPPER cp -r $CONFIG_DIR $BBDIR\n"
        s += "    fi\n"
        s += "\n"

        s += "    t2=$(date +%s.%N)\n"
        s += "    tdiff1=$(echo \"$t2 - $t1\" | bc -l)\n"
        s += "    echo \"TIME STAGE_IN $tdiff1\" | $WRAPPER tee -a $OUTPUT_FILE\n"
        s += "\n"

        s += "    $WRAPPER mkdir -p $INPUT_DIR\n"
        s += "\n"

        s += "    #If we did not stage any input files\n"
        s += "    if [[ -f \"$($WRAPPER ls -A $INPUT_DIR)\" ]]; then\n"
        s += "        INPUT_DIR=$INPUT_DIR_PFS\n"
        s += "        echo \"No files staged in, INPUT_DIR set as $INPUT_DIR\"\n"
        s += "    fi\n"
        s += "\n"

        #if we stage in executable
        s += "    if (( \"$STAGE_EXEC\" == 1 )); then\n"
        s += "        EXE=\"$WRAPPER $BBDIR/swarp\"\n"
        s += "    fi\n"
        s += "\n"

        s += "    RESAMPLE_FILES=\"$OUTPUT_DIR/input_files.txt\"\n"
        s += "    $WRAPPER $FILE_MAP -I $INPUT_DIR_PFS -B $INPUT_DIR -O $RESAMPLE_FILES -R $IMAGE_PATTERN  | $WRAPPER tee -a $OUTPUT_FILE\n"
        s += "\n"

        s += "    dsize=$($WRAPPER du -sh $INPUT_DIR | awk '{print $1}')\n"
        s += "    nbfiles=$($WRAPPER ls -al $INPUT_DIR | grep '^-' | wc -l)\n"
        s += "    echo \"$nbfiles $dsize\" | $WRAPPER tee $DU_RES\n"
        s += "\n"
        s += "    input_files=$($WRAPPER cat $RESAMPLE_FILES)\n"
        
        s += "    cd ${LOCAL_OUTPUT_DIR}\n"


        s += "    echo \"Starting RESAMPLE... $(date --rfc-3339=ns)\" | $WRAPPER tee -a $OUTPUT_FILE\n"

        s += "    echo \"\" > resample.conf\n"
        s += "    for process in $(seq 0 ${TASK_COUNT}); do\n"
        s += "        echo \"#!/bin/bash\" > \"wrapper-${process}.sh\"\n"
        s += "        echo \"$MONITORING $EXE -c ${OUTPUT_DIR}/${process}/resample.swarp $input_files\" >> \"wrapper-${process}.sh\"\n"
        s += "        chmod +x \"wrapper-${process}.sh\"\n"
        s += "        echo -e \"app ${process}: sh wrapper-${process}.sh\" >> resample.conf \n"
        s += "    done\n"
        s += "\n"

        s += "    echo \"cpu_index_using: logical\" >> resample.conf\n"
        s += "    echo \"overlapping_rs: warn\" >> resample.conf\n"
        s += "    echo \"oversubscribe_cpu: warn\" >> resample.conf\n"
        s += "    echo \"oversubscribe_gpu: allow\" >> resample.conf\n"
        s += "    echo \"oversubscribe_mem: allow\" >> resample.conf\n"
        s += "    echo \"launch_distribution: packed\" >> resample.conf\n\n"
        
        s += "    proc_start=0\n"
        s += "    proc_end=3\n"
        s += "    for process in $(seq 0 ${TASK_COUNT}); do\n"
        s += "        echo \"rank: ${process}: { host: 1; cpu: {$proc_start:$proc_end} ; mem: * } : app ${process}\" >> resample.conf\n"
        s += "        proc_start=$(echo \"$proc_start+4\" | bc -l)\n"
        s += "        proc_end=$(echo \"$proc_end+4\" | bc -l)\n"
        s += "    done\n"

        s += "    echo \"Launching  $(echo \"$TASK_COUNT+1\" | bc -l) RESAMPLE process at:$(date --rfc-3339=ns) ... \" | $WRAPPER tee -a $OUTPUT_FILE\n"

        s += "    $JSRUN --stdio_mode individual -o %t/stat.resample.%p_%t.xml -k %t/error.resample.%p_%t --erf_input resample.conf  &\n"

        s += "    t1=$(date +%s.%N)\n"
        s += "    wait\n"
        s += "    t2=$(date +%s.%N)\n"
        s += "    tdiff2=$(echo \"$t2 - $t1\" | bc -l)\n"
        s += "    echo \"TIME RESAMPLE $tdiff2\" | $WRAPPER tee -a $OUTPUT_FILE\n"
        s += "\n"

        s += "    for process in $(seq 0 ${TASK_COUNT}); do\n"
        s += "        dsize=$($WRAPPER du -sh ${OUTPUT_DIR}/${process}/$RESAMP_DIR/ | awk '{print $1}')\n"
        s += "        nbfiles=$($WRAPPER ls -al ${OUTPUT_DIR}/${process}/$RESAMP_DIR/ | grep '^-' | wc -l)\n"
        s += "        echo \"BB ${OUTPUT_DIR}/${process}/$RESAMP_DIR/ $nbfiles $dsize\" | $WRAPPER tee -a $DU_RESAMP\n\n"

        s += "        dsize_pfs=$(du -sh $rsmpl_dir/${process}/ | awk '{print $1}')\n"
        s += "        nbfiles_pfs=$(ls -al $rsmpl_dir/${process}/ | grep '^-' | wc -l)\n"
        s += "        echo \"PFS $rsmpl_dir/${process}/ $nbfiles_pfs $dsize_pfs\" | $WRAPPER tee -a $DU_RESAMP\n"
        s += "    done\n"
        s += "\n"

        s += "    echo \"Starting COMBINE... $(date --rfc-3339=ns)\" | $WRAPPER tee -a $OUTPUT_FILE\n"
        s += "\n"

        s += "\n"
        s += "    echo \"\" > combine.conf\n"

        s += "    for process in $(seq 0 ${TASK_COUNT}); do\n"
        s += "      if (( \"$STAGE_FITS\" == \"0\" )); then\n"
        s += "          rsmpl_files=$(ls $rsmpl_dir/${process}/${RESAMPLE_PATTERN})\n"
        s += "      else\n"
        s += "          rsmpl_files=$($WRAPPER bash -c \"ls ${OUTPUT_DIR}/${process}/$RESAMP_DIR/${RESAMPLE_PATTERN}\")\n"
        s += "      fi\n"
        s += "      echo \"#!/bin/bash\" > \"wrapper-${process}.sh\"\n"
        s += "      echo \"$MONITORING $EXE -c ${OUTPUT_DIR}/${process}/combine.swarp \" $rsmpl_files >> \"wrapper-${process}.sh\"\n"
        s += "      chmod +x \"wrapper-${process}.sh\"\n"
        s += "      echo \"app ${process}: sh wrapper-${process}.sh\" >> combine.conf \n"
        s += "    done\n"
        s += "\n"

        s += "    echo \"cpu_index_using: logical\" >> combine.conf\n"
        s += "    echo \"overlapping_rs: warn\" >> combine.conf\n"
        s += "    echo \"oversubscribe_cpu: warn\" >> combine.conf\n"
        s += "    echo \"oversubscribe_gpu: allow\" >> combine.conf\n"
        s += "    echo \"oversubscribe_mem: allow\" >> combine.conf\n"
        s += "    echo \"launch_distribution: packed\" >> combine.conf\n\n"

        s += "    proc_start=0\n"
        s += "    proc_end=3\n"
        s += "    for process in $(seq 0 ${TASK_COUNT}); do\n"
        s += "        echo \"rank: ${process}: { host: 1; cpu: {$proc_start:$proc_end} ; mem: * } : app ${process}\" >> combine.conf\n"
        s += "        proc_start=$(echo \"$proc_start+4\" | bc -l)\n"
        s += "        proc_end=$(echo \"$proc_end+4\" | bc -l)\n"
        s += "    done\n"

        s += "    echo \"Launching COMBINE process  $(echo \"$TASK_COUNT+1\" | bc -l) at:$(date --rfc-3339=ns) ... \" | $WRAPPER tee -a $OUTPUT_FILE\n"
        s += "    $JSRUN --stdio_mode individual -o %t/stat.combine.%p_%t.xml -k %t/error.combine.%p_%t --erf_input combine.conf &\n"
        s += "\n"
        s += "    t1=$(date +%s.%N)\n"
        s += "    wait\n"
        s += "    t2=$(date +%s.%N)\n"
        s += "    tdiff3=$(echo \"$t2 - $t1\" | bc -l)\n"
        s += "    echo \"TIME COMBINE $tdiff3\" | $WRAPPER tee -a $OUTPUT_FILE\n"
        s += "\n"
        s += "    $WRAPPER du -sh $BBDIR | $WRAPPER tee -a $OUTPUT_FILE\n"
        s += "\n"
        #s += "    env | grep SLURM > $OUTPUT_DIR/slurm.env\n"
        #s += "\n"

        #s += "    end=$(date --rfc-3339=seconds)\n"
        #s += "    echo $end > $SCONTROL_END\n"
        #s += "    echo $end > $SQUEUE_END\n"
        #s += "    scontrol show burst --local -o -d >> $SCONTROL_END\n"
        #s += "    squeue -t RUNNING --noconvert --format=%all >> $SQUEUE_END\n"


        s += "    echo \"Starting STAGE_OUT... $(date --rfc-3339=ns)\" | $WRAPPER tee -a $OUTPUT_FILE\n"
        s += "    for process in $(seq 0 ${TASK_COUNT}); do\n"
        s += "        echo \"Launching STAGEOUT process ${process} at:$(date --rfc-3339=ns) ... \" | $WRAPPER tee -a $OUTPUT_FILE\n"
        #s += "        $COPY -i $OUTPUT_DIR -o $OUTPUT_DIR_NAME/${k} -a \"stage-out\" -d $OUTPUT_DIR_NAME/${k}\n"
        #s += "        cd ${OUTPUT_DIR}/${process}\n"
        # s += "        tree $DW_JOB_STRIPED/\n"
        s += "        $WRAPPER $COPY -i \"${process}/\" -o \"$CURRENT_DIR/$OUTPUT_DIR_NAME/${k}/${process}/\" -a \"stage-out\" -d \"$CURRENT_DIR/$OUTPUT_DIR_NAME/${k}/${process}/\" &\n"
        #s += "        cd ..\n"
        s += "        echo \"done\"\n"
        s += "        echo \"\"\n"
        s += "    done\n"
        s += "    t1=$(date +%s.%N)\n"
        s += "    wait\n"
        s += "    $WRAPPER $COPY -i \"${OUTPUT_DIR}/\" -o \"$CURRENT_DIR/$OUTPUT_DIR_NAME/${k}/\" -a \"stage-out\" -d \"$CURRENT_DIR/$OUTPUT_DIR_NAME/${k}/\"\n"
        s += "    t2=$(date +%s.%N)\n"
        s += "    tdiff4=$(echo \"$t2 - $t1\" | bc -l)\n"
        s += "\n"
        s += "    echo \"TIME STAGE_OUT $tdiff4\" | $WRAPPER tee -a $OUTPUT_FILE\n"
        s += "\n"
        s += "    OUTPUT_FILE=$CURRENT_DIR/$OUTPUT_DIR_NAME/${k}/output.log\n"
        s += "    echo \"######################################################\" | tee -a $OUTPUT_FILE\n"

        s += "    tdiff=$(echo \"$tdiff1 + $tdiff2 + $tdiff3 + $tdiff4\" | bc -l)\n"
        s += "    echo \"TIME TOTAL $tdiff\" | tee -a $OUTPUT_FILE\n"

        s += "    echo \"=== Cleaning run $k... $(date --rfc-3339=ns)\"\n"
        s += "    echo \"=== Cleaning .fits files in output $k... $(date --rfc-3339=ns)\"\n"
        s += "    cd \"$CURRENT_DIR/$OUTPUT_DIR_NAME/${k}\"\n"
        s += "    for process in $(seq 0 ${TASK_COUNT}); do\n"
        #s += "        rm -rf ${OUTPUT_DIR}/${process}/$RESAMP_DIR/\n"
        s += "        rm -rf $rsmpl_dir/${process}/\n"
        #s += "        cd \"${process}\"\n"
        s += "        rm -rf \"coadd.fits\" \"coadd.weight.fits\" \"combine.xml\" \"resample.xml\"\n"
        #s += "        cd ..\n"
        s += "    done\n"

        #s += "    rm -rf \"$CURRENT_DIR/$OUTPUT_DIR_NAME/${k}/*/*.fits\"\n"

        s += "\n"
        #s += "    set -x\n"
        #s += "    set +x\n"
        s += "\n"
        s += "    $WRAPPER rm -rf $BBDIR/*\n"
        s += "    echo \"#### Ending run $k... $(date --rfc-3339=ns)\"\n"
        s += "done\n"

        s += "\n\n"

        # s += "OUTPUT_LSFJOB=$CURRENT_DIR/$OUTPUT_DIR_NAME/lsf.log\n"
        # s += "echo \"LSB_ACCUMULATED_CPUTIME=$LSB_ACCUMULATED_CPUTIME\" | tee $OUTPUT_LSFJOB\n"
        # s += "echo \"LSB_MAX_MEM_RUSAGE=$LSB_MAX_MEM_RUSAGE\" | tee -a $OUTPUT_LSFJOB\n"
        # s += "echo \"LSB_MAX_SWAP_RUSAGE=$LSB_MAX_SWAP_RUSAGE\" | tee -a $OUTPUT_LSFJOB\n"
        # s += "echo \"LSB_MAX_PROCESSES_RUSAGE=$LSB_MAX_PROCESSES_RUSAGE\" | tee -a $OUTPUT_LSFJOB\n"
        # s += "echo \"LSB_MAX_THREADS_RUSAGE=$LSB_MAX_THREADS_RUSAGE\" | tee -a $OUTPUT_LSFJOB\n"
        # s += "echo \"LSB_JOB_SUBMIT_TIME=$LSB_JOB_SUBMIT_TIME\" | tee -a $OUTPUT_LSFJOB\n"
        # s += "echo \"LSB_JOB_START_TIME=$LSB_JOB_START_TIME\" | tee -a $OUTPUT_LSFJOB\n"
        # s += "echo \"LSB_JOB_END_TIME=$LSB_JOB_END_TIME\" | tee -a $OUTPUT_LSFJOB\n"
        # s += "echo \"LSB_JOB_PEND_TIME=$LSB_JOB_PEND_TIME\" | tee -a $OUTPUT_LSFJOB\n"
        # s += "echo \"LSB_JOB_STATUS=$LSB_JOB_STATUS\" | tee -a $OUTPUT_LSFJOB\n"

        # s += "base_tar=$(dirname $CURRENT_DIR)\n"
        # s += "target=$(basename $base_tar)\n"

        # s += "echo \"Make tarball \"$target.tar.bz2\" ... $(date --rfc-3339=ns)\"\n"
        # s += "tar jcf \"$target.tar.bz2\" -C \"$base_tar\" \"$target\"\n"
        # s += "echo \"Done. Finished at $(date --rfc-3339=ns)\"\n"

        return s

    # If nb_files_on_bb = 0 -> no files on BB
    # If nb_files_on_bb = 32 -> all files on BB
    # If pairs = True means that if PTFX.w.fits is on BB then PTFX.w.weight.fits is also taken
    # So if pairs = True and nb_files_on_bb = 16 then all files are on BB
    def file_to_stage(self, count, pairs=False):
        s = ''
        s += "{}/input/PTF201111015420_2_o_32874_06.w.fits {}/PTF201111015420_2_o_32874_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111015420_2_o_32874_06.w.weight.fits {}/PTF201111015420_2_o_32874_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111025412_2_o_33288_06.w.fits {}/PTF201111025412_2_o_33288_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111025412_2_o_33288_06.w.weight.fits {}/PTF201111025412_2_o_33288_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111025428_2_o_33289_06.w.fits {}/PTF201111025428_2_o_33289_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111025428_2_o_33289_06.w.weight.fits {}/PTF201111025428_2_o_33289_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111035427_2_o_33741_06.w.fits {}/PTF201111035427_2_o_33741_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111035427_2_o_33741_06.w.weight.fits {}/PTF201111035427_2_o_33741_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111085228_2_o_34301_06.w.fits {}/PTF201111085228_2_o_34301_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111085228_2_o_34301_06.w.weight.fits {}/PTF201111085228_2_o_34301_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111095206_2_o_34706_06.w.fits {}/PTF201111095206_2_o_34706_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111095206_2_o_34706_06.w.weight.fits {}/PTF201111095206_2_o_34706_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111155050_2_o_35570_06.w.fits {}/PTF201111155050_2_o_35570_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111155050_2_o_35570_06.w.weight.fits {}/PTF201111155050_2_o_35570_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111165032_2_o_35994_06.w.fits {}/PTF201111165032_2_o_35994_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111165032_2_o_35994_06.w.weight.fits {}/PTF201111165032_2_o_35994_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111184953_2_o_36749_06.w.fits {}/PTF201111184953_2_o_36749_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111184953_2_o_36749_06.w.weight.fits {}/PTF201111184953_2_o_36749_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111224851_2_o_37387_06.w.fits {}/PTF201111224851_2_o_37387_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111224851_2_o_37387_06.w.weight.fits {}/PTF201111224851_2_o_37387_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111234857_2_o_37754_06.w.fits {}/PTF201111234857_2_o_37754_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111234857_2_o_37754_06.w.weight.fits {}/PTF201111234857_2_o_37754_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111265053_2_o_38612_06.w.fits {}/PTF201111265053_2_o_38612_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111265053_2_o_38612_06.w.weight.fits {}/PTF201111265053_2_o_38612_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111274755_2_o_38996_06.w.fits {}/PTF201111274755_2_o_38996_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111274755_2_o_38996_06.w.weight.fits {}/PTF201111274755_2_o_38996_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111284696_2_o_39396_06.w.fits {}/PTF201111284696_2_o_39396_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111284696_2_o_39396_06.w.weight.fits {}/PTF201111284696_2_o_39396_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111294943_2_o_39822_06.w.fits {}/PTF201111294943_2_o_39822_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111294943_2_o_39822_06.w.weight.fits {}/PTF201111294943_2_o_39822_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111304878_2_o_40204_06.w.fits {}/PTF201111304878_2_o_40204_06.w.fits\n".format(SWARP_DIR, "@INPUT@")
        s += "{}/input/PTF201111304878_2_o_40204_06.w.weight.fits {}/PTF201111304878_2_o_40204_06.w.weight.fits\n".format(SWARP_DIR, "@INPUT@")

        arr = [x+"\n" for x in s.split('\n')[:-1]]
        short_s = ''
        if count < 0:
            count = len(arr)
        count = min(count, len(arr))

        if pairs:
            if count % 2 != 0:
                count += 1

            count = min(count, len(arr)//2)
            
            for i in range(count):
                short_s += arr[i]
                short_s += arr[i+16]

        else:
            for i in range(count):
                short_s += arr[i]

        return short_s

    def salloc_str(self):
        s = 'PROJECT=\"CSC355\"\n'
        s = "bsub -Is -P $PROJECT -nnodes 1 -W 1:30 -alloc_flags \"NVME\" $SHELL\n"
        return s

    @staticmethod
    def launch():
        string = "#!/bin/bash\n"
        #string += "echo \"STAMP SYNC LAUNCH BEGIN $(date --rfc-3339=ns)\"\n"
        string += "exec \"$@\"\n"
        return string

    @staticmethod
    def write_launch(file=WRAPPER, overide=True):
        if os.path.exists(file):
            raise FileNotFoundError("file {} already exists.".format(file))
        with open(file, 'w') as f:
            f.write(SwarpInstance.launch())
        os.chmod(file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user


    def write_interactive_launch(self, file="start_interactive.sh", overide=True):
        if os.path.exists(file):
            raise FileNotFoundError("file {} already exists.".format(file))

        with open(file, 'w') as f:
            f.write(self.salloc_str())
        
        os.chmod(file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user


    def write(self, file, manual_stage=True, overide=True):
        if not overide and os.path.exists(file):
            raise FileNotFoundError("file {} already exists".format(file))

        if os.path.exists(file):
            sys.stderr.write(" === SWarp script: file {} already exists and will be re-written.\n".format(file))

        # TODO: fix this temporary thing
        with open("files_to_stage.txt", 'w', encoding='utf-8') as f:
            f.write(self.file_to_stage(count=self.nb_files_on_bb))

        with open(file, 'w', encoding='utf-8') as f:
            f.write(self.scheduler_header())
            f.write(self.script_modules())
            f.write(self.script_headers())
            f.write(self.average_loop())

        os.chmod(file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user

        with open("interactive_"+file, 'w', encoding='utf-8') as f:
            f.write(self.script_modules())
            f.write(self.script_headers(interactive=True))
            f.write(self.average_loop())

        os.chmod("interactive_"+file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user

        try:
            SwarpInstance.write_launch(overide=overide)
        except FileNotFoundError:
            sys.stderr.write(" === SWarp script: file {} already exists and will be re-written.\n".format(WRAPPER))
            pass
        try:
            self.write_interactive_launch(overide=overide)
        except FileNotFoundError:
            sys.stderr.write(" === SWarp script: file {} already exists and will be re-written.\n".format(WRAPPER))
            pass

class SwarpRun:
    def __init__(self, pipelines=[1]):
        self._pipelines = pipelines
        self._num_pipelines = len(pipelines)

    def pipeline_to_str(self):
        res = "{}".format(str(self._pipelines[0]))
        for i in range(1,self._num_pipelines-1):
            res += " {}".format(str(self._pipelines[i]))
        if self._num_pipelines > 1:
            res += " {}".format(str(self._pipelines[-1]))
        return res

    def pipelines(self):
        return self._pipelines

    def num_pipelines(self):
        return self._num_pipelines

    def standalone(self, file, count, manual_stage=True, overide=False, ):
        if not overide and os.path.exists(file):
            raise FileNotFoundError("file {} already exists".format(file))

        if os.path.exists(file):
            sys.stderr.write(" === Submit script: file {} already exists and will be re-written.\n".format(file))

        with open(file, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write("#set -x\n")
            f.write("for i in {}; do\n".format(self.pipeline_to_str()))
            # f.write("    for k in $(seq 1 {}); do\n".format(self.nb_averages))
            if platform.system() == "Darwin":
                f.write("    outdir=$(mktemp -d -t swarp-run-${i}N-"+str(count)+"F.XXXXXX)\n")
            else:
                f.write("    outdir=$(mktemp --directory --tmpdir=$(/bin/pwd) swarp-run-${i}N-"+str(count)+"F.XXXXXX)\n")
            # f.write("    lfs setstripe -c 1 -S 2048m \"${outdir}\"\n")
            f.write("    script=\"run-swarp-scaling-bb-${i}N.sh\"\n")
            f.write("    echo $outdir\n")
            f.write("    echo $script\n")
            f.write("    sed \"s/@NODES@/${i}/g\" \"run-swarp-scaling-bb.sh\" > ${outdir}/${script}\n")
            f.write("    sed \"s/@NODES@/${i}/g\" \"interactive_run-swarp-scaling-bb.sh\" > ${outdir}/interactive_run-swarp-scaling-bb-${i}N.sh\n")
            f.write("    sed \"s/@NODES@/${i}/g\" \"start_interactive.sh\" > ${outdir}/start_interactive-${i}N.sh\n")
            f.write("    cp " + SWARP_DIR + "/tools/copy.py "+ SWARP_DIR +"/tools/build_filemap.py files_to_stage.txt \"" + WRAPPER + "\" \"resample.swarp\" \"combine.swarp\" \"${outdir}\"\n")
            f.write("    chmod u+x ${outdir}/start_interactive-${i}N.sh ${outdir}/interactive_run-swarp-scaling-bb-${i}N.sh\n")
            f.write("    cd \"${outdir}\"\n")
            f.write("    bsub ${script}\n")
            f.write("    cd ..\n")
            f.write("done\n")

        os.chmod(file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user

    # def standalone_count(self, file, manual_stage=True, overide=False):
    #     if not overide and os.path.exists(file):
    #         raise FileNotFoundError("file {} already exists".format(file))

    #     if os.path.exists(file):
    #         sys.stderr.write(" === Submit script: file {} already exists and will be re-written.\n".format(file))

    #     with open(file, 'w') as f:
    #         f.write("#!/bin/bash\n")
    #         f.write("#set -x\n")
    #         f.write("for i in {}; do\n".format(self.pipeline_to_str()))
    #         # f.write("    for k in $(seq 1 {}); do\n".format(self.nb_averages))
    #         if platform.system() == "Darwin":
    #             f.write("    outdir=$(mktemp -d -t swarp-run-"+str(self.sched_config.nodes())+"N-${i}F.XXXXXX)\n")
    #         else:
    #             f.write("    outdir=$(mktemp --directory --tmpdir=$(/bin/pwd) swarp-run-"+str(self.sched_config.nodes())+"N-${i}F.XXXXXX)\n")
    #         f.write("    script=\"run-swarp-scaling-bb-${i}N.sh\"\n")
    #         f.write("    echo $outdir\n")
    #         f.write("    echo $script\n")
    #         f.write("    sed \"s/@COUNT@/${i}/g\" \"run-swarp-scaling-bb.sh\" > ${outdir}/${script}\n")
    #         #If we want to use DW to stage file
    #         if not manual_stage:
    #             f.write("    for j in $(seq ${i} -1 1); do\n")
    #             f.write("        stage_in=\"#DW stage_in source=" + SWARP_DIR + "/input destination=\$DW_JOB_STRIPED/input/${j} type=directory\"\n")    
    #             f.write("        sed -i \"s|@STAGE@|@STAGE@\\n${stage_in}|\" ${outdir}/${script}\n")
    #             f.write("    done\n")
    #         f.write("    cp ../copy.py ../build_filemap.py files_to_stage.txt \"" + BBINFO +"\" \"" + WRAPPER + "\" \"${outdir}\"\n")
    #         f.write("    cd \"${outdir}\"\n")
    #         f.write("    sbatch ${script}\n")
    #         #TODO ADD waiting time debug queue
    #         f.write("    cd ..\n")
    #         f.write("done\n")

    #     os.chmod(file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Generate SWarp configuration files and scripts')
    
    parser.add_argument('--threads', '-p', type=int, nargs='?', default=1,
                        help='Number of POSIX threads per workflow tasks (1 by default)')
    parser.add_argument('--nodes', '-n', type=int, nargs='?', default=1,
                        help='Number of compute nodes requested (750GB of BB per node)')
    parser.add_argument('--workflows', '-w', type=int, nargs='+', default=[1],
                        help='Number of identical SWarp workflows running in parallel. List of values (1 by default)')
    parser.add_argument('--input-sharing', '-x', action='store_true',
                        help='Use this flag if you want to only have the same input files shared by all workflows (NOT SUPPORTED)')
    parser.add_argument('--nb-run', '-r', type=int, nargs='?', default=5,
                        help='Number of runs to average on (5 by default)')
    parser.add_argument('--queue', '-q', type=str, nargs='?', default="batch",
                        help='Queue to execute the workflow')
    parser.add_argument('--timeout', '-t', type=str, nargs='?', default="00:30",
                        help='Timeout in hh:mm (00:30 for 30 minutes, DO NOT USE SECONDS)')
    parser.add_argument('--count', '-c', type=int, nargs='+', default=[-1],
                        help='Number of files staged in BB (-1 means all). Must be even. List of values (-1 by default)')

    parser.add_argument('--stage-fits', '-z', action="store_true",
                        help='Stage .resamp.fits for all pipelines in the Burst Buffer')

    parser.add_argument('--submit', '-S', action='store_true',
                        help='After the generation directly submit the job to the batch scheduler')

    # parser.add_argument('--slurm-profile', '-z', action="store_true",
    #                     help='Deactivate kickstart monitoring and activate slurm-based profiling')


    args = parser.parse_args()
    print(args)

    sys.stderr.write(" === Generate LSF  scripts for SWarp workflow\n")
    today = time.localtime()
    sys.stderr.write(" === Executed: {}-{}-{} at {}:{}:{}.\n".format(today.tm_mday,
                                                    today.tm_mon, 
                                                    today.tm_year, 
                                                    today.tm_hour, 
                                                    today.tm_min, 
                                                    today.tm_sec)
                                                )
    sys.stderr.write(" === Machine: {}.\n".format(platform.platform()))

    args.workflows = list(set(args.workflows))
    args.workflows = sorted([int(x) for x in args.workflows])
    if len(args.workflows) == 1:
        short_workflow = str(args.workflows[0])
    else:
        short_workflow = str(min(args.workflows))+'_'+str(max(args.workflows))


    args.count = list(set(args.count))
    args.count = sorted([int(x) for x in args.count])
    if len(args.count) == 1:
        short_count = str(args.count[0])
        if short_count == '-1':
            short_count = '32'

        if int(short_count) % 2 !=0:
            sys.stderr.write("[error] --count={} cannot be an odd number.\n".format(short_count))
            exit(-1)
    else:
        short_count = str(min(args.count))+'_'+str(max(args.count))

    # tempfile.mkstemp(suffix=None, prefix=None, dir=None, text=False)

    if args.stage_fits:
        output_dir = "swarp-{}-{}C-1400B-{}W-{}F-{}_{}_{}_{}-summit-stagefits/".format(args.queue, args.threads, short_workflow, short_count, today.tm_mon, today.tm_mday, today.tm_hour, today.tm_min)
    else:
        output_dir = "swarp-{}-{}C-1400B-{}W-{}F-{}_{}_{}_{}-summit/".format(args.queue, args.threads, short_workflow, short_count, today.tm_mon, today.tm_mday, today.tm_hour, today.tm_min)

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
        sys.stderr.write(" === Directory {} created.\n".format(output_dir))

    old_path = os.getcwd()+'/'
    os.chdir(old_path+output_dir)
    sys.stderr.write(" === Current directory {}\n".format(os.getcwd()))

    resample_config = SwarpWorkflowConfig(task_type=TaskType.RESAMPLE, nthreads=args.threads, resample_dir='@DIR@')
    resample_config.write(overide=True) #Write out the resample.swarp

    combine_config = SwarpWorkflowConfig(task_type=TaskType.COMBINE, nthreads=args.threads, resample_dir='@DIR@')
    combine_config.write(overide=True) #Write out the combine.swarp

    sched_config = SwarpSchedulerConfig(num_nodes=args.nodes, queue=args.queue, timeout=args.timeout, num_cores=args.threads)
    bb_config = SwarpBurstBufferConfig(
                size_bb=14000, #1.4TB per node
                stage_input_dirs=[
                    SWARP_DIR + "/input"],
                stage_input_files=[],
                stage_output_dirs=[
                    SWARP_DIR + "/output"],
                access_mode="private", 
                bbtype="summit"
                )

    instance1core = SwarpInstance(script_dir=output_dir,
                                resample_config=resample_config, 
                                combine_config=combine_config, 
                                sched_config=sched_config,
                                nb_files_on_bb=args.count,
                                bb_config=bb_config,
                                stagein_fits=args.stage_fits,
                                nb_avg=args.nb_run,
                                input_sharing=args.input_sharing,
                                )

    instance1core.write(file="run-swarp-scaling-bb.sh", manual_stage=True, overide=True)
    
    run1 = SwarpRun(pipelines=args.workflows)

    if bb_config.size() < run1.num_pipelines() * SIZE_ONE_PIPELINE/1024.0:
        sys.stderr.write(" WARNING: Burst buffers allocation seems to be too small.\n")
        sys.stderr.write(" WARNING: Estimated size needed by {} pipelines -> {} GB (you asked for {} GB).\n".format(run1.num_pipelines(), run1.num_pipelines() * SIZE_ONE_PIPELINE/1024.0, bb_config.size()))

    submit_file = "submit.sh"

    run1.standalone(file=submit_file, count=args.count[0], manual_stage=True, overide=True)
    #run1.standalone_count(file="submit_files.sh", manual_stage=True, overide=True)

    if args.submit:
        result = subprocess.run(['bash', submit_file], stdout=subprocess.PIPE)
        sys.stderr.write(" === Job submitted.\n")
        print(result.stdout.decode('utf-8'))

    os.chdir(old_path)
    sys.stderr.write(" === Switched back to initial directory {}\n".format(os.getcwd()))

