#!/usr/bin/env python3

import os
import sys
import stat
import platform
import time
import tempfile
import subprocess as sb
import argparse

from enum import Enum,unique,auto

SWARP_DIR = "/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/"
BBINFO = "bbinfo.sh"
WRAPPER = "wrapper.sh"

# Cori fragment size -> 20.14GiB

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
    def __init__(self, num_nodes, num_cores, slurm_options=None):
        self.num_nodes = num_nodes #Number of nodes requested
        self.num_cores = num_cores #Cores per nodes

    def nodes(self):
        return self.num_nodes

    def cores(self):
        return self.num_cores

class SwarpBurstBufferConfig:
    def __init__(self, size_bb, stage_input_dirs, stage_input_files, stage_output_dirs, access_mode="striped", bbtype="scratch"):
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
    def __init__(self, script_dir, resample_config, combine_config, sched_config, bb_config, standalone=True, no_stagein=True):
        self.standalone = standalone
        self.no_stagein = no_stagein

        self.resample_config = resample_config
        self.combine_config = combine_config

        self.bb_config = bb_config
        self.sched_config = sched_config
        self.script_dir = script_dir

    def slurm_header(self):
        string = "#!/bin/bash -l\n"
        string += "#SBATCH -p debug\n"
        if self.standalone:
            string += "#SBATCH -N @NODES@\n"
        else:
            string += "#SBATCH -N {}\n".format(self.sched_config.nodes())
        string += "#SBATCH -C haswell\n"
        string += "#SBATCH -t 00:30:00\n"
        string += "#SBATCH -J swarp-scaling\n"
        string += "#SBATCH -o output.%j\n"
        string += "#SBATCH -e error.%j\n"
        string += "#SBATCH --mail-user=lpottier@isi.edu\n"
        string += "#SBATCH --mail-type=FAIL\n"
        string += "#SBATCH --export=ALL\n"
        string += "#SBATCH -d singleton\n"
        return string

    def dw_temporary(self):
        string = "#DW jobdw capacity={}GB access_mode={} type={}\n".format(self.bb_config.size(), self.bb_config.mode(), self.bb_config.type())
        if self.standalone or self.no_stagein:
            string += "#@STAGE@\n"
        else:
            for directory in self.bb_config.indirs():                
                if directory.split('/')[-1] == '':
                    #end with / so we should take the second last one
                    target = directory.split('/')[-2]
                else:
                    target = directory.split('/')[-1]

                string += "#DW stage_in source={} destination=$DW_JOB_STRIPED/{}/ type=directory\n".format(directory, target)

            for file in self.bb_config.infiles():
                if file.split('/')[-1] == '':
                    #end with / so we should take the second last one
                    target = file.split('/')[-2]
                else:
                    target = file.split('/')[-1]

                string += "#DW stage_in source={} destination=$DW_JOB_STRIPED/{}/ type=file\n".format(file, target)

            # string += "#DW stage_in source={}/config destination=$DW_JOB_STRIPED/config type=directory\n".format(SWARP_DIR)
            for directory in self.bb_config.outdirs():
                if directory.split('/')[-1] == '':
                    #end with / so we should take the second last one
                    target = directory.split('/')[-2]
                else:
                    target = directory.split('/')[-1]

                string += "#DW stage_out source=$DW_JOB_STRIPED/{}/  destination={} type=directory\n".format(target, directory)

        string += "\n"
        return string

    def script_modules(self):
        string = "module unload darshan\n"
        string += "module load perftools-base perftools\n"
        string += "\n"
        return string

    def script_header(self):
        s = ''
        s += "usage()\n"
        s += "{\n"
        s += "    echo \"usage: $0 [[[-f=file ] [-c=COUNT]] | [-h]]\"\n"
        s += "}"
        s += "\n"
        s += "if [ -z \"$DW_JOB_STRIPED\" ]; then\n"
        s += "    echo \"Error: burst buffer allocation found. Run start_nostage.sh first\"\n"
        s += "    exit\n"
        s += "fi\n"
        s += "\n"
        s += "FILES_TO_STAGE=\"files_to_stage.txt\"\n"
        s += "COUNT=0\n"
        s += "\n"
        s += "# Test code to verify command line processing\n\n"
        s += "if [ -f \"$FILES_TO_STAGE\" ]; then\n"
        s += "    echo \"File list used: $FILES_TO_STAGE\"\n"
        s += "else\n"
        s += "    echo \"$FILES_TO_STAGE does not seem to exist\"\n"
        s += "    exit\n"
        s += "fi\n"
        s += "\n"

        s += "if (( \"$COUNT\" < 0 )); then\n"
        s += "    COUNT=$(cat $FILES_TO_STAGE | wc -l)\n"
        s += "fi\n"
        s += "\n"

        s += "echo $FILES_TO_STAGE\n"
        s += "echo $COUNT\n"
        s += "\n"

        s += "IMAGE_PATTERN='PTF201111*.w.fits'\n"
        s += "IMAGE_WEIGHT_PATTERN='PTF201111*.w.weight.fits'\n"
        s += "RESAMPLE_PATTERN='PTF201111*.w.resamp.fits'\n"
        s += "\n"

        #BASE="/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/"
        s += "SWARP_DIR=workflow-io-bb/real-workflows/swarp\n"
        s += "BASE=\"$SCRATCH/$SWARP_DIR/{}\"\n".format(self.script_dir)
        s += "LAUNCH=\"$SCRATCH/$SWARP_DIR/{}/{}\"\n".format(self.script_dir, WRAPPER)
        s += "EXE={}/bin/swarp\n".format(SWARP_DIR)
        s += "COPY={}/copy.py\n".format(SWARP_DIR)
        s += "FILE_MAP={}/build_filemap.py\n".format(SWARP_DIR)
        s += "\n"

        s += "NODE_COUNT=@NODES@   # Number of compute nodes requested by srun\n"
        s += "TASK_COUNT=@NODES@   # Number of tasks allocated by srun\n"
        s += "CORE_COUNT={}        # Number of cores used by both tasks\n".format(self.sched_config.cores())
        s += "\n"

        s += "STAGE_EXEC=0        #0 no stage. 1 -> stage exec in BB\n"
        s += "STAGE_CONFIG=0      #0 no stage. 1 -> stage config dir in BB\n"
        s += "NB_AVG={}            # Number of identical runs\n".format("5")
        s += "\n"

        s += "CONFIG_DIR=$BASE\n"
        s += "if (( \"$STAGE_CONFIG\" == 1 )); then\n"
        s += "    CONFIG_DIR=$DW_JOB_STRIPED/config\n"
        s += "fi\n"
        s += "RESAMPLE_CONFIG=${CONFIG_DIR}/resample.swarp\n"
        s += "COMBINE_CONFIG=${CONFIG_DIR}/combine.swarp\n"
        s += "\n"

        s += "CONFIG_FILES=\"${RESAMPLE_CONFIG} ${COMBINE_CONFIG}\"\n"
        s += "\n"

        s += "INPUT_DIR_PFS=$BASE/input\n"
        s += "INPUT_DIR=$DW_JOB_STRIPED/input\n"
        s += "\n"

        s += "OUTPUT_DIR_NAME=$SLURM_JOB_NAME.batch.${CORE_COUNT}c.${COUNT}f.$SLURM_JOB_ID/\n"
        s += "export GLOBAL_OUTPUT_DIR=$DW_JOB_STRIPED/$OUTPUT_DIR_NAME\n"
        s += "mkdir -p $GLOBAL_OUTPUT_DIR\n"
        s += "chmod 777 $GLOBAL_OUTPUT_DIR\n"
        s += "\n"

        s += "mkdir -p $OUTPUT_DIR_NAME\n"
        s += "\n"
        return s

    def file_to_stage(self):
        s = ''
        s += "{}/input/PTF201111015420_2_o_32874_06.w.fits $DW_JOB_STRIPED/input/PTF201111015420_2_o_32874_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111025412_2_o_33288_06.w.fits $DW_JOB_STRIPED/input/PTF201111025412_2_o_33288_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111025428_2_o_33289_06.w.fits $DW_JOB_STRIPED/input/PTF201111025428_2_o_33289_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111035427_2_o_33741_06.w.fits $DW_JOB_STRIPED/input/PTF201111035427_2_o_33741_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111085228_2_o_34301_06.w.fits $DW_JOB_STRIPED/input/PTF201111085228_2_o_34301_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111095206_2_o_34706_06.w.fits $DW_JOB_STRIPED/input/PTF201111095206_2_o_34706_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111155050_2_o_35570_06.w.fits $DW_JOB_STRIPED/input/PTF201111155050_2_o_35570_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111165032_2_o_35994_06.w.fits $DW_JOB_STRIPED/input/PTF201111165032_2_o_35994_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111184953_2_o_36749_06.w.fits $DW_JOB_STRIPED/input/PTF201111184953_2_o_36749_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111224851_2_o_37387_06.w.fits $DW_JOB_STRIPED/input/PTF201111224851_2_o_37387_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111234857_2_o_37754_06.w.fits $DW_JOB_STRIPED/input/PTF201111234857_2_o_37754_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111265053_2_o_38612_06.w.fits $DW_JOB_STRIPED/input/PTF201111265053_2_o_38612_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111274755_2_o_38996_06.w.fits $DW_JOB_STRIPED/input/PTF201111274755_2_o_38996_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111284696_2_o_39396_06.w.fits $DW_JOB_STRIPED/input/PTF201111284696_2_o_39396_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111294943_2_o_39822_06.w.fits $DW_JOB_STRIPED/input/PTF201111294943_2_o_39822_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111304878_2_o_40204_06.w.fits $DW_JOB_STRIPED/input/PTF201111304878_2_o_40204_06.w.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111015420_2_o_32874_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111015420_2_o_32874_06.w.weight.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111025412_2_o_33288_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111025412_2_o_33288_06.w.weight.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111025428_2_o_33289_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111025428_2_o_33289_06.w.weight.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111035427_2_o_33741_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111035427_2_o_33741_06.w.weight.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111085228_2_o_34301_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111085228_2_o_34301_06.w.weight.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111095206_2_o_34706_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111095206_2_o_34706_06.w.weight.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111155050_2_o_35570_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111155050_2_o_35570_06.w.weight.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111165032_2_o_35994_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111165032_2_o_35994_06.w.weight.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111184953_2_o_36749_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111184953_2_o_36749_06.w.weight.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111224851_2_o_37387_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111224851_2_o_37387_06.w.weight.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111234857_2_o_37754_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111234857_2_o_37754_06.w.weight.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111265053_2_o_38612_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111265053_2_o_38612_06.w.weight.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111274755_2_o_38996_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111274755_2_o_38996_06.w.weight.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111284696_2_o_39396_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111284696_2_o_39396_06.w.weight.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111294943_2_o_39822_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111294943_2_o_39822_06.w.weight.fits\n".format(SWARP_DIR)
        s += "{}/input/PTF201111304878_2_o_40204_06.w.weight.fits $DW_JOB_STRIPED/input/PTF201111304878_2_o_40204_06.w.weight.fits\n".format(SWARP_DIR)
        return s

    def average_loop(self):
        s = ''
        s += "for k in $(seq 1 1 $NB_AVG); do\n"
        s += "    echo \"#### Starting run $k... $(date --rfc-3339=ns)\"\n"
        s += "    rm -rf $DW_JOB_STRIPED/*\n"
        s += "\n"

        s += "    export OUTPUT_DIR=$GLOBAL_OUTPUT_DIR/${k}\n"
        s += "    #The local version\n"
        s += "    mkdir -p $OUTPUT_DIR_NAME/${k}\n"
        s += "\n"

        s += "    echo $OUTPUT_DIR\n"
        s += "\n"

        s += "    OUTPUT_FILE=$OUTPUT_DIR/output.log\n"
        s += "    BB_INFO=$OUTPUT_DIR/bb.log\n"
        s += "    DU_RES=$OUTPUT_DIR/data-stagedin.log\n"
        s += "    BB_ALLOC=$OUTPUT_DIR/bb_alloc.log\n"
        s += "\n"

        s += "    mkdir -p $OUTPUT_DIR\n"
        s += "    chmod 777 $OUTPUT_DIR\n"
        s += "\n"

        s += "    export RESAMP_DIR=$DW_JOB_STRIPED/resamp\n"
        s += "\n"

        s += "    mkdir -p $RESAMP_DIR\n"
        s += "    chmod 777 $RESAMP_DIR\n"
        s += "\n"

        s += "    #rm -f {error,output}.*\n"
        s += "\n"

        # FIX THIS
        s += "    #### To select file to stage\n"
        s += "    ## To modify the lines 1 to 5 to keep 5 files on the PFS (by default they all go on the BB)\n"
        s += "    cp $FILES_TO_STAGE $OUTPUT_DIR/\n"
        s += "    LOC_FILES_TO_STAGE=\"$OUTPUT_DIR/$FILES_TO_STAGE\"\n"
        s += "    #sed -i -e \"1,${COUNT}s|\(\$DW_JOB_STRIPED\/\)|${BASE}|\" $FILES_TO_STAGE\n"
        s += "    #We want to unstage the w.fits and the corresponding w.weight.fits\n"
        s += "    if (( \"$COUNT\" > 0 )); then\n"
        s += "        sed -i -e \"1,${COUNT}s|\(\$DW_JOB_STRIPED\/\)\(.*w.fits\)|${BASE}\2|\" $LOC_FILES_TO_STAGE\n"
        s += "        ## TODO: Fix this, only work if files are sorted w.fits first and with 16 files....\n"
        s += "        x=$(echo \"$COUNT+16\" | bc)\n"
        s += "        sed -i -e \"16,${x}s|\(\$DW_JOB_STRIPED\/\)\(.*w.weight.fits\)|${BASE}\2|\" $LOC_FILES_TO_STAGE\n"
        s += "    fi\n"
        s += "\n"

        s += "    echo \"Number of files kept in PFS:$(echo \"$COUNT*2\" | bc)/$(cat $LOC_FILES_TO_STAGE | wc -l)\" | tee $OUTPUT_FILE\n"
        s += "    echo \"NODE=$NODE_COUNT\" | tee -a $OUTPUT_FILE\n"
        s += "    echo \"TASK=$TASK_COUNT\" | tee -a $OUTPUT_FILE\n"
        s += "    echo \"CORE=$CORE_COUNT\" | tee -a $OUTPUT_FILE\n"
        s += "\n"

        s += "    MONITORING=\"env OUTPUT_DIR=$OUTPUT_DIR RESAMP_DIR=$RESAMP_DIR CORE_COUNT=$CORE_COUNT pegasus-kickstart -z\"\n"
        s += "\n"

        s += "    echo bbinfo is deactivated TEMPORARILY\n"
        # s += "    module load dws\n"
        # s += "    sessID=$(dwstat sessions | grep $SLURM_JOBID | awk '{print $1}')\n"
        # s += "    echo \"session ID is: \"${sessID} | tee $BB_INFO\n"
        # s += "    instID=$(dwstat instances | grep $sessID | awk '{print $1}')\n"
        # s += "    echo \"instance ID is: \"${instID} | tee -a $BB_INFO\n"
        # s += "    echo \"fragments list:\" | tee -a $BB_INFO\n"
        # s += "    echo \"frag state instID capacity gran node\" | tee -a $BB_INFO\n"
        # s += "    dwstat fragments | grep ${instID} | tee -a $BB_INFO\n"
        # s += "\n"

        # s += "    bballoc=$(dwstat fragments | grep ${instID} | awk '{print $4}')\n"
        # s += "    echo \"$bballoc\" > $BB_ALLOC\n"
        # s += "\n"

        s += "    echo \"Starting STAGE_IN... $(date --rfc-3339=ns)\" | tee -a $OUTPUT_FILE\n"
        s += "    t1=$(date +%s.%N)\n"
        s += "    if [ -f \"$LOC_FILES_TO_STAGE\" ]; then\n"
        s += "        $COPY -f $LOC_FILES_TO_STAGE -d $OUTPUT_DIR\n"
        s += "    else\n"
        s += "        $COPY -i $INPUT_DIR_PFS -o $INPUT_DIR -d $OUTPUT_DIR\n"
        s += "    fi\n"
        s += "\n"

        s += "    if (( \"$STAGE_EXEC\" == 1 )); then\n"
        s += "        cp -r $EXE $DW_JOB_STRIPED\n"
        s += "    fi\n"
        s += "\n"

        s += "    if (( \"$STAGE_CONFIG\" == 1 )); then\n"
        s += "        cp -r $CONFIG_DIR $DW_JOB_STRIPED\n"
        s += "    fi\n"
        s += "\n"

        s += "    t2=$(date +%s.%N)\n"
        s += "    tdiff1=$(echo \"$t2 - $t1\" | bc -l)\n"
        s += "    echo \"TIME STAGE_IN $tdiff1\" | tee -a $OUTPUT_FILE\n"
        s += "\n"

        s += "    mkdir -p $INPUT_DIR\n"
        s += "\n"

        s += "    #If we did not stage any input files\n"
        s += "    if [[ -f \"$(ls -A $INPUT_DIR)\" ]]; then\n"
        s += "        INPUT_DIR=$INPUT_DIR_PFS\n"
        s += "        echo \"INPUT_DIR set as $INPUT_DIR (no input in the BB)\"\n"
        s += "    fi\n"
        s += "\n"

        #if we stage in executable
        s += "    if (( \"$STAGE_EXEC\" == 1 )); then\n"
        s += "        EXE=$DW_JOB_STRIPED/swarp\n"
        s += "    fi\n"
        s += "\n"

        s += "    RESAMPLE_FILES=\"$OUTPUT_DIR/resample_files.txt\"\n"
        s += "    $FILE_MAP -I $INPUT_DIR_PFS -B $INPUT_DIR -O $RESAMPLE_FILES -R $IMAGE_PATTERN  | tee -a $OUTPUT_FILE\n"
        s += "\n"

        s += "    dsize=$(du -sh $INPUT_DIR | awk '{print $1}')\n"
        s += "    nbfiles=$(ls -al $INPUT_DIR | grep '^-' | wc -l)\n"
        s += "    echo \"$nbfiles $dsize\" | tee $DU_RES\n"
        s += "\n"

        s += "    echo \"Starting RESAMPLE... $(date --rfc-3339=ns)\" | tee -a $OUTPUT_FILE\n"
        s += "    t1=$(date +%s.%N)\n"
        s += "\n"

        s += "    srun --ntasks=1 --cpus-per-task=$CORE_COUNT -o \"$OUTPUT_DIR/output.resample\" -e \"$OUTPUT_DIR/error.resample\" $MONITORING -l \"$OUTPUT_DIR/stat.resample.xml\" $EXE -c $RESAMPLE_CONFIG $(cat $RESAMPLE_FILES)\n"
        s += "\n"

        s += "    t2=$(date +%s.%N)\n"
        s += "    tdiff2=$(echo \"$t2 - $t1\" | bc -l)\n"
        s += "    echo \"TIME RESAMPLE $tdiff2\" | tee -a $OUTPUT_FILE\n"
        s += "\n"

        s += "    echo \"Starting COMBINE... $(date --rfc-3339=ns)\" | tee -a $OUTPUT_FILE\n"
        s += "    t1=$(date +%s.%N)\n"
        s += "\n"

        s += "    ###\n"
        s += "    ## TODO: Copy back from the PFS the resamp files so we an play also with the alloc there\n"
        s += "    ###\n"
        s += "\n"

        s += "    srun --ntasks=1 --cpus-per-task=$CORE_COUNT -o \"$OUTPUT_DIR/output.coadd\" -e \"$OUTPUT_DIR/error.coadd\" $MONITORING -l \"$OUTPUT_DIR/stat.combine.xml\" $EXE -c $COMBINE_CONFIG ${RESAMP_DIR}/${RESAMPLE_PATTERN}\n"
        s += "\n"

        s += "    t2=$(date +%s.%N)\n"
        s += "    tdiff3=$(echo \"$t2 - $t1\" | bc -l)\n"
        s += "    echo \"TIME COMBINE $tdiff3\" | tee -a $OUTPUT_FILE\n"
        s += "\n"
        s += "    du -sh $DW_JOB_STRIPED/ | tee -a $OUTPUT_FILE\n"
        s += "\n"
        s += "    env | grep SLURM > $OUTPUT_DIR/slurm.env\n"
        s += "\n"

        s += "    echo \"Starting STAGE_OUT... $(date --rfc-3339=ns)\" | tee -a $OUTPUT_FILE\n"
        s += "    t1=$(date +%s.%N)\n"
        s += "    $COPY -i $OUTPUT_DIR -o $OUTPUT_DIR_NAME/${k} -a \"stage-out\" -d $OUTPUT_DIR_NAME/${k}\n"
        s += "    t2=$(date +%s.%N)\n"
        s += "    tdiff4=$(echo \"$t2 - $t1\" | bc -l)\n"
        s += "\n"
        s += "    OUTPUT_FILE=$OUTPUT_DIR_NAME/${k}/output.log\n"
        s += "    echo \"TIME STAGE_OUT $tdiff4\" | tee -a $OUTPUT_FILE\n"
        s += "\n"
        s += "    echo \"========\" | tee -a $OUTPUT_FILE\n"
        s += "    tdiff=$(echo \"$tdiff1 + $tdiff2 + $tdiff3 + $tdiff4\" | bc -l)\n"
        s += "    echo \"TIME TOTAL $tdiff\" | tee -a $OUTPUT_FILE\n"
        s += "\n"
        s += "    set -x\n"
        s += "    rm -rf \"$OUTPUT_DIR_NAME/${k}/*.fits\"\n"
        s += "    set +x\n"
        s += "\n"
        s += "    echo \"#### Ending run $k... $(date --rfc-3339=ns)\"\n"
        s += "done\n"
        return s

    def script_globalvars(self):

        # FILES_TO_STAGE="files_to_stage.txt"
        # COUNT=0

        # # Test code to verify command line processing
        # if [ -f "$FILES_TO_STAGE" ]; then
        #     echo "File list used: $FILES_TO_STAGE"
        # else
        #     echo "$FILES_TO_STAGE does not seem to exist"
        #     exit
        # fi

        # if (( "$COUNT" < 0 )); then
        #     COUNT=$(cat $FILES_TO_STAGE | wc -l)
        # fi

        # echo $FILES_TO_STAGE
        # echo $COUNT

        string = "#set -x\n"
        string += "SWARP_DIR=workflow-io-bb/real-workflows/swarp\n"
        string += "BASE=\"$SCRATCH/$SWARP_DIR/{}\"\n".format(self.script_dir)
        string += "LAUNCH=\"$SCRATCH/$SWARP_DIR/{}/{}\"\n".format(self.script_dir, WRAPPER)
        string += "EXE=$SCRATCH/$SWARP_DIR/bin/swarp\n"
        #string += "export CONTROL_FILE=\"$SCRATCH/control_file.txt\"\n\n"

        # COPY=$BASE/copy.py
        # FILE_MAP=$BASE/build_filemap.py

        # NODE_COUNT=1        # Number of compute nodes requested by srun
        # TASK_COUNT=1        # Number of tasks allocated by srun
        # CORE_COUNT=1        # Number of cores used by both tasks


        string += "CORES_PER_PROCESS={}\n".format(self.sched_config.cores())
        string += "CONFIG_DIR=$BASE\n"
        string += "RESAMPLE_CONFIG=${CONFIG_DIR}/resample.swarp\n"
        string += "COMBINE_CONFIG=${CONFIG_DIR}/combine.swarp\n"

        # CONFIG_DIR=$BASE/config
        # if (( "$STAGE_CONFIG" == 1 )); then
        #     CONFIG_DIR=$DW_JOB_STRIPED/config
        # fi

        string += "FILE_PATTERN='PTF201111*'\n"
        string += "IMAGE_PATTERN='PTF201111*.w.fits'\n"
        string += "RESAMPLE_PATTERN='PTF201111*.w.resamp.fits'\n"

        string += "echo \"NUM NODES ${SLURM_JOB_NUM_NODES}\"\n"
        string += "echo \"STAMP PREPARATION $(date --rfc-3339=ns)\"\n"
        string += "\n"
        return string


    def create_output_dirs(self):
        string = "# Create the final output directory and run directory\n"
        string += "outdir=\"$(pwd)/output\"; mkdir ${outdir}\n"
        string +=  "sh {}\n".format(BBINFO)
        string +=  "rundir=$DW_JOB_STRIPED/swarp-run\n"
        string +=  "mkdir $rundir\n"
        string += "# Create a output and run directory for each SWarp process\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    mkdir -p ${rundir}/${process}\n"
        string += "    mkdir -p ${outdir}/${process}\n"
        string += "done\n"
        string += "\n"
        return string

    def stage_in_files(self):
        string = "# Copy manually files in BB\n"
        string += "echo \"TIME STAGE_IN:${SLURM_JOB_NUM_NODES} $(date --rfc-3339=ns)\"\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    echo \"Copy files for process ${process}\"\n"
        for directory in self.bb_config.indirs():
            if directory.split('/')[-1] == '':
                #end with / so we should take the second last one
                target = directory.split('/')[-2]
            else:
                target = directory.split('/')[-1]

            string = string + "    cp -r " + directory + " $DW_JOB_STRIPED/" + target + "/${process} &\n"
        for file in self.bb_config.infiles():
            string = string + "    cp " + file + " $DW_JOB_STRIPED/ &\n"
        string += "done\n"
        string += "t1=$(date +%s.%N)\n"
        string += "wait\n"
        string += "t2=$(date +%s.%N)\n"
        string += "tdiff=$(echo \"$t2 - $t1\" | bc -l)\n"
        string += "echo \"TIME STAGE_IN:${SLURM_JOB_NUM_NODES} $tdiff\"\n"
        string += "du -sh $DW_JOB_STRIPED/ > ${outdir}/size_staged_in.out\n"
        string += "\n"
        return string

    def stage_out_files(self):
        string = "# Copy manually files in PFS from BB\n"
        string += "echo \"TIME STAGE_OUT:${SLURM_JOB_NUM_NODES} $(date --rfc-3339=ns)\"\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    echo \"Copy files for process ${process}\"\n"
        string += "    cp -r ${rundir} ${outdir} &\n"
        string += "done\n"
        string += "t1=$(date +%s.%N)\n"
        string += "wait\n"
        string += "t2=$(date +%s.%N)\n"
        string += "tdiff=$(echo \"$t2 - $t1\" | bc -l)\n"
        string += "echo \"TIME STAGE_OUT:${SLURM_JOB_NUM_NODES} $tdiff\"\n"
        string += "du -sh ${rundir} > ${outdir}/size_staged_out.out\n"
        string += "\n"
        return string

    def script_run_resample(self):
        string = "cd ${rundir}\n"
        string += "du -sh $DW_JOB_STRIPED/input ${rundir} > ${outdir}/du_init.out\n"
        string += "echo \"STAMP RESAMPLE:${SLURM_JOB_NUM_NODES} PREP $(date --rfc-3339=ns)\"\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    echo \"Launching resample process ${process}\"\n"
        string += "    indir=\"$DW_JOB_STRIPED/input/${process}\" # This data has already been staged in\n"
        string += "    cd ${process}\n"
        string += "    srun --cpus-per-task=${CORES_PER_PROCESS} -o \"output.resample.%j.${process}\" -e \"error.resample.%j.${process}\" pegasus-kickstart -z -l stat.resample.xml $LAUNCH $EXE -c $RESAMPLE_CONFIG ${indir}/${IMAGE_PATTERN} & \n"
        string += "    cd ..\n"
        string += "done\n"
        string += "echo \"STAMP RESAMPLE:${SLURM_JOB_NUM_NODES} $(date --rfc-3339=ns)\"\n"
        string += "\n"
        # string += "sleep 10\n"
        # string += "touch $CONTROL_FILE\n"
        string += "echo \"STAMP RESAMPLE:${SLURM_JOB_NUM_NODES} $(date --rfc-3339=ns)\"\n"
        string += "t1=$(date +%s.%N)\n"
        string += "wait\n"
        # string += "rm $CONTROL_FILE\n"
        string += "t2=$(date +%s.%N)\n"
        string += "tdiff=$(echo \"$t2 - $t1\" | bc -l)\n"
        string += "echo \"TIME RESAMPLE:${SLURM_JOB_NUM_NODES} $tdiff\"\n"
        string += "du -sh $DW_JOB_STRIPED/input ${rundir} > ${outdir}/du_resample.out\n"
        string += "\n"
        return string

    def script_copy_resample(self):
        string = "# Copy the stdout, stderr, SWarp XML files\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    cp -n -v ${process}/{output*,error*,*.xml} ${outdir}/${process}\n"
        string += "done\n"
        string += "\n"
        return string

    def script_run_combine(self):
        string = "echo \"STAMP COMBINE:${SLURM_JOB_NUM_NODES} PREP $(date --rfc-3339=ns)\"\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    echo \"Launching coadd process ${process}\"\n"
        string += "    cd ${process}\n"
        string += "    srun --cpus-per-task=${CORES_PER_PROCESS} -o \"output.coadd.%j.${process}\" -e \"error.coadd.%j.${process}\" pegasus-kickstart -z -l stat.combine.xml $LAUNCH $EXE -c -c $COMBINE_CONFIG ${RESAMPLE_PATTERN} &\n"
        string += "    cd ..\n"
        string += "done\n"
        string += "\n"
        # string += "sleep 10\n"
        # string += "touch $CONTROL_FILE\n"
        string += "echo \"STAMP COMBINE:${SLURM_JOB_NUM_NODES} $(date --rfc-3339=ns)\"\n"
        string += "t1=$(date +%s.%N)\n"
        string += "wait\n"
        # string += "rm $CONTROL_FILE\n"
        string += "t2=$(date +%s.%N)\n"
        string += "tdiff=$(echo \"$t2 - $t1\" | bc -l)\n"
        string += "echo \"TIME COMBINE:${SLURM_JOB_NUM_NODES} $tdiff\"\n"
        string += "\n"
        return string

    def script_ending(self):
        string = "# Copy the stdout, stderr, SWarp XML files and IPM XML file\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    ls -lh ${rundir}/${process}/*.fits $DW_JOB_STRIPED/input/${process}/*.fits > ${outdir}/${process}/list_of_files.out\n"
        string += "    cp -n -v ${process}/{output*,error*,*.xml} ${outdir}/${process}\n"
        string += "done\n"
        string += "du -sh $DW_JOB_STRIPED/input ${rundir} > ${outdir}/disk_usage.out\n"

        string += "\n"

        string += "echo \"STAMP CLEANUP:${SLURM_JOB_NUM_NODES} $(date --rfc-3339=ns)\"\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    rm -v ${process}/*.fits\n"
        string += "done\n"
        string += "echo \"STAMP DONE:${SLURM_JOB_NUM_NODES} $(date --rfc-3339=ns)\"\n"
        return string

    @staticmethod
    def bbinfo():
        string = "#!/bin/bash\n"
        string += "module load dws\n"
        string += "sessID=$(dwstat sessions | grep $SLURM_JOBID | awk '{print $1}')\n"
        string += "echo \"session ID is: \"${sessID}\n"
        string += "instID=$(dwstat instances | grep $sessID | awk '{print $1}')\n"
        string += "echo \"instance ID is: \"${instID}\n"
        string += "echo \"fragments list:\"\n"
        string += "echo \"frag state instID capacity gran node\"\n"
        string += "dwstat fragments | grep ${instID}\n"
        return string

    @staticmethod
    def write_bbinfo(file=BBINFO, overide=False):
        if os.path.exists(file):
            raise FileNotFoundError("file {} already exists.".format(file))
        with open(file, 'w') as f:
            f.write(SwarpInstance.bbinfo())
        os.chmod(file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user

    @staticmethod
    def launch():
        string = "#!/bin/bash\n"
        #string += "echo \"STAMP SYNC LAUNCH BEGIN $(date --rfc-3339=ns)\"\n"
        string += "exec \"$@\"\n"
        return string

    @staticmethod
    def write_launch(file=WRAPPER, overide=False):
        if os.path.exists(file):
            raise FileNotFoundError("file {} already exists.".format(file))
        with open(file, 'w') as f:
            f.write(SwarpInstance.launch())
        os.chmod(file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user

    def write(self, file, manual_stage=True, overide=False):
        if not overide and os.path.exists(file):
            raise FileNotFoundError("file {} already exists".format(file))

        if os.path.exists(file):
            sys.stderr.write(" === SWarp script: file {} already exists and will be re-written.\n".format(file))

        with open(file, 'w') as f:
            f.write(self.slurm_header())
            f.write(self.dw_temporary())
            f.write(self.script_modules())
            f.write(self.script_header())
            f.write(self.average_loop())

            # f.write(self.script_globalvars())
            # f.write(self.create_output_dirs())
            # if manual_stage:
            #     f.write(self.stage_in_files())

            # f.write(self.script_run_resample())
            # f.write(self.script_copy_resample())
            # f.write(self.script_run_combine())
            # if manual_stage:
            #     f.write(self.stage_out_files())

            # f.write(self.script_ending())

        # TODO: fix this temporary thing
        with open("files_to_stage.txt", 'w') as f:
            f.write(self.file_to_stage())

        os.chmod(file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user

        try:
            SwarpInstance.write_bbinfo(overide=overide)
        except FileNotFoundError:
            sys.stderr.write(" === SWarp script: file {} already exists and will be re-written.\n".format(BBINFO))
            pass
        try:
            SwarpInstance.write_launch(overide=overide)
        except FileNotFoundError:
            sys.stderr.write(" === SWarp script: file {} already exists and will be re-written.\n".format(WRAPPER))
            pass

class SwarpRun:
    def __init__(self, pipelines=[1], number_avg=1):
        self._pipelines = pipelines
        self._num_pipelines = len(pipelines)
        self.nb_averages = number_avg

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

    def standalone(self, file, manual_stage=True, overide=False):
        if not overide and os.path.exists(file):
            raise FileNotFoundError("file {} already exists".format(file))

        if os.path.exists(file):
            sys.stderr.write(" === Submit script: file {} already exists and will be re-written.\n".format(file))

        with open(file, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write("#set -x\n")
            f.write("for i in {}; do\n".format(self.pipeline_to_str()))
            f.write("    for k in $(seq 1 {}); do\n".format(self.nb_averages))
            if platform.system() == "Darwin":
                f.write("        outdir=$(mktemp -d -t swarp-run-${k}-${i}N.XXXXXX)\n")
            else:
                f.write("        outdir=$(mktemp --directory --tmpdir=$(/bin/pwd) swarp-run-${k}-${i}N.XXXXXX)\n")
            f.write("        script=\"run-swarp-scaling-bb-${i}N.sh\"\n")
            f.write("        echo $outdir\n")
            f.write("        echo $script\n")
            f.write("        sed \"s/@NODES@/${i}/g\" \"run-swarp-scaling-bb.sh\" > ${outdir}/${script}\n")
            #If we want to use DW to stage file
            if not manual_stage:
                f.write("        for j in $(seq ${i} -1 1); do\n")
                f.write("           stage_in=\"#DW stage_in source=" + SWARP_DIR + "/input destination=\$DW_JOB_STRIPED/input/${j} type=directory\"\n")    
                f.write("           sed -i \"s|@STAGE@|@STAGE@\\n${stage_in}|\" ${outdir}/${script}\n")
                f.write("        done\n")
            f.write("        cp ../copy.py ../build_filemap.py files_to_stage.txt \"" + BBINFO +"\" \"" + WRAPPER + "\" \"${outdir}\"\n")
            f.write("        cd \"${outdir}\"\n")
            f.write("        sbatch ${script}\n")
            f.write("        cd ..\n")
            f.write("    done\n")
            f.write("done\n")

        os.chmod(file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Generate SWarp configuration files and scripts')
    
    parser.add_argument('--threads', '-C', type=int, nargs='?', default=1,
                        help='Number of POSIX threads per workflow tasks')
    parser.add_argument('--nodes', '-N', type=int, nargs='?', default=1,
                        help='Number of compute nodes requested')
    parser.add_argument('--bbsize', '-B', type=int, nargs='?', default=50,
                        help='Burst buffers allocation in GB (because of Cray API and Slurm, no decimal notation allowed)')
    parser.add_argument('--workflows', '-W', type=int, nargs='?', default=1,
                        help='Number of identical SWarp workflows running in parallel')
    parser.add_argument('--input-sharing', '-s', action='store_true',
                        help='Use this flag if you want to only have the same input files shared by all workflows (NOT SUPPORTED)')
    parser.add_argument('--nb-run', '-r', type=int, nargs='?', default=1,
                        help='Number of runs to average on')

    args = parser.parse_args()
    print(args)

    sys.stderr.write(" === Generate Slurm scripts for SWarp workflow\n")
    today = time.localtime()
    sys.stderr.write(" === Executed: {}-{}-{} at {}:{}:{}.\n".format(today.tm_mday,
                                                    today.tm_mon, 
                                                    today.tm_year, 
                                                    today.tm_hour, 
                                                    today.tm_min, 
                                                    today.tm_sec)
                                                )
    sys.stderr.write(" === Machine: {}.\n".format(platform.platform()))

    # tempfile.mkstemp(suffix=None, prefix=None, dir=None, text=False)
    if args.input_sharing:
        output_dir = "build_shared-{}N-{}C-{}W-{}B/".format(args.nodes, args.threads, args.workflows, args.bbsize)
    else:
        output_dir = "build-{}N-{}C-{}W-{}B/".format(args.nodes, args.threads, args.workflows, args.bbsize)
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
        sys.stderr.write(" === Directory {} created.\n".format(output_dir))

    old_path = os.getcwd()+'/'
    os.chdir(old_path+output_dir)
    sys.stderr.write(" === Current directory {}\n".format(os.getcwd()))

    resample_config = SwarpWorkflowConfig(task_type=TaskType.RESAMPLE, nthreads=args.threads, resample_dir='.')
    resample_config.write(overide=True) #Write out the resample.swarp

    combine_config = SwarpWorkflowConfig(task_type=TaskType.COMBINE, nthreads=args.threads, resample_dir='.')
    combine_config.write(overide=True) #Write out the combine.swarp

    sched_config = SwarpSchedulerConfig(num_nodes=args.nodes, num_cores=args.threads)
    bb_config = SwarpBurstBufferConfig(
                size_bb=args.bbsize,
                stage_input_dirs=[
                    SWARP_DIR + "/input"],
                stage_input_files=[],
                stage_output_dirs=[
                    SWARP_DIR + "/output"]
                )

    instance1core = SwarpInstance(script_dir=output_dir,
                                resample_config=resample_config, 
                                combine_config=combine_config, 
                                sched_config=sched_config, 
                                bb_config=bb_config)

    instance1core.write(file="run-swarp-scaling-bb.sh", manual_stage=True, overide=True)
    
    run1 = SwarpRun(pipelines=[1], number_avg=args.nb_run)

    if bb_config.size() < run1.num_pipelines() * SIZE_ONE_PIPELINE/1024.0:
        sys.stderr.write(" WARNING: Burst buffers allocation seems to be too small.\n")
        sys.stderr.write(" WARNING: Estimated size needed by {} pipelines -> {} GB (you asked for {} GB).\n".format(run1.num_pipelines(), run1.num_pipelines() * SIZE_ONE_PIPELINE/1024.0, bb_config.size()))

    run1.standalone(file="submit.sh", manual_stage=True, overide=True)
    
    os.chdir(old_path)
    sys.stderr.write(" === Switched back to initial directory {}\n".format(os.getcwd()))



    
