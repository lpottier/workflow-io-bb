#!/usr/bin/env python3

import os
import sys
import stat
import platform
import time
import tempfile
import subprocess as sb

from enum import Enum,unique,auto

SWARP_DIR = "/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/"

@unique
class TaskType(Enum):
    RESAMPLE = auto()
    COMBINE = auto()
    BOTH = auto()

class SwarpWorkflowConfig:

    def __init__(self, task_type, 
                    nthreads=1, 
                    resample_dir='.', 
                    vmem_max=31744, 
                    mem_max=31744, 
                    combine_bufsize=24576, 
                    existing_file=None
                ):
        self.task_type = task_type
        self.nthreads = nthreads
        self.resample_dir = resample_dir
        self.vmem_max = vmem_max
        self.mem_max = mem_max
        self.combine_bufsize = combine_bufsize
        self.existing_file = existing_file

        if not isinstance(self.task_type, TaskType):
            raise ValueError("Bad task type: must be a TaskType")

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
            print(" === workflow: file {} already exists and will be used.".format(self.existing_file))

        if os.path.exists(file):
            print(" === workflow: file {} already exists and will be re-written.".format(file))

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
    def __init__(self, size_bb, stage_input_dirs, stage_output_dirs, access_mode="striped", bbtype="scratch"):
        self.size_bb = size_bb
        self.stage_input_dirs = stage_input_dirs #List of input dirs
        self.stage_output_dirs = stage_output_dirs #List of output dirs (usually one)
        self.access_mode = access_mode
        self.bbtype = bbtype

    def size(self):
        return self.size_bb

    def input_dirs(self):
        return self.stage_input_dirs

    def output_dirs(self):
        return self.stage_output_dirs

    def mode(self):
        return self.access_mode

    def type(self):
        return self.bbtype

class SwarpInstance:
    def __init__(self, script_dir, resample_config, combine_config, sched_config, bb_config, standalone=True):
        self.standalone = standalone

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
        string += "#SBATCH -t 00:15:00\n"
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
        if self.standalone:
            string += "#@STAGE@\n"
        else:
            string += "#DW stage_in source=/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/input destination=$DW_JOB_STRIPED/input/ type=directory\n"
            string += "#DW stage_in source=/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/config destination=$DW_JOB_STRIPED/config type=directory\n"
            string += "#DW stage_out source=$DW_JOB_STRIPED/output/  destination=/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/output type=directory\n"
        string += "\n"
        return string

    def script_modules(self):
        string = "module unload darshan\n"
        string += "module load perftools-base perftools\n"
        string += "\n"
        return string

    def script_globalvars(self):
        string = "set -x\n"
        string += "SWARP_DIR=workflow-io-bb/real-workflows/swarp\n"
        string += "LAUNCH=\"$SCRATCH/$SWARP_DIR/{}/sync_launch.sh\"\n".format(self.script_dir)
        string += "EXE=$SCRATCH/$SWARP_DIR/bin/swarp\n"
        string += "export CONTROL_FILE=\"$SCRATCH/control_file.txt\"\n\n"

        string += "CORES_PER_PROCESS={}\n".format(self.sched_config.cores())
        string += "CONFIG_DIR=$SCRATCH/$SWARP_DIR/config\n"
        string += "RESAMPLE_CONFIG=${CONFIG_DIR}/resample-orig.swarp\n"
        string += "COMBINE_CONFIG=${CONFIG_DIR}/combine-orig.swarp\n"

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
        string +=  "sh bbinfo.sh\n"
        string +=  "rundir=$DW_JOB_STRIPED/swarp-run\n"
        string +=  "mkdir $rundir\n"
        string += "# Create a output and run directory for each SWarp process\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    mkdir -p ${rundir}/${process}\n"
        string += "    mkdir -p ${outdir}/${process}\n"
        string += "done\n"
        string += "\n"
        return string

    def script_run_resample(self):
        string = "cd ${rundir}\n"
        string += "echo \"STAMP RESAMPLE PREP $(date --rfc-3339=ns)\"\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    echo \"Launching resample process ${process}\"\n"
        string += "    indir=\"$DW_JOB_STRIPED/input/${process}\" # This data has already been staged in\n"
        string += "    cd ${process}\n"
        string += "    srun \\ \n"
        string += "        -N 1 \\ \n"
        string += "        -n 1 \\ \n"
        string += "        -c ${CORES_PER_PROCESS} \\ \n"
        string += "        -o \"output.resample.%j.${process}\" \\ \n"
        string += "        -e \"error.resample.%j.${process}\" \\ \n"
        string += "        $LAUNCH $EXE -c $RESAMPLE_CONFIG ${indir}/${IMAGE_PATTERN} &\n"
        string += "    cd ..\n"
        string += "done\n"
        string += "echo \"STAMP RESAMPLE $(date --rfc-3339=ns)\"\n"
        string += "\n"
        string += "wait\n"
        string += "\n"
        return string

    def script_copy_resample(self):
        string = "# Copy the stdout, stderr, SWarp XML files and IPM XML file\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    cp -n -v ${process}/{output*,error*,*.xml} ${outdir}/${process}\n"
        string += "done\n"
        string += "\n"
        return string

    def script_run_combine(self):
        string = "echo \"STAMP COMBINE PREP $(date --rfc-3339=ns)\"\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    echo \"Launching coadd process ${process}\"\n"
        string += "    cd ${process}\n"
        string += "    srun \\ \n"
        string += "    \t-N 1 \\ \n"
        string += "    \t-n 1 \\ \n"
        string += "    \t-c ${CORES_PER_PROCESS} \\ \n"
        string += "    \t-o \"output.coadd.%j.${process}\" \\ \n"
        string += "    \t-e \"error.coadd.%j.${process}\" \\ \n"
        string += "    \t$LAUNCH $EXE -c -c $COMBINE_CONFIG ${RESAMPLE_PATTERN} &\n"
        string += "    cd ..\n"
        string += "done\n"
        string += "\n"
        string += "wait\n"
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

        string += "echo \"STAMP CLEANUP $(date --rfc-3339=ns)\"\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    rm -v ${process}/*.fits\n"
        string += "done\n"
        string += "echo \"STAMP DONE $(date --rfc-3339=ns)\"\n"
        string += "\n"
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
    def write_bbinfo(file="bbinfo.sh", overide=False):
        if os.path.exists(file):
            raise FileNotFoundError("file {} already exists.".format(file))
        with open(file, 'w') as f:
            f.write(SwarpInstance.bbinfo())
        os.chmod(file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user

    @staticmethod
    def launch():
        string = "#!/bin/bash\n"
        string += "echo \"STAMP SYNC LAUNCH BEGIN $(date --rfc-3339=ns)\"\n"
        string += "exec \"$@\"\n"
        return string

    @staticmethod
    def write_launch(file="sync_launch.sh", overide=False):
        if os.path.exists(file):
            raise FileNotFoundError("file {} already exists.".format(file))
        with open(file, 'w') as f:
            f.write(SwarpInstance.launch())
        os.chmod(file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user

    def write(self, file, overide=False):
        if not overide and os.path.exists(file):
            raise FileNotFoundError("file {} already exists".format(file))

        if os.path.exists(file):
            print(" === SWarp script: file {} already exists and will be re-written.".format(file))

        with open(file, 'w') as f:
            f.write(self.slurm_header())
            f.write(self.dw_temporary())
            f.write(self.script_modules())
            f.write(self.script_globalvars())
            f.write(self.create_output_dirs())
            f.write(self.script_run_resample())
            f.write(self.script_copy_resample())
            f.write(self.script_run_combine())
            f.write(self.script_ending())

        os.chmod(file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user

        try:
            SwarpInstance.write_bbinfo(overide=overide)
        except FileNotFoundError:
            print(" === SWarp script: file {} already exists and will be re-written.".format("bbinfo.sh"))
            pass
        try:
            SwarpInstance.write_launch(overide=overide)
        except FileNotFoundError:
            print(" === SWarp script: file {} already exists and will be re-written.".format("bbinfo.sh"))
            pass

class SwarpRun:
    def __init__(self, pipelines=[1]):
        self.pipelines = pipelines
        self.num_pipelines = len(pipelines)

    def pipeline_to_str(self):
        res = ""
        for i in range(self.num_pipelines):
            if i > 0 and i < self.num_pipelines:
                res += "{} ".format(str(self.pipelines[i]))
            else:
                 res += "{}".format(str(self.pipelines[i]))
        return res


    def standalone(self, file, overide=False):
        if not overide and os.path.exists(file):
            raise FileNotFoundError("file {} already exists".format(file))

        if os.path.exists(file):
            print(" === Submit script: file {} already exists and will be re-written.".format(file))

        with open(file, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write("#set -x\n")
            f.write("for i in {}; do\n".format(self.pipeline_to_str()))
            if platform.system() == "Darwin":
                f.write("    outdir=$(mktemp -d -t swarp-run-${i}N.XXXXXX)\n")
            else:
                f.write("    outdir=$(mktemp --directory --tmpdir=$(/bin/pwd) swarp-run-${i}N.XXXXXX)\n")
            f.write("    script=\"run-swarp-scaling-bb-${i}N.sh\"\n")
            f.write("    echo $outdir\n")
            f.write("    echo $script\n")
            f.write("    sed \"s/@NODES@/${i}/\" \"run-swarp-scaling-bb.sh\" > ${outdir}/${script}\n")
            f.write("    for j in $(seq ${i} -1 1); do\n")
            f.write("       stage_in=\"#DW stage_in source=/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/input destination=\$DW_JOB_STRIPED/input/${j} type=directory\"\n")    
            f.write("       sed -i \"s|@STAGE@|@STAGE@\\n${stage_in}|\" ${outdir}/${script}\n")
            f.write("    done\n")
            f.write("    cp \"bbinfo.sh\" \"sync_launch.sh\" \"${outdir}\"\n")
            f.write("    cd \"${outdir}\"\n")
            f.write("    sbatch ${script}\n")
            f.write("    cd ..\n")
            f.write("done\n")

        os.chmod(file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user

if __name__ == '__main__':
    print(" === Generate Slurm scripts for SWarp workflow")
    today = time.localtime()
    print(" === Executed: {}-{}-{} at {}:{}:{}.".format(today.tm_mday,
                                                    today.tm_mon, 
                                                    today.tm_year, 
                                                    today.tm_hour, 
                                                    today.tm_min, 
                                                    today.tm_sec)
                                                )
    print(" === Machine: {}".format(platform.platform()))

    # tempfile.mkstemp(suffix=None, prefix=None, dir=None, text=False)
    if not os.path.exists("build"):
        os.mkdir("build")
        print(" === Directory {}/ created".format("build"))

    old_path = os.getcwd()
    os.chdir(old_path+"/build/")
    print(" === Current directory {}".format(os.getcwd()))

    resample_config = SwarpWorkflowConfig(task_type=TaskType.RESAMPLE, nthreads=1, resample_dir='.')
    resample_config.write(overide=True) #Write out the resample.swarp

    combine_config = SwarpWorkflowConfig(task_type=TaskType.COMBINE, nthreads=1, resample_dir='.')
    combine_config.write(overide=True) #Write out the combine.swarp

    sched_config = SwarpSchedulerConfig(num_nodes=1, num_cores=1)
    bb_config = SwarpBurstBufferConfig(
                size_bb=50, 
                stage_input_dirs=[
                    "/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/input", 
                    "/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/config"],
                stage_output_dirs=[
                    "/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/output"]
                )

    instance1core = SwarpInstance(script_dir="build",
                                resample_config=resample_config, 
                                combine_config=combine_config, 
                                sched_config=sched_config, 
                                bb_config=bb_config)

    instance1core.write(file="run-swarp-scaling-bb.sh", overide=True)
    
    run1 = SwarpRun(pipelines=[1])
    run1.standalone(file="submit.sh", overide=True)
    
    os.chdir(old_path)
    print(" === Switch to initial directory {}".format(os.getcwd()))



    