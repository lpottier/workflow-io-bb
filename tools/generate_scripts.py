#!/usr/bin/env python3

import os
import sys
import stat
import platform
import time
import subprocess as sb

class SwarpInstance:

    def __init__(self, standalone=True, num_nodes=1, num_cores=1, size_bb=50):
        self.standalone = standalone
        self.num_nodes = num_nodes
        self.num_cores = num_cores
        self.size_bb = size_bb

    def slurm_header(self):
        string = "#!/bin/bash -l\n"
        string += "#SBATCH -p debug\n"
        if self.standalone:
            string += "#SBATCH -N @NODES@\n"
        else:
            string += "#SBATCH -N {}\n".format(self.num_nodes)
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
        string = "#DW jobdw capacity={}GB access_mode=striped type=scratch\n".format(self.size_bb)
        if self.standalone:
            string += "#@STAGE@\n"
        else:
            string += "#DW stage_in source=/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/input destination=$DW_JOB_STRIPED/input/ type=directory\n"
            string += "#DW stage_in source=/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/config destination=$DW_JOB_STRIPED/config type=directory\n"
            string += "#DW stage_out source=$DW_JOB_STRIPED/output/  destination=/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/output type=directory\n"
        return string

    def script_modules(self):
        string = "module unload darshan\n"
        return string

    def script_globalvars(self):
        string = "set -x\n"
        string += "SWARP_DIR=workflow-io-bb/real-workflows/swarp\n"
        string += "LAUNCH=\"$SCRATCH/$SWARP_DIR/burst_buffers_scripts/sync_launch.sh\"\n"
        string += "EXE=$SCRATCH/$SWARP_DIR/bin/swarp\n"
        string += "export CONTROL_FILE=\"$SCRATCH/control_file.txt\"\n\n"

        string += "CORES_PER_PROCESS={}\n".format(self.num_cores)
        string += "CONFIG_DIR=$SCRATCH/$SWARP_DIR/config\n"
        string += "RESAMPLE_CONFIG=${CONFIG_DIR}/resample-orig.swarp\n"
        string += "COMBINE_CONFIG=${CONFIG_DIR}/combine-orig.swarp\n"

        string += "FILE_PATTERN='PTF201111*'\n"
        string += "IMAGE_PATTERN='PTF201111*.w.fits'\n"
        string += "RESAMPLE_PATTERN='PTF201111*.w.resamp.fits'\n"

        string += "echo \"NUM NODES ${SLURM_JOB_NUM_NODES}\"\n"
        string += "echo \"STAMP PREPARATION $(date --rfc-3339=ns)\"\n"
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
        return string

    def script_run_resample(self):
        string = "cd ${rundir}\n"
        string += "echo \"STAMP RESAMPLE PREP $(date --rfc-3339=ns)\"\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    echo \"Launching resample process ${process}\"\n"
        string += "    indir=\"$DW_JOB_STRIPED/input/${process}\" # This data has already been staged in\n"
        string += "    cd ${process}\n"
        string += "    srun \\ \n"
        string += "    -N 1 \\ \n"
        string += "    -n 1 \\ \n"
        string += "    -c ${CORES_PER_PROCESS} \\ \n"
        string += "    -o \"output.resample.%j.${process}\" \\ \n"
        string += "    -e \"error.resample.%j.${process}\" \\ \n"
        string += "    $LAUNCH $EXE -c $RESAMPLE_CONFIG ${indir}/${IMAGE_PATTERN} \n"
        string += "    cd ..\n"
        string += "done\n"
        string += "echo \"STAMP RESAMPLE $(date --rfc-3339=ns)\"\n"
        return string

    def script_copy_resample(self):
        string = "# Copy the stdout, stderr, SWarp XML files and IPM XML file\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    cp -n -v ${process}/{output*,error*,*.xml} ${outdir}/${process}\n"
        string += "done\n"
        return string

    def script_run_combine(self):
        string = "cd ${rundir}\n"
        string += "echo \"STAMP COMBINE PREP $(date --rfc-3339=ns)\"\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    echo \"Launching coadd process ${process}\"\n"
        string += "    indir=\"$DW_JOB_STRIPED/input/${process}\" # This data has already been staged in\n"
        string += "    cd ${process}\n"
        string += "    srun \\ \n"
        string += "    -N 1 \\ \n"
        string += "    -n 1 \\ \n"
        string += "    -c ${CORES_PER_PROCESS} \\ \n"
        string += "    -o \"output.coadd.%j.${process}\" \\ \n"
        string += "    -e \"error.coadd.%j.${process}\" \\ \n"
        string += "    $LAUNCH $EXE -c -c $COMBINE_CONFIG ${RESAMPLE_PATTERN} \n"
        string += "    cd ..\n"
        string += "done\n"
        string += "echo \"STAMP RESAMPLE $(date --rfc-3339=ns)\"\n"
        return string    

    def script_ending(self):
        string = "# Copy the stdout, stderr, SWarp XML files and IPM XML file\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    ls -lh ${rundir}/${process}/*.fits $DW_JOB_STRIPED/input/${process}/*.fits > ${outdir}/${process}/list_of_files.out\n"
        string += "    cp -n -v ${process}/{output*,error*,*.xml} ${outdir}/${process}\n"
        string += "done\n"
        string += "du -sh $DW_JOB_STRIPED/input ${rundir} > ${outdir}/disk_usage.out\n"


        string += "echo \"STAMP CLEANUP $(date --rfc-3339=ns)\"\n"
        string += "for process in $(seq 1 ${SLURM_JOB_NUM_NODES}); do\n"
        string += "    rm -v ${process}/*.fits\n"
        string += "done\n"
        string += "echo \"STAMP DONE $(date --rfc-3339=ns)\"\n"
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
            raise FileNotFoundError("File {} already exists.".format(file))
        with open(file, 'w') as f:
            f.write(SwarpInstance.bbinfo())
        os.chmod(file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user


    def write(self, file, overide=False):
        if not overide and os.path.exists(file):
            raise FileNotFoundError("File {} already exists.".format(file))

        if os.path.exists(file):
            print("File {} already exists and will be re-written.".format(file))

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
            print("File {} already exists and will be re-written.".format("bbinfo.sh"))
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
            raise FileNotFoundError("File {} already exists.".format(file))

        if os.path.exists(file):
            print("File {} already exists and will be re-written.".format(file))

        with open(file, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write("set -x\n")
            f.write("for i in {}; do\n".format(self.pipeline_to_str()))
            if platform.system() == "Darwin":
                f.write("    outdir=$(mktemp -d -t swarp-run-${i}N.XXXXXX)\n")
            else:
                f.write("    outdir=$(mktemp --directory --tmpdir=$(/bin/pwd) swarp-run-${i}N.XXXXXX)\n")
            f.write("    script=\"run-swarp-scaling-bb-${i}N.sh\"\n")
            f.write("    echo $outdir\n")
            f.write("    echo $script\n")
            f.write("    for j in $(seq ${i} -1 1); do\n")
            f.write("        stage_in=\"#DW stage_in source=/global/cscratch1/sd/lpottier/workflow-io-bb/real-workflows/swarp/input destination=\$DW_JOB_STRIPED/input/${j} type=directory\"\n")    
            f.write("        sed -i \"s|@STAGE@|@STAGE@\\n${stage_in}|\" ${outdir}/${script}\n")
            f.write("    done\n")
            f.write("    cp \"bbinfo.sh\" \"sync_launch.sh\" \"${outdir}\"\n")
            f.write("    cd \"${outdir}\"\n")
            f.write("    sbatch ${script}\n")
            f.write("    cd ..\n")
            f.write("done\n")

        os.chmod(file, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH) #make the script executable by the user


if __name__ == '__main__':
    print("=== Generate Slurm scripts for SWarp workflow")
    today = time.localtime()
    print("=== Executed: {}-{}-{} at {}:{}:{}.".format(today.tm_mday,
                                                    today.tm_mon, 
                                                    today.tm_year, 
                                                    today.tm_hour, 
                                                    today.tm_min, 
                                                    today.tm_sec)
                                                )
    print("=== Machine: {}".format(platform.platform()))

    instance1core = SwarpInstance()
    instance1core.write(file="run-swarp-scaling-bb.sh", overide=True)
    
    run1 = SwarpRun(pipelines=[1])
    run1.standalone(file="submit.sh", overide=True)
    
