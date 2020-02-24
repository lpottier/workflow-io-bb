#!/usr/bin/env python3

import os
import pwd
import sys
import time
import stat
import math
import glob
import argparse
import importlib
import shlex # for shlex.split
import xml.etree.ElementTree as ET
from collections import deque
import networkx as nx
import networkx.drawing.nx_pydot as pydot

import matplotlib.pyplot as plt
limits = plt.axis('off')
options = {
    'node_color': 'black',
    'node_size': 100,
    'width': 2,
}

# For Haswell
#name: (max_node, max_walltime_hour,maxjob_submission, maxjob_execution)
nersc_queue = {
    "debug": (64,0.5,5,2), 
    "regular": (1932, 48, 5000,math.inf), 
    "interactive":(64,4,2,2),
    "premium":(5,1772,48,math.inf),
}

class Job:
    def __init__(self, xml_job, schema):
        self._raw_data = xml_job
        self._schema = schema
        self._id = None 
        self._name = None
        self._namespace = None
        self._node_label = None
        self._argument = ''
        self._input_files = []
        self._stdin = []
        self._output_files = []
        self._cores = None 
        self._runtime = None

        self.parse()

    def parse(self):
        if "id" in self._raw_data.attrib:
            self._id = self._raw_data.attrib["id"]
        if "name" in self._raw_data.attrib:
            self._name = self._raw_data.attrib["name"]
        if "namespace" in self._raw_data.attrib:
            self._namespace = self._raw_data.attrib["namespace"]
        if "node-label" in self._raw_data.attrib:
            self._node_label = self._raw_data.attrib["node-label"]

        for elem in self._raw_data:
            if elem.tag == self._schema+"argument":
                if elem.text:
                    self._argument = str(elem.text)
                for file in elem:
                    #parse files
                    if file.tag == self._schema+"file":
                        self._argument = self._argument + str(file.attrib["name"])

            if elem.tag == self._schema+"stdin":
                if elem.attrib["link"] == "input":
                    self._stdin.append(elem.attrib["name"])

            if elem.tag == self._schema+"uses":
                if elem.attrib["link"] == "input":
                    self._input_files.append(elem.attrib["name"])
                if elem.attrib["link"] == "output":
                    self._output_files.append(elem.attrib["name"])

            if elem.tag == self._schema+"profile":
                if elem.attrib["key"] == "cores":
                    self._cores = int(elem.text)
                if elem.attrib["key"] == "runtime":
                    self._runtime = float(elem.text)

class Executable:
    def __init__(self, xml_job, schema):
        self._raw_data = xml_job
        self._schema = schema
        self._name = None
        self._namespace = None
        self._arch = None
        self._os = None
        self._installed = None
        self._pfn = ''

        self.parse()

    def parse(self):
        if "name" in self._raw_data.attrib:
            self._namespace = self._raw_data.attrib["name"]
        if "namespace" in self._raw_data.attrib:
            self._name = self._raw_data.attrib["namespace"]
        if "arch" in self._raw_data.attrib:
            self._node_label = self._raw_data.attrib["arch"]
        if "os" in self._raw_data.attrib:
            self._node_label = self._raw_data.attrib["os"]
        if "installed" in self._raw_data.attrib:
            if self._raw_data.attrib["installed"] == "true":
                self._installed = True
            else:
                self._installed = False
        for elem in self._raw_data:
            if elem.tag == self._schema+"pfn":
                self._pfn = elem.attrib["url"]


class ResultParsing:
    def __init__(self, schema, executables, jobs, parents, dependencies):
        self._schema = schema
        self._executables = executables
        self._jobs = jobs
        self._childs_to_father = parents
        self._dependencies = dependencies


class AbstractDag(nx.DiGraph):
    """ docstring for AbstractDag """
    def __init__(self, dax, add_stagein=False, add_stageout=False):
        short = os.path.basename(dax).split('.')[0]
        super().__init__(id=short,name=dax)
        self._n = 0
        self._order = None

        # create element tree object 
        tree = ET.parse(dax) 
      
        # get root element 
        root = tree.getroot()
        schema = root.attrib["{http://www.w3.org/2001/XMLSchema-instance}schemaLocation"]
        schema = '{'+schema.split()[0]+'}'

        executables = {}
        jobs = {}
        parents = {}

        # Parse
        for elem in root:
            if elem.tag == schema+"executable":
                executables[elem.attrib["name"]] = Executable(elem, schema)
            if elem.tag == schema+"job":
                jobs[elem.attrib["id"]] = Job(elem, schema)
            if elem.tag == schema+"child":
                parents[elem.attrib["ref"]] = []
                for p in elem:
                    parents[elem.attrib["ref"]].append(p.attrib["ref"])

        #Reverse the dependency list
        adjacency_list = {}
        for child in parents:
            adjacency_list[child] = []
            for parent in parents[child]:
                if parent in adjacency_list:
                    adjacency_list[parent].append(child)
                else:
                    adjacency_list[parent] = [child]

        # Create the DAG
        # print(adjacency_list)

        for father,childs in adjacency_list.items():
            # print(father,childs,jobs[father]._name)
            
            name_exec = jobs[father]._name

            self.add_node(father,
                label=jobs[father]._node_label,
                args=jobs[father]._argument, 
                input=jobs[father]._input_files,
                stdin=jobs[father]._stdin,
                output=jobs[father]._output_files,
                exe=executables[name_exec]._pfn,
                cores=jobs[father]._cores,
                runtime=jobs[father]._runtime)

            for c in childs:
                self.add_node(c)
                self.add_edge(father, c)

        if add_stagein:
            roots = self.roots()

            # We compute the difference between file used as input and file produced as output
            # To stage in only initial input files
            all_input = [self.nodes[u]["input"] for u in self]
            flatten_input = []
            for x in all_input:
                flatten_input += x

            set_input = set(flatten_input)

            all_output = [self.nodes[u]["output"] for u in self]
            flatten_output = []
            for x in all_output:
                flatten_output += x

            set_output = set(flatten_output)
            set_final = list(set_input.difference(set_output))

            args_cmd = ''
            for x in set_final:
                args_cmd = args_cmd + x + ' '
            
            self.add_node("stagein",
                    label="stagein",
                    args=args_cmd,
                    input=set_final,
                    stdin=[],
                    output=[],
                    exe="stage-in.sh",
                    cores=1,
                    runtime=100
            )

            for r in roots:
                self.add_edge("stagein", r)

        if add_stageout:
            leafs = self.leafs()
            # TODO: avoid transfer all output files, maybe only the one with the transfer=True flag
            all_output = [self.nodes[u]["output"] for u in self]
            flatten_output = []
            for x in all_output:
                flatten_output += x

            set_output = list(set(flatten_output))
            args_cmd = ''
            for x in set_output:
                args_cmd = args_cmd + x + ' '

            self.add_node("stageout",
                    label="stageout",
                    args=args_cmd,
                    input=[],
                    stdin=[],
                    output=set_output,
                    exe="stage-out.sh",
                    cores=1,
                    runtime=100
            )

            for l in leafs:
                self.add_edge(l,"stageout")

    def __iter__(self):
        self._n = 0
        self._order = list(nx.topological_sort(self))
        return self

    def __next__(self):
        if self._n < len(self):
            val = self._order[self._n]
            self._n += 1
            return val
        else:
            raise StopIteration

    def roots(self):
        return [n for n,d in self.in_degree() if d == 0]

    def leafs(self):
        return [n for n,d in self.out_degree() if d == 0]

    def write(self, dot_file=None):
        A = nx.nx_agraph.to_agraph(self)
        if not dot_file:
            dot_file = self.graph["id"]+".dot"
        nx.nx_agraph.write_dot(self, dot_file)

    def draw(self, output_file=None):
        A = nx.nx_agraph.to_agraph(self)
        pos = nx.nx_pydot.graphviz_layout(self, prog='dot')
        nx.draw(G, pos=pos, with_labels=True)
        if not output_file:
            output_file = self.graph["id"]+".pdf"
        plt.savefig(output_file)



def slurm_sync(queue, max_jobs, nb_jobs, freq_sec):
    if max_jobs < nb_jobs:
        return ''
    s = "echo -n \"== waiting for a slot in the queue\"\n"
    s += "until (( $(squeue -p {} -u $(whoami) -o \"%A\" -h | wc -l) == {} )); do\n".format(queue, nb_jobs)
    s += "    sleep {}\n".format(freq_sec)
    s += "    echo -n \".\"\n"
    s += "done\n"
    s += "echo \"\"\n"
    s += "echo \"== {} queue contains {}/{} jobs, starting up to {} new jobs\"\n".format(queue,nb_jobs,max_jobs, max_jobs-nb_jobs)
    return s

# take as input a ADAG
def create_slurm_workflow(adag, output, bin_dir, input_dir, queue, wrapper=False):
    job_wrapper = [
        "#SBATCH -p {}".format(queue),
        "#SBATCH -C haswell",
        "#SBATCH -t 00:30:00",
        "#SBATCH -o output.%j",
        "#SBATCH -e error.%j",
        "#SBATCH --mail-user=lpottier@isi.edu",
        "#SBATCH --mail-type=FAIL"
    ]

    # burst_buffer = [
    #     "#DW jobdw capacity={}GB access_mode={} type={}",
    # ]

    if queue == "debug":
        walltime = "--time=00:30:00"
    else:
        walltime = "--time=1:00:00"

    dep = "--dependency=afterok:"
    prefix_cmd = "sbatch --parsable --export=OUTPUT_DIR=$OUTPUT_DIR,RUN_DIR=$RUN_DIR --mail-user=lpottier@isi.edu --mail-type=FAIL --constraint=haswell --nodes=1 --ntasks=1 {}".format(walltime)
    job_name = "--job-name="
    #opt = "--ntasks=1 --ntasks-per-core=1"

    USER = pwd.getpwuid(os.getuid())[0]

    if wrapper:
        sys.stderr.write(" === Generate wrappers for each job\n")
        for u in G:
            with open(u+".sh", 'w') as f:
                f.write("#!/bin/bash\n\n")
                for l in job_wrapper:
                    f.write(l+"\n")
                f.write("\n")
                f.write("#{} {}\n".format("creator", "%s@%s" % (USER, os.uname()[1])))
                f.write("#{} {}\n\n".format("created", time.ctime()))
                f.write("\n\n")
                f.write("OUTPUT_DIR=\"output-sns\"\n")
                f.write("mkdir -p $OUTPUT_DIR\n")
                f.write("RUN_DIR=\"$OUTPUT_DIR\"\n")
                f.write("mkdir -p $RUN_DIR\n")

                f.write("echo \"Task {}\"\n".format(u))
                input_args = G.nodes[u]["args"]
                if input_dir:
                    input_args = input_dir +'/' + G.nodes[u]["args"]

                if bin_dir:
                    bin_args = bin_dir+'/'+os.path.basename(G.nodes[u]["exe"])
                else:
                    bin_args = G.nodes[u]["exe"]

                cmd = "{} {}".format(bin_args, input_args)
                f.write("srun {}\n".format(cmd))
                f.write("\n")

            os.chmod(u+".sh", stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)
    else:
        if bin_dir:
            sys.stderr.write(" === Use wrapper from {}\n".format(bin_dir))
        else:
            sys.stderr.write(" === Use PFN defined in {}\n".format(adag["name"]))

    if input_dir:
        sys.stderr.write(" === Prefix all input files with dir \"{}\"\n".format(input_dir))
    else:
        sys.stderr.write(" === Use input files as defined in {}\n".format(adag["name"]))

    
    # all_input = [G.nodes[u]["input"] for u in G]
    # flatten_input = []
    # for x in all_input:
    #     flatten_input += x

    # set_input = set(flatten_input)

    with open(output, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write("#{} {}\n".format("creator", "%s@%s" % (USER, os.uname()[1])))
        f.write("#{} {}\n\n".format("created", time.ctime()))

        f.write("OUTPUT_DIR=\"output-sns-$(date \"+%M-%H_%d-%Y\")\"\n")
        f.write("mkdir -p $OUTPUT_DIR\n")
        f.write("RUN_DIR=\"$OUTPUT_DIR\"\n")
        f.write("mkdir -p $RUN_DIR\n\n")

        roots = G.roots()
        f.write("echo \"Number of jobs : {}\"\n".format(len(adag)))
        f.write("echo \"OUTPUT_DIR     : $OUTPUT_DIR\"\n")
        f.write("echo \"RUN_DIR        : $RUN_DIR\"\n")
        f.write("\n")


        max_job_sub = nersc_queue[queue][3]

        for i,u in enumerate(G):
            if i >= max_job_sub:
                # We have reach the max job submission, so we need to wait until we have a free slot
                # otherwise Slurm will kick us out
                # "max_job_sub-1 means here that we wait for one free slot available
                # "max_job_sub-max_job_sub=0 would mean that we wait the queue is empty before re-submitting jobs
                f.write(slurm_sync(queue, max_job_sub, max_job_sub-1, 10))
                f.write("\n")

            if wrapper:
                cmd = "{}.sh".format(u)
            else:
                if u == "stagein":
                    input_args = ' '
                    if input_dir:
                        for elem in shlex.split(G.nodes[u]["args"]) + G.nodes[u]["stdin"]:
                            if elem.startswith('--') or elem.startswith('-'):
                                input_args += elem + ' '
                            else:
                                input_args += input_dir + '/' + elem + ' '

                    if bin_dir:
                        bin_args = bin_dir+'/'+os.path.basename(G.nodes[u]["exe"])
                        input_bin_args = ' '
                        for e in glob.glob(bin_dir+'/*'):
                            input_bin_args = input_bin_args + e + ' '
                    else:
                        bin_args = G.nodes[u]["exe"]
                        # input_bin_args = ' '
                        # for e in glob.glob(bin_dir+'*'):
                        #     input_bin_args = input_bin_args + e + ' '
                            

                    cmd = "{} {} {}".format(bin_args, input_args, input_bin_args)
                    cmd = cmd + ' $RUN_DIR'
                else:
                    input_args = ' '
                    for file in G.nodes[u]["stdin"]:
                        input_args = input_args + file + ' '
                    
                    cmd = "{} {}".format(os.path.basename(G.nodes[u]["exe"]), G.nodes[u]["args"]+input_args)

            #TODO: handle stage out

            #TODO: add --bbf=filename or --bb="capacity=100gb" no file staging supported with --bb
            chdir = "--chdir=$RUN_DIR"

            if u == "stagein":
                chdir = ''
                
            if u in roots:
                f.write("{}=$({} {} --output=$OUTPUT_DIR/{}.%j.output --error=$OUTPUT_DIR/{}.%j.error --job-name={} {})\n".format(u, prefix_cmd, chdir, adag.graph["id"]+"-"+u, adag.graph["id"]+"-"+u, adag.graph["id"]+"-"+u, cmd))
            else:
                pred = ''
                for v in G.pred[u]:
                    if pred == '':
                        pred = pred + "${}".format(v)
                    else:
                        pred = pred + ":${}".format(v)

                f.write("{}=$({} {} --output=$OUTPUT_DIR/{}.%j.output --error=$OUTPUT_DIR/{}.%j.error --job-name={} --dependency=afterok:{} {})\n".format(u, prefix_cmd, chdir, adag.graph["id"]+"-"+u, adag.graph["id"]+"-"+u, adag.graph["id"]+"-"+u, pred, cmd))

            f.write("echo \"={}= Job ${} scheduled on queue {} at $(date --rfc-3339=ns)\"\n\n".format(i+1,u,queue))

    os.chmod(output, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate Slurm compatible workflow from DAX files')
    
    parser.add_argument('--dax', '-d', type=str, nargs='?',
                        help='DAX file')
    parser.add_argument('--scheduler', '-s', type=str, nargs='?', default="slurm",
                        help='Scheduler (slurm or lsf (NOT SUPPORTED YET))')

    parser.add_argument('--bin', '-b', type=str, nargs='?', default="bin",
                        help='Bin directory  (replace the \"bin\"" directory provided by the DAX)')

    parser.add_argument('--input', '-i', type=str, nargs='?', default="input",
                        help='Input directory (replace the \"input\"" directory provided by the DAX)')

    parser.add_argument('--queue', '-q', type=str, nargs='?', default="debug",
                        help='Queue to execute the workflow: debug, interactive, regular, premium')

    args = parser.parse_args()
    dax_id = os.path.basename(args.dax).split('.')[0]

    submit_script = "submit.sh"

    if args.queue not in nersc_queue:
        sys.stderr.write(" === Unknown submission queue -> use \"debug\" queue\n")
        args.queue = "debug"

    sys.stderr.write(" === Generate compatible workflow for HPC schedulers from DAX files\n")
    today = time.localtime()
    sys.stderr.write(" === Executed: {}-{}-{} at {}:{}:{}\n".format(today.tm_mday,
                                                    today.tm_mon, 
                                                    today.tm_year, 
                                                    today.tm_hour, 
                                                    today.tm_min, 
                                                    today.tm_sec)
                                                )
    sys.stderr.write(" === DAX file             : {}\n".format(args.dax))
    sys.stderr.write("     Scheduler            : {}\n".format(args.scheduler))
    sys.stderr.write("     Bin directory        : {}\n".format(args.bin))
    sys.stderr.write("     Input directory      : {}\n".format(args.input))
    sys.stderr.write("     Queue                : {}\n".format(args.queue))
    sys.stderr.write("     Job submission limit : {}\n".format(nersc_queue[args.queue][3]))


    # output_dir = "{}-{}/".format(dax_id, args.scheduler)
    # if not os.path.exists(output_dir):
    #     os.mkdir(output_dir)
    #     sys.stderr.write(" === Directory {} created\n".format(output_dir))

    G = AbstractDag(args.dax, add_stagein=True)

    # old_path = os.getcwd()+'/'
    # os.chdir(old_path+output_dir)
    # sys.stderr.write(" === Current directory {}\n".format(os.getcwd()))


    create_slurm_workflow(
        adag=G, 
        output=submit_script, 
        bin_dir=args.bin, 
        input_dir=args.input,
        queue=args.queue)

    sys.stderr.write(" === {} written in current directory {}\n".format(submit_script,os.getcwd()))

    # print(G.graph, len(G))

    # for u in G:
    #     print(u, G[u], G.nodes[u])
    # print(G.roots()))

    graphviz_found = importlib.util.find_spec('pygraphviz')

    if graphviz_found is not None:
        G.write()
        G.draw()
    
    #nx.draw(G, pos=nx.spring_layout(G), with_labels=True)

    # pos = nx.nx_agraph.graphviz_layout(G)
    # nx.draw(G, pos=pos)
    # plt.savefig("dag.pdf")
    # write_dot(G, 'file.dot')

    #TODO: Extend the pegasus API instead of reparsing this?

    # os.chdir(old_path)
    # sys.stderr.write(" === Switched back to initial directory {}\n".format(os.getcwd()))



