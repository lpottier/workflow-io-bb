#!/usr/bin/env python3

import os
import pwd
import sys
import time
import stat
import argparse
import importlib
import xml.etree.ElementTree as ET
from collections import deque
import networkx as nx
import networkx.drawing.nx_pydot as pydot

import matplotlib.pyplot as plt
limits = plt.axis('off')
# options = {
#     'node_color': 'black',
#     'node_size': 100,
#     'width': 2,
# }

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
    def __init__(self, dax):
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

        #print(root.tag, schema)

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
        for father,childs in adjacency_list.items():
            # print(father,childs,jobs[father]._name)
            
            name_exec = jobs[father]._name

            self.add_node(father,
                label=jobs[father]._node_label,
                args=jobs[father]._argument, 
                input=jobs[father]._input_files,
                output=jobs[father]._output_files,
                exe=executables[name_exec]._pfn,
                cores=jobs[father]._cores,
                runtime=jobs[father]._runtime)

            for c in childs:
                self.add_node(c)
                self.add_edge(father, c)

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

# def _adjacency_list(self):
#     #{'ID0000003': ['ID0000002'], 'ID0000004': ['ID0000003'], 'ID0000005': ['ID0000001', 'ID0000004'], 'ID0000006': ['ID0000001', 'ID0000003']}
#     #reverse the parent list from child -> parent to parent -> child
#     res = {}
#     for child in original:
#         res[child] = []
#         for parent in original[child]:
#             if parent in res:
#                 res[parent].append(child)
#             else:
#                 res[parent] = [child]
#     return res

# def build_adag_from_parentlist(hashmap, data_jobs):
#     def _rec(L):
#         for node in L:
#             new_node = AbstractDag(new_node, data_jobs[node], _rec(adjency_list[node], data_jobs) )
#         return new_node
#     return _rec(adjency_list(hashmap), data_jobs)

def build_adag_from_parentlist(hashmap, data_jobs):
    H = adjacency_list(hashmap)
    a = set([y for x in hashmap.values() for y in x])
    b = set([x for x in hashmap])
    #find roots
    roots = list(a - (a & b)) # node which are parent but not child of any nodes
    # root = AbstractDag("init", None, None, roots)
    seen = {}

    # for node in roots:
    #     current = AbstractDag(node, data_jobs[node], root, adjency_list[node])
    #     for child in H[node]:
    #         if not child in seen:
    #             seen.add(child)
    #             # new_node = AbstractDag(new_node, data_jobs[node], _rec(adjency_list[node], data_jobs) )

# class AbstractWorkflow:
#     def __init__(self, dax_file, scheduler):
#         self.executable = {}
#         self.jobs = {}
#         self.scheduler = scheduler

#         if self.scheduler == "slurm":
#             self.job_wrapper = "#SBATCH -p debug \
#                             #SBATCH -C haswell \
#                             #SBATCH -t 00:20:00 \
#                             #SBATCH -J swarp-bb \
#                             #SBATCH -o output.%j \
#                             #SBATCH -e error.%j \
#                             #SBATCH --mail-user=lpottier@isi.edu \
#                             #SBATCH --mail-type=FAIL \
#                             #SBATCH --export=ALL"

#             self.dependency = "--dependency=afterok:"
#         elif self.scheduler == "lsf":
#             #TODO
#             self.job_wrapper = ""
#             pass
#         else:
#             raise ValueError("{} is not a supported scheduler.".format(self.scheduler))


#         #self.scheduler_interface = {}
#         #either Slurm or BSUB
#         # run -> srun/jsrun
#         #pragma -> SBATCH/BSUB
#         #alloc_bb -> #DW ... /#BSUB -alloc "nvme"
#         # if self.scheduler == "slurm":
#         #     self.scheduler_interface["exec"] = "srun"
#         #     self.scheduler_interface["exec"] = "srun"
#         #     self.scheduler_interface["alloc_bb"] = "#DW jobdw capacity=50GB access_mode=striped type=scratch"
#         #     self.scheduler_interface["queue"] =  


# def parse_dax_xml(dax_xml_file):
#     # create element tree object 
#     tree = ET.parse(dax_xml_file) 
  
#     # get root element 
#     root = tree.getroot()
#     schema = root.attrib["{http://www.w3.org/2001/XMLSchema-instance}schemaLocation"]
#     schema = '{'+schema.split()[0]+'}'

#     executables = {}
#     jobs = {}
#     parents = {}

#     print(root.tag, schema)

#     for elem in root:
#         if elem.tag == schema+"executable":
#             executables[elem.attrib["name"]] = Executable(elem, schema)
#         if elem.tag == schema+"job":
#             jobs[elem.attrib["id"]] = Job(elem, schema)
#         if elem.tag == schema+"child":
#             parents[elem.attrib["ref"]] = []
#             for p in elem:
#                 parents[elem.attrib["ref"]].append(p.attrib["ref"])

#     parsing_result = ResultParsing(schema, executables, jobs, parents, adjacency_list(parents))
#     H = adjacency_list(parsing_result._childs_to_father)
#     a = set([y for x in parsing_result._childs_to_father.values() for y in x])
#     b = set([x for x in parsing_result._childs_to_father])
#     #find roots
#     roots = list(a - (a & b)) # node which are parent but not child of any nodes

#     return AbstractDag(H)


# TODO:
# echo -n "waiting for an empty queue"
# until (( $(squeue -p $QUEUE -u $WHO -o "%A" -h | wc -l) == 0  )); do
#     sleep $SEC
#     echo -n "."
# done
# echo " $QUEUE queue is empty, start new batch of jobs"

def slurm_sync(queue, max_jobs, nb_jobs, freq_sec):
    if max_jobs < nb_jobs:
        return ''
    s = "echo -n \"waiting for an empty queue\"\n"
    s += "until (( $(squeue -p {} -u $(whoami) -o \"%A\" -h | wc -l) == {} )); do\n".format(queue, nb_jobs)
    s += "    sleep {}\n".format(freq_sec)
    s += "    echo -n \".\"\n"
    s += "done\n"
    s += "echo \" {} queue contains {}/{} jobs, starting up to {} new jobs\"\n".format(queue,nb_jobs,max_jobs, max_jobs-nb_jobs)
    return s

# take as input a ADAG
def create_slurm_workflow(adag, output, queue=("debug",5)):
    job_wrapper = [
        "#SBATCH -p debug".format(queue[0]),
        "#SBATCH -C haswell",
        "#SBATCH -t 00:20:00",
        "#SBATCH -o output.%j",
        "#SBATCH -e error.%j",
        "#SBATCH --mail-user=lpottier@isi.edu",
        "#SBATCH --mail-type=FAIL",
        "#SBATCH --export=ALL"
    ]

    # burst_buffer = [
    #     "#DW jobdw capacity={}GB access_mode={} type={}",
    # ]

    dep = "--dependency=afterok:"
    cmd = "sbatch --parsable"
    job_name = "--job-name="
    #opt = "--ntasks=1 --ntasks-per-core=1"

    USER = pwd.getpwuid(os.getuid())[0]

    for u in G:
        with open(u+".sh", 'w') as f:
            f.write("#!/bin/bash\n\n")
            for l in job_wrapper:
                f.write(l+"\n")
            f.write("\n")
            f.write("#{} {}\n".format("creator", "%s@%s" % (USER, os.uname()[1])))
            f.write("#{} {}\n\n".format("created", time.ctime()))

            f.write("echo \"Task {}\"\n".format(u))
            f.write("srun {}\n".format("hostname"))
            f.write(l+"\n")

        os.chmod(u+".sh", stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

    with open(output, 'w') as f:
        f.write("#!/bin/bash\n\n")
        f.write("#{} {}\n".format("creator", "%s@%s" % (USER, os.uname()[1])))
        f.write("#{} {}\n\n".format("created", time.ctime()))

        #jid2=$(sbatch --dependency=afterany:$jid1  job2.sh)
        roots = G.roots()
        f.write("echo \"Number of jobs: {}\"\n\n".format(len(adag)))
        for i,u in enumerate(G):
            if i >= queue[1]:
                #We have reach the max job submission
                # queue[1]-1 means here that we wait for one free slot available
                # queue[1]-queue[1] would mean that we wait the queue is empty before re-submitting jobs
                f.write(slurm_sync(queue[0], queue[1], queue[1]-1, 10))
                f.write("\n")
            #cmd = "{} {}".format(os.basemame(G.nodes[u]["exe"]), G.nodes[u]["args"])
            cmd = "{}.sh".format(u)

            if u in roots:
                f.write("{}=$(sbatch --parsable --job-name={} {})\n".format(u, adag.graph["id"]+"-"+u, cmd))
            else:
                pred = ''
                for v in G.pred[u]:
                    if pred == '':
                        pred = pred + "${}".format(v)
                    else:
                        pred = pred + ":${}".format(v)

                f.write("{}=$(sbatch --parsable --job-name={} --dependency=afterok:{} {})\n".format(u, adag.graph["id"]+"-"+u, pred, cmd))

            f.write("echo \"== Job ${} scheduled on queue {} at $(date --rfc-3339=ns)\"\n\n".format(u,queue[0]))

    os.chmod(output, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate Slurm/LSF compatible workflow from DAX files')
    
    parser.add_argument('--dax', '-d', type=str, nargs='?',
                        help='DAX file')
    parser.add_argument('--scheduler', '-s', type=str, nargs='?', default="slurm",
                        help='Scheduler (slurm or lsf)')

    args = parser.parse_args()
    dax_id = os.path.basename(args.dax).split('.')[0]

    sys.stderr.write(" === Generate Slurm compatible workflow from DAX files\n")
    today = time.localtime()
    sys.stderr.write(" === Executed: {}-{}-{} at {}:{}:{}\n".format(today.tm_mday,
                                                    today.tm_mon, 
                                                    today.tm_year, 
                                                    today.tm_hour, 
                                                    today.tm_min, 
                                                    today.tm_sec)
                                                )
    sys.stderr.write(" === DAX file  : {}\n".format(args.dax))
    sys.stderr.write("     Scheduler : {}\n".format(args.scheduler))


    output_dir = "{}-{}/".format(dax_id, args.scheduler)
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
        sys.stderr.write(" === Directory {} created\n".format(output_dir))

    #G = parse_dax_xml("dax.xml")
    G = AbstractDag(args.dax)

    old_path = os.getcwd()+'/'
    os.chdir(old_path+output_dir)
    sys.stderr.write(" === Current directory {}\n".format(os.getcwd()))


    create_slurm_workflow(G, "submit.sh")

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

    os.chdir(old_path)
    sys.stderr.write(" === Switched back to initial directory {}\n".format(os.getcwd()))



