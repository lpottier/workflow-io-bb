#!/usr/bin/env python3

import os
import xml.etree.ElementTree as ET
from collections import deque
import networkx as nx
# import matplotlib.pyplot as plt

class AbstractDag(nx.DiGraph):
    """ docstring for AbstractDag """
    def __init__(self, adjacency_list):
        super(AbstractDag, self).__init__()
        
        for father,childs in adjacency_list.items():
            self.add_node(father)
            for c in childs:
                self.add_node(c)
                self.add_edge(father, c)

def adjacency_list(original):
    #{'ID0000003': ['ID0000002'], 'ID0000004': ['ID0000003'], 'ID0000005': ['ID0000001', 'ID0000004'], 'ID0000006': ['ID0000001', 'ID0000003']}
    #reverse the parent list from child -> parent to parent -> child
    res = {}
    for child in original:
        res[child] = []
        for parent in original[child]:
            if parent in res:
                res[parent].append(child)
            else:
                res[parent] = [child]
    return res

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

class AbstractWorkflow:
    def __init__(self, dax_file, scheduler):
        self.executable = {}
        self.jobs = {}
        self.scheduler = scheduler

        if self.scheduler == "slurm":
            self.job_wrapper = "#SBATCH -p debug \
                            #SBATCH -C haswell \
                            #SBATCH -t 00:20:00 \
                            #SBATCH -J swarp-bb \
                            #SBATCH -o output.%j \
                            #SBATCH -e error.%j \
                            #SBATCH --mail-user=lpottier@isi.edu \
                            #SBATCH --mail-type=FAIL \
                            #SBATCH --export=ALL"

            self.dependency = "--dependency=afterok:"
        elif self.scheduler == "lsf":
            #TODO
            self.job_wrapper = ""
            pass
        else:
            raise ValueError("{} is not a supported scheduler.".format(self.scheduler))


        #self.scheduler_interface = {}
        #either Slurm or BSUB
        # run -> srun/jsrun
        #pragma -> SBATCH/BSUB
        #alloc_bb -> #DW ... /#BSUB -alloc "nvme"
        # if self.scheduler == "slurm":
        #     self.scheduler_interface["exec"] = "srun"
        #     self.scheduler_interface["exec"] = "srun"
        #     self.scheduler_interface["alloc_bb"] = "#DW jobdw capacity=50GB access_mode=striped type=scratch"
        #     self.scheduler_interface["queue"] =  

class ResultParsing:
    def __init__(self, schema, executables, jobs, parents, dependencies):
        self._schema = schema
        self._executables = executables
        self._jobs = jobs
        self._childs_to_father = parents
        self._dependencies = dependencies

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

        self.parse()

    def parse(self):
        if "id" in self._raw_data.attrib:
            self._id = self._raw_data.attrib["id"]
        if "name" in self._raw_data.attrib:
            self._namespace = self._raw_data.attrib["name"]
        if "namespace" in self._raw_data.attrib:
            self._name = self._raw_data.attrib["namespace"]
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

def parse_dax_xml(dax_xml_file):
    # create element tree object 
    tree = ET.parse(dax_xml_file) 
  
    # get root element 
    root = tree.getroot()
    schema = root.attrib["{http://www.w3.org/2001/XMLSchema-instance}schemaLocation"]
    schema = '{'+schema.split()[0]+'}'

    executables = {}
    jobs = {}
    parents = {}

    print(root.tag, schema)

    for elem in root:
        if elem.tag == schema+"executable":
            executables[elem.attrib["name"]] = Executable(elem, schema)
        if elem.tag == schema+"job":
            jobs[elem.attrib["id"]] = Job(elem, schema)
        if elem.tag == schema+"child":
            parents[elem.attrib["ref"]] = []
            for p in elem:
                parents[elem.attrib["ref"]].append(p.attrib["ref"])

    return ResultParsing(schema, executables, jobs, parents, adjacency_list(parents))

# take as input a ResultParsing
def create_slurm_workflow(parsing_result):
    job_wrapper = "#SBATCH -p debug \
                #SBATCH -C haswell \
                #SBATCH -t 00:20:00 \
                #SBATCH -J swarp-bb \
                #SBATCH -o output.%j \
                #SBATCH -e error.%j \
                #SBATCH --mail-user=lpottier@isi.edu \
                #SBATCH --mail-type=FAIL \
                #SBATCH --export=ALL"

    dep = "--dependency=afterok:"

    H = adjacency_list(parsing_result._childs_to_father)
    a = set([y for x in parsing_result._childs_to_father.values() for y in x])
    b = set([x for x in parsing_result._childs_to_father])
    #find roots
    roots = list(a - (a & b)) # node which are parent but not child of any nodes

    for j in roots:
        print(parsing_result._jobs[j]._id)
        for child in parsing_result._dependencies[j]:
            print("\t" + parsing_result._jobs[child]._id)
            for grand_child in parsing_result._dependencies[child]:
                print("\t\t" + parsing_result._jobs[grand_child]._id)

    steps = deque()
    for i,(father,child) in enumerate(parsing_result._dependencies.items()):
        print(i,father,child)


    dag = AbstractDag(H)

    return dag



if __name__ == '__main__':
    print("Generate Slurm compatible workflow from DAX files")

    result = parse_dax_xml("dax.xml")
    G = create_slurm_workflow(result)
    
    #Extend the pegasus API instead of reparsing this?
