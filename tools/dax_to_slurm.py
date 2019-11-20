#!/usr/bin/env python3

import os
import xml.etree.ElementTree as ET

class AbstractDag:
    def __init__(self, id_node, xml_data, father, childs):
        self._id = id_node             # the id of the node ID000000X
        self._xml_data = xml_data      # Element object describing the node
        self._father = father          # Pointer on childs
        self._childs = childs          # Pointer on childs

def adjency_list(original):
    #{'ID0000003': ['ID0000002'], 'ID0000004': ['ID0000003'], 'ID0000005': ['ID0000001', 'ID0000004'], 'ID0000006': ['ID0000001', 'ID0000003']}
    #reverse the parent list from child -> parent to parent -> child
    adjency_list = {}
    for child in original:
        adjency_list[child] = []
        for parent in original[child]:
            if parent in adjency_list:
                adjency_list[parent].append(child)
            else:
                adjency_list[parent] = [child]
    return adjency_list

# def build_adag_from_parentlist(hashmap, data_jobs):
#     def _rec(L):
#         for node in L:
#             new_node = AbstractDag(new_node, data_jobs[node], _rec(adjency_list[node], data_jobs) )
#         return new_node
#     return _rec(adjency_list(hashmap), data_jobs)

def build_adag_from_parentlist(hashmap, data_jobs):
    H = adjency_list(hashmap)
    a = set([y for x in hashmap.values() for y in x])
    b = set([x for x in hashmap])
    #find roots
    roots = list(a - (a & b)) # node which are parent but not child of any nodes
    root = AbstractDag("init", None, None, roots)
    seen = {}

    for node in roots:
        current = AbstractDag(node, data_jobs[node], root, adjency_list[node])
        for child in H[node]:
            if not child in seen:
                seen.add(child)
                # new_node = AbstractDag(new_node, data_jobs[node], _rec(adjency_list[node], data_jobs) )

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
            executables[elem.attrib["name"]] = elem
        if elem.tag == schema+"job":
            jobs[elem.attrib["id"]] = elem
        if elem.tag == schema+"child":
            parents[elem.attrib["ref"]] = []
            for p in elem:
                parents[elem.attrib["ref"]].append(p.attrib["ref"])

    print(executables)
    print(jobs)
    #print(parents)
    l = adjency_list(parents)
    print(l)

if __name__ == '__main__':
    print("Generate Slurm compatible workflow from DAX files")

    parse_dax_xml("dax.xml");
    
    #Extend the pegasus API instead of reparsing this?
