#!/usr/bin/env python3

import statistics
import os
import csv
import re
import xml.etree.ElementTree as xml

from enum import Enum,unique,auto
from pathlib import Path
from collections import OrderedDict

XML_PREFIX="{http://pegasus.isi.edu/schema/invocation}"

# import importlib
# seaborn_found = importlib.util.find_spec('seaborn')
# import seaborn as sns
# import pandas as pd
# import matplotlib.pyplot as plt
# sns.set(style="ticks", color_codes=True)

@unique
class FileType(Enum):
    XML = auto()
    YAML = auto()

def check_multiple_xml_file(xml_file, sep = r"(<\?xml[^>]+\?>)"):
    with open(xml_file, 'r') as f:
        matches = re.findall(sep, f.read())
        if len(matches) > 1:
            return True
    return False


def split_multiple_xml_file(xml_file, sep = r"(<\?xml[^>]+\?>)"):
    with open(xml_file, 'r') as f:
        data = re.split(sep, f.read())

    i = 1
    preambule = ''
    written_files = []
    for d in data:
        if d == '':
            continue
        m = re.search(sep, d)
        if m:
            preambule = d # we found the <?xml version="1.0" encoding="UTF-8"?>
        else:
            new_file = str(xml_file)+'.workflow'+str(i)
            with open(new_file, 'w') as f:
                f.write(preambule+d)
            written_files.append(new_file)
            i += 1

    return written_files

class Machine:
    """
    Machine statistics from KickStart record:

        <machine page-size="4096">
          <stamp>2019-10-24T11:00:58.701-07:00</stamp>
          <uname system="linux" nodename="nid00691" 
          release="4.12.14-25.22_5.0.79-cray_ari_c" machine="x86_64">
            #1 SMP Fri Aug 9 16:20:09 UTC 2019 (d32c384)
          </uname>
          <linux>
            <ram total="131895024" free="128941776" shared="419248" buffer="9364"/>
            <swap total="0" free="0"/>
            <boot idle="47413210.140">2019-10-11T21:04:33.772-07:00</boot>
            <cpu count="64" speed="2301" vendor="GenuineIntel">
                Intel(R) Xeon(R) CPU E5-2698 v3 @ 2.30GHz
            </cpu>
            <load min1="0.29" min5="9.62" min15="19.75"/>
            <procs total="818" running="1" sleeping="817" vmsize="9492916" rss="390460"/>
            <task total="891" running="1" sleeping="890"/>
          </linux>
        </machine>

    """
    def __init__(self, machine_element_tree):

        if machine_element_tree.tag != XML_PREFIX+"machine":
            raise ValueError("Incompatible ElementTree {}".format(element_tree.tag))

        for elem in machine_element_tree:
            if elem.tag == XML_PREFIX+"uname":
                self.system     =       elem.attrib["system"]
                self.nodename   =       elem.attrib["nodename"]
                self.release    =       elem.attrib["release"]
                self.machine    =       elem.attrib["machine"]
                break

        self.ram = None
        self.swap = None
        self.cpu = None
        self.load = None
        self.procs = None
        self.task = None

        for elem in machine_element_tree:
            if elem.tag == XML_PREFIX+self.system:
                for subelem in elem:
                    if subelem.tag == XML_PREFIX+"ram":
                        self.ram = subelem.attrib
                    if subelem.tag == XML_PREFIX+"swap":
                        self.swap = subelem.attrib
                    if subelem.tag == XML_PREFIX+"cpu":
                        self.cpu = subelem.attrib
                    if subelem.tag == XML_PREFIX+"load":
                        self.load = subelem.attrib
                    if subelem.tag == XML_PREFIX+"procs":
                        self.procs = subelem.attrib
                    if subelem.tag == XML_PREFIX+"task":
                        self.task = subelem.attrib

    def __str__(self):
        s = ''
        for name,val in vars(self).items():
            if val == {}:
                val = "Not found"
            s = s + '{:<10} -> {:<72}\n'.format(name, str(val))
        return s[:-2] # To remove the last \n

    def __repr__(self):
        s = ''
        for name,val in vars(self).items():
            if val == {}:
                val = "Not found"
            s = s + '{:<10} -> {:<72}\n'.format(name, str(val))
        return s[:-2] # To remove the last \n

class File:
    """
    File statistics from KickStart record:

        <file name="/dev/null" 
        bread="67154752" nread="55" bwrite="0" nwrite="0" 
        bseek="0" nseek="0" size="33583680"/>

    """
    def __init__(self, hashmap):
        # Hashmap must be the attrib from <file> elem
        self.name       =       hashmap["name"]
        self.size       =       hashmap["size"]
        self.bread      =       hashmap["bread"]
        self.nread      =       hashmap["nread"]
        self.bwrite     =       hashmap["bwrite"]
        self.nwrite     =       hashmap["nwrite"]
        self.bseek      =       hashmap["bseek"]
        self.nseek      =       hashmap["nseek"]

    def __str__(self):
        s = ''
        for name,val in vars(self).items():
            s = s + '{:<8} -> {:>10}\n'.format(name, str(val))
        return s[:-2] # To remove the last \n

    def __repr__(self):
        s = ''
        for name,val in vars(self).items():
            s = s + '{:<8} -> {:>10}\n'.format(name, str(val))
        return s[:-2] # To remove the last \n


class Processus:
    """
    Job statistics from KickStart record

    <proc ppid="14774" pid="14775" exe="/bin/bash" start="1571940058.795641" 
        stop="1571940058.796241" utime="0.000" stime="0.000" iowait="0.000" 
        finthreads="1" maxthreads="0" totthreads="0" vmpeak="12144" rsspeak="2664" 
        rchar="0" wchar="100" rbytes="0" wbytes="0" cwbytes="0" syscr="0" syscw="3">

        <file name="/dev/null" bread="0" nread="0" bwrite="0" nwrite="0" bseek="0" nseek="0" size="0"/>
        <file name="/tmp/ks.out.fRUVe3" bread="0" nread="0" bwrite="0" nwrite="0" bseek="0" nseek="0" size="0"/>
        <file name="/tmp/ks.err.vP02l5" bread="0" nread="0" bwrite="0" nwrite="0" bseek="0" nseek="0" size="0"/>
        <file name="pipe:[8212470]" bread="0" nread="0" bwrite="100" nwrite="3" bseek="0" nseek="0" size="0"/>
        <file name="/dev/kgni0" bread="0" nread="0" bwrite="0" nwrite="0" bseek="0" nseek="0" size="0"/>
    </proc>

    """
    def __init__(self, xml_root):

        if xml_root.tag != XML_PREFIX+"proc":
            raise ValueError("Not a {} ElementTree {}".format("proc",element_tree.tag))
       
        self.pid        =       xml_root.attrib["pid"]
        self.ppid       =       xml_root.attrib["ppid"]

        # Hashmap must be the attrib from <proc> elem
        self.exe        =       xml_root.attrib["exe"]
        self.start      =       xml_root.attrib["start"]
        self.stop       =       xml_root.attrib["stop"]
        self.utime      =       xml_root.attrib["utime"]
        self.stime      =       xml_root.attrib["stime"]
        self.iowait     =       xml_root.attrib["iowait"]
        self.finthreads =       xml_root.attrib["finthreads"]
        self.maxthreads =       xml_root.attrib["maxthreads"]
        self.totthreads =       xml_root.attrib["totthreads"]
        self.vmpeak     =       xml_root.attrib["vmpeak"]
        self.rsspeak    =       xml_root.attrib["rsspeak"]
        self.rchar      =       xml_root.attrib["rchar"]
        self.wchar      =       xml_root.attrib["wchar"]
        self.rbytes     =       xml_root.attrib["rbytes"]
        self.wbytes     =       xml_root.attrib["wbytes"]
        self.cwbytes    =       xml_root.attrib["cwbytes"]
        self.syscr      =       xml_root.attrib["syscr"]
        self.syscw      =       xml_root.attrib["syscw"]

        self.files = {} #key is name and value a File object
        self.parse_files(xml_root)

    def parse_files(self, xml_root):
        for file in xml_root:
            idx = file.attrib["name"]
            self.files[idx] = File(file.attrib)

    def __str__(self):
        s = ''
        for name,val in vars(self).items():
            if name == "files":
                continue
            s = s + '{:<10} -> {:<10}\n'.format(name, str(val))
        return s[:-2] # To remove the last \n

    def __repr__(self):
        s = ''
        for name,val in vars(self).items():
            if name == "files":
                continue
            s = s + '{:<10} -> {:<10}\n'.format(name, str(val))
        return s[:-2] # To remove the last \n

class Usage:
    """
    Usage statistics from KickStart record 
    """
    def __init__(self, xml_tree):
        # Hashmap must be the attrib from <mainjob> or the real root elem
        # Must be the parent of <usage> and <statcall>
        usage_tree = None
        for elem in xml_tree:
            if elem.tag == XML_PREFIX+"usage":
                usage_tree = elem
                break

        self.utime      =       usage_tree.attrib["utime"]
        self.stime      =       usage_tree.attrib["stime"]
        self.maxrss     =       usage_tree.attrib["maxrss"]
        self.minflt     =       usage_tree.attrib["minflt"]
        self.majflt     =       usage_tree.attrib["majflt"]
        self.nswap      =       usage_tree.attrib["nswap"]
        self.inblock    =       usage_tree.attrib["inblock"]
        self.outblock   =       usage_tree.attrib["outblock"]
        self.msgsnd     =       usage_tree.attrib["msgsnd"]
        self.msgrcv     =       usage_tree.attrib["msgrcv"]
        self.nsignals   =       usage_tree.attrib["nsignals"] 
        self.nvcsw      =       usage_tree.attrib["nvcsw"]
        self.nivcsw     =       usage_tree.attrib["nivcsw"]

        # Data from statcall: stdin, stdout, stderr and metadata
        self.data = {}
        for elem in xml_tree:
            if elem.tag == XML_PREFIX+"statcall" and "id" in elem.attrib:
                self.data[elem.attrib["id"]] = None
                for data in elem:
                    if data.tag == XML_PREFIX+"data":
                        self.data[elem.attrib["id"]] = data.text

    def __str__(self):
        s = ''
        short_data = {}
        for u,v in self.data.items():
            if v:
                short_data[u] = len(v)
            else:
                short_data[u] = v
        for name,val in vars(self).items():
            if type(val) is dict:
                s = s + '{:<10} -> {:<10}\n'.format(name, str(short_data))
            else:
                s = s + '{:<10} -> {:<10}\n'.format(name, str(val))
        return s[:-2] # To remove the last \n

    def __repr__(self):
        s = ''
        short_data = {}
        for u,v in self.data.items():
            if v:
                short_data[u] = len(v)
            else:
                short_data[u] = v
        for name,val in vars(self).items():
            if type(val) is dict:
                s = s + '{:<10} -> {:<10}\n'.format(name, str(short_data))
            else:
                s = s + '{:<10} -> {:<10}\n'.format(name, str(val))
        return s[:-2] # To remove the last \n

class KickstartEntry(object):
    """
    KickstartEntry
    """
    def __init__(self, path, file_type=FileType.XML):
        #super(KickstartEntry, self).__init__()

        if file_type != FileType.XML and file_type != FileType.YAML:
            raise ValueError("{} is not a regular Kickstart output format. Please use XML or YAML.".format(file_type.name))
        elif file_type == FileType.YAML:
            raise NotImplementedError("YAML supports is not implemented yet. Please use XML.")

        self._path = Path(str(path))
        if not self._path.exists():
            raise ValueError("{} does not exist.".format(path))
        elif not self._path.is_file():
            raise ValueError("{} is not a regular file.".format(path))
        self._ftype = file_type

        try:
            self._tree = xml.ElementTree()
            self._tree.parse(self._path)
            self._root = self._tree.getroot()
        except xml.ParseError as e:
            if check_multiple_xml_file(self.orig_path):
                print ("[warning]", self._path,": multiple kickstart record detected")

            else:
                print ("[error]", self._path,":",e)
                exit(-1)

        # Parse machine
        self.machine = Machine(self._get_elem("machine"))

        self.usage = Usage(self._get_elem("mainjob"))
        self.scheduler_usage = Usage(self._root)  # Usage (global, not mainjob)


        #Store all processes involved
        # key is pid and value a Processus object
        # each processus object store a map of the files accessed
        self.processes = {}
        self.parse_proc(self._get_elem("mainjob"))

        # print(list(self.processes.values())[0])

        self._duration = float(self._get_elem("mainjob").attrib["duration"])

        self._files = {} 
        self._files_in_bb = 0
        self._size_in_bb = 0
        for u,v in self.processes.items():
            for name,e in v.files.items():
                if not name in self._files:
                    if name.startswith("/var/opt/"):
                        self._files_in_bb += 1
                        self._size_in_bb += int(e.size)
                        self._files[name] = e
                    elif name.startswith("/global/cscratch1/"):
                        self._files[name] = e
                    else:
                        # We filter all the /tmp/, /dev/ library accesses
                        continue

        self._data_bread = 0 # Byte read
        self._data_nread = 0 # Number of read
        self._data_bwrite = 0 # Byte written
        self._data_nwrite = 0 # Number of write
        for _,v in self._files.items():
            self._data_bread += int(v.bread)
            self._data_nread += int(v.nread)
            self._data_bwrite += int(v.bwrite)
            self._data_nwrite += int(v.nwrite)
            



    def parse_proc(self, xml_root):
        for proc in xml_root:
            if proc.tag == XML_PREFIX+"proc":
                idx = proc.attrib["pid"]
                self.processes[idx] = Processus(proc)

    def path(self):
        return self._path

    def stat(self):
        return self._path.stat()

    def _get_elem(self, key):
        for elem in self._root:
            if elem.tag == XML_PREFIX+str(key):
                return elem
        return None

    def duration(self):
        return float(self._duration)

    def utime(self):
        return float(self.usage.utime)

    def stime(self):
        return float(self.usage.stime)

    def ttime(self):
        return self.stime() + self.utime()

    def efficiency(self):
        return self.ttime() / self.duration()

    def files():
        return self._files

    def cores(self):
        pass

    def tot_bread(self):
        return self._data_bread

    def tot_nread(self):
        return self._data_nread

    def tot_bwrite(self):
        return self._data_bwrite

    def tot_nwrite(self):
        return self._data_nwrite

    def avg_read_size(self):
        return float(self.tot_bread()) / self.tot_nread()

    def avg_write_size(self):
        return float(self.tot_bwrite()) / self.tot_nwrite()
    
    def avg_io_size(self):
        return float(self.tot_bwrite()+self.tot_bread()) / (self.tot_nwrite()+self.tot_nread())


# #In case there multiple kickstart record in one XML (so multiple workflow running)        
# class KickStartMultiEntry(KickstartEntry):
#     # Multiple workflow of one runs
#     # We take the longest one which represent the makespan of the X parallel workflow
#     def __init__(self, path, file_type=FileType.XML):
#         self.orig_path = path
#         if not check_multiple_xml_file(self.orig_path):
#             super().__init__(path=self.orig_path, file_type=file_type)
#         else:
#             files = split_multiple_xml_file(self.orig_path)

#             self.records = []
#             makespan = 0
#             longest_entry = None

#             for r in files:
#                 self.records.append(KickstartEntry(r))
#                 if self.records[-1].duration() > makespan:
#                     makespan = self.records[-1].duration()
#                     longest_entry = r

#             self.nb_records = len(self.records)

#             for r in self.records:
#                 if r.path() == Path(longest_entry):
#                     print(r.path().name, " -> ", r.duration(), " (LONGEST)")
#                 else:
#                     print(r.path().name, " -> ", r.duration())

#             #Found the longest one (the makespan)

#             super().__init__(path=longest_entry, file_type=file_type)


class KickstartRecord:
    # Manage averaged runs of one experiments
    # A list of KickstartEntry for the same experiments
    def __init__(self,  kickstart_entries, file_type=FileType.XML):
        self.records = []
        for r in set(kickstart_entries):                
            self.records.append(KickstartEntry(r))

        self.nb_records = len(self.records)

    def paths(self):
        return [e.path() for e in self.records]

    def data(self):
        return {e.path():e for e in self.records}

    def utime(self):
        sample = [e.utime() for e in self.records]
        return (statistics.mean(sample),statistics.stdev(sample))

    def stime(self):
        sample = [e.stime() for e in self.records]
        return (statistics.mean(sample),statistics.stdev(sample))

    def ttime(self):
        sample = [e.ttime() for e in self.records]
        return (statistics.mean(sample),statistics.stdev(sample))

    def duration(self):
        sample = [e.duration() for e in self.records]
        return (statistics.mean(sample),statistics.stdev(sample))

    def efficiency(self):
        sample = [e.efficiency() for e in self.records]
        return (statistics.mean(sample), statistics.stdev(sample))

    def files(self):
        return self.records[0].files()

    def tot_bread(self):
        sample = [e.tot_bread() for e in self.records]
        return (statistics.mean(sample), statistics.stdev(sample))

    def tot_nread(self):
        sample = [e.tot_nread() for e in self.records]
        return (statistics.mean(sample), statistics.stdev(sample))

    def tot_bwrite(self):
        sample = [e.tot_bwrite() for e in self.records]
        return (statistics.mean(sample), statistics.stdev(sample))

    def tot_nwrite(self):
        sample = [e.tot_nwrite() for e in self.records]
        return (statistics.mean(sample), statistics.stdev(sample))

    def avg_read_size(self):
        sample = [e.avg_read_size() for e in self.records]
        return (statistics.mean(sample), statistics.stdev(sample))

    def avg_write_size(self):
        sample = [e.avg_write_size() for e in self.records]
        return (statistics.mean(sample), statistics.stdev(sample))
    
    def avg_io_size(self):
        sample = [e.avg_io_size() for e in self.records]
        return (statistics.mean(sample), statistics.stdev(sample))
 


# Wall-clock time seen from the user perspective
class OutputLog:
    def __init__(self, log_file):
        self.file = log_file
        self.nodes = 0
        self.tasks = 0
        self.cores = 0
        # self.files_staged = 0
        # self.total_number_files = 0
        # self.data_staged = 0 #In MB

        #From the scheduler perspective
        self.time_stage_in = 0
        self.time_resample = 0
        self.time_coadd = 0
        self.time_stage_out = 0
        self.time_total = 0

        with open(self.file, 'r') as f:
            for line in f:
                if line.startswith("NODE"):
                    self.nodes = int(line.split('=')[1])
                elif line.startswith("TASK"):
                    self.tasks = int(line.split('=')[1])
                elif line.startswith("CORE"):
                    self.cores = int(line.split('=')[1])
                elif line.startswith("TIME STAGE_IN"):
                    start = len("TIME STAGE_IN ")
                    self.time_stage_in = float(line[start:])
                elif line.startswith("TIME RESAMPLE"):
                    start = len("TIME RESAMPLE ")
                    self.time_resample = float(line[start:])
                elif line.startswith("TIME COMBINE"):
                    start = len("TIME COMBINE ")
                    self.time_coadd = float(line[start:])
                elif line.startswith("TIME STAGE_OUT"):
                    start = len("TIME STAGE_OUT ")
                    self.time_stage_out = float(line[start:])
                elif line.startswith("TIME TOTAL"):
                    start = len("TIME TOTAL ")
                    self.time_total = float(line[start:])

class AvgOutputLog:
    def __init__(self, list_log_files):
        self.nodes = 0
        self.tasks = 0
        self.cores = 0
        # self.files_staged = 0
        # self.total_number_files = 0
        # self.data_staged = 0 #In MB

        #From the scheduler perspective
        tmp_stage_in = []
        tmp_resample = []
        tmp_coadd = []
        tmp_stage_out = []
        tmp_total = []

        for f in list_log_files:
            log = OutputLog(log_file=f)

            tmp_stage_in.append(log.time_stage_in)
            tmp_resample.append(log.time_resample)
            tmp_coadd.append(log.time_coadd)
            tmp_stage_out.append(log.time_stage_out)
            tmp_total.append(log.time_total)

            self.nodes = log.nodes
            self.tasks = log.tasks
            self.cores = log.cores


        self.time_stage_in = (statistics.mean(tmp_stage_in), statistics.stdev(tmp_stage_in))
        self.time_resample = (statistics.mean(tmp_resample), statistics.stdev(tmp_resample))
        self.time_coadd = (statistics.mean(tmp_coadd), statistics.stdev(tmp_coadd))
        self.time_stage_out = (statistics.mean(tmp_stage_out), statistics.stdev(tmp_stage_out))
        self.time_total = (statistics.mean(tmp_total), statistics.stdev(tmp_total))

        print("TIME STAGE_IN ", self.time_stage_in)
        print("TIME RESAMPLE ", self.time_resample)
        print("TIME COMBINE ", self.time_coadd)
        print("TIME STAGE_OUT ", self.time_stage_out)
        print("TIME TOTAL ", self.time_total)


## For stage-*-bb.csv and stage-*-pfs.csv
# class DetailedStageInTask:
#     def __init__(self, csv_file, sep=' '):
#         self._csv = csv_file
#         self._sep = sep
#         self._data = []

#         self._data_transfered   = 0       # In MB
#         self._tranfer_time      = 0       # In S
#         self._bandwidth         = 0       # In MB

#         with open(self._csv) as f:
#             for row in csv.DictReader(f, delimiter=self._sep):
#                 self._data.append(OrderedDict(row))

#         for row in self._data:
#             self._data_transfered += float(row['SIZE(MB)'])
#             self._tranfer_time += float(row['TOTAL(S)'])
#             print (row)

#         print(self._tranfer_time,self._data_transfered,self._data_transfered/self._tranfer_time)

## For stage-*-bb-global.csv and stage-*-pfs-global.csv
# NB_FILES TOTAL_SIZE(MB) NB_FILES_TRANSFERED TRANSFERED_SIZE(MB) TRANSFER_RATIO DURATION(S) STIME(S) UTIME(S) BANDWIDTH(MB/S) EFFICIENCY
# 32 768.515625 32 768.515625 1.0 0.062945 0.704128 0.885696506 867.6963494761715 0.8660675466185027
class StageInTask:
    def __init__(self, csv_file, sep=' '):
        self._csv = csv_file
        self._sep = sep
        self._data = None

        with open(self._csv) as f:
            for row in csv.DictReader(f, delimiter=self._sep):
                self._data = OrderedDict(row) #WE NOW THERE ONLY ONE ROW (PLUS THE HEADER)

        for k in self._data:
            self._data[k] = float(self._data[k])

        # print(self._data) 


class AvgStageInTask:
    def __init__(self, list_csv_files, sep=' '):
        self._csv_files = list_csv_files
        self._data = OrderedDict()
        for f in self._csv_files:
            csv = StageInTask(csv_file=f, sep=sep)

            for k in csv._data:
                if not k in self._data:
                    self._data[k] = []
                self._data[k].append(csv._data[k])
    
        for k in self._data:
            pair = (statistics.mean(self._data[k]), statistics.stdev(self._data[k]))
            self._data[k] = pair

            #print(k, " : ", self._data[k][0])


class KickstartDirectory:
    """
        Directory must follow this kind of pattern:
        self.dir = swarp-bb.batch.1c.0f.25856221
            swarp-bb.batch.1c.0f.25856221
            ├── output.25823425
            ├── output.25823427
            ├── output.25823428
            ├── output.25823430
            ├── output.25823433
            ├── output.25823579
            ├── output.25823580
            ├── output.25823581
            ├── output.25823583
            ├── output.batch.1c.0f.25823425
                ├──1/
                    ├── combine.xml
                    ├── error.coadd
                    ├── error.resample
                    ├── files_to_stage.txt
                    ├── output.coadd
                    ├── output.log
                    ├── output.resample
                    ├── resample.xml
                    ├── resample_files.txt
                    ├── slurm.env
                    ├── stat.combine.xml
                    └── stat.resample.xml
                ├──X/
                    ├── combine.xml
                    ├── error.coadd
                    ├── error.resample
                    ├── files_to_stage.txt
                    ├── output.coadd
                    ├── output.log
                    ├── output.resample
                    ├── resample.xml
                    ├── resample_files.txt
                    ├── slurm.env
                    ├── stat.combine.xml
                    └── stat.resample.xml
            ├── output.batch.1c.10f.25823579
            ├── output.batch.1c.12f.25823580
            ├── output.batch.1c.14f.25823581
            ├── output.batch.1c.16f.25823583
            ├── output.batch.1c.2f.25823427
            ├── output.batch.1c.4f.25823428
            ├── output.batch.1c.6f.25823430
            └── output.batch.1c.8f.25823433
    """
    def __init__(self,  directory, file_type=FileType.XML):
        self.dir = Path(os.path.abspath(directory))
        if not self.dir.is_dir():
            raise ValueError("error: {} is not a valid directory.".format(self.dir))

        #print(self.dir)
        self.dir_exp = sorted([x for x in self.dir.iterdir() if x.is_dir()])
        self.log = []
        self.resample = {}
        self.combine = {}
        self.outputlog = {}
        self.stagein = {}
        self.log = 'output.log' #Contains wallclock time
        self.stage_in_log = 'stage-in-bb-global.csv' #CSV 
        self.size_in_bb = {}
        self.nb_files_stagein = {}
        self.exp_setup = {} #swarp-bb.batch.1c.0f.25856221 -> swarp-bb.batch.{core}c.{File_in_BB}f.{slurm job id}

        self.makespan = {}  # Addition of tasks' execution time

        print(self.dir)
        for i,d in enumerate(self.dir_exp):
            dir_at_this_level = sorted([x for x in d.iterdir() if x.is_dir()])
            # folder should be named like that: 
            #   swarp-queue-xC-xB-x_yW-x_yF-day-month
            print ("dir at this level: ", [x.name for x in dir_at_this_level])
            raw_resample = []
            raw_combine = []
            output_log = []
            stage_in = []
            bb_info = []

            if len(dir_at_this_level) != 1:
                #Normally just swarp-scaling.batch ...
                print("[error]: we need only one directory at this level")

            pid_run = dir_at_this_level[0].name.split('.')[-1]

            print("PID:", pid_run)

            avg_dir = [x for x in dir_at_this_level[0].iterdir() if x.is_dir()]

            for avg in avg_dir:
                print("run: ", avg.name)
                #print(dir_at_this_level,avg)
                output_log.append(avg / self.log)
                stage_in.append(avg / self.stage_in_log)
                bb_info.append(avg / 'data-stagedin.log')

                pipeline_set = sorted([x for x in avg.iterdir() if x.is_dir()])
                for pipeline in pipeline_set:
                    # TODO: Find here the longest pipeline among the one launched
                    nb_pipeline = pipeline.parts[-1]
                    print ("Dealing with number of pipelines:", nb_pipeline)

                    resmpl_path = "stat.resample." + pid_run + "." + nb_pipeline + ".xml"
                    raw_resample.append(pipeline / resmpl_path)
                    combine_path = "stat.combine." + pid_run + "." + nb_pipeline + ".xml"
                    raw_combine.append(pipeline / combine_path)

                    # Check how to average for all pipeline
                    # Check how to average over each pipeline
                    self.resample[d] = KickstartRecord(kickstart_entries=raw_resample)
                    self.combine[d] = KickstartRecord(kickstart_entries=raw_combine)

            self.stagein[d] = AvgStageInTask(list_csv_files=stage_in)
            self.outputlog[d] = AvgOutputLog(list_log_files=output_log)
            break


        # # ## PRINTING TEST
        for run,d in self.resample.items():
            data_run = d.data()
            print("*** \"{}\" averaged on {} runs:".format(run.name, len(data_run)))
            # for u,v in data_run.items():
            #     print("==> Run: {}".format(u))
            #     print("        duration   : {:.3f}".format(v.duration()))
            #     print("        ttime      : {:.3f}".format(v.ttime()))
            #     print("        utime      : {:.3f}".format(v.utime()))
            #     print("        stime      : {:.3f}".format(v.stime()))
            #     print("        efficiency : {:.3f}".format(v.efficiency()*100))
            #     print("        read       : {:.3f}".format(v.tot_bread()/(10**6)))
            #     print("        write      : {:.3f}".format(v.tot_bwrite()/(10**6)))
            print("  == duration   : {:.3f} | {:.3f}".format(d.duration()[0], d.duration()[1]))
            print("  == ttime      : {:.3f} | {:.3f}".format(d.ttime()[0],d.ttime()[1]))
            print("  == utime      : {:.3f} | {:.3f}".format(d.utime()[0],d.utime()[1]))
            print("  == stime      : {:.3f} | {:.3f}".format(d.stime()[0],d.stime()[1]))
            print("  == efficiency : {:.3f} | {:.3f}".format(d.efficiency()[0],d.efficiency()[1]))
            print("  == read       : {:.3f} | {:.3f}".format(d.tot_bread()[0],d.tot_bread()[1]))
            print("  == write      : {:.3f} | {:.3f}".format(d.tot_bwrite()[0],d.tot_bwrite()[1]))

    def root_dir(self):
        return self.dir

    def dirs(self):
        return self.dir_exp

    def write_csv_all_by_pipeline(csv_file, sep = ' '):
        header="ID NB_PIPELINE BB_ALLOC_SIZE(MB) NB_CORES TOTAL_NB_FILES BB_NB_FILES TOTAL_SIZE_FILES(MB) BB_SIZE_FILES(MB) MEAN_MAKESPAN(S) SD_MAKESPAN MEAN_WALLTIME(S) SD_WALLTIME STAGEIN_MEAN_TIME(S) STAGEIN_SD_TIME STAGEIN_MEAN_WALLTIME(S) STAGEIN_SD_WALLTIME RESAMPLE_MEAN_TIME(S) RESAMPLE_SD_TIME RESAMPLE_MEAN_WALLTIME(S) RESAMPLE_SD_WALLTIME COMBINE_MEAN_TIME(S) COMBINE_SD_TIME COMBINE_MEAN_WALLTIME(S) COMBINE_SD_WALLTIME STAGEOUT_MEAN_TIME(S) STAGEOUT_SD_TIME STAGEOUT_MEAN_WALLTIME(S) STAGEOUT_SD_WALLTIME".split(' ')
        with open(csv_file, 'w', newline='') as f:
            write = csv.writer(f, delimiter=sep, quotechar='|', quoting=csv.QUOTE_MINIMAL)
            write.writerow(header)





    # def plot_makespan_by_pipeline():
    #     if seaborn_found is None:
    #         return



if __name__ == "__main__":

    #test_record = KickstartEntry("/Users/lpottier/research/usc-isi/projects/workflow-io-bb/real-workflows/swarp/dec17/swarp-regular-16C-100B-1_64W-0F-17-12/swarp-run-8N-0F.6TXXSk/swarp-scaling.batch.16c.0f.26829339/2/3/stat.resample.26829339.3.xml")
    # print(test_record.path())
    # print(test_record.time())
    # print(test_record.efficiency())

    # test_record2 = KickstartEntry("stat.combine.xml")
    # print(test_record2.path())
    # print(test_record2.time())
    # print(test_record2.efficiency())

    # exp1 = KickstartRecord(["stat.resample.xml", "stat.combine.xml"])

    # print(exp1.paths())
    # print(exp1.time())
    # print(exp1.efficiency())

    exp_dir = "/Users/lpottier/research/usc-isi/projects/workflow-io-bb/real-workflows/swarp/jan9/"
    # print(check_multiple_xml_file("test_exp/test.xml"))
    # print(split_multiple_xml_file("test_exp/test.xml"))

    test = KickstartDirectory(exp_dir+"swarp-regular-1C-100B-1_8W-32F-11-1/")


