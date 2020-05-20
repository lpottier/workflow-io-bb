#!/usr/bin/env python3

import statistics
import os
import csv
import re
import math
import glob
import importlib
import time
import argparse

import concurrent.futures

import xml.etree.ElementTree as xml

from enum import Enum,unique,auto
from pathlib import Path
from collections import OrderedDict


VERBOSE = 1
# VERBOSE = 0 -> nothing besides error messages if any
# VERBOSE = 1 -> verbose but no debug message
# VERBOSE = 2 -> more debug messages
# VERBOSE = 3 -> all

XML_PREFIX="{http://pegasus.isi.edu/schema/invocation}"

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
            if check_multiple_xml_file(self._path):
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


class KickStartPipeline(KickstartEntry):
    # Multiple workflow of one runs
    # We take the longest one which represent the makespan of the X parallel workflow
    def __init__(self, ks_entries, file_type=FileType.XML):
        self.entries = ks_entries

        self.records = []
        makespan = 0
        longest_entry = None

        for r in self.entries:
            self.records.append(KickstartEntry(r))
            if self.records[-1].duration() > makespan:
                makespan = self.records[-1].duration()
                longest_entry = r

        self.nb_records = len(self.records)

        if VERBOSE >= 3:
            for r in self.records:
                if r.path() == Path(longest_entry):
                    print(r.path().name, " -> ", r.duration(), " (LONGEST)")
                else:
                    print(r.path().name, " -> ", r.duration())

        #Found the longest one (the makespan)

        super().__init__(path=longest_entry, file_type=file_type)


class KickstartRecord:
    # Manage averaged runs of one experiments
    # A list of KickstartEntry for the same experiments
    def __init__(self,  kickstart_entries, file_type=FileType.XML):
        self.records = []
        for r in set(kickstart_entries):                
            self.records.append(r)

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

        self.start_date = 0 # Contains date like YYYY-MM-DD
        self.start_time = 0 # Contains time like HH:MM:SS.NS-08:00
        self.end_date = 0
        self.end_time = 0


        with open(self.file, 'r') as f:
            for line in f:
                if line.startswith("Starting STAGE_IN..."):
                    self.start_date = line.split('...')[1].split(' ')[0]
                    self.start_time = line.split('...')[1].split(' ')[1]
                if line.startswith("Starting STAGE_OUT..."):
                    self.end_date = line.split('...')[1].split(' ')[0]
                    self.end_time = line.split('...')[1].split(' ')[1]
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

    def walltime(self):
        return self.time_total

    def walltime_stagein(self):
        return self.time_stage_in

    def walltime_resample(self):
        return self.time_resample

    def walltime_combine(self):
        return self.time_coadd

    def walltime_stageout(self):
        return self.time_stage_out

    def start(self):
        return (self.start_date, self.start_time)

    def end(self):
        return (self.end_date, self.end_time)


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

        if VERBOSE >= 2:
            print("TIME STAGE_IN ", self.time_stage_in)
            print("TIME RESAMPLE ", self.time_resample)
            print("TIME COMBINE ", self.time_coadd)
            print("TIME STAGE_OUT ", self.time_stage_out)
            print("TIME TOTAL ", self.time_total)

    def walltime(self):
        return self.time_total

    def walltime_stagein(self):
        return self.time_stage_in

    def walltime_resample(self):
        return self.time_resample

    def walltime_combine(self):
        return self.time_coadd

    def walltime_stageout(self):
        return self.time_stage_out


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

    def duration(self):
        return float(self._data["DURATION(S)"])

    def bw(self):
        return float(self._data["BANDWIDTH(MB/S)"])

class StageOutTask:
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

    def duration(self):
        return float(self._data["DURATION(S)"])

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

    def duration(self):
        return (float(self._data["DURATION(S)"][0]), float(self._data["DURATION(S)"][1]))

class AvgStageOutTask:
    def __init__(self, list_csv_files, sep=' '):
        self._csv_files = list_csv_files
        self._data = OrderedDict()
        for f in self._csv_files:
            csv = StageOutTask(csv_file=f, sep=sep)

            for k in csv._data:
                if not k in self._data:
                    self._data[k] = []
                self._data[k].append(csv._data[k])
    
        for k in self._data:
            pair = (statistics.mean(self._data[k]), statistics.stdev(self._data[k]))
            self._data[k] = pair

            #print(k, " : ", self._data[k][0])

    def duration(self):
        return (float(self._data["DURATION(S)"][0]), float(self._data["DURATION(S)"][1]))


class RawKickstartDirectory:
    """
    Raw output of KS data
    """

    # If concat_pipeline == True then we only record the pipeline value and drop the others
    def __init__(self,  directory, concat_pipeline=False, file_type=FileType.XML):
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
        self.stageout = {}
        self.log = 'output.log' #Contains wallclock time
        self.stage_in_log = 'stage-in-bb-global.csv' #CSV 
        self.stage_out_log = 'stage-out-bb-global.csv' #CSV 
        self.size_in_bb = {}
        self.nb_files_stagein = {}
        self.setup = {} #swarp-16.batch.1c.0f.27440714 -> swarp-bb.batch.{core}c.{File_in_BB}f.{slurm job id}

        self.makespan = {}  # Addition of tasks' execution time
        self._stagefits = "stagefits" in str(self.dir.name)
        self._concat_pipeline = concat_pipeline

        # Keys iterate on data
        self.id = set({})
        self.avg = set({})
        self.pipeline = set({})

        self.max_pipeline = -1

        if "private" in str(self.dir.name):
            self._bbtype = "PRIVATE"
        elif "striped" in str(self.dir.name):
            self._bbtype = "STRIPED"
        elif "summit" in str(self.dir.name):
            self._bbtype = "ONNODE"  
        else:
            self._bbtype = "UNKNOWN"

        if VERBOSE >= 2:
            print(self.dir)

        for i,d in enumerate(self.dir_exp):
            dir_at_this_level = sorted([x for x in d.iterdir() if x.is_dir()])
            # folder should be named like that: 
            #   swarp-XWorkflow.batch.Xc.Xf.JOBID
            if VERBOSE >= 2:
                print ("dir at this level: ", [x.name for x in dir_at_this_level])
            # output_log = []
            # stage_in = []
            # stage_out = []
            bb_info = [] # data-stagedin.log -> 4 97M
            avg_resample = {}
            avg_combine = {}

            if len(dir_at_this_level) != 1:
                #Normally just swarp-scaling.batch ...
                print("[error]: we need only one directory at this level")

            try:
                pid_run = dir_at_this_level[0].name.split('.')[-1]
                self.setup[pid_run] = {}

                first_part = dir_at_this_level[0].name.split('.')[0]

                self.setup[pid_run]['name'] = first_part.split('-')[0]
                self.setup[pid_run]['pipeline'] = int(d.name.split('-')[2][:-1])
            
            except Exception as e:
                print("", flush=True)
                print (e, " : ", d.name.split('-'))
                exit(-1)

            self.id = self.id.union({pid_run})

            self.setup[pid_run]['core'] = dir_at_this_level[0].name.split('.')[-3]
            self.setup[pid_run]['core'] = int(self.setup[pid_run]['core'][:-1]) #to remove the 'c' at the end

            self.setup[pid_run]['file'] = dir_at_this_level[0].name.split('.')[-2]
            self.setup[pid_run]['file'] = int(self.setup[pid_run]['file'][:-1]) #to remove the 'f' at the end

            self.resample[pid_run] = {}
            self.combine[pid_run] = {}
            self.outputlog[pid_run] = {}
            self.stagein[pid_run] = {}
            self.stageout[pid_run] = {}
            self.makespan[pid_run] = {}

            if VERBOSE >= 2:
                print(self.setup[pid_run])

            avg_dir = [x for x in dir_at_this_level[0].iterdir() if x.is_dir()]

            for avg in avg_dir:
                if VERBOSE >= 2:
                    print("run: ", avg.name)
                # print(avg)
                idavg = int(avg.name)
                self.avg = self.avg.union({idavg})

                self.outputlog[pid_run][idavg] = OutputLog(avg / self.log)
                self.stagein[pid_run][idavg] = StageInTask(avg / self.stage_in_log)
                self.stageout[pid_run][idavg] = StageOutTask(avg / self.stage_out_log)

                self.resample[pid_run][idavg] = {}
                self.combine[pid_run][idavg] = {}
                self.makespan[pid_run][idavg] = {}

                bb_info.append(avg / 'data-stagedin.log')
                with open(bb_info[-1]) as bb_log:
                    data_bb_log = bb_log.read().split(' ')
                    self.setup[pid_run]['file'] = int(data_bb_log[0])
                    if self.setup[pid_run]['file'] > 0:
                        data_bb_log[1] = data_bb_log[1].split('\n')[0]
                        self.setup[pid_run]['size_in_bb'] = int(data_bb_log[1][:-1])
                    else:
                        self.setup[pid_run]['size_in_bb'] = 0

                with open(avg / 'bb_alloc.log') as bb_alloc:
                    data_bb_alloc = bb_alloc.read().split('\n')
                    total = 0
                    self.setup[pid_run]['size_fragment'] = 0
                    self.setup[pid_run]['bb_alloc'] = 0
                    for elem in data_bb_alloc:
                        if elem == '':
                            continue
                        fragment = float(elem[:-3])
                        total += fragment # Remove at the end GiB
                        self.setup[pid_run]['size_fragment'] = fragment
                        self.setup[pid_run]['bb_alloc'] = total

                pipeline_resample = []
                pipeline_combine = []

                pipeline_set = sorted([x for x in avg.iterdir() if x.is_dir()])
                for pipeline in pipeline_set:
                    nb_pipeline = pipeline.parts[-1]
                    try:
                        _ = int(nb_pipeline)
                    except ValueError as e:
                        continue

                    self.max_pipeline = int(nb_pipeline)

                    if VERBOSE >= 2:
                        print ("Dealing with number of pipelines:", nb_pipeline)

                    # print(str(pid_run))
                    # print(glob.glob(str(pipeline) + "/stat.resample." + "*"))
                    assert(len(glob.glob(str(pipeline) + "/stat.resample." + "*")) == 1) 
                    resmpl_path = glob.glob(str(pipeline) + "/stat.resample." + "*")[0]
                    pipeline_resample.append(resmpl_path)

                    assert(len(glob.glob(str(pipeline) + "/stat.combine." + "*")) == 1) 
                    combine_path = glob.glob(str(pipeline) + "/stat.combine." + "*")[0]
                    pipeline_combine.append(combine_path)

                if self._concat_pipeline:
                    id_max_pipeline = int(nb_pipeline.name)
                    self.pipeline = self.pipeline.union({id_max_pipeline})
                    self.resample[pid_run][idavg][id_max_pipeline] = KickStartPipeline(ks_entries=pipeline_resample)
                    self.combine[pid_run][idavg][id_max_pipeline] = KickStartPipeline(ks_entries=pipeline_combine)
                else:
                    idpipe = int(pipeline.name)
                    self.pipeline = self.pipeline.union({idpipe})
                    for rsmpl,coadd in zip(pipeline_resample,pipeline_combine):
                        self.resample[pid_run][idavg][idpipe] = KickstartEntry(rsmpl)
                        self.combine[pid_run][idavg][idpipe] = KickstartEntry(coadd)

                if self._concat_pipeline:
                    self.makespan[pid_run][idavg] = float(self.stagein[pid_run][idavg].duration()[0]) + float(self.resample[pid_run][idavg][int(nb_pipeline.name)].duration()[0]) + float(self.combine[pid_run][idavg][int(nb_pipeline.name)].duration()[0])
                else:
                    self.makespan[pid_run][idavg] = -1

        # FITS="N"
        # if self._stagefits:
        #     # BB_NB_FILES += (32 * int(self.setup[run]['pipeline'])) #32 files produced by resample per pipeline
        #     # BB_SIZE_FILES_MB += (918 * int(self.setup[run]['pipeline'])) # 918 Mo per pipeline
        #     FITS="Y"

        # print("ID\t\tPIPELINE\tAVG\tFITS\tMAKESPAN_S\tWALLTIME_S\tSTAGEIN_WALLTIME_S\tRESAMPLE_WALLTIME_S\tCOMBINE_WALLTIME_S\tSTAGEOUT_WALLTIME_S")

        # for run in self.id:
        #     for pipeline in self.pipeline:
        #         for avg in sorted(self.avg):
        #             print ("{}\t{}\t\t{}\t{}\t{}\t\t{}\t\t{}\t\t{}\t\t{}\t\t{}".format(
        #                 run,
        #                 pipeline,
        #                 avg,
        #                 FITS,
        #                 float(self.makespan[run][avg]),
        #                 round(float(self.outputlog[run][avg].walltime()),2),
        #                 round(float(self.outputlog[run][avg].walltime_stagein()),2),
        #                 round(float(self.outputlog[run][avg].walltime_resample()),2),
        #                 round(float(self.outputlog[run][avg].walltime_combine()),2),
        #                 round(float(self.outputlog[run][avg].walltime_stageout()),2),
        #                 # self.stagein[run].duration()[0],
        #                 # self.outputlog[run].walltime_stagein()[0],
        #                 # self.resample[run].duration()[0],
        #                 # self.outputlog[run].walltime_resample()[0],
        #                 # self.combine[run].duration()[0],
        #                 # self.outputlog[run].walltime_combine()[0],
        #                 # self.stageout[run].duration()[0],
        #                 # self.outputlog[run].walltime_stageout()[0],
        #                 )
        #             )

    def root_dir(self):
        return self.dir

    def dirs(self):
        return self.dir_exp

    def write_csv_global_by_pipeline(self, csv_file, write_header=False, sep = ' '):
        header="ID START END FITS NB_PIPELINE NB_CORES AVG PIPELINE BB_TYPE BB_ALLOC_SIZE_MB TOTAL_NB_FILES BB_NB_FILES TOTAL_SIZE_FILES_MB BB_SIZE_FILES_MB BANDWIDTH_MBS MAKESPAN_S WALLTIME_S STAGEIN_TIME_S STAGEIN_WALLTIME_S RESAMPLE_TIME_S RESAMPLE_WALLTIME_S COMBINE_TIME_S COMBINE_WALLTIME_S STAGEOUT_TIME_S STAGEOUT_WALLTIME_S".split(' ')
        if write_header:
            open_flag = 'w'
        else:
            open_flag = 'a'
        with open(csv_file, open_flag, newline='') as f:
            csv_writer = csv.writer(f, delimiter=sep, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if write_header:
                csv_writer.writerow(header)

            FITS="N"
            if self._stagefits:
                # BB_NB_FILES += (32 * int(self.setup[run]['pipeline'])) #32 files produced by resample per pipeline
                # BB_SIZE_FILES_MB += (918 * int(self.setup[run]['pipeline'])) # 918 Mo per pipeline
                FITS="Y"

            for run in self.id:
                for pipeline in sorted(self.pipeline):
                    for avg in sorted(self.avg):

                        BB_NB_FILES=int(self.stagein[run][avg]._data['NB_FILES_TRANSFERED'])
                        BB_SIZE_FILES_MB=float(self.stagein[run][avg]._data['TRANSFERED_SIZE(MB)'])

                        if not self._concat_pipeline:
                            MKSP = self.stagein[run][avg].duration() + self.resample[run][avg][self.max_pipeline].duration() + self.combine[run][avg][self.max_pipeline].duration()
                            line = "{} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {}".format(
                                run,
                                self.outputlog[run][avg].start()[0],
                                self.outputlog[run][avg].end()[0],
                                FITS,
                                int(self.setup[run]['pipeline']),
                                int(self.setup[run]['core']),
                                avg,
                                pipeline,
                                self._bbtype,
                                float(self.setup[run]['bb_alloc']),
                                32 * int(self.setup[run]['pipeline']),
                                BB_NB_FILES,
                                768.515625 * int(self.setup[run]['pipeline']),
                                BB_SIZE_FILES_MB,
                                self.stagein[run][avg].bw(),
                                MKSP,
                                self.outputlog[run][avg].walltime(),
                                self.stagein[run][avg].duration(),
                                self.outputlog[run][avg].walltime_stagein(),
                                self.resample[run][avg][pipeline].duration(),
                                self.outputlog[run][avg].walltime_resample(),
                                self.combine[run][avg][pipeline].duration(),
                                self.outputlog[run][avg].walltime_combine(),
                                self.stageout[run][avg].duration(),
                                self.outputlog[run][avg].walltime_stageout(),
                            )
                        else:
                            line = "{} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {}".format(
                                run,
                                self.outputlog[run][avg].start()[0],
                                self.outputlog[run][avg].end()[0],
                                FITS,
                                int(self.setup[run]['pipeline']),
                                int(self.setup[run]['core']),
                                avg,
                                self.max_pipeline,
                                self._bbtype,
                                float(self.setup[run]['bb_alloc']),
                                32 * int(self.setup[run]['pipeline']),
                                BB_NB_FILES,
                                768.515625 * int(self.setup[run]['pipeline']),
                                BB_SIZE_FILES_MB,
                                self.stagein[run][avg].bw(),
                                self.makespan[run][avg],
                                self.outputlog[run][avg].walltime(),
                                self.stagein[run][avg].duration(),
                                self.outputlog[run][avg].walltime_stagein(),
                                self.resample[run][avg][self.max_pipeline].duration(),
                                self.outputlog[run][avg].walltime_resample(),
                                self.combine[run][avg][self.max_pipeline].duration(),
                                self.outputlog[run][avg].walltime_combine(),
                                self.stageout[run][avg].duration(),
                                self.outputlog[run][avg].walltime_stageout(),
                            )

                        csv_writer.writerow(line.split(" "))



class KickstartDirectory:
    """
        Directory must follow this kind of pattern:
        self.dir = swarp-bb.batch.1c.0f.25856221
        swarp-premium-1C-50B-1_16W-0F-15-1
        ├── bbf.conf
        ├── bbinfo.sh
        ├── combine.swarp
        ├── files_to_stage.txt
        ├── interactive_run-swarp-scaling-bb.sh
        ├── resample.swarp
        ├── run-swarp-scaling-bb.sh
        ├── start_interactive.sh
        ├── submit.sh
        ├── swarp-run-16N-0F.wtb3xr
        │   ├── bbinfo.sh
        │   ├── build_filemap.py
        │   ├── combine.swarp
        │   ├── copy.py
        │   ├── error.27440714
        │   ├── files_to_stage.txt
        │   ├── output.27440714
        │   ├── resample.swarp
        │   ├── run-swarp-scaling-bb-16N.sh
        │   ├── swarp-16.batch.1c.0f.27440714
        │   │   ├── 1
        │   │   │   ├── 1
        │   │   │   │   ├── error.combine.27440714.1
        │   │   │   │   ├── error.resample.27440714.1
        │   │   │   │   ├── output.combine.27440714.1
        │   │   │   │   ├── output.resample.27440714.1
        │   │   │   │   ├── stage-out-bb-global.csv
        │   │   │   │   ├── stage-out-bb.csv
        │   │   │   │   ├── stage-out-pfs-global.csv
        │   │   │   │   ├── stage-out-pfs.csv
        │   │   │   │   ├── stat.combine.27440714.1.xml
        │   │   │   │   └── stat.resample.27440714.1.xml
        │   │   │   ├── 10/
        │   │   │   │   ├── ....
        │   │   │   │   ├── ....
        │   │   │   ├── X/
        │   │   │   │   ├── ....
        │   │   │   │   ├── ....
        │   │   │   ├── bb.log
        │   │   │   ├── bb_alloc.log
        │   │   │   ├── data-stagedin.log
        │   │   │   ├── files_to_stage.txt
        │   │   │   ├── output.log
        │   │   │   ├── resample_files.txt
        │   │   │   ├── slurm.env
        │   │   │   ├── stage-in-bb-global.csv
        │   │   │   ├── stage-in-bb.csv
        │   │   │   ├── stage-in-pfs-global.csv
        │   │   │   ├── stage-in-pfs.csv
        │   │   │   ├── stage-out-bb-global.csv
        │   │   │   ├── stage-out-bb.csv
        │   │   │   ├── stage-out-pfs-global.csv
        │   │   │   ├── stage-out-pfs.csv
        │   │   ├── 2
        │   │   │   ├── 1
        │   │   │   │   ├── error.combine.27440714.1
        │   │   │   │   ├── error.resample.27440714.1
        ....

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
        self.stageout = {}
        self.log = 'output.log' #Contains wallclock time
        self.stage_in_log = 'stage-in-bb-global.csv' #CSV 
        self.stage_out_log = 'stage-out-bb-global.csv' #CSV 
        self.size_in_bb = {}
        self.nb_files_stagein = {}
        self.setup = {} #swarp-16.batch.1c.0f.27440714 -> swarp-bb.batch.{core}c.{File_in_BB}f.{slurm job id}

        self.makespan = {}  # Addition of tasks' execution time
        self._stagefits = "stagefits" in str(self.dir.name)

        if "private" in str(self.dir.name):
            self._bbtype = "PRIVATE"
        elif "striped" in str(self.dir.name):
            self._bbtype = "STRIPED"
        elif "summit" in str(self.dir.name):
            self._bbtype = "ONNODE"        
        else:
            self._bbtype = "UNKNOWN"


        if VERBOSE >= 2:
            print(self.dir)

        for i,d in enumerate(self.dir_exp):
            dir_at_this_level = sorted([x for x in d.iterdir() if x.is_dir()])
            # folder should be named like that: 
            #   swarp-XWorkflow.batch.Xc.Xf.JOBID
            if VERBOSE >= 2:
                print ("dir at this level: ", [x.name for x in dir_at_this_level])
            output_log = []
            stage_in = []
            stage_out = []
            bb_info = [] # data-stagedin.log -> 4 97M
            avg_resample = []
            avg_combine = []

            if len(dir_at_this_level) != 1:
                #Normally just swarp-scaling.batch ...
                print("[error]: we need only one directory at this level")

            try:
                pid_run = dir_at_this_level[0].name.split('.')[-1]
                self.setup[pid_run] = {}

                first_part = dir_at_this_level[0].name.split('.')[0]

                self.setup[pid_run]['name'] = first_part.split('-')[0]
                self.setup[pid_run]['pipeline'] = int(d.name.split('-')[2][:-1])
            
            except Exception as e:
                print("", flush=True)
                print (e, " : ", d.name.split('-'))
                exit(-1)

            self.setup[pid_run]['core'] = dir_at_this_level[0].name.split('.')[-3]
            self.setup[pid_run]['core'] = int(self.setup[pid_run]['core'][:-1]) #to remove the 'c' at the end

            self.setup[pid_run]['file'] = dir_at_this_level[0].name.split('.')[-2]
            self.setup[pid_run]['file'] = int(self.setup[pid_run]['file'][:-1]) #to remove the 'f' at the end



            if VERBOSE >= 2:
                print(self.setup[pid_run])

            avg_dir = [x for x in dir_at_this_level[0].iterdir() if x.is_dir()]

            for avg in avg_dir:
                if VERBOSE >= 2:
                    print("run: ", avg.name)
                #print(dir_at_this_level,avg)

                # TODO deal with output log for multiple pipeline
                output_log.append(avg / self.log)
                stage_in.append(avg / self.stage_in_log)
                stage_out.append(avg / self.stage_out_log)

                bb_info.append(avg / 'data-stagedin.log')
                with open(bb_info[-1]) as bb_log:
                    data_bb_log = bb_log.read().split(' ')
                    self.setup[pid_run]['file'] = int(data_bb_log[0])
                    if self.setup[pid_run]['file'] > 0:
                        data_bb_log[1] = data_bb_log[1].split('\n')[0]
                        self.setup[pid_run]['size_in_bb'] = int(data_bb_log[1][:-1])
                    else:
                        self.setup[pid_run]['size_in_bb'] = 0

                with open(avg / 'bb_alloc.log') as bb_alloc:
                    data_bb_alloc = bb_alloc.read().split('\n')
                    total = 0
                    self.setup[pid_run]['size_fragment'] = 0
                    self.setup[pid_run]['bb_alloc'] = 0
                    for elem in data_bb_alloc:
                        if elem == '':
                            continue
                        fragment = float(elem[:-3])
                        total += fragment # Remove at the end GiB
                        self.setup[pid_run]['size_fragment'] = fragment
                        self.setup[pid_run]['bb_alloc'] = total


                raw_resample = []
                raw_combine = []

                pipeline_set = sorted([x for x in avg.iterdir() if x.is_dir()])
                for pipeline in pipeline_set:
                    # TODO: Find here the longest pipeline among the one launched
                    nb_pipeline = pipeline.parts[-1]
                    try:
                        _ = int(nb_pipeline)
                    except ValueError as e:
                        continue

                    if VERBOSE >= 2:
                        print ("Dealing with number of pipelines:", nb_pipeline)

                    assert(len(glob.glob(str(pipeline) + "/stat.resample." + "*")) == 1)
                    resmpl_path = glob.glob(str(pipeline) + "/stat.resample." + "*")[0]
                    pipeline_resample.append(resmpl_path)

                    assert(len(glob.glob(str(pipeline) + "/stat.combine." + "*")) == 1)
                    combine_path = glob.glob(str(pipeline) + "/stat.combine." + "*")[0]
                    pipeline_combine.append(combine_path)

                avg_resample.append(KickStartPipeline(ks_entries=raw_resample))
                avg_combine.append(KickStartPipeline(ks_entries=raw_combine))

            self.resample[pid_run] = KickstartRecord(kickstart_entries=avg_resample)
            self.combine[pid_run] = KickstartRecord(kickstart_entries=avg_combine)

            self.stagein[pid_run] = AvgStageInTask(list_csv_files=stage_in)
            self.stageout[pid_run] = AvgStageOutTask(list_csv_files=stage_out)

            self.outputlog[pid_run] = AvgOutputLog(list_log_files=output_log)

            mean_makespan = float(self.stagein[pid_run].duration()[0]) + float(self.resample[pid_run].duration()[0]) + float(self.combine[pid_run].duration()[0])
            sd_makespan = math.sqrt(float(self.stagein[pid_run].duration()[1])**2 + float(self.resample[pid_run].duration()[1])**2 + float(self.combine[pid_run].duration()[1])**2)

            self.makespan[pid_run] = (mean_makespan, sd_makespan)

        # # # ## PRINTING TEST
        # for run,d in self.resample.items():
        #     data_run = d.data()
        #     print("*** Resample: \"{}\" averaged on {} runs:".format(run, len(data_run)))
        #     # for u,v in data_run.items():
        #     #     print("==> Run: {}".format(u))
        #     #     print("        duration   : {:.3f}".format(v.duration()))
        #     #     print("        ttime      : {:.3f}".format(v.ttime()))
        #     #     print("        utime      : {:.3f}".format(v.utime()))
        #     #     print("        stime      : {:.3f}".format(v.stime()))
        #     #     print("        efficiency : {:.3f}".format(v.efficiency()*100))
        #     #     print("        read       : {:.3f}".format(v.tot_bread()/(10**6)))
        #     #     print("        write      : {:.3f}".format(v.tot_bwrite()/(10**6)))
        #     print("  == duration   : {:.3f} | {:.3f}".format(d.duration()[0], d.duration()[1]))
        #     print("  == ttime      : {:.3f} | {:.3f}".format(d.ttime()[0],d.ttime()[1]))
        #     print("  == utime      : {:.3f} | {:.3f}".format(d.utime()[0],d.utime()[1]))
        #     print("  == stime      : {:.3f} | {:.3f}".format(d.stime()[0],d.stime()[1]))
        #     print("  == efficiency : {:.3f} | {:.3f}".format(d.efficiency()[0],d.efficiency()[1]))
        #     print("  == read       : {:.3f} | {:.3f}".format(d.tot_bread()[0],d.tot_bread()[1]))
        #     print("  == write      : {:.3f} | {:.3f}".format(d.tot_bwrite()[0],d.tot_bwrite()[1]))

        # print("ID NB_PIPELINE BB_ALLOC_SIZE(GB) NB_CORES TOTAL_NB_FILES BB_NB_FILES TOTAL_SIZE_FILES(MB) BB_SIZE_FILES(MB) MEAN_MAKESPAN(S) SD_MAKESPAN MEAN_WALLTIME(S) SD_WALLTIME STAGEIN_MEAN_TIME(S) STAGEIN_SD_TIME STAGEIN_MEAN_WALLTIME(S) STAGEIN_SD_WALLTIME RESAMPLE_MEAN_TIME(S) RESAMPLE_SD_TIME RESAMPLE_MEAN_WALLTIME(S) RESAMPLE_SD_WALLTIME COMBINE_MEAN_TIME(S) COMBINE_SD_TIME COMBINE_MEAN_WALLTIME(S) COMBINE_SD_WALLTIME STAGEOUT_MEAN_TIME(S) STAGEOUT_SD_TIME STAGEOUT_MEAN_WALLTIME(S) STAGEOUT_SD_WALLTIME")
        # for run in self.setup:
        #     print ("{} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {}".format(
        #         run,
        #         self.setup[run]['pipeline'],
        #         self.setup[run]['bb_alloc'],
        #         self.setup[run]['core'],
        #         32,
        #         self.stagein[run]._data['NB_FILES_TRANSFERED'][0],
        #         768.515625,
        #         self.stagein[run]._data['TRANSFERED_SIZE(MB)'][0],
        #         self.makespan[run][0],
        #         self.makespan[run][1],
        #         self.outputlog[run].walltime()[0],
        #         self.outputlog[run].walltime()[1],
        #         self.stagein[run].duration()[0],
        #         self.stagein[run].duration()[1],
        #         self.outputlog[run].walltime_stagein()[0],
        #         self.outputlog[run].walltime_stagein()[1],
        #         self.resample[run].duration()[0],
        #         self.resample[run].duration()[1],
        #         self.outputlog[run].walltime_resample()[0],
        #         self.outputlog[run].walltime_resample()[1],
        #         self.combine[run].duration()[0],
        #         self.combine[run].duration()[1],
        #         self.outputlog[run].walltime_combine()[0],
        #         self.outputlog[run].walltime_combine()[1],
        #         self.stageout[run].duration()[0],
        #         self.stageout[run].duration()[1],
        #         self.outputlog[run].walltime_stageout()[0],
        #         self.outputlog[run].walltime_stageout()[1],
        #         )
        #     )

    def root_dir(self):
        return self.dir

    def dirs(self):
        return self.dir_exp

    # def write_csv_resample_by_pipeline(csv_file, sep = ' '):
    #     pass

    # def write_csv_combine_by_pipeline(csv_file, sep = ' '):
    #     pass

    def write_csv_global_by_pipeline(self, csv_file, write_header=False, sep = ' '):
        header="ID FITS NB_PIPELINE BB_TYPE BB_ALLOC_SIZE_MB NB_CORES TOTAL_NB_FILES BB_NB_FILES TOTAL_SIZE_FILES_MB BB_SIZE_FILES_MB MEAN_MAKESPAN_S SD_MAKESPAN MEAN_WALLTIME_S SD_WALLTIME STAGEIN_MEAN_TIME_S STAGEIN_SD_TIME STAGEIN_MEAN_WALLTIME_S STAGEIN_SD_WALLTIME RESAMPLE_MEAN_TIME_S RESAMPLE_SD_TIME RESAMPLE_MEAN_WALLTIME_S RESAMPLE_SD_WALLTIME COMBINE_MEAN_TIME_S COMBINE_SD_TIME COMBINE_MEAN_WALLTIME_S COMBINE_SD_WALLTIME STAGEOUT_MEAN_TIME_S STAGEOUT_SD_TIME STAGEOUT_MEAN_WALLTIME_S STAGEOUT_SD_WALLTIME".split(' ')
        if write_header:
            open_flag = 'w'
        else:
            open_flag = 'a'
        with open(csv_file, open_flag, newline='') as f:
            csv_writer = csv.writer(f, delimiter=sep, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if write_header:
                csv_writer.writerow(header)
            for run in self.setup:
                #print(BB_NB_FILES,self.stagein[run]._data)
                BB_NB_FILES=int(self.stagein[run]._data['NB_FILES_TRANSFERED'][0])
                BB_SIZE_FILES_MB=float(self.stagein[run]._data['TRANSFERED_SIZE(MB)'][0])
                FITS="N"
                if self._stagefits:
                    # BB_NB_FILES += (32 * int(self.setup[run]['pipeline'])) #32 files produced by resample per pipeline
                    # BB_SIZE_FILES_MB += (918 * int(self.setup[run]['pipeline'])) # 918 Mo per pipeline
                    FITS="Y"

                line = "{} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {}".format(
                    run,
                    FITS,
                    self.setup[run]['pipeline'],
                    self._bbtype,
                    self.setup[run]['bb_alloc'],
                    self.setup[run]['core'],
                    32 * int(self.setup[run]['pipeline']),
                    BB_NB_FILES,
                    768.515625 * int(self.setup[run]['pipeline']),
                    BB_SIZE_FILES_MB,
                    self.makespan[run][0],
                    self.makespan[run][1],
                    self.outputlog[run].walltime()[0],
                    self.outputlog[run].walltime()[1],
                    self.stagein[run].duration()[0],
                    self.stagein[run].duration()[1],
                    self.outputlog[run].walltime_stagein()[0],
                    self.outputlog[run].walltime_stagein()[1],
                    self.resample[run].duration()[0],
                    self.resample[run].duration()[1],
                    self.outputlog[run].walltime_resample()[0],
                    self.outputlog[run].walltime_resample()[1],
                    self.combine[run].duration()[0],
                    self.combine[run].duration()[1],
                    self.outputlog[run].walltime_combine()[0],
                    self.outputlog[run].walltime_combine()[1],
                    self.stageout[run].duration()[0],
                    self.stageout[run].duration()[1],
                    self.outputlog[run].walltime_stageout()[0],
                    self.outputlog[run].walltime_stageout()[1],
                    )
                csv_writer.writerow(line.split(" "))


def create_data_from_exp_mt(exp_dir, pattern='*', csv_file=None, threads=None, plot=None):

    if not Path(exp_dir).exists():
        print("[error] {} does not exist.\n[error] Abort.".format(exp_dir))
        exit(-1)

    if not Path(exp_dir).is_dir():
        print("[error] {} is not a valid directory.\n[error] Abort.".format(exp_dir))
        exit(-1)

    directories = [Path(x) for x in sorted(glob.glob(exp_dir+pattern)) if Path(x).is_dir()]
    if csv_file == None:
        csv_file = Path(Path(exp_dir) / Path(Path(exp_dir).name+".csv"))
    else:
        csv_file = Path(csv_file)

    start = []
    end = []

    if not threads:
        # If threads is not specified, create as many threads as threads available or directories
        threads = min(os.cpu_count(),len(directories))
    

    # add_done_callback(fn) to attach a printing function to give feedback
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:

        data = {executor.submit(KickstartDirectory, directory): directory for directory in directories}
        print("{} threads spawned....".format(threads))
        header = True
        for future in concurrent.futures.as_completed(data):
            exp = data[future]
            try:
                res_exp = future.result()
            except Exception as e:
                print('[error] %r: %s' % (exp.name, e))
            else:
                res_exp.write_csv_global_by_pipeline(csv_file, write_header=header)
                print(" {:<60s} => {:<20s} ".format(exp.name, csv_file.name))
            header = False


def create_data_from_exp(exp_dir, pattern='*', csv_file=None, plot=None, compact=None):

    if not Path(exp_dir).exists():
        print("[error] {} does not exist.\n[error] Abort.".format(exp_dir))
        exit(-1)

    if not Path(exp_dir).is_dir():
        print("[error] {} is not a valid directory.\n[error] Abort.".format(exp_dir))
        exit(-1)

    directories = [Path(x) for x in sorted(glob.glob(str(exp_dir)+str(pattern))) if Path(x).is_dir()]
    if csv_file == None:
        csv_file = Path(Path(exp_dir) / Path(Path(exp_dir).name+".csv"))
    else:
        csv_file = Path(csv_file)

    start = []
    end = []

    if compact:
        csv_compact = Path(csv_file.stem + '-compact' + csv_file.suffix)

    start.append(time.time())
    
    print(" {:<60s} => ".format(directories[0].name), end='', flush=True)
    if compact:
        exp = KickstartDirectory(directories[0])
        exp.write_csv_global_by_pipeline(csv_compact, write_header=True)

    exp_full = RawKickstartDirectory(directories[0])
    exp_full.write_csv_global_by_pipeline(csv_file, write_header=True)

    end.append(time.time())

    print("{:<20s} [{:.2f} sec]".format(csv_file.name, end[-1] - start[-1]))

    for d in sorted(directories[1:]):
        start.append(time.time())
        print(" {:<60s} => ".format(d.name), end='', flush=True)
        if compact:
            exp = KickstartDirectory(d)
            exp.write_csv_global_by_pipeline(csv_compact)

        exp_full = RawKickstartDirectory(d)
        exp_full.write_csv_global_by_pipeline(csv_file)
        end.append(time.time())
        print("{:<20s} [{:.2f} sec]".format(csv_file.name, end[-1] - start[-1]))

    print ("Output: {}. {} directories processed in {:.2f} seconds ({:.2f} sec/directory).".format(
        csv_file.name, len(directories), end[-1] - start[0], (end[-1] - start[0])/len(directories) )
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Parse kickstart record to produce CSV')
    
    parser.add_argument('--dir', '-d', required=True, action="append", help='Directory that contains all the directory generated bu SWARP runs')
    parser.add_argument('--csv', '-o', required=False, help='Name of the output files')
    parser.add_argument('--compact', '-x',  action="store_true", help='Generate a compact CSV files with averages and standard deviation computed')

    args = parser.parse_args()

    for d in args.dir:
        directory = Path(d)
        if args.csv == None:
            args.csv = str(directory.name)+".csv" 

        create_data_from_exp(directory, pattern="/swarp-*", csv_file=args.csv, compact=args.compact)

