#!/usr/bin/env python3

import xml.etree.ElementTree as xml

from enum import Enum,unique,auto
from pathlib import Path
import statistics
import os
import csv

XML_PREFIX="{http://pegasus.isi.edu/schema/invocation}"

@unique
class FileType(Enum):
    XML = auto()
    YAML = auto()

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
        self.file       =       hashmap["name"]
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
            raise ValueError("Nota {} ElementTree {}".format("proc",element_tree.tag))
       
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
        super(KickstartEntry, self).__init__()

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
        self._tree = xml.ElementTree()
        self._tree.parse(self._path)
        self._root = self._tree.getroot()

        # Parse machine
        self.machine = Machine(self._get_elem("machine"))

        # Usage (global, not mainjob)
        self.usage = Usage(self._root)

        #Store all processes involved
        # key is pid and value a Processus object
        # each processus object store a map of the files accessed
        self.processes = {}
        self.parse_proc(self._get_elem("mainjob"))

        # print(list(self.processes.values())[0])


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

    def utime(self):
        return float(self.usage.utime)

    def stime(self):
        return float(self.usage.stime)

    def time(self):
        return self.stime() + self.utime()

    def efficiency(self):
        return self.stime() / (self.stime() + self.utime())

    def cores(self):
        pass

    def data_read(self):
        pass

    def data_written(self):
        pass

    def data_used(self):
        pass
 
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

    def utime(self):
        sample = [e.utime() for e in self.records]
        return (statistics.mean(sample),statistics.stdev(sample))

    def stime(self):
        sample = [e.stime() for e in self.records]
        return (statistics.mean(sample),statistics.stdev(sample))

    def time(self):
        sample = [e.time() for e in self.records]
        return (statistics.mean(sample),statistics.stdev(sample))

    def efficiency(self):
        sample = [e.efficiency() for e in self.records]
        return (statistics.mean(sample),statistics.stdev(sample))

    def data_read(self):
        pass

    def data_written(self):
        pass

    def data_used(self):
        pass

class OutputLog:
    def __init__(self, file):
        self.file = file
        self.nodes = 0
        self.tasks = 0
        self.cores = 0
        self.files_staged = 0
        self.total_number_files = 0
        self.data_staged = 0 #In MB

        #From the scheduler perspective
        self.time_stage_in = 0
        self.time_resample = 0
        self.time_coadd = 0
        self.time_stage_out = 0

        with open(self.file, 'r') as f:
            for line in f:
                if line.startswith("Number of files kept in PFS: "):
                    start = len("Number of files kept in PFS: ")
                    end = line.index('/')
                    self.files_staged = int(line[start:end])
                    self.total_number_files = int(line[end+1:])
                if line.startswith("NODE"):
                    self.nodes = int(line.split(' ')[1])
                elif line.startswith("TASK"):
                    self.tasks = int(line.split(' ')[1])
                elif line.startswith("CORE"):
                    self.cores = int(line.split(' ')[1])
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
                    #TODO
                    self.time_stage_out = 0


class KickstartDirectory:
    """
        Directory must follow this kind of pattern:
        self.dir = swarp_cori_1w_1c
            swarp_cori_1w_1c
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
        self.dir = directory
        if not os.path.isdir(self.dir):
            raise ValueError("error: {} is not a valid directory.".format(self.dir))

        self.dir_exp = os.listdir(self.dir)
        self.log = []
        self.resample = []
        self.coadd = []
        self.log = "output.log"

        for d in self.dir_exp:
            print(d)
            #No average yet
            self.resample.append(KickstartRecord([d+'/'+"stat.resample.xml"]))
            self.coadd.append(KickstartRecord([d+'/'+"stat.coadd.xml"]))

    def root_dir(self):
        return self.dir

    def dirs(self):
        return self.dir_exp

    def parse_outputlog():
        for d in self.dir_exp:


    # def write(csv_file):
    #     with open(csv_file, 'w', newline='') as f:
    #         write = csv.writer(f, delimiter=' ',
    #                                 quotechar='|', quoting=csv.QUOTE_MINIMAL)
    #         write.writerow([])



if __name__ == "__main__":
    test_record = KickstartEntry("stat.resample.xml")
    print(test_record.path())
    print(test_record.time())
    print(test_record.efficiency())

    test_record2 = KickstartEntry("stat.combine.xml")
    print(test_record2.path())
    print(test_record2.time())
    print(test_record2.efficiency())

    exp1 = KickstartRecord(["stat.resample.xml", "stat.combine.xml"])

    print(exp1.paths())
    print(exp1.time())
    print(exp1.efficiency())

    test = KickstartDirectory("swarp_cori_1w_1c/")


