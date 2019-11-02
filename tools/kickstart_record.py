#!/usr/bin/env python3

import xml.etree.ElementTree as xml

from enum import Enum,unique,auto
from pathlib import Path

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
        return s

    def __repr__(self):
        s = ''
        for name,val in vars(self).items():
            if val == {}:
                val = "Not found"
            s = s + '{:<10} -> {:<72}\n'.format(name, str(val))
        return s

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
        return s

    def __repr__(self):
        s = ''
        for name,val in vars(self).items():
            s = s + '{:<8} -> {:>10}\n'.format(name, str(val))
        return s


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
        return s

    def __repr__(self):
        s = ''
        for name,val in vars(self).items():
            if name == "files":
                continue
            s = s + '{:<10} -> {:<10}\n'.format(name, str(val))
        return s

class Usage:
    """
    Usage statistics from KickStart record 
    """
    def __init__(self, xml_tree):
        # Hashmap must be the attrib from <usage> elem
        self.utime      =       xml_tree.attrib["utime"]
        self.stime      =       xml_tree.attrib["stime"]
        self.maxrss     =       xml_tree.attrib["maxrss"]
        self.minflt     =       xml_tree.attrib["minflt"]
        self.majflt     =       xml_tree.attrib["majflt"]
        self.nswap      =       xml_tree.attrib["nswap"]
        self.inblock    =       xml_tree.attrib["inblock"]
        self.outblock   =       xml_tree.attrib["outblock"]
        self.msgsnd     =       xml_tree.attrib["msgsnd"]
        self.msgrcv     =       xml_tree.attrib["msgrcv"]
        self.nsignals   =       xml_tree.attrib["nsignals"] 
        self.nvcsw      =       xml_tree.attrib["nvcsw"]
        self.nivcsw     =       xml_tree.attrib["nivcsw"]

        # Data from statcall: stdin, stdout, stderr and metadata
        self.data = {}
        for elem in xml_tree:
            if elem.tag == XML_PREFIX+"statcall":
                self.data[elem.attrib["id"]] = None
                for data in elem:
                    if data.tag == XML_PREFIX+"data":
                        self.data[elem.attrib["id"]] = data.text

    def __str__(self):
        s = ''
        for name,val in vars(self).items():
            s = s + '{:<10} -> {:<10}\n'.format(name, str(val))
        return s

    def __repr__(self):
        s = ''
        for name,val in vars(self).items():
            s = s + '{:<10} -> {:<10}\n'.format(name, str(val))
        return s

class KickstartRecord(object):
    """
    KickstartRecord
    """
    def __init__(self, path, file_type=FileType.XML):
        super(KickstartRecord, self).__init__()

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
        print(self.machine)

        # Usage (global, not mainjob)
        self.usage = Usage(self._get_elem("usage"))
        print(self.usage)

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

if __name__ == "__main__":
    test_record = KickstartRecord("stat.resample.xml")
    print(test_record.path())

