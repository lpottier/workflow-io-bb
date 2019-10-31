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
                self.ram = elem.attrib
                self.swap = elem.attrib
                self.cpu = elem.attrib
                self.load = elem.attrib
                self.procs = elem.attrib
                self.task = elem.attrib
            

    def __str__(self):
        s = ''
        for name,val in vars(self).items():
            if val == {}:
                val = "Not found"
            s = s + '{:<8} -> {:>40}\n'.format(name, val)
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


class Processus:
    """
    Job statistics from KickStart record 
    """
    def __init__(self, hashmap):
        self.pid        =       hashmap["pid"]
        self.ppid       =       hashmap["ppid"]

        # Hashmap must be the attrib from <proc> elem
        self.exe        =       hashmap["exe"]
        self.start      =       hashmap["start"]
        self.stop       =       hashmap["stop"]
        self.utime      =       hashmap["utime"]
        self.stime      =       hashmap["stime"]
        self.iowait     =       hashmap["iowait"]
        self.finthreads =       hashmap["finthreads"]
        self.maxthreads =       hashmap["maxthreads"]
        self.totthreads =       hashmap["totthreads"]
        self.vmpeak     =       hashmap["vmpeak"]
        self.rsspeak    =       hashmap["rsspeak"]
        self.rchar      =       hashmap["rchar"]
        self.wchar      =       hashmap["wchar"]
        self.rbytes     =       hashmap["rbytes"]
        self.wbytes     =       hashmap["wbytes"]
        self.cwbytes    =       hashmap["cwbytes"]
        self.syscr      =       hashmap["syscr"]
        self.syscw      =       hashmap["syscw"]


class Usage:
    """
    Usage statistics from KickStart record 
    """
    def __init__(self, hashmap):
        # Hashmap must be the attrib from <usage> elem
        self.utime      =       hashmap["utime"]
        self.stime      =       hashmap["stime"]
        self.maxrss     =       hashmap["maxrss"]
        self.minflt     =       hashmap["minflt"]
        self.majflt     =       hashmap["majflt"]
        self.nswap      =       hashmap["nswap"]
        self.inblock    =       hashmap["inblock"]
        self.outblock   =       hashmap["outblock"]
        self.msgsnd     =       hashmap["msgsnd"]
        self.msgrcv     =       hashmap["msgrcv"]
        self.nsignals   =       hashmap["nsignals"] 
        self.nvcsw      =       hashmap["nvcsw"]
        self.nivcsw     =       hashmap["nivcsw"]


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

        self._exec = {}
        self._exec["files"] = []
        self._statcalls = {}
        self._jobs = []                  #Multiple jobs?
        self.machine = Machine(self._get_elem("machine"))
        print(self.machine)

        self._get_statcalls_data()

    def path(self):
        return self._path

    def stat(self):
        return self._path.stat()

    # @staticmethod
    # def findall_and_store(xml_root, elem, hashmap, key):
    #     # Store information about the machine
    #     target = XML_PREFIX+elem
    #     targets_found = xml_root.findall(target)

    #     if not targets_found:
    #         print ("{} not found, or it has no subelements".format(target))
    #     if targets_found is None:
    #         print ("{} not found".format(target))

    #     for elem in targets_found:
    #         for e in elem.iter():
    #             if e.tag == XML_PREFIX+key:
    #                 hashmap[key] = (e.text, e.attrib)

    def _get_statcalls_data(self):
        for elem in self._root:
            if elem.tag == XML_PREFIX+"statcall":
                self._statcalls[elem.attrib["id"]] = None
                for data in elem:
                    if data.tag == XML_PREFIX+"data":
                        self._statcalls[elem.attrib["id"]] = data.text

    def _get_elem(self, key):
        for elem in self._root:
            if elem.tag == XML_PREFIX+str(key):
                return elem
        return None

    def parse(self):
        tree = xml.ElementTree()
        tree.parse(self._path)
        root = tree.getroot()
        for elem in root:
            if elem.tag == XML_PREFIX+"usage":
                self._job = elem.attrib
            # print(" tag=",elem.tag, " \ntext=", elem.text ," \ntail=", elem.tail, " \nattrib=", elem.attrib)
            # print("==========================================================================================")

        for elem in root.iter():
            if elem.tag == XML_PREFIX+"file" and elem.attrib['name'] != None:
                self._exec["files"].append(elem.attrib['name'])
            if elem.tag == XML_PREFIX+"cwd":
                self._exec["cwd"] = elem.text
            if elem.tag == XML_PREFIX+"uname":
                self._uname = elem.attrib
                self._uname['text']=elem.text

        #print(self._exec)
        print(self._uname)
        
        print(self._statcalls.keys())
        print(self._job)


if __name__ == "__main__":
    test_record = KickstartRecord("stat.resample.xml")
    print(test_record.path())
    test_record.parse()

