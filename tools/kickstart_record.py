#!/usr/bin/env python3

import xml.etree.ElementTree as xml

from enum import Enum,unique,auto
from pathlib import Path

XML_PREFIX="{http://pegasus.isi.edu/schema/invocation}"

@unique
class FileType(Enum):
    XML = auto()
    YAML = auto()

# class Job():
#     """
#     Job statistics from KickStart record 
#     """
#     def __init__(self, file, pid, hashmap):
#         self.file       =       file
#         self.pid        =       pid

#         # Hashmap must be the attrib from <usage> elem
#         self.utime      =       hashmap["utime"]
#         self.stime      =       hashmap["stime"]
#         self.maxrss     =       hashmap["maxrss"]
#         self.minflt     =       hashmap["minflt"]
#         self.majflt     =       hashmap["majflt"]
#         self.nswap      =       hashmap["nswap"]
#         self.inblock    =       hashmap["inblock"]
#         self.outblock   =       hashmap["outblock"]
#         self.msgsnd     =       hashmap["msgsnd"]
#         self.msgrcv     =       hashmap["msgrcv"]
#         self.nsignals   =       hashmap["nsignals"] 
#         self.nvcsw      =       hashmap["nvcsw"]
#         self.nivcsw     =       hashmap["nivcsw"]


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

        self._exec = {}
        self._exec["files"] = []
        self._stdout = []
        self._jobs = []                  #Multiple jobs?
        self._uname = {}

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

    def parse(self):
        tree = xml.ElementTree()
        tree.parse(self._path)
        root = tree.getroot()
        for elem in root.iter():
            if elem.tag == XML_PREFIX+"file" and elem.attrib['name'] != None:
                self._exec["files"].append(elem.attrib['name'])
            if elem.tag == XML_PREFIX+"cwd":
                self._exec["cwd"] = elem.text
            if elem.tag == XML_PREFIX+"uname":
                self._uname = elem.attrib
                self._uname['text']=elem.text

            if elem.tag == XML_PREFIX+"data":
                self._stdout += [elem.text]

        print (root.findall('.//usage...'))

                #self._job.(Job(file, pid, hashmap)

            # print(" tag=",elem.tag, " \ntext=", elem.text ," \ntail=", elem.tail, " \nattrib=", elem.attrib)
            # print("==========================================================================================")

        # 'usage' nodes that are children of nodes with name='invocation' (the root node)
        #root.findall(".//*[@name='invocation']/usage")
        print(tree.getroot().findall("invocation"))
        for elem in root.findall("invocation/usage"):
            print(" tag=",elem.tag, " \ntext=", elem.text ," \ntail=", elem.tail, " \nattrib=", elem.attrib)
            print("==========================================================================================")


        # # Store information about the machine
        # KickstartRecord.findall_and_store(root, "machine",  self._attr, "uname")
        # KickstartRecord.findall_and_store(root, "cwd",  self._attr, "cwd")

        # print("===")

        # for data in root.findall(XML_PREFIX+"data"):
        #     #for elem in data.iter():
        #     print(data.tag, " ", data.tail, " ", data.attrib)
        #         #self._attr["data"] = (elem.tail, elem.attrib)

        # # for statcall in root.findall("{http://pegasus.isi.edu/schema/invocation}machine"):
        # #     for child in statcall:
        # #         print(child.tag, " ", elem.text, " ", child.attrib)

        #print(self._attr)

        print(self._exec)
        print(self._uname)
        print(self._stdout)
        #print(self._job)


if __name__ == "__main__":
    test_record = KickstartRecord("example_kickstart.xml")
    print(test_record.path())
    test_record.parse()

