#!/usr/bin/env python3

import xml.etree.ElementTree as xml

from enum import Enum,unique,auto
from pathlib import Path

@unique
class FileType(Enum):
    XML = auto()
    YAML = auto()

class KickstartRecord(object):
    """
    docstring for KickstartRecord
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

        self._attr = {}

    def path(self):
        return self._path

    def stat(self):
        return self._path.stat()

    def parse(self):
        tree = xml.parse(self._path)
        root = tree.getroot()
        for child in root:
            print(child.tag, " ", child.attrib)
        print("===")

        # Store machine information
        machines = root.findall("{http://pegasus.isi.edu/schema/invocation}machine")
        if not machines:  # careful!
            print ("{http://pegasus.isi.edu/schema/invocation}machine not found, or element has no subelements")

        if machines is None:
            print ("{http://pegasus.isi.edu/schema/invocation}machine not found")

        for machine in machines:
            for elem in machine.iter():
                if elem.tag == "{http://pegasus.isi.edu/schema/invocation}uname":
                    self._attr["uname"] = (elem.text, elem.attrib)


        cwd = root.findall("{http://pegasus.isi.edu/schema/invocation}cwd")
        for command in root.findall("{http://pegasus.isi.edu/schema/invocation}cwd"):
            for elem in command.iter():
                self._attr["cwd"] = (elem.text, elem.attrib)

        for data in root.findall("{http://pegasus.isi.edu/schema/invocation}data"):
            for elem in data.iter():
                print(elem.tag, " ", elem.text, " ", elem.attrib)
                #self.data["cwd"] = (elem.text, elem.attrib)

        # for statcall in root.findall("{http://pegasus.isi.edu/schema/invocation}machine"):
        #     for child in statcall:
        #         print(child.tag, " ", elem.text, " ", child.attrib)


if __name__ == "__main__":
    test_record = KickstartRecord("example_kickstart.xml")
    print(test_record.path())
    test_record.parse()

