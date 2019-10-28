#!/usr/bin/env python3

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
        self._path = path
        self._ftype = file_type

    def path():
        path = str(self._path)
        return path

if __name__ == "__main__":
    pass