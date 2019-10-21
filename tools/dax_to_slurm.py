#!/usr/bin/env python3

import os
import xml.etree.ElementTree as ET

def parse_dax_xml(dax_xml_file):
    # create element tree object 
    tree = ET.parse(dax_xml_file) 
  
    # get root element 
    root = tree.getroot()
    print(root)

if __name__ == '__main__':
    print("Generate Slurm compatible workflow from DAX files")

    parse_dax_xml("dax.xml");
    
