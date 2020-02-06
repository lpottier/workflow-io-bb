#!/usr/bin/python3

import argparse
import collections
import csv
import logging
import os
import xml.etree.ElementTree

logger = logging.getLogger(__name__)
files = {}
runtimes = {}
jobs = {}


def _configure_logging(debug):
    """
    Configure the application's logging.
    :param debug: whether debugging is enabled
    """
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def _parse_stagein(stagein_file):
    """
    Parse a StageIn file
    :param stagein_file: a StageIn file
    """
    logger.info('Parsing StageIn file: ' + os.path.basename(stagein_file))

    with open(stagein_file, 'r') as file:
        lines = file.readlines()[1:]
        for line in lines:
            runtimes['stagein'] = line.split(' ')[5]


def _parse_kickstart(kickstart_file):
    """
    Parse a Kickstart file
    :param kickstart_file: a Kickstart file
    """
    logger.info('Parsing Kickstart file: ' + os.path.basename(kickstart_file))

    try:
        e = xml.etree.ElementTree.parse(kickstart_file).getroot()
        for j in e.findall('{http://pegasus.isi.edu/schema/invocation}mainjob'):
            for a in j.findall('{http://pegasus.isi.edu/schema/invocation}argument-vector'):
                for r in a.findall('{http://pegasus.isi.edu/schema/invocation}arg'):
                    if 'combine' in r.text:
                        runtimes['combine'] = j.get('duration')
                        break
                    elif 'resample' in r.text:
                        runtimes['resample'] = j.get('duration')
                        break
            for p in j.findall('{http://pegasus.isi.edu/schema/invocation}proc'):
                for f in p.findall('{http://pegasus.isi.edu/schema/invocation}file'):
                    files[os.path.basename(f.get('name'))] = f.get('size')

    except xml.etree.ElementTree.ParseError as ex:
        logger.warning(str(ex))


def _parse_dax(dax_file):
    """
    Parse the DAX file
    :param dax_file: the DAX file
    """
    logger.info('Parsing DAX file: ' + os.path.basename(dax_file))

    try:
        e = xml.etree.ElementTree.parse(dax_file).getroot()
        for j in e.findall('{http://pegasus.isi.edu/schema/DAX}job'):
            job = collections.OrderedDict()
            job['name'] = str(j.get('name'))
            job['id'] = str(j.get('id'))
            job['type'] = 'compute'
            job['runtime'] = str(runtimes[job['name']])
            job['parents'] = []
            job['files'] = []

            for f in j.findall('{http://pegasus.isi.edu/schema/DAX}uses'):
                file = collections.OrderedDict()
                file['link'] = f.get('link')
                file['name'] = f.get('name') if not f.get('name') == None else f.get('file')
                file['size'] = files[file['name']]
                job['files'].append(file)

            jobs[job['id']] = job

        for c in e.findall('{http://pegasus.isi.edu/schema/DAX}child'):
            for p in c.findall('{http://pegasus.isi.edu/schema/DAX}parent'):
                jobs[c.get('ref')]['parents'].append(p.get('ref'))

    except xml.etree.ElementTree.ParseError as ex:
        logger.warning(str(ex))


def _write_dax(output_file):
    """
    Write the WRENCH DAX output file
    :param output_file: WRENCH DAX output file
    """
    logger.info('Writing WRENCH DAX file: ' + os.path.basename(output_file))

    with open(output_file, 'w') as out:
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        out.write('<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-2.1.xsd" version="2.1" count="1" index="0" name="test" jobCount="3" fileCount="0" childCount="2">\n')
        for id in jobs:
            out.write('  <job id="' + id + '" name="' + jobs[id]['name'] + '" version="1.0" runtime="' + jobs[id]['runtime'] + '">\n')
            for file in jobs[id]['files']:
                out.write('    <uses file="' + file['name'] + '" link="' + file['link'] +  '" register="false" transfer="false" optional="false" type="data" size="' + file['size'] + '"/>\n')
            out.write('  </job>\n')
        for id in jobs:
            if len(jobs[id]['parents']) > 0:
                out.write('  <child ref="' + id + '">\n')
                for parent in jobs[id]['parents']:
                    out.write('    <parent ref="' + parent + '"/>\n')
                out.write('  </child>\n')
        out.write('</adag>\n')


def main():
    # Application's arguments
    parser = argparse.ArgumentParser(description='Parse Kickstart files to generate WRENCH DAX.')
    parser.add_argument('-x', '--dax', dest='dax_file', action='store', help='DAX file name', default='dax.xml')
    parser.add_argument('-k', '--kickstart', dest='kickstart_files', action='append', help='Kickstart file name')
    parser.add_argument('-i', '--stagein', dest='stagein_file', action='store', help='StageIn file name', default='stage-in.csv')
    parser.add_argument('-o', dest='output', action='store', help='Output filename', default='wrench.dax')
    parser.add_argument('-d', '--debug', action='store_true', help='Print debug messages to stderr')
    args = parser.parse_args()

    # Configure logging
    _configure_logging(args.debug)

    # Sanity check
    if not os.path.isfile(args.dax_file):
        logger.error('The provided DAX file does not exist:\n\t' + args.dax_file)
        exit(1)

    for f in args.kickstart_files:
        if not os.path.isfile(f):
            logger.error('The provided Kickstart file does not exist:\n\t' + f)
            exit(1)
        _parse_kickstart(f)

    if not os.path.isfile(args.stagein_file):
        logger.error('The provided StageIn file does not exist:\n\t' + args.stagein_file)
        exit(1)

    # parse StageIn file
    _parse_stagein(args.stagein_file)

    # parse DAX file
    _parse_dax(args.dax_file)

    # write WRENCH DAX
    _write_dax(args.output)


if __name__ == '__main__':
    main()
