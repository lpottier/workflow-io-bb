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


def _parse_stagein(stagein_file, io_fraction):
    """
    Parse a StageIn file
    :param stagein_file: a StageIn file
    """
    logger.info('Parsing StageIn file: ' + os.path.basename(stagein_file))

    with open(stagein_file, 'r') as file:
        lines = file.readlines()[1:]
        for line in lines:
            runtimes['stagein'] = float(line.split(' ')[5])*(1-float(io_fraction))


def _parse_kickstart(kickstart_file, io_fraction):
    """
    Parse a Kickstart file
    :param kickstart_file: a Kickstart file
    :param io_fraction: the fraction of the execution time taken by I/O (default: 0.0)
    """
    logger.info('Parsing Kickstart file: ' + os.path.basename(kickstart_file))
    logger.info('Using I/O fraction: ' + str(io_fraction))

    try:
        e = xml.etree.ElementTree.parse(kickstart_file).getroot()
        for j in e.findall('{http://pegasus.isi.edu/schema/invocation}mainjob'):
            for a in j.findall('{http://pegasus.isi.edu/schema/invocation}argument-vector'):
                for r in a.findall('{http://pegasus.isi.edu/schema/invocation}arg'):
                    if 'combine' in r.text:
                        runtimes['combine'] = float(j.get('duration'))*(1-float(io_fraction))
                        break
                    elif 'resample' in r.text:
                        runtimes['resample'] = float(j.get('duration'))*(1-float(io_fraction))
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
    parser.add_argument('-z', '--io', dest='io_frac', action='append', help='I/O fraction (between [0,1]) for each kickstart files (must be in the same order as kickstart files)')
    parser.add_argument('-k', '--kickstart', dest='kickstart_files', action='append', help='Kickstart file name')
    parser.add_argument('-i', '--stagein', dest='stagein_file', action='store', help='StageIn file name', default='stage-in.csv')
    parser.add_argument('--io-stagein', type=float, default=1.0, dest='io_frac_stagein', action='store', help='I/O fraction for the stage-in task (usually 1)')
    parser.add_argument('-o', dest='output', action='store', help='Output filename', default='wrench.dax')
    parser.add_argument('-d', '--debug', action='store_true', help='Print debug messages to stderr')
    args = parser.parse_args()

    # Configure logging
    _configure_logging(args.debug)

    # Sanity check
    if not os.path.isfile(args.dax_file):
        logger.error('The provided DAX file does not exist:\n\t' + args.dax_file)
        exit(1)


    if len(args.kickstart_files) != len(args.io_frac):
        logger.error("Number of kickstart files and the number of I/O fractions do not match.")
        exit(1)

    for f,io in zip(args.kickstart_files,args.io_frac):
        if not os.path.isfile(f):
            logger.error('The provided Kickstart file does not exist:\n\t' + f)
            exit(1)
        try:
            io_cast = float(io)
        except ValueError as e:
            logger.error('The I/O fraction furnished is not a number:\n\t' + e)
            exit(1)
        if io_cast < 0 or io_cast > 1:
            logger.error('The I/O fraction must not between 0 and 1:\n\t' + io_cast)
            exit(1)

        _parse_kickstart(f,io_cast)


    if not os.path.isfile(args.stagein_file):
        logger.error('The provided StageIn file does not exist:\n\t' + args.stagein_file)
        exit(1)

    # parse StageIn file
    _parse_stagein(args.stagein_file, args.io_frac_stagein)

    # parse DAX file
    _parse_dax(args.dax_file)

    # write WRENCH DAX
    _write_dax(args.output)


if __name__ == '__main__':
    main()
