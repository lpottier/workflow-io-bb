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
cores = {}
jobs = {}

SWARP_DATA = {'null': '0', 'ks.out.7SqNTw': '0', 'ks.err.VAN4u5': '15668', 'anon_inode:[eventpoll]': '0', 'pipe:[629424]': '0', 'socket:[629427]': '0', 'socket:[825042]': '0', 'pipe:[825041]': '0', 'kgni0': '0', 'ld.so.cache': '345151', 'libpthread-2.26.so': '144048', 'libm-2.26.so': '1355752', 'libc-2.26.so': '2038472', 'resample.swarp': '4982', 'Pacific': '2836', 'PTF201111234857_2_o_37754_06.w.fits': '33583680', 'PTF201111234857_2_o_37754_06.w.weight.fits': '16781760', 'PTF201111095206_2_o_34706_06.w.fits': '33583680', 'PTF201111095206_2_o_34706_06.w.weight.fits': '16781760', 'PTF201111294943_2_o_39822_06.w.fits': '33583680', 'PTF201111294943_2_o_39822_06.w.weight.fits': '16781760', 'PTF201111184953_2_o_36749_06.w.fits': '33583680', 'PTF201111184953_2_o_36749_06.w.weight.fits': '16781760', 'PTF201111284696_2_o_39396_06.w.fits': '33583680', 'PTF201111284696_2_o_39396_06.w.weight.fits': '16781760', 'PTF201111025412_2_o_33288_06.w.fits': '33583680', 'PTF201111025412_2_o_33288_06.w.weight.fits': '16781760', 'PTF201111224851_2_o_37387_06.w.fits': '33583680', 'PTF201111224851_2_o_37387_06.w.weight.fits': '16781760', 'PTF201111265053_2_o_38612_06.w.fits': '33583680', 'PTF201111265053_2_o_38612_06.w.weight.fits': '16781760', 'PTF201111274755_2_o_38996_06.w.fits': '33583680', 'PTF201111274755_2_o_38996_06.w.weight.fits': '16781760', 'PTF201111035427_2_o_33741_06.w.fits': '33583680', 'PTF201111035427_2_o_33741_06.w.weight.fits': '16781760', 'PTF201111015420_2_o_32874_06.w.fits': '33583680', 'PTF201111015420_2_o_32874_06.w.weight.fits': '16781760', 'PTF201111085228_2_o_34301_06.w.fits': '33583680', 'PTF201111085228_2_o_34301_06.w.weight.fits': '16781760', 'PTF201111165032_2_o_35994_06.w.fits': '33583680', 'PTF201111165032_2_o_35994_06.w.weight.fits': '16781760', 'PTF201111304878_2_o_40204_06.w.fits': '33583680', 'PTF201111304878_2_o_40204_06.w.weight.fits': '16781760', 'PTF201111155050_2_o_35570_06.w.fits': '33583680', 'PTF201111155050_2_o_35570_06.w.weight.fits': '16781760', 'PTF201111025428_2_o_33289_06.w.fits': '33583680', 'PTF201111025428_2_o_33289_06.w.weight.fits': '16781760', 'PTF201111234857_2_o_37754_06.w.resamp.fits': '30110400', 'PTF201111234857_2_o_37754_06.w.resamp.weight.fits': '30110400', 'PTF201111095206_2_o_34706_06.w.resamp.fits': '30075840', 'PTF201111095206_2_o_34706_06.w.resamp.weight.fits': '30075840', 'PTF201111294943_2_o_39822_06.w.resamp.fits': '30035520', 'PTF201111294943_2_o_39822_06.w.resamp.weight.fits': '30035520', 'PTF201111184953_2_o_36749_06.w.resamp.fits': '30090240', 'PTF201111184953_2_o_36749_06.w.resamp.weight.fits': '30090240', 'PTF201111284696_2_o_39396_06.w.resamp.fits': '30110400', 'PTF201111284696_2_o_39396_06.w.resamp.weight.fits': '30110400', 'PTF201111025412_2_o_33288_06.w.resamp.fits': '30032640', 'PTF201111025412_2_o_33288_06.w.resamp.weight.fits': '30032640', 'PTF201111224851_2_o_37387_06.w.resamp.fits': '30081600', 'PTF201111224851_2_o_37387_06.w.resamp.weight.fits': '30081600', 'PTF201111265053_2_o_38612_06.w.resamp.fits': '30150720', 'PTF201111265053_2_o_38612_06.w.resamp.weight.fits': '30150720', 'PTF201111274755_2_o_38996_06.w.resamp.fits': '30165120', 'PTF201111274755_2_o_38996_06.w.resamp.weight.fits': '30165120', 'PTF201111035427_2_o_33741_06.w.resamp.fits': '30084480', 'PTF201111035427_2_o_33741_06.w.resamp.weight.fits': '30084480', 'PTF201111015420_2_o_32874_06.w.resamp.fits': '30035520', 'PTF201111015420_2_o_32874_06.w.resamp.weight.fits': '30035520', 'PTF201111085228_2_o_34301_06.w.resamp.fits': '30061440', 'PTF201111085228_2_o_34301_06.w.resamp.weight.fits': '30061440', 'PTF201111165032_2_o_35994_06.w.resamp.fits': '30075840', 'PTF201111165032_2_o_35994_06.w.resamp.weight.fits': '30075840', 'PTF201111304878_2_o_40204_06.w.resamp.fits': '30119040', 'PTF201111304878_2_o_40204_06.w.resamp.weight.fits': '30119040', 'PTF201111155050_2_o_35570_06.w.resamp.fits': '30029760', 'PTF201111155050_2_o_35570_06.w.resamp.weight.fits': '30029760', 'PTF201111025428_2_o_33289_06.w.resamp.fits': '29995200', 'PTF201111025428_2_o_33289_06.w.resamp.weight.fits': '29995200', 'resample.xml': '24585', 'online': '4096', 'libgcc_s.so.1': '96928', 'ks.out.TdNgBR': '0', 'ks.err.btmvug': '7811', 'pipe:[622190]': '0', 'socket:[622193]': '0', 'socket:[827395]': '0', 'pipe:[827394]': '0', 'combine.swarp': '4982', 'coadd.fits': '51845760', 'coadd.weight.fits': '51845760', 'combine.xml': '26007'}

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


def _parse_stagein(stagein_file, io_fraction, core_stagein):
    """
    Parse a StageIn file
    :param stagein_file: a StageIn file
    :param io_fraction:  I/O fraction of the stage-in task (1 if only I/O)
    :param core_stagein: number of cores to use
    """
    logger.info('Parsing StageIn file: ' + os.path.basename(stagein_file))
    cores['stagein'] = core_stagein
    with open(stagein_file, 'r') as file:
        lines = file.readlines()[1:]
        for line in lines:
            # We set the runtime of stage in at 0, the simulator we will simulate that execution time
            runtimes['stagein'] = float(line.split(' ')[5])*(1-float(io_fraction))


def _parse_kickstart(kickstart_file, io_fraction, core):
    """
    Parse a Kickstart file
    :param kickstart_file: a Kickstart file
    :param io_fraction: the fraction of the execution time taken by I/O
    :param core: the number of cores the task can use
    """
    logger.info('Parsing Kickstart file: ' + os.path.basename(kickstart_file))
    logger.info('Using I/O fraction: ' + str(io_fraction))
    
    global files

    try:
        e = xml.etree.ElementTree.parse(kickstart_file).getroot()
        for j in e.findall('{http://pegasus.isi.edu/schema/invocation}mainjob'):
            for a in j.findall('{http://pegasus.isi.edu/schema/invocation}argument-vector'):
                for r in a.findall('{http://pegasus.isi.edu/schema/invocation}arg'):
                    if 'combine' in r.text:
                        runtimes['combine'] = float(j.get('duration'))*(1-float(io_fraction)) * float(core) #/(core*2.3*16*10**9)
                        cores['combine'] = core
                        break
                    elif 'resample' in r.text:
                        runtimes['resample'] = float(j.get('duration'))*(1-float(io_fraction)) * float(core) #/(core*2.3*16*10**9)
                        cores['resample'] = core
                        break

            for p in j.findall('{http://pegasus.isi.edu/schema/invocation}proc'):
                for f in p.findall('{http://pegasus.isi.edu/schema/invocation}file'):
                    files[os.path.basename(f.get('name'))] = f.get('size')

    except xml.etree.ElementTree.ParseError as ex:
        logger.warning(str(ex))

    if files == {}:
        logger.warning("It appears that the Kickstart record: {0} does not contain file size information. To include file size and I/O runs Kickstart with the option -z or -Z".format(kickstart_file))
        ## Use SWARP data: Temporary fix
        files = SWARP_DATA

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
            job['core'] = str(cores[job['name']])
            job['parents'] = []
            job['files'] = []
            
            for f in j.findall('{http://pegasus.isi.edu/schema/DAX}uses'):
                file = collections.OrderedDict()
                file['link'] = f.get('link')
                file['name'] = f.get('name') if not f.get('name') == None else f.get('file')
                if len(file['name'].split('-')) == 2:
                    #Dirty fix:  To deal with multi pipeline workflow where some files are prefixed with 'W'X'-' where X is the id of the pipeline
                    file['size'] = files[file['name'].split('-')[1]]
                else:
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
            out.write('  <job id="' + id + '" name="' + jobs[id]['name'] + '" version="1.0" runtime="' + jobs[id]['runtime'] + '" numcores="' + jobs[id]['core'] + '">\n')
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
    parser.add_argument('-c', '--cores', dest='core_tasks', action='append', help='Number of cores per tasks (must be in the same order as kickstart files!!)')
    parser.add_argument('--cores-stagein', dest='core_stagein', action='store', help='Number of cores for stage-in (must be in the same order as kickstart files!!)')
    parser.add_argument('-o', dest='output', action='store', help='Output filename', default='wrench.dax')
    parser.add_argument('-d', '--debug', action='store_true', help='Print debug messages to stderr')
    args = parser.parse_args()

    # Configure logging
    _configure_logging(args.debug)

    # Sanity check
    if not os.path.isfile(args.dax_file):
        logger.error('The provided DAX file does not exist:\n\t' + args.dax_file)
        exit(1)

    if len(args.kickstart_files) != 2 or len(args.kickstart_files) != 2 or len(args.core_tasks) != 2:
        logger.error("Must furnish 2 kickstart files, 2 io-fraction and 2 cores values")
        exit(1)

    if len(args.kickstart_files) != len(args.io_frac) != len(args.core_tasks):
        logger.error("Number of kickstart files, the number of I/O fractions do not match and the number of cores must match.")
        exit(1)

    for f,io,core in zip(args.kickstart_files, args.io_frac, args.core_tasks):
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

        if int(core) <= 0:
            logger.error('The number of cores must be positive:\n\t' + core)
            exit(1)

        _parse_kickstart(f, io_cast, core)


    if not os.path.isfile(args.stagein_file):
        logger.error('The provided StageIn file does not exist:\n\t' + args.stagein_file)
        exit(1)

    # parse StageIn file
    _parse_stagein(args.stagein_file, args.io_frac_stagein, args.core_stagein)

    # parse DAX file
    _parse_dax(args.dax_file)

    # write WRENCH DAX
    _write_dax(args.output)


if __name__ == '__main__':
    main()
