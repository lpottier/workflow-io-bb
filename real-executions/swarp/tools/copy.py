#!/usr/bin/env python3

import shutil
import sys
import os
import argparse
import glob
import fnmatch
import subprocess
import shlex                    # for shlex.split
import resource                 # for resource.getrusage
import csv
import statistics
from timeit import default_timer as timer

#read a file source dst

DEF_COPY_CMD  = ["cp", "-f", "-p"]
DEF_MKDIR_CMD = ["mkdir", "-p"]
VERBOSE = 0


# remove the common part of both string
def shorten_strings(s1, s2):
    last_valid_slash = 0

    for i in range(min(len(s1), len(s2))):
        if s1[i] != s2[i]:
            break
        if s1[i] == '/':
            last_valid_slash = i

    return (s1[last_valid_slash+1:],s2[last_valid_slash+1:], s1[:last_valid_slash+1])

def wrap_cmd(cmdline):
    try:
        if args.wrapper:
            cmdline = shlex.split(args.wrapper) + cmdline
        subprocess.run(cmdline, check=True)
    except subprocess.CalledProcessError as e:
        return False
    else:
        return True

# Needed for interface like IBM MVMe burst buffers
# Without that python like open() will fail

def dir_exists(path):
    return wrap_cmd(["test", "-d", str(path)])

def file_exists(path):
    return wrap_cmd(["test", "-f", str(path)])

def create_file(file):
    return wrap_cmd(["touch", str(file)])

def copy_fromlist(args):
    if not os.path.isfile(args.file):
        print("[error] IO: {} is not a valid file.".format(args.file))
        exit(1)

    files_notransfered = []
    files_transfered = []
    size_files = []
    size_files_notransfer = []
    utime_files = []
    stime_files = []
    #index = 0

    start_duration = timer()
    global_start = resource.getrusage(resource.RUSAGE_CHILDREN)
    with open(args.file, 'r') as f:
        for line in f:
            try:
                src, dest = [e for e in line.split(args.sep) if len(e) > 0]
            except ValueError as e:
                print(e)
                exit(1)
            src = os.path.expandvars(src)
            dest = os.path.expandvars(dest)
            file_src = os.path.basename(src)
            dir_src = os.path.dirname(src)

            file_dest = os.path.basename(dest)
            dir_dest = os.path.dirname(dest)

            if file_dest == '':
                file_dest = file_src

            if not os.path.isfile(src):
                #raise IOError("[error] IO: {} is not a file".format(src))
                if VERBOSE == 1:
                    print("[warning] directory {} skipped".format(src), file=sys.stderr)
                continue

            if not fnmatch.fnmatch(file_src, args.pattern) or (dir_src == dir_dest and file_src == file_dest):
                if VERBOSE == 1:
                    print("{} skipped.".format(file_src), file=sys.stderr)
                else:
                    pass

                files_notransfered.append((dir_src, dir_dest, file_src))
                size_files_notransfer.append(os.path.getsize(src))
                continue

            #if not os.path.isdir(dir_dest):
                # raise IOError("[error] IO: {} is not a valid directory".format(dir_dest))
            if not dir_exists(dir_dest) or True:
                try:
                    # os.mkdir(dir_dest)
                    cmdline = [*DEF_MKDIR_CMD, str(dir_dest)]     
                    if args.wrapper:
                        cmdline = shlex.split(args.wrapper) + cmdline

                    subprocess.run(cmdline, check=True)
                except subprocess.CalledProcessError as e:
                    if VERBOSE == 1:
                        print(e, file=sys.stderr)
                    else:
                        pass
            
            try:
                #shutil.copy(src, dir_dest)
                cmdline = [*DEF_COPY_CMD, str(src), str(dir_dest)+'/']
                if args.wrapper:
                    cmdline = shlex.split(args.wrapper) + cmdline

                usage_start = resource.getrusage(resource.RUSAGE_CHILDREN)
                subprocess.run(cmdline, check=True)
                
                usage_end = resource.getrusage(resource.RUSAGE_CHILDREN)
                utime_files.append(usage_end.ru_utime - usage_start.ru_utime)
                stime_files.append(usage_end.ru_stime - usage_start.ru_stime)
            # except IOError as e:
            #     print(e)
            except subprocess.CalledProcessError as e:
                size_files_notransfer.append(os.path.getsize(src))
                if VERBOSE == 1:
                    print(e, file=sys.stderr)
                else:
                    pass
            else:
                size_files.append(os.path.getsize(src))
                s,d,common = shorten_strings(dir_src, dir_dest)
                print("/{}/{:<50} ({:.3} MB) => /{:<20}".format( 
                    s,
                    file_src,
                    size_files[-1]/(1024.0**2),
                    d+'/'),
                    file=sys.stderr)
                files_transfered.append((dir_src, dir_dest, file_src))

    global_end = resource.getrusage(resource.RUSAGE_CHILDREN)
    total_duration = timer() - start_duration
    global_utime = global_end.ru_utime - global_start.ru_utime
    global_stime = global_end.ru_stime - global_start.ru_stime

    total_data_notransfer = sum(size_files_notransfer)/(1024.0**2)
    total_data = sum(size_files)/(1024.0**2)
    total_utime = sum(utime_files)
    total_stime = sum(stime_files)
    total_time = float(total_utime+total_stime)
    try:
        efficiency = total_stime / total_time
        bandwith = total_data / total_time
    except ZeroDivisionError as e:
        efficiency = 0.0
        bandwith = 0.0
    finally:
        print("{:<20}: {:.5}".format("SIZE (MB)", total_data) )
        print("{:<20}: {:.5} {:.5}".format("TIME (S)", total_time, global_utime+global_stime) )
        print("{:<20}: {:.5}".format("DURATION (S)", total_duration) )
        print("{:<20}: {:.5}".format("EFFICIENCY", efficiency) )
        print("{:<20}: {:.5}".format("BANDWITH (MB/S)", bandwith) )

    if args.stats != None:
        if args.dir != '':
            if not dir_exists(args.dir):
                try:
                    # os.mkdir(dir_dest)
                    cmdline = [*DEF_MKDIR_CMD, str(args.dir)]
                    if args.wrapper:
                        cmdline = shlex.split(args.wrapper) + cmdline
                    subprocess.run(cmdline, check=True)

                except subprocess.CalledProcessError as e:
                    if VERBOSE == 1:
                        print(e, file=sys.stderr)
                    else:
                        pass
            args.dir = args.dir+'/'

        header = ["SRC", "DEST", "FILE", "SIZE(MB)", "TOTAL(S)", "STIME(S)", "UTIME(S)"]
        
        create_file(str(args.dir)+str(args.stats)+"-pfs.csv")
        create_file(str(args.dir)+str(args.stats)+"-bb.csv")

        with open(str(args.dir)+str(args.stats)+"-pfs.csv", 'w', newline='') as pfs_file, open(str(args.dir)+str(args.stats)+"-bb.csv", 'w', newline='') as bb_file:
            writer_pfs = csv.writer(pfs_file, delimiter=' ',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer_bb = csv.writer(bb_file, delimiter=' ',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer_pfs.writerow(header)
            writer_bb.writerow(header)

            for i in range(len(files_notransfered)):
                writer_pfs.writerow([
                    files_notransfered[i][0],
                    files_notransfered[i][1],
                    files_notransfered[i][2], 
                    os.path.getsize(files_notransfered[i])/(1024.0**2),
                    0.0,
                    0.0,
                    0.0
                    ]
                )
            for i in range(len(files_transfered)):
                writer_bb.writerow([
                    files_transfered[i][0],
                    files_transfered[i][1],
                    files_transfered[i][2],
                    size_files[i]/(1024.0**2), 
                    utime_files[i]+stime_files[i],
                    utime_files[i],
                    stime_files[i]
                    ]
                )

        header = ["NB_FILES", "TOTAL_SIZE(MB)", "NB_FILES_TRANSFERED",  "TRANSFERED_SIZE(MB)", "TRANSFER_RATIO", "DURATION(S)",  "UTIME(S)", "STIME(S)", "BANDWIDTH(MB/S)", "EFFICIENCY", "AVG_UTIME(S)",  "SD_UTIME", "AVG_STIME(S)", "SD_STIME"]
        
        create_file(str(args.dir)+str(args.stats)+"-pfs-global.csv")
        create_file(str(args.dir)+str(args.stats)+"-bb-global.csv")

        with open(str(args.dir)+str(args.stats)+"-pfs-global.csv", 'w', newline='') as pfs_file, open(str(args.dir)+str(args.stats)+"-bb-global.csv", 'w', newline='') as bb_file:
            writer_pfs = csv.writer(pfs_file, delimiter=' ',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer_bb = csv.writer(bb_file, delimiter=' ',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer_pfs.writerow(header)
            writer_bb.writerow(header)

            try:
                ratio = float(total_data - total_data_notransfer)/float(total_data)
            except ZeroDivisionError as e:
                ratio = 0

            try:
                utime_mean = statistics.mean(utime_files)
                utime_sd = statistics.stdev(utime_files)
                stime_mean = statistics.mean(stime_files)
                stime_sd = statistics.stdev(stime_files)
            except statistics.StatisticsError as e:
                utime_mean = 0.0
                utime_sd = 0.0
                stime_mean = 0.0
                stime_sd = 0.0


            writer_pfs.writerow([
                len(files_notransfered)+len(files_transfered),
                total_data_notransfer+total_data,
                0,
                0.0,
                1 - ratio,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0
            ])


            writer_bb.writerow([
                len(files_notransfered)+len(files_transfered),
                total_data_notransfer+total_data,
                len(files_transfered),
                total_data,
                ratio,
                total_duration,
                global_utime,
                global_stime,
                total_data/total_duration,
                (global_utime+global_stime)/total_duration,
                utime_mean,
                utime_sd, 
                stime_mean,
                stime_sd,
            ])

def copy_dir(args):
    src = os.path.expandvars(args.src)
    dest = os.path.expandvars(args.dest)
    if src == dest:
        return
    if not os.path.isdir(src):
        print("[error] IO: {} is not a valid directory.".format(args.file))
        exit(1)
    size_files = []
    utime_files = []
    stime_files = []
    files_notransfered = []
    files_transfered = []
    size_files_notransfer = []

    #print(os.path.abspath(args.src))
    all_files = glob.glob(src+'/*')
    files_to_copy = glob.glob(src+'/'+str(os.path.expandvars(args.pattern)))
    files_to_copy = [f for f in files_to_copy if os.path.isfile(f)]
    # print (files_to_copy)
    if not dir_exists(dest):
        try:
            # os.mkdir(dir_dest)
            cmdline = [*DEF_MKDIR_CMD, str(dest)]
            if args.wrapper:
                cmdline = shlex.split(args.wrapper) + cmdline
            subprocess.run(cmdline, check=True)

        except subprocess.CalledProcessError as e:
            if VERBOSE == 1:
                print(e, file=sys.stderr)
            else:
                pass

    start_duration = timer()
    global_start = resource.getrusage(resource.RUSAGE_CHILDREN)
    for f in files_to_copy:
        try:
            cmdline = [*DEF_COPY_CMD, str(f), str(args.dest)+'/']
            if args.wrapper:
                cmdline = shlex.split(args.wrapper) + cmdline

            usage_start = resource.getrusage(resource.RUSAGE_CHILDREN)                
            
            subprocess.run(cmdline, check=True)

            usage_end = resource.getrusage(resource.RUSAGE_CHILDREN)
            utime_files.append(usage_end.ru_utime - usage_start.ru_utime)
            stime_files.append(usage_end.ru_stime - usage_start.ru_stime)

        except subprocess.CalledProcessError as e:
            size_files_notransfer.append(os.path.getsize(f))
            files_notransfered.append(f)
            if VERBOSE == 1:
                print(e, file=sys.stderr)
            else:
                pass
        else:
            size_files.append(os.path.getsize(f))
            print("{:<50} ({:.3} MB) => {:<20}".format(
                os.path.basename(f),
                size_files[-1]/(1024.0**2),
                dest+'/'),
                file=sys.stderr)
            files_transfered.append(f)

    global_end = resource.getrusage(resource.RUSAGE_CHILDREN)
    total_duration = timer() - start_duration
    global_utime = global_end.ru_utime - global_start.ru_utime
    global_stime = global_end.ru_stime - global_start.ru_stime

    total_data = sum(size_files)/(1024.0**2)
    total_data_notransfer = (len(all_files)-len(size_files))/(1024.0**2)
    total_utime = sum(utime_files)
    total_stime = sum(stime_files)
    total_time = float(total_utime+total_stime)
    try:
        efficiency = total_stime / total_time
        bandwith = total_data / total_time
    except ZeroDivisionError as e:
        efficiency = 0.0
        bandwith = 0.0
    finally:
        print("{:<20}: {:.5}".format("SIZE (MB)", total_data) )
        print("{:<20}: {:.5} {:.5}".format("TIME (S)", total_time, global_utime+global_stime) )
        print("{:<20}: {:.5}".format("DURATION (S)", total_duration ))
        print("{:<20}: {:.5}".format("EFFICIENCY", efficiency) )
        print("{:<20}: {:.5}".format("BANDWITH (MB/S)", bandwith) )

    if args.stats != None:
        if args.dir != '':
            if not dir_exists(args.dir):
                try:
                    # os.mkdir(dir_dest)
                    cmdline = [*DEF_MKDIR_CMD, str(args.dir)]
                    if args.wrapper:
                        cmdline = shlex.split(args.wrapper) + cmdline
                    subprocess.run(cmdline, check=True)

                except subprocess.CalledProcessError as e:
                    if VERBOSE == 1:
                        print(e, file=sys.stderr)
                    else:
                        pass
            args.dir = args.dir+'/'

        header = ["SRC", "DEST", "FILE", "SIZE(MB)", "TOTAL(S)", "UTIME(S)", "STIME(S)"]

        create_file(str(args.dir)+str(args.stats)+"-pfs.csv")
        create_file(str(args.dir)+str(args.stats)+"-bb.csv")

        with open(str(args.dir)+str(args.stats)+"-pfs.csv", 'w', newline='') as pfs_file, open(str(args.dir)+str(args.stats)+"-bb.csv", 'w', newline='') as bb_file:
            writer_pfs = csv.writer(pfs_file, delimiter=' ',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer_bb = csv.writer(bb_file, delimiter=' ',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer_pfs.writerow(header)
            writer_bb.writerow(header)
            
            for i in range(len(files_notransfered)):
                writer_pfs.writerow([
                    src,
                    src,
                    files_notransfered[i], 
                    os.path.getsize(files_notransfered[i])/(1024.0**2),
                    0.0,
                    0.0,
                    0.0
                    ]
                )

            for i in range(len(files_transfered)):
                writer_bb.writerow([
                    src,
                    dest,
                    files_transfered[i],
                    size_files[i]/(1024.0**2), 
                    utime_files[i]+stime_files[i],
                    utime_files[i],
                    stime_files[i]
                    ]
                )

        header = ["NB_FILES", "TOTAL_SIZE(MB)", "NB_FILES_TRANSFERED",  "TRANSFERED_SIZE(MB)", "TRANSFER_RATIO", "DURATION(S)",  "UTIME(S)", "STIME(S)", "BANDWIDTH(MB/S)", "EFFICIENCY", "AVG_UTIME(S)",  "SD_UTIME", "AVG_STIME(S)", "SD_STIME"]
        
        create_file(str(args.dir)+str(args.stats)+"-pfs-global.csv")
        create_file(str(args.dir)+str(args.stats)+"-bb-global.csv")

        with open(str(args.dir)+str(args.stats)+"-pfs-global.csv", 'w', newline='') as pfs_file, open(str(args.dir)+str(args.stats)+"-bb-global.csv", 'w', newline='') as bb_file:
            writer_pfs = csv.writer(pfs_file, delimiter=' ',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer_bb = csv.writer(bb_file, delimiter=' ',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer_pfs.writerow(header)
            writer_bb.writerow(header)

            try:
                ratio = float(total_data - total_data_notransfer)/float(total_data)
            except ZeroDivisionError as e:
                ratio = 0

            try:
                utime_mean = statistics.mean(utime_files)
                utime_sd = statistics.stdev(utime_files)
                stime_mean = statistics.mean(stime_files)
                stime_sd = statistics.stdev(stime_files)
            except statistics.StatisticsError as e:
                utime_mean = 0.0
                utime_sd = 0.0
                stime_mean = 0.0
                stime_sd = 0.0


            writer_pfs.writerow([
                len(all_files),
                sum([os.path.getsize(x)/(1024.0**2) for x in all_files]),
                0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0
            ])

            writer_bb.writerow([
                    len(all_files),
                    sum([os.path.getsize(x)/(1024.0**2) for x in all_files]),
                    len(size_files),
                    total_data,
                    ratio,
                    total_duration,
                    global_utime,
                    global_stime,
                    total_data/total_duration,
                    (global_utime+global_stime)/total_duration,
                    utime_mean,
                    utime_sd, 
                    stime_mean,
                    stime_sd,
            ])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Copy files')

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument('--src', '-i', type=str, nargs='?',
                        help='Source directory')

    group.add_argument('--file', '-f', type=str, nargs='?',
                        help='File with src dest')

    parser.add_argument('--dest', '-o', type=str, nargs='?',
                        help='Destination directory')

    #parser.add_argument('--count', '-c', type=int, nargs='?', default=float('inf'),
    #                    help='Number of files copied')

    parser.add_argument('--sep', type=str, nargs='?', default=' ',
                        help='Separator')

    parser.add_argument('--wrapper', '-w', type=str, nargs='?', default=None,
                       help='Wrapper command (for ex. "jsrun -n1" on Summit')

    parser.add_argument('--dir', '-d', type=str, nargs='?', default='',
                       help='Set the directory to output files')

    parser.add_argument('--stats', '-a', type=str, nargs='?', default='stage-in',
                       help='Output file allocations in stage-in-bb.csv and stage-in-pfs.csv')

    parser.add_argument('--reversed', '-r', type=str, nargs='?', required=False,
            help='Output reversed file (can be used as input to reverse the copy)')

    parser.add_argument('--pattern', '-p', type=str, nargs='?', default='*',
            help='Copy only files that match the pattern (for ex. "PTF201111*.w.fits"), all files matched by default.')

    args = parser.parse_args()

    if args.src != None and args.dest == None:
        print("[error] argument: --dest DIR is missing.".format(args.file))
        exit(1)

    # shlex.split is similar to s.split(' ') but follow POSIX argument and handles complex cases
    # shlex.split is better to split arguments from POSIX-like comments
    if args.wrapper != None and shutil.which(shlex.split(args.wrapper)[0]) == None:
        print("Warning: wrapper -> {} does not seem to exist, no wrapper will be used.".format(shlex.split(args.wrapper)[0]))
        args.wrapper = None

    if args.file != None and args.src == None and args.dest == None:
        copy_fromlist(args)
    elif args.file == None and args.src != None and args.dest != None:
         copy_dir(args)

