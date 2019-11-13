#!/usr/bin/env python3

import shutil
import os
import argparse
import glob
import fnmatch
import subprocess
import shlex                    # for shlex.split
import resource                 # for resource.getrusage

#read a file source dst

DEF_COPY_CMD  = ["cp", "-f", "-p"]
DEF_MKDIR_CMD = ["mkdir", "-p"]

# remove the common part of both string
def shorten_strings(s1, s2):
    last_valid_slash = 0

    for i in range(min(len(s1), len(s2))):
        if s1[i] != s2[i]:
            break
        if s1[i] == '/':
            last_valid_slash = i

    return (s1[last_valid_slash+1:],s2[last_valid_slash+1:], s1[:last_valid_slash+1])



def copy_fromlist(args):
    if not os.path.isfile(args.file):
        print("[error] IO: {} is not a valid file.".format(args.file))
        exit(1)

    size_files = []
    utime_files = []
    stime_files = []
    #index = 0

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

            if not fnmatch.fnmatch(file_src, args.pattern):
                continue

            if file_dest == '':
                file_dest = file_src
            
            if not os.path.isfile(src):
                raise IOError("[error] IO: {} is not a file".format(src))
            
            #if not os.path.isdir(dir_dest):
                # raise IOError("[error] IO: {} is not a valid directory".format(dir_dest))
            try:
                # os.mkdir(dir_dest)                
                subprocess.run([*DEF_MKDIR_CMD, str(dir_dest)], check=True, capture_output=True)
            except subprocess.CalledProcessError as e:
                print(e)
                raise IOError(e)

            try:
                #shutil.copy(src, dir_dest)
                usage_start = resource.getrusage(resource.RUSAGE_CHILDREN)

                if args.wrapper:
                    subprocess.run([str(args.wrapper), *DEF_COPY_CMD, str(src), str(dir_dest)], check=True, capture_output=True)
                else:
                    subprocess.run([*DEF_COPY_CMD, str(src), str(dir_dest)], check=True, capture_output=True)

                usage_end = resource.getrusage(resource.RUSAGE_CHILDREN)
                utime_files.append(usage_end.ru_utime - usage_start.ru_utime)
                stime_files.append(usage_end.ru_stime - usage_start.ru_stime)
            # except IOError as e:
            #     print(e)
            except subprocess.CalledProcessError as e:
                print(e)
                raise IOError(e)
            else:
                size_files.append(os.path.getsize(src))
                s,d,common = shorten_strings(dir_src, dir_dest)
                print("{}/{:<50} ({:.3} MB) => {:<20}".format( 
                    s,
                    file_src,
                    size_files[-1]/(1024.0**2),
                    d+'/')
                )
        #index = index + 1
        #if index >= args.count:
        #    break


def copy_dir(args):
    src = os.path.expandvars(args.src)
    dest = os.path.expandvars(args.dest)
    if not os.path.isdir(src):
        print("[error] IO: {} is not a valid directory.".format(args.file))
        exit(1)
    size_files = []
    utime_files = []
    stime_files = []

    #print(os.path.abspath(args.src))
    files_to_copy = glob.glob(src+'/'+str(os.path.expandvars(args.pattern)))
    # print (files_to_copy)
    try:
        # os.mkdir(dir_dest)
        subprocess.run([*DEF_MKDIR_CMD, str(dest)], check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        print(e)
        raise IOError(e)

    for f in files_to_copy:
        try:
            usage_start = resource.getrusage(resource.RUSAGE_CHILDREN)
            
            if args.wrapper:
                subprocess.run([str(args.wrapper), *DEF_COPY_CMD, str(f), str(args.dest)], check=True, capture_output=True)
            else:
                subprocess.run([*DEF_COPY_CMD, str(f), str(args.dest)], check=True, capture_output=True)

            usage_end = resource.getrusage(resource.RUSAGE_CHILDREN)
            utime_files.append(usage_end.ru_utime - usage_start.ru_utime)
            stime_files.append(usage_end.ru_stime - usage_start.ru_stime)
            # shutil.copy(f, args.dest)
        # except IOError as e:
        #     print(e)
        except subprocess.CalledProcessError as e:
            print(e)
        else:
            size_files.append(os.path.getsize(f))
            print("{:<50} ({:.3} MB) => {:<20}".format(
                os.path.basename(f),
                size_files[-1]/(1024.0**2),
                dest+'/')
            )

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

    parser.add_argument('--stats', '-s', type=str, nargs='?', default=' ',
                        help='Separator')

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

