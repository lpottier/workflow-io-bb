#!/usr/bin/env python3

import shutil
import os
import argparse
import glob
import fnmatch
#read a file source dst

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
    time_files = []
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
                os.mkdir(dir_dest)
            except FileExistsError as e:
                pass

            try:
                shutil.copy(src, dir_dest)
            except IOError as e:
                print(e)
            else:
                size_files.append(os.path.getsize(src))
                s,d,common = shorten_strings(dir_src, dir_dest)
                print("{}/{:<50} ({:.3} MB) => {:<20}".format( 
                    s,
                    file_src,
                    size_files[-1]/(1024.0**2),
                    d+'/')
                )


def copy_dir(args):
    src = os.path.expandvars(args.src)
    dest = os.path.expandvars(args.dest)
    if not os.path.isdir(src):
        print("[error] IO: {} is not a valid directory.".format(args.file))
        exit(1)
    size_files = []
    time_files = []

    #print(os.path.abspath(args.src))
    files_to_copy = glob.glob(src+'/'+str(os.path.expandvars(args.pattern)))
    # print (files_to_copy)
    if not os.path.exists(dest):
        try:
            os.mkdir(dest)
        except FileExistsError as e:
            pass
    for f in files_to_copy:
        try:
            shutil.copy(f, args.dest)
        except IOError as e:
            print(e)
        else:
            size_files.append(os.path.getsize(f))
            print("{}/{:<50} ({:.3} MB) => {:<20}".format(
                os.path.basename(src), 
                os.path.basename(f),
                size_files[-1]/(1024.0**2),
                os.path.dirname(dest))
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

    parser.add_argument('--sep', type=str, nargs='?', default=' ',
                        help='Separator')

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

    if args.file != None and args.src == None and args.dest == None:
        copy_fromlist(args)
    elif args.file == None and args.src != None and args.dest != None:
         copy_dir(args)

