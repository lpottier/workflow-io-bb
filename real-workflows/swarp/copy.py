#!/usr/bin/env python3

import shutil
import os
import argparse

#read a file source dst

def copy(args):
    with open(args.input, 'r') as f:
        for line in f:
            src,dest = [e for e in line.split(args.sep) if len(e) > 0]
            if os.path.isfile(src):
                if os.path.basename(dest) == '' and os.path.isdir(dest):
                    try:
                        shutil.copy2(src, dest)
                    except IOError as e:
                        print(e)
                

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Copy files')
    
    parser.add_argument('--input', '-I', type=str, nargs='?', required=True,
                        help='Input file directory')
    parser.add_argument('--sep', '-S', type=str, nargs='?', default=' ',
                        help='Separator')
    parser.add_argument('--output', '-O', type=str, nargs='?', required=True,
            help='Output file (can be used as input to reverse the copy)')
    parser.add_argument('--pattern', '-R', type=str, nargs='?', default='*',
            help='Pattern to match only certain files (for ex. PTF201111*.w.fits ), all files matched by default')
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        print("error: {} is not a valid file.".format(args.input))
        exit(1)

    copy(args)

