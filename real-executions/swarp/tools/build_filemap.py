#!/usr/bin/env python3

import os
import sys
import argparse
import glob

def build_hashmap(input_dir, burst_buffer, pattern='*'):
    pfs_dir = glob.glob(input_dir+'/'+pattern)
    bb_dir = glob.glob(burst_buffer+'/'+pattern)
    # This would print all the files and directories
    res = []
    size_bb = 0
    size_total = 0
    bb_dir_short = [os.path.basename(f) for f in bb_dir]
    for f in pfs_dir:
        if os.path.basename(f) in bb_dir_short:
            res.append(os.path.join(os.path.abspath(burst_buffer), os.path.basename(f)))
            size_bb += os.path.getsize(res[-1])
        else:
            res.append(os.path.join(os.path.abspath(input_dir), f))
        size_total += os.path.getsize(res[-1])

    return res,size_bb,size_total


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Build filemap location')
    
    parser.add_argument('--input', '-I', type=str, nargs='?', required=True,
                        help='Input files directory')
    parser.add_argument('--bb', '-B', type=str, nargs='?', required=True,
                        help='Burst buffer directory')
    parser.add_argument('--output', '-O', type=str, nargs='?', required=False,
            help='Output file')
    parser.add_argument('--pattern', '-R', type=str, nargs='?', default='*',
            help='Pattern to match certain files (for ex. PTF201111*.w.fits ), all files matched by default')
    args = parser.parse_args()
    if not os.path.isdir(args.input):
        print("error: {} is not a valid directory.".format(args.input))
        exit(1)

    if not os.path.isdir(args.bb):
        print("error: {} is not a valid directory.".format(args.bb))
        exit(1)

    res,size_bb,size_total = build_hashmap(args.input, args.bb)
    
    for e in res:
        print("{:<150} {:>10} MB".format(e, os.path.getsize(e)/(1024.0**2)), file=sys.stderr)
   
    print("\n{:<150} {:>10}".format("ON BURST BUFFERS", str(100*(round(size_bb/size_total, 3)))+'% ('+ str(round(size_bb/(1024.0**2),2))+' MB)' ), file=sys.stderr)

    if args.output != None:
        res_pattern, _, _ = build_hashmap(args.input, args.bb, args.pattern)
        with open(args.output, 'w') as f:
            f.writelines([r+' ' for r in res_pattern])
            f.write("\n")


