#!/usr/bin/env python3

import os
import argparse

def build_hashmap(input_dir, burst_buffer):
    pfs_dir = os.listdir(input_dir)
    bb_dir = os.listdir(burst_buffer)
    # This would print all the files and directories
    res = []
    size_bb = 0
    size_total = 0
    for file in bb_dir:
        if os.path.isfile(file):
            res.append(os.path.join(os.path.abspath(burst_buffer), file))
            size_bb += os.path.getsize(file)
        else:
            res.append(os.path.join(os.path.abspath(input_dir), file))
        size_total += os.path.getsize(file)

    return res,size_bb,size_total

# def get_size():
#     os.path.getsize()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Build filemap location')
    
    parser.add_argument('--input', '-I', type=str, nargs='?', required=True,
                        help='Input files directory')
    parser.add_argument('--bb', '-B', type=str, nargs='?', required=True,
                        help='Burst buffer directory')

    if not os.path.isdir(args.input):
        print("error: {} is not a valid directory.")
        exit(1)

    if not os.path.isdir(args.bb):
        print("error: {} is not a valid directory.")
        exit(1)

    res,size_bb,size_total = build_hashmap(args.input, args.bb)

    print(res)
    print(size_bb/size_total)
