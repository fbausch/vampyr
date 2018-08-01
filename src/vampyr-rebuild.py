#!/usr/bin/python3

import os
import getopt
import logging
import sys


def usage():
    print("""
This tool tries to rebuild file or RBD contents by putting object contents
with the same object name prefix together.

At first extract object data using Vampyr and the --xall option. Afterwards
your directory will contain subdirectories (e.g. rbd_data.<id>) where you
will find "object_<id>" files.

Call this tool using
vampyr-rebuild.py --dir <directory> [--blocksize <size>]

Parameters:
  --dir        A directory that contains "object_<id>" files.
  --blocksize  The size of the objects. Defaults to 4194304 (4M).
               If unsure, have a look at the "object_<id>" files
               and select the largest file size you find.

Output:
You will find a file called "rebuild" in the working directory.
""")


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd:v",
                                   ["help",
                                    "verbose",
                                    "dir=",
                                    "blocksize="])
    except getopt.GetoptError as exception:
        print(str(exception))
        sys.exit(1)

    objdir = None
    verbose = False
    blocksize = 4194304
    for opt, arg in opts:
        if opt in ("-v", "--verbose"):
            verbose = True
        elif opt in ("-h", "--help"):
            usage()
            sys.exit(0)
        elif opt in ("-d", "--dir"):
            objdir = arg
        elif opt in ("--blocksize"):
            blocksize = int(arg, 0)

        else:
            print("1")
            usage()
            sys.exit(1)

    if not os.path.isdir(objdir):
        print("%s is not a directory." % objdir)
        usage()
        sys.exit(1)

    print("Checking files.")

    files = {}
    maxstripe = 0
    for f in os.listdir(objdir):
        if not f.startswith("object_"):
            continue
        if f.endswith(".md5"):
            continue
        stripe = int(f[7:], 16)
        maxstripe = max(maxstripe, stripe)
        fullpath = os.path.join(objdir, f)
        files[stripe] = fullpath
        if os.path.getsize(fullpath) > blocksize:
            print("3")
            usage()
            sys.exit(1)

    print("Rebuilding.")

    out = os.path.join(objdir, "rebuild")
    with open(out, 'wb') as o:
        for f in sorted(files.keys()):
            if verbose:
                print("Applying stripe 0x%x" % f)
            o.seek(f * blocksize)
            path = files[f]
            with open(path, 'rb') as r:
                o.write(r.read(blocksize))

    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error("Something went wrong:")
        logging.exception(e)
